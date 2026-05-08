"""Ingestion pipeline — bulk backfill and daemon mode.

This module orchestrates fetching trades from a DataSource and storing
them in the database, with checkpointing, progress logging, and
graceful shutdown support.
"""

import logging
import os
import signal
import time as time_mod
from datetime import UTC, datetime, timedelta

from arcana.ingestion.base import DataSource
from arcana.ingestion.candles import candle_to_trade, granularity_seconds
from arcana.ingestion.coinbase import CANDLE_LIMIT, CoinbaseSource
from arcana.ingestion.models import Trade
from arcana.storage.database import Database

logger = logging.getLogger(__name__)

BATCH_SIZE = 1000  # Trades per DB commit
DEFAULT_WINDOW = timedelta(minutes=15)
DAEMON_INTERVAL = 15 * 60  # 15 minutes in seconds


class GracefulShutdown:
    """Catches SIGINT/SIGTERM and sets a flag for clean exit."""

    def __init__(self) -> None:
        self.should_stop = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum: int, frame: object) -> None:
        sig_name = signal.Signals(signum).name
        logger.info("Received %s — finishing current batch before shutdown...", sig_name)
        self.should_stop = True


def ingest_backfill(
    source: DataSource,
    db: Database,
    pair: str,
    since: datetime,
    until: datetime | None = None,
    window: timedelta = DEFAULT_WINDOW,
) -> int:
    """Bulk backfill trades from `since` to `until` (or now).

    Walks forward through time in windows, committing each batch
    to the database. Resumable — on restart, starts from the last
    stored timestamp.

    Args:
        source: Data source to fetch from.
        db: Database to store trades in.
        pair: Trading pair, e.g. 'ETH-USD'.
        since: Start date for backfill.
        until: End date for backfill. Defaults to now.
        window: Size of each time window to query.

    Returns:
        Total number of new trades inserted.
    """
    shutdown = GracefulShutdown()

    end = until or datetime.now(UTC)

    # Resume from last stored trade within the backfill range.
    last_ts = db.get_last_timestamp(pair, source.name, before=end)
    if last_ts and last_ts > since:
        logger.info("Resuming from %s (found existing data)", last_ts.isoformat())
        since = last_ts
    total_windows = max(1, int((end - since) / window) + 1)
    current = since
    window_num = 0
    total_inserted = 0
    batch_buffer: list[Trade] = []
    start_time = time_mod.time()

    logger.info(
        "Starting backfill: %s %s from %s to %s (~%d windows)",
        source.name,
        pair,
        since.strftime("%Y-%m-%d %H:%M"),
        end.strftime("%Y-%m-%d %H:%M"),
        total_windows,
    )

    while current < end:
        if shutdown.should_stop:
            logger.info("Shutdown requested — committing remaining buffer...")
            if batch_buffer:
                total_inserted += db.insert_trades(batch_buffer)
                batch_buffer.clear()
            break

        window_end = min(current + window, end)
        window_num += 1

        try:
            trades = source.fetch_all_trades(pair=pair, start=current, end=window_end)
        except Exception:
            logger.exception(
                "Failed to fetch window %d (%s → %s). Halting backfill.",
                window_num,
                current.isoformat(),
                window_end.isoformat(),
            )
            # Commit what we have before stopping
            if batch_buffer:
                total_inserted += db.insert_trades(batch_buffer)
            raise

        batch_buffer.extend(trades)

        # Checkpoint: commit when buffer reaches BATCH_SIZE
        if len(batch_buffer) >= BATCH_SIZE:
            inserted = db.insert_trades(batch_buffer)
            total_inserted += inserted
            batch_buffer.clear()

        # Progress logging
        elapsed = time_mod.time() - start_time
        rate = total_inserted / elapsed if elapsed > 0 else 0
        remaining_windows = total_windows - window_num
        eta_seconds = (remaining_windows * elapsed / window_num) if elapsed > 0 else 0

        logger.info(
            "Window %d/%d | %s → %s | %d trades this window | "
            "Total: %d stored | %.1f trades/sec | ETA: %s",
            window_num,
            total_windows,
            current.strftime("%Y-%m-%d %H:%M"),
            window_end.strftime("%Y-%m-%d %H:%M"),
            len(trades),
            total_inserted + len(batch_buffer),
            rate,
            _format_eta(eta_seconds),
        )

        current = window_end
        rate_delay = float(os.environ.get("ARCANA_RATE_DELAY", 0.12))
        time_mod.sleep(rate_delay)

    # Final flush
    if batch_buffer:
        total_inserted += db.insert_trades(batch_buffer)

    elapsed = time_mod.time() - start_time
    logger.info(
        "Backfill complete: %d trades inserted in %s",
        total_inserted,
        _format_eta(elapsed),
    )
    return total_inserted


def backfill_candles(
    source: CoinbaseSource,
    db: Database,
    pair: str,
    since: datetime,
    until: datetime | None = None,
    granularity: str = "1m",
) -> int:
    """One-shot historical candle backfill (REST, not websocket).

    Walks forward in chunks of 350 candles per API call, synthesizes one
    Trade per candle (price=HLC/3, side='unknown', data_quality='candle_<g>'),
    and inserts into raw_trades alongside any tick data. The data_quality
    tag is the boundary marker — bars built across tick/candle rows are NOT
    comparable, and downstream consumers must filter or split on this field.

    Resume logic uses ``data_quality='candle_<g>'`` so prior tick rows in the
    same range don't suppress backfilling.

    Args:
        source: Coinbase REST source. (Candle endpoint is Coinbase-specific
            for now; the abstract DataSource doesn't define it.)
        db: Database to store synthesized trades in.
        pair: Trading pair, e.g. 'ETH-USD'.
        since: Start of historical range (UTC).
        until: End of historical range (UTC). Defaults to now.
        granularity: Candle bucket size — '1m', '5m', '15m', '30m', '1h',
            '2h', '6h', or '1d'. Default '1m' minimizes information loss.

    Returns:
        Total number of synthesized trades actually inserted.
    """
    shutdown = GracefulShutdown()
    end = until or datetime.now(UTC)
    quality_tag = f"candle_{granularity}"

    # Resume from the last candle-derived row in the requested range.
    last_ts = db.get_last_timestamp(pair, source.name, before=end, data_quality=quality_tag)
    if last_ts and last_ts > since:
        logger.info(
            "Resuming candle backfill from %s (existing %s data)",
            last_ts.isoformat(),
            quality_tag,
        )
        # Step forward one bucket so we don't refetch the last candle.
        bucket = timedelta(seconds=granularity_seconds(granularity))
        since = last_ts + bucket

    if since >= end:
        logger.info("Candle backfill range already covered for %s %s", pair, quality_tag)
        return 0

    bucket_seconds = granularity_seconds(granularity)
    window = timedelta(seconds=CANDLE_LIMIT * bucket_seconds)
    total_buckets = max(1, int((end - since).total_seconds() // bucket_seconds))
    total_inserted = 0
    batch_buffer: list[Trade] = []
    current = since
    chunk_num = 0
    start_time = time_mod.time()

    logger.info(
        "Starting candle backfill: %s %s @ %s from %s to %s (~%d buckets)",
        source.name,
        pair,
        granularity,
        since.strftime("%Y-%m-%d %H:%M"),
        end.strftime("%Y-%m-%d %H:%M"),
        total_buckets,
    )
    logger.warning(
        "Candle data is NOT tick-equivalent. Rows tagged data_quality=%r. "
        "Do NOT build tick/imbalance/run bars across tick→candle boundaries.",
        quality_tag,
    )

    while current < end:
        if shutdown.should_stop:
            logger.info("Shutdown requested — committing remaining buffer...")
            if batch_buffer:
                total_inserted += db.insert_trades(batch_buffer)
                batch_buffer.clear()
            break

        chunk_end = min(current + window, end)
        chunk_num += 1

        try:
            candles = source.fetch_candles(
                pair=pair, start=current, end=chunk_end, granularity=granularity
            )
        except Exception:
            logger.exception(
                "Failed to fetch candle chunk %d (%s → %s). Halting backfill.",
                chunk_num,
                current.isoformat(),
                chunk_end.isoformat(),
            )
            if batch_buffer:
                total_inserted += db.insert_trades(batch_buffer)
            raise

        batch_buffer.extend(candle_to_trade(c) for c in candles)

        if len(batch_buffer) >= BATCH_SIZE:
            inserted = db.insert_trades(batch_buffer)
            total_inserted += inserted
            batch_buffer.clear()

        elapsed = time_mod.time() - start_time
        rate = total_inserted / elapsed if elapsed > 0 else 0
        logger.info(
            "Chunk %d | %s → %s | %d candles | Total: %d stored | %.1f rows/sec",
            chunk_num,
            current.strftime("%Y-%m-%d %H:%M"),
            chunk_end.strftime("%Y-%m-%d %H:%M"),
            len(candles),
            total_inserted + len(batch_buffer),
            rate,
        )

        current = chunk_end
        rate_delay = float(os.environ.get("ARCANA_RATE_DELAY", 0.12))
        time_mod.sleep(rate_delay)

    if batch_buffer:
        total_inserted += db.insert_trades(batch_buffer)

    elapsed = time_mod.time() - start_time
    logger.info(
        "Candle backfill complete: %d synthesized trades inserted in %s",
        total_inserted,
        _format_eta(elapsed),
    )
    return total_inserted


def run_daemon(
    source: DataSource,
    db: Database,
    pair: str,
    interval: int = DAEMON_INTERVAL,
) -> None:
    """Run the ingestion daemon — poll for new trades on a timer.

    On startup, detects last stored timestamp and catches up any gap.
    Then enters a poll loop, fetching new trades every `interval` seconds.

    Args:
        source: Data source to fetch from.
        db: Database to store trades in.
        pair: Trading pair, e.g. 'ETH-USD'.
        interval: Seconds between poll cycles.
    """
    shutdown = GracefulShutdown()

    last_ts = db.get_last_timestamp(pair, source.name)
    if last_ts is None:
        raise RuntimeError(
            f"No trades found for {pair}. Run 'arcana ingest {pair} --since <date>' first."
        )

    logger.info(
        "Daemon starting for %s %s | Last trade: %s | Poll interval: %ds",
        source.name,
        pair,
        last_ts.isoformat(),
        interval,
    )

    # Catch-up phase: fill gap from last stored trade to now
    gap = datetime.now(UTC) - last_ts
    if gap.total_seconds() > interval:
        logger.info("Catching up: %s gap detected", _format_eta(gap.total_seconds()))
        ingest_backfill(source, db, pair, since=last_ts)
        last_ts = db.get_last_timestamp(pair, source.name) or last_ts

    # Poll loop
    cycle = 0
    while not shutdown.should_stop:
        cycle += 1
        now = datetime.now(UTC)

        try:
            trades = source.fetch_all_trades(pair=pair, start=last_ts, end=now)
            if trades:
                inserted = db.insert_trades(trades)
                new_last = db.get_last_timestamp(pair, source.name)
                logger.info(
                    "Cycle %d | %d trades fetched, %d new | Last: %s",
                    cycle,
                    len(trades),
                    inserted,
                    new_last.isoformat() if new_last else "?",
                )
                if new_last:
                    last_ts = new_last
            else:
                logger.info("Cycle %d | No new trades", cycle)

        except Exception:
            logger.exception("Cycle %d failed. Will retry next cycle.", cycle)

        # Sleep in small increments so we can respond to shutdown quickly
        for _ in range(interval):
            if shutdown.should_stop:
                break
            time_mod.sleep(1)

    total = db.get_trade_count(pair)
    logger.info("Daemon stopped. Total trades for %s: %d", pair, total)


def _format_eta(seconds: float) -> str:
    """Format seconds into a human-readable string."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.1f}m"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"
