"""Ingestion pipeline — bulk backfill, bar construction, and daemon mode.

This module orchestrates fetching trades from a DataSource and storing
them in the database, with checkpointing, progress logging, and
graceful shutdown support.
"""

import logging
import os
import signal
import time as time_mod
from datetime import UTC, datetime, timedelta

from arcana.bars.base import BarBuilder
from arcana.ingestion.base import DataSource
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
            f"No trades found for {pair}. "
            f"Run 'arcana ingest {pair} --since <date>' first."
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


TRADE_BATCH = 100_000  # Trades per DB fetch for bar construction


def build_bars(
    builder: BarBuilder,
    db: Database,
    pair: str,
    source: str = "coinbase",
) -> int:
    """Build bars from stored trades using the given builder.

    Loads trades in batches from the database, processes them through
    the bar builder, and stores completed bars. Resumable — on restart,
    starts from the last emitted bar's time_end.

    Args:
        builder: Configured BarBuilder instance.
        db: Database with stored trades.
        pair: Trading pair, e.g. 'ETH-USD'.
        source: Data source name.

    Returns:
        Total number of bars emitted and stored.
    """
    shutdown = GracefulShutdown()

    # Resume from last emitted bar, or start from first trade
    last_bar_time = db.get_last_bar_time(builder.bar_type, pair, source)
    if last_bar_time:
        since = last_bar_time

        # Restore EWMA state BEFORE deleting stale bars — the last bar's
        # metadata carries the EWMA estimator state needed for warm restart.
        last_metadata = db.get_last_bar_metadata(builder.bar_type, pair, source)
        if last_metadata:
            builder.restore_state(last_metadata)
            logger.info(
                "Restored builder state from last bar metadata (EWMA=%.4f)",
                last_metadata.get("ewma_expected", 0.0),
            )

        # Delete bars from the resume point onward — the last bar batch
        # may have been incomplete, and plain INSERT (no upsert) requires
        # a clean slate to avoid duplicates.
        deleted = db.delete_bars_since(builder.bar_type, pair, since, source)
        logger.info(
            "Resuming %s bar construction from %s (cleared %d stale bars)",
            builder.bar_type,
            since.isoformat(),
            deleted,
        )
    else:
        first_ts = db.get_first_timestamp(pair, source)
        if first_ts is None:
            logger.error("No trades found for %s. Run 'arcana ingest' first.", pair)
            return 0
        # Subtract 1µs so the first trade is included (query uses timestamp > since)
        since = first_ts - timedelta(microseconds=1)
        logger.info(
            "Building %s bars from first trade at %s",
            builder.bar_type,
            first_ts.isoformat(),
        )

    total_bars = 0
    total_trades = 0
    since_trade_id: str | None = None  # composite cursor
    start_time = time_mod.time()

    while not shutdown.should_stop:
        trades = db.get_trades_since(
            pair, since, source, limit=TRADE_BATCH, since_trade_id=since_trade_id,
        )
        if not trades:
            break

        bars = builder.process_trades(trades)
        if bars:
            db.insert_bars(bars)
            total_bars += len(bars)

        total_trades += len(trades)
        since = trades[-1].timestamp
        since_trade_id = trades[-1].trade_id

        elapsed = time_mod.time() - start_time
        rate = total_trades / elapsed if elapsed > 0 else 0
        logger.info(
            "Processed %d trades | %d bars emitted | %.0f trades/sec",
            total_trades,
            total_bars,
            rate,
        )

        # Under limit means we've consumed all available trades
        if len(trades) < TRADE_BATCH:
            break

    # Flush partial bar at end
    if not shutdown.should_stop:
        final_bar = builder.flush()
        if final_bar:
            db.insert_bars([final_bar])
            total_bars += 1

    elapsed = time_mod.time() - start_time
    logger.info(
        "Bar construction complete: %d %s bars from %d trades in %s",
        total_bars,
        builder.bar_type,
        total_trades,
        _format_eta(elapsed),
    )
    return total_bars


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
