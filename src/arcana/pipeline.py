"""Ingestion pipeline — bulk backfill and daemon mode.

This module orchestrates fetching trades from a DataSource and storing
them in the database, with checkpointing, progress logging, and
graceful shutdown support.
"""

import logging
import os
import signal
import time as time_mod
from datetime import datetime, timedelta, timezone

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

    end = until or datetime.now(timezone.utc)

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
        eta_seconds = remaining_windows / (window_num / elapsed) if window_num > 0 else 0

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
        logger.error(
            "No data found for %s. Run 'arcana ingest %s --since <date>' first.",
            pair,
            pair,
        )
        return

    logger.info(
        "Daemon starting for %s %s | Last trade: %s | Poll interval: %ds",
        source.name,
        pair,
        last_ts.isoformat(),
        interval,
    )

    # Catch-up phase: fill gap from last stored trade to now
    gap = datetime.now(timezone.utc) - last_ts
    if gap.total_seconds() > interval:
        logger.info("Catching up: %s gap detected", _format_eta(gap.total_seconds()))
        ingest_backfill(source, db, pair, since=last_ts)
        last_ts = db.get_last_timestamp(pair, source.name) or last_ts

    # Poll loop
    cycle = 0
    while not shutdown.should_stop:
        cycle += 1
        now = datetime.now(timezone.utc)

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
