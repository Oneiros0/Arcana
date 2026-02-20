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


def calibrate_dollar_threshold(
    db: Database,
    pair: str,
    bars_per_day: int = 50,
    source: str = "coinbase",
) -> int:
    """Auto-calibrate dollar bar threshold from trade data.

    Computes: threshold = total_dollar_volume / (days × bars_per_day)

    Args:
        db: Database with stored trades.
        pair: Trading pair, e.g. 'ETH-USD'.
        bars_per_day: Target number of bars per calendar day.
        source: Data source name.

    Returns:
        Dollar threshold rounded to a clean value.

    Raises:
        ValueError: If no trade data exists for the pair.
    """
    stats = db.get_dollar_volume_stats(pair, source)
    if stats is None:
        raise ValueError(f"No trade data for {pair}. Run 'arcana ingest' first.")

    total_dollar_vol, days = stats
    target_bars = days * bars_per_day
    raw_threshold = total_dollar_vol / target_bars

    # Round to a clean value (nearest power-of-10 significant digit)
    # e.g. 213847 → 200000, 58312 → 50000
    import math

    magnitude = 10 ** int(math.log10(raw_threshold))
    threshold = round(raw_threshold / magnitude) * magnitude

    logger.info(
        "Calibrated dollar threshold for %s: $%s "
        "(%.1f days, $%.0fM total vol, target %d bars/day → ~%d total bars)",
        pair,
        f"{threshold:,}",
        days,
        total_dollar_vol / 1e6,
        bars_per_day,
        int(total_dollar_vol / threshold),
    )
    return threshold


def calibrate_tick_threshold(
    db: Database,
    pair: str,
    bars_per_day: int = 50,
    source: str = "coinbase",
) -> int:
    """Auto-calibrate tick bar threshold from trade data.

    threshold = total_trades / (days × bars_per_day)

    Returns:
        Tick count threshold (integer, minimum 1).

    Raises:
        ValueError: If no trade data exists for the pair.
    """
    stats = db.get_trade_volume_stats(pair, source)
    if stats is None:
        raise ValueError(f"No trade data for {pair}. Run 'arcana ingest' first.")

    total_trades, _, days = stats
    raw = total_trades / (days * bars_per_day)
    threshold = max(1, round(raw))

    logger.info(
        "Calibrated tick threshold for %s: %d ticks "
        "(%.1f days, %d total trades, target %d bars/day → ~%d total bars)",
        pair,
        threshold,
        days,
        int(total_trades),
        bars_per_day,
        int(total_trades / threshold),
    )
    return threshold


def calibrate_volume_threshold(
    db: Database,
    pair: str,
    bars_per_day: int = 50,
    source: str = "coinbase",
) -> float:
    """Auto-calibrate volume bar threshold from trade data.

    threshold = total_volume / (days × bars_per_day)

    Returns:
        Volume threshold rounded to a clean value.

    Raises:
        ValueError: If no trade data exists for the pair.
    """
    stats = db.get_trade_volume_stats(pair, source)
    if stats is None:
        raise ValueError(f"No trade data for {pair}. Run 'arcana ingest' first.")

    _, total_volume, days = stats
    raw = total_volume / (days * bars_per_day)

    # Round to a clean value
    import math

    if raw >= 1.0:
        magnitude = 10 ** int(math.log10(raw))
        threshold = round(raw / magnitude) * magnitude
    else:
        threshold = round(raw, 4)
    threshold = max(threshold, 0.0001)

    logger.info(
        "Calibrated volume threshold for %s: %.4f "
        "(%.1f days, %.0f total volume, target %d bars/day → ~%d total bars)",
        pair,
        threshold,
        days,
        total_volume,
        bars_per_day,
        int(total_volume / threshold),
    )
    return threshold


def calibrate_info_bar_initial_expected(
    db: Database,
    pair: str,
    bar_kind: str,
    bars_per_day: int = 50,
    source: str = "coinbase",
) -> float:
    """Calibrate E₀ for information-driven bars (Prado Ch. 2).

    Imbalance bars (tib/vib/dib):
      E₀ = E[T] × max(|2P[buy]-1|, 0.1) × E[|contribution|]
      Where E[T] = expected ticks per bar, the cumulative imbalance
      grows proportional to trades × direction bias.

    Run bars (trb/vrb/drb):
      E₀ = E[run_length] × E[|contribution|]
      Where E[run_length] = p_same / (1 - p_same) is the expected
      geometric run length before a direction change, and
      p_same = max(P[buy], 1-P[buy]) is the probability of the
      dominant direction continuing.

    Args:
        bar_kind: One of 'tib', 'vib', 'dib', 'trb', 'vrb', 'drb'.

    Returns:
        Initial expected value for the EWMA estimator.

    Raises:
        ValueError: If insufficient trade data or unknown bar_kind.
    """
    # E[T]: expected ticks per bar
    trade_stats = db.get_trade_volume_stats(pair, source)
    if trade_stats is None:
        raise ValueError(f"No trade data for {pair}. Run 'arcana ingest' first.")
    total_trades, _, days = trade_stats
    expected_ticks_per_bar = total_trades / (days * bars_per_day)

    # Imbalance statistics from recent trades
    imbalance_stats = db.get_imbalance_stats(pair, source)
    if imbalance_stats is None:
        raise ValueError(f"Insufficient trade data for {pair} imbalance stats.")
    avg_size, avg_dollar, buy_fraction = imbalance_stats

    # Contribution per trade by bar variant
    imbalance_kinds = {"tib", "vib", "dib"}
    run_kinds = {"trb", "vrb", "drb"}
    tick_kinds = {"tib", "trb"}
    volume_kinds = {"vib", "vrb"}

    if bar_kind not in (imbalance_kinds | run_kinds):
        raise ValueError(f"Unknown bar kind: {bar_kind}")

    if bar_kind in tick_kinds:
        avg_contribution = 1.0
    elif bar_kind in volume_kinds:
        avg_contribution = avg_size
    else:  # dollar kinds
        avg_contribution = avg_dollar

    if bar_kind in imbalance_kinds:
        # Imbalance: cumulative signed sum grows ~ E[T] × |2P-1|
        direction_bias = max(abs(2 * buy_fraction - 1), 0.1)
        e0 = expected_ticks_per_bar * direction_bias * avg_contribution
        logger.info(
            "Calibrated E₀ for %s on %s: %.2f "
            "(E[T]=%.0f ticks, P[buy]=%.3f, bias=%.3f, E[|c|]=%.4f)",
            bar_kind, pair, e0,
            expected_ticks_per_bar, buy_fraction, direction_bias, avg_contribution,
        )
    else:
        # Run bars: expected geometric run length before direction change.
        # p_same = probability the next trade continues the dominant direction.
        # E[run] = p_same / (1 - p_same) for a geometric distribution.
        # Floor p_same at 0.55 and cap at 0.95 for numerical stability.
        p_same = max(buy_fraction, 1 - buy_fraction)
        p_same = min(max(p_same, 0.55), 0.95)
        expected_run_length = p_same / (1 - p_same)
        e0 = expected_run_length * avg_contribution
        logger.info(
            "Calibrated E₀ for %s on %s: %.2f "
            "(P[same]=%.3f, E[run]=%.1f trades, E[|c|]=%.4f)",
            bar_kind, pair, e0,
            p_same, expected_run_length, avg_contribution,
        )

    return e0


TRADE_BATCH = 100_000  # Trades per DB fetch for bar construction


def build_bars(
    builder: BarBuilder,
    db: Database,
    pair: str,
    source: str = "coinbase",
    rebuild: bool = False,
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
        rebuild: If True, delete all existing bars and rebuild from scratch.

    Returns:
        Total number of bars emitted and stored.
    """
    shutdown = GracefulShutdown()

    # Full rebuild: wipe existing bars, start fresh
    if rebuild:
        deleted = db.delete_all_bars(builder.bar_type, pair, source)
        if deleted:
            logger.info(
                "Rebuild: deleted %d existing %s bars for %s",
                deleted,
                builder.bar_type,
                pair,
            )

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
            pair,
            since,
            source,
            limit=TRADE_BATCH,
            since_trade_id=since_trade_id,
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
