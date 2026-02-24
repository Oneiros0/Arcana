"""Arcana CLI — command-line interface for the trading data pipeline."""

from __future__ import annotations

import logging
import os
import re
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

import click

from arcana.config import ArcanaConfig, DatabaseConfig
from arcana.ingestion.coinbase import CoinbaseSource
from arcana.pipeline import (
    DAEMON_INTERVAL,
    build_bars,
    calibrate_dollar_threshold,
    calibrate_info_bar_initial_expected,
    calibrate_tick_threshold,
    calibrate_volume_threshold,
    ingest_backfill,
    run_daemon,
)
from arcana.storage.database import Database

if TYPE_CHECKING:
    from arcana.bars.base import BarBuilder


def _db_config_from_options(
    host: str, port: int, database: str, user: str, password: str
) -> DatabaseConfig:
    """Build DatabaseConfig from CLI options, with env var fallbacks."""
    return DatabaseConfig(
        host=os.environ.get("ARCANA_DB_HOST", host),
        port=int(os.environ.get("ARCANA_DB_PORT", port)),
        database=os.environ.get("ARCANA_DB_NAME", database),
        user=os.environ.get("ARCANA_DB_USER", user),
        password=os.environ.get("ARCANA_DB_PASSWORD", password),
    )


LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")


def _setup_logging(log_level: str) -> None:
    level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )


@click.group()
@click.option(
    "--log-level",
    type=click.Choice(LOG_LEVELS, case_sensitive=False),
    default="INFO",
    help="Set logging verbosity.",
)
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to arcana.toml config file.",
)
@click.pass_context
def cli(ctx: click.Context, log_level: str, config_path: str | None) -> None:
    """Arcana - Quantitative trading data pipeline.

    \b
    Quick start:
      1. arcana db init                            Initialize the database
      2. arcana ingest ETH-USD --since 2025-01-01  Backfill historical trades
      3. arcana bars build tick_500 ETH-USD        Build bars from trade data
      4. arcana summon ETH-USD                     Start the live daemon

    \b
    Database connection:
      Set via environment variables (recommended):
        ARCANA_DB_HOST  ARCANA_DB_PORT  ARCANA_DB_NAME
        ARCANA_DB_USER  ARCANA_DB_PASSWORD
      Or pass --host/--port/--database/--user/--password to any command.
    """
    _setup_logging(log_level)
    ctx.ensure_object(dict)
    ctx.obj["config"] = ArcanaConfig.find_and_load(config_path)


# --- Database commands ---


@cli.group()
def db() -> None:
    """Database setup and management."""
    pass


@db.command("init")
@click.option("--host", default="localhost", help="Database host.", hidden=True)
@click.option("--port", default=5432, type=int, help="Database port.", hidden=True)
@click.option("--database", default="arcana", help="Database name.", hidden=True)
@click.option("--user", default="arcana", help="Database user.", hidden=True)
@click.option("--password", default="", help="Database password.", hidden=True)
def db_init(host: str, port: int, database: str, user: str, password: str) -> None:
    """Initialize the database schema."""
    config = _db_config_from_options(host, port, database, user, password)
    try:
        with Database(config) as db:
            db.init_schema()
        click.echo("Database schema initialized successfully.")
    except Exception as exc:
        click.echo(f"Failed to initialize database: {exc}", err=True)
        raise SystemExit(1)


# --- Ingestion commands ---


@cli.command()
@click.argument("pair")
@click.option(
    "--since",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]),
    help="Start date for backfill (e.g. 2025-01-01).",
)
@click.option(
    "--until",
    default=None,
    type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]),
    help="End date for backfill (default: now).",
)
@click.option("--host", default="localhost", help="Database host.", hidden=True)
@click.option("--port", default=5432, type=int, help="Database port.", hidden=True)
@click.option("--database", default="arcana", help="Database name.", hidden=True)
@click.option("--user", default="arcana", help="Database user.", hidden=True)
@click.option("--password", default="", help="Database password.", hidden=True)
def ingest(
    pair: str,
    since: datetime,
    until: datetime | None,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
) -> None:
    """Backfill historical trades from Coinbase.

    Resumes automatically if interrupted.

    \b
    Examples:
      arcana ingest ETH-USD --since 2025-01-01
      arcana ingest BTC-USD --since 2025-01-01 --until 2025-06-01
    """
    since_utc = since.replace(tzinfo=UTC)
    until_utc = until.replace(tzinfo=UTC) if until else None
    config = _db_config_from_options(host, port, database, user, password)

    end_label = until_utc.date() if until_utc else "now"
    click.echo(f"Ingesting {pair} from {since_utc.date()} to {end_label}...")

    with CoinbaseSource() as source, Database(config) as db_conn:
        db_conn.init_schema()
        total = ingest_backfill(source, db_conn, pair, since=since_utc, until=until_utc)

    click.echo(f"Done. {total} new trades ingested.")


# --- Daemon command ---


@cli.command()
@click.argument("pair")
@click.option(
    "--interval",
    default=DAEMON_INTERVAL,
    type=int,
    help="Poll interval in seconds (default: 900 = 15 min).",
)
@click.option("--host", default="localhost", help="Database host.", hidden=True)
@click.option("--port", default=5432, type=int, help="Database port.", hidden=True)
@click.option("--database", default="arcana", help="Database name.", hidden=True)
@click.option("--user", default="arcana", help="Database user.", hidden=True)
@click.option("--password", default="", help="Database password.", hidden=True)
def summon(
    pair: str,
    interval: int,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
) -> None:
    """Start the live ingestion daemon.

    Polls Coinbase for new trades every --interval seconds.
    Catches up any missed trades on startup.

    \b
    Examples:
      arcana summon ETH-USD
      arcana summon ETH-USD --interval 300
    """
    config = _db_config_from_options(host, port, database, user, password)

    click.echo(f"Summoning daemon for {pair} (poll every {interval}s)...")
    click.echo("Press Ctrl+C to banish.")

    try:
        with CoinbaseSource() as source, Database(config) as db_conn:
            run_daemon(source, db_conn, pair, interval=interval)
    except RuntimeError as exc:
        click.echo(f"Error: {exc}", err=True)
        raise SystemExit(1)

    click.echo("Daemon banished.")


# --- Status command ---


@cli.command()
@click.argument("pair", required=False)
@click.option("--host", default="localhost", help="Database host.", hidden=True)
@click.option("--port", default=5432, type=int, help="Database port.", hidden=True)
@click.option("--database", default="arcana", help="Database name.", hidden=True)
@click.option("--user", default="arcana", help="Database user.", hidden=True)
@click.option("--password", default="", help="Database password.", hidden=True)
def status(
    pair: str | None,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
) -> None:
    """Show trade counts and data freshness.

    \b
    Examples:
      arcana status             Show all pairs
      arcana status ETH-USD     Show a specific pair
    """
    config = _db_config_from_options(host, port, database, user, password)

    try:
        with Database(config) as db_conn:
            total = db_conn.get_trade_count(pair)
            last_ts = db_conn.get_last_timestamp(pair or "ETH-USD")

            click.echo(f"{'Pair: ' + pair if pair else 'All pairs'}")
            click.echo(f"  Total trades: {total:,}")
            if last_ts:
                click.echo(f"  Last trade:   {last_ts.isoformat()}")
                gap = datetime.now(UTC) - last_ts
                click.echo(f"  Data gap:     {gap}")
            else:
                click.echo("  No trades stored yet.")
    except Exception as exc:
        click.echo(f"Failed to connect: {exc}", err=True)
        raise SystemExit(1)


# --- Bar commands ---

_TIME_UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}
_BAR_SPEC_PATTERN = re.compile(
    r"^(tick|volume|dollar)_(\d+(?:\.\d+)?)$"
    r"|^time_(\d+)([smhd])$"
    r"|^(tib|vib|dib|trb|vrb|drb)_(\d+)$"
    r"|^(dollar|tick|volume)_auto(?:_(\d+))?$"
)


def _parse_bar_spec(
    spec: str,
    source: str,
    pair: str,
    db: Database | None = None,
    bars_per_day: int = 50,
    initial_expected: float | None = None,
) -> BarBuilder:
    """Parse a bar spec string like 'tick_500' or 'tib_20' into a BarBuilder.

    For auto-calibrated specs (*_auto), a database connection is required.
    For info-driven bars, E₀ is auto-calibrated from trade data when a DB
    is available, unless overridden by initial_expected.
    """
    from arcana.bars.imbalance import (
        DollarImbalanceBarBuilder,
        TickImbalanceBarBuilder,
        VolumeImbalanceBarBuilder,
    )
    from arcana.bars.runs import (
        DollarRunBarBuilder,
        TickRunBarBuilder,
        VolumeRunBarBuilder,
    )
    from arcana.bars.standard import (
        DollarBarBuilder,
        TickBarBuilder,
        TimeBarBuilder,
        VolumeBarBuilder,
    )

    m = _BAR_SPEC_PATTERN.match(spec)
    if not m:
        raise click.BadParameter(
            f"Invalid bar spec '{spec}'. "
            "Expected: tick_N, volume_N, dollar_N, "
            "tick_auto[_N], volume_auto[_N], dollar_auto[_N], time_Nu, "
            "tib_N, vib_N, dib_N, trb_N, vrb_N, or drb_N. "
            "Examples: tick_500, tick_auto, time_5m, dollar_auto_50, tib_20",
            param_hint="'BAR_SPEC'",
        )

    if m.group(7) is not None:
        # Auto-calibrated standard bars: tick_auto, volume_auto, dollar_auto
        auto_type = m.group(7)
        if db is None:
            raise click.UsageError(f"{auto_type}_auto requires a database connection to calibrate.")
        bpd = int(m.group(8)) if m.group(8) else bars_per_day
        if auto_type == "dollar":
            threshold = calibrate_dollar_threshold(db, pair, bpd, source)
            click.echo(f"Auto-calibrated: dollar_{threshold} ({bpd} bars/day target)")
            return DollarBarBuilder(source, pair, threshold=Decimal(threshold))
        elif auto_type == "tick":
            threshold = calibrate_tick_threshold(db, pair, bpd, source)
            click.echo(f"Auto-calibrated: tick_{threshold} ({bpd} bars/day target)")
            return TickBarBuilder(source, pair, threshold=threshold)
        else:  # volume
            threshold = calibrate_volume_threshold(db, pair, bpd, source)
            click.echo(f"Auto-calibrated: volume_{threshold} ({bpd} bars/day target)")
            return VolumeBarBuilder(source, pair, threshold=Decimal(str(threshold)))
    elif m.group(1):  # tick, volume, or dollar (fixed threshold)
        bar_type = m.group(1)
        value = m.group(2)
        if bar_type == "tick":
            return TickBarBuilder(source, pair, threshold=int(value))
        elif bar_type == "volume":
            return VolumeBarBuilder(source, pair, threshold=Decimal(value))
        else:
            return DollarBarBuilder(source, pair, threshold=Decimal(value))
    elif m.group(3):  # time
        amount = int(m.group(3))
        unit = _TIME_UNITS[m.group(4)]
        return TimeBarBuilder(source, pair, interval=timedelta(**{unit: amount}))
    else:  # information-driven (imbalance or run)
        bar_kind = m.group(5)
        ewma_window = int(m.group(6))

        # Determine E₀: explicit override > auto-calibrate from DB > error
        e0 = initial_expected
        if e0 is None and db is not None:
            try:
                e0 = calibrate_info_bar_initial_expected(
                    db, pair, bar_kind, bars_per_day, source
                )
            except ValueError as exc:
                raise click.UsageError(
                    f"Cannot auto-calibrate E₀ for {bar_kind}: {exc}. "
                    "Ensure sufficient trade data exists, or set "
                    "initial_expected in arcana.toml."
                )
        elif e0 is None:
            raise click.UsageError(
                f"No database available to auto-calibrate E₀ for {bar_kind}. "
                "Provide a database connection or set initial_expected "
                "in arcana.toml."
            )

        builder_map = {
            "tib": TickImbalanceBarBuilder,
            "vib": VolumeImbalanceBarBuilder,
            "dib": DollarImbalanceBarBuilder,
            "trb": TickRunBarBuilder,
            "vrb": VolumeRunBarBuilder,
            "drb": DollarRunBarBuilder,
        }
        return builder_map[bar_kind](
            source, pair, ewma_window=ewma_window, initial_expected=e0
        )


@cli.group()
def bars() -> None:
    """Build and manage bar data."""
    pass


@bars.command("build")
@click.argument("bar_spec")
@click.argument("pair")
@click.option(
    "--rebuild", is_flag=True, default=False,
    help="Delete existing bars and rebuild from scratch.",
)
@click.option("--host", default="localhost", help="Database host.", hidden=True)
@click.option("--port", default=5432, type=int, help="Database port.", hidden=True)
@click.option("--database", default="arcana", help="Database name.", hidden=True)
@click.option("--user", default="arcana", help="Database user.", hidden=True)
@click.option("--password", default="", help="Database password.", hidden=True)
@click.pass_context
def bars_build(
    ctx: click.Context,
    bar_spec: str,
    pair: str,
    rebuild: bool,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
) -> None:
    """Build bars from stored trade data.

    BAR_SPEC defines the bar type and threshold:

    \b
      Standard (fixed threshold):
        tick_500         500-trade bars
        tick_auto        Auto-calibrated tick bars (50 bars/day)
        volume_100       100-unit volume bars
        volume_auto      Auto-calibrated volume bars
        dollar_50000     $50k notional bars
        dollar_auto      Auto-calibrated dollar bars (50 bars/day)
        dollar_auto_100  Auto-calibrated (100 bars/day)
        time_5m          5-minute time bars (s/m/h/d)

    \b
      Information-driven (EWMA adaptive, auto-calibrated E0):
        tib_20        Tick imbalance bars (EWMA window=20)
        vib_20        Volume imbalance bars
        dib_20        Dollar imbalance bars
        trb_10        Tick run bars (EWMA window=10)
        vrb_10        Volume run bars
        drb_10        Dollar run bars

    Resumes automatically from the last built bar.

    \b
    Examples:
      arcana bars build tick_500 ETH-USD
      arcana bars build tick_auto ETH-USD
      arcana bars build dollar_auto ETH-USD
      arcana bars build time_1h ETH-USD
      arcana bars build tib_20 ETH-USD
    """
    # Validate bar spec format before connecting to DB
    if not _BAR_SPEC_PATTERN.match(bar_spec):
        raise click.BadParameter(
            f"Invalid bar spec '{bar_spec}'. "
            "Expected: tick_N, volume_N, dollar_N, "
            "tick_auto[_N], volume_auto[_N], dollar_auto[_N], time_Nu, "
            "tib_N, vib_N, dib_N, trb_N, vrb_N, or drb_N. "
            "Examples: tick_500, tick_auto, time_5m, dollar_auto, tib_20",
            param_hint="'BAR_SPEC'",
        )

    # Look up config overrides for this bar spec
    arcana_cfg = ctx.obj.get("config") if ctx.obj else None
    bpd = 50
    ie = None
    if arcana_cfg:
        for bar_cfg in arcana_cfg.bars:
            if bar_cfg.spec == bar_spec:
                bpd = bar_cfg.bars_per_day or arcana_cfg.pipeline.bars_per_day
                ie = bar_cfg.initial_expected
                break
        else:
            bpd = arcana_cfg.pipeline.bars_per_day

    config = _db_config_from_options(host, port, database, user, password)

    try:
        with Database(config) as db_conn:
            builder = _parse_bar_spec(
                bar_spec,
                source="coinbase",
                pair=pair,
                db=db_conn,
                bars_per_day=bpd,
                initial_expected=ie,
            )
            if rebuild:
                click.echo(f"Rebuilding {builder.bar_type} bars for {pair} (dropping existing)...")
            else:
                click.echo(f"Building {builder.bar_type} bars for {pair}...")
            total = build_bars(builder, db_conn, pair, rebuild=rebuild)
    except Exception as exc:
        click.echo(f"Failed to build bars: {exc}", err=True)
        raise SystemExit(1)

    click.echo(f"Done. {total} bars built.")


DEFAULT_BAR_SPECS = [
    "dollar_auto",
    "tick_auto",
    "volume_auto",
    "tib_20",
    "vib_20",
    "dib_20",
    "trb_20",
    "vrb_20",
    "drb_20",
]


@bars.command("build-all")
@click.argument("pair")
@click.option(
    "--rebuild", is_flag=True, default=False,
    help="Delete existing bars and rebuild from scratch.",
)
@click.option(
    "--bars-per-day", "bars_per_day", default=None, type=int,
    help="Global bars/day target for calibration (default: 50, or from config).",
)
@click.option("--host", default="localhost", help="Database host.", hidden=True)
@click.option("--port", default=5432, type=int, help="Database port.", hidden=True)
@click.option("--database", default="arcana", help="Database name.", hidden=True)
@click.option("--user", default="arcana", help="Database user.", hidden=True)
@click.option("--password", default="", help="Database password.", hidden=True)
@click.pass_context
def bars_build_all(
    ctx: click.Context,
    pair: str,
    rebuild: bool,
    bars_per_day: int | None,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
) -> None:
    """Build all bar types with auto-calibrated thresholds.

    Builds standard bars (dollar, tick, volume) and all six information-driven
    bars (TIB, VIB, DIB, TRB, VRB, DRB) with thresholds/E₀ dynamically
    calibrated from trade data to target Prado's recommended ~50 bars/day.

    \b
    Per-bar overrides (bars_per_day, initial_expected) can be set in arcana.toml:
      [[bars]]
      spec = "vib_20"
      bars_per_day = 200

    \b
    Examples:
      arcana bars build-all ETH-USD
      arcana bars build-all ETH-USD --rebuild
      arcana bars build-all ETH-USD --bars-per-day 100
    """
    arcana_cfg = ctx.obj.get("config") if ctx.obj else None

    # Determine which bar specs to build
    if arcana_cfg and arcana_cfg.bars:
        bar_specs = [b.spec for b in arcana_cfg.bars if b.enabled]
    else:
        bar_specs = list(DEFAULT_BAR_SPECS)

    # Global BPD: CLI flag > config > default 50
    global_bpd = bars_per_day or (
        arcana_cfg.pipeline.bars_per_day if arcana_cfg else 50
    )

    config = _db_config_from_options(host, port, database, user, password)

    click.echo(f"Building all bars for {pair} (global bpd={global_bpd})...")
    if arcana_cfg and arcana_cfg.bars:
        click.echo(f"Using {len(bar_specs)} bar specs from config.")
    else:
        click.echo(f"Using {len(bar_specs)} default bar specs.")

    try:
        with Database(config) as db_conn:
            results: list[tuple[str, int]] = []

            for i, spec in enumerate(bar_specs, 1):
                # Per-bar overrides from config
                bpd = global_bpd
                ie: float | None = None
                if arcana_cfg:
                    for bar_cfg in arcana_cfg.bars:
                        if bar_cfg.spec == spec:
                            bpd = bar_cfg.bars_per_day or global_bpd
                            ie = bar_cfg.initial_expected
                            break

                click.echo(f"\n[{i}/{len(bar_specs)}] ", nl=False)

                builder = _parse_bar_spec(
                    spec, source="coinbase", pair=pair,
                    db=db_conn, bars_per_day=bpd, initial_expected=ie,
                )

                action = "Rebuilding" if rebuild else "Building"
                click.echo(f"{action} {builder.bar_type} (bpd={bpd})...")

                total = build_bars(builder, db_conn, pair, rebuild=rebuild)
                results.append((builder.bar_type, total))
                click.echo(f"  → {total:,} bars")

            # Summary
            trade_stats = db_conn.get_trade_volume_stats(pair, "coinbase")
            days = trade_stats[2] if trade_stats else 1.0

            click.echo(f"\n{'═' * 55}")
            click.echo(f"Build-all complete for {pair} ({days:.1f} days):\n")
            click.echo(f"  {'Bar Type':<22} {'Bars':>10}  {'Bars/Day':>10}  Status")
            click.echo(f"  {'─' * 22} {'─' * 10}  {'─' * 10}  {'─' * 6}")

            for bar_type, count in results:
                bpd_actual = count / days if days > 0 else 0
                if 30 <= bpd_actual <= 100:
                    status = "✓"
                elif 10 <= bpd_actual < 30 or 100 < bpd_actual <= 200:
                    status = "~"
                else:
                    status = "✗"
                click.echo(
                    f"  {bar_type:<22} {count:>10,}  {bpd_actual:>9.1f}  {status}"
                )

            click.echo("\n  ✓ = 30-100/day (ideal)  ~ = near range  ✗ = out of range")

    except Exception as exc:
        click.echo(f"Failed: {exc}", err=True)
        raise SystemExit(1)
