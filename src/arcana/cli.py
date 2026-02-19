"""Arcana CLI — command-line interface for the trading data pipeline."""

import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

import click

from arcana.config import DatabaseConfig
from arcana.ingestion.coinbase import CoinbaseSource
from arcana.pipeline import DAEMON_INTERVAL, build_bars, ingest_backfill, run_daemon
from arcana.storage.database import Database


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
def cli(log_level: str) -> None:
    """Arcana — Quantitative trading data pipeline."""
    _setup_logging(log_level)


# --- Database commands ---


@cli.group()
def db() -> None:
    """Database management commands."""
    pass


@db.command("init")
@click.option("--host", default="localhost", help="Database host.")
@click.option("--port", default=5432, type=int, help="Database port.")
@click.option("--database", default="arcana", help="Database name.")
@click.option("--user", default="arcana", help="Database user.")
@click.option("--password", default="", help="Database password.")
def db_init(host: str, port: int, database: str, user: str, password: str) -> None:
    """Initialize database schema (creates tables if they don't exist)."""
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
@click.option("--host", default="localhost", help="Database host.")
@click.option("--port", default=5432, type=int, help="Database port.")
@click.option("--database", default="arcana", help="Database name.")
@click.option("--user", default="arcana", help="Database user.")
@click.option("--password", default="", help="Database password.")
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
    """Bulk ingest historical trades for a trading pair.

    Example: arcana ingest ETH-USD --since 2025-01-01
    """
    since_utc = since.replace(tzinfo=timezone.utc)
    until_utc = until.replace(tzinfo=timezone.utc) if until else None
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
@click.option("--host", default="localhost", help="Database host.")
@click.option("--port", default=5432, type=int, help="Database port.")
@click.option("--database", default="arcana", help="Database name.")
@click.option("--user", default="arcana", help="Database user.")
@click.option("--password", default="", help="Database password.")
def run(
    pair: str,
    interval: int,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
) -> None:
    """Run the ingestion daemon for a trading pair.

    Polls Coinbase for new trades every --interval seconds.
    Catches up any missed trades on startup.

    Example: arcana run ETH-USD
    """
    config = _db_config_from_options(host, port, database, user, password)

    click.echo(f"Starting daemon for {pair} (poll every {interval}s)...")
    click.echo("Press Ctrl+C to stop.")

    with CoinbaseSource() as source, Database(config) as db_conn:
        run_daemon(source, db_conn, pair, interval=interval)

    click.echo("Daemon stopped.")


# --- Status command ---


@cli.command()
@click.argument("pair", required=False)
@click.option("--host", default="localhost", help="Database host.")
@click.option("--port", default=5432, type=int, help="Database port.")
@click.option("--database", default="arcana", help="Database name.")
@click.option("--user", default="arcana", help="Database user.")
@click.option("--password", default="", help="Database password.")
def status(
    pair: str | None,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
) -> None:
    """Show ingestion status and trade counts.

    Example: arcana status ETH-USD
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
                gap = datetime.now(timezone.utc) - last_ts
                click.echo(f"  Data gap:     {gap}")
            else:
                click.echo("  No trades stored yet.")
    except Exception as exc:
        click.echo(f"Failed to connect: {exc}", err=True)
        raise SystemExit(1)


# --- Bar commands ---

_TIME_UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}
_BAR_SPEC_PATTERN = re.compile(
    r"^(tick|volume|dollar)_(\d+(?:\.\d+)?)$|^time_(\d+)([smhd])$"
)


def _parse_bar_spec(spec: str, source: str, pair: str) -> "BarBuilder":
    """Parse a bar spec string like 'tick_500' or 'time_5m' into a BarBuilder."""
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
            "Expected: tick_N, volume_N, dollar_N, or time_Nu "
            "(where u is s/m/h/d). Examples: tick_500, time_5m, dollar_50000",
            param_hint="'BAR_SPEC'",
        )

    if m.group(1):  # tick, volume, or dollar
        bar_type = m.group(1)
        value = m.group(2)
        if bar_type == "tick":
            return TickBarBuilder(source, pair, threshold=int(value))
        elif bar_type == "volume":
            return VolumeBarBuilder(source, pair, threshold=Decimal(value))
        else:
            return DollarBarBuilder(source, pair, threshold=Decimal(value))
    else:  # time
        amount = int(m.group(3))
        unit = _TIME_UNITS[m.group(4)]
        return TimeBarBuilder(source, pair, interval=timedelta(**{unit: amount}))


@cli.group()
def bars() -> None:
    """Bar construction commands."""
    pass


@bars.command("build")
@click.argument("bar_spec")
@click.argument("pair")
@click.option("--host", default="localhost", help="Database host.")
@click.option("--port", default=5432, type=int, help="Database port.")
@click.option("--database", default="arcana", help="Database name.")
@click.option("--user", default="arcana", help="Database user.")
@click.option("--password", default="", help="Database password.")
def bars_build(
    bar_spec: str,
    pair: str,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
) -> None:
    """Build bars from stored trade data.

    BAR_SPEC defines the bar type and threshold:

    \b
      tick_500      500-trade bars
      volume_100    100-unit volume bars
      dollar_50000  $50k notional bars
      time_5m       5-minute time bars (s/m/h/d)

    Resumes automatically from the last built bar.

    \b
    Examples:
      arcana bars build tick_500 ETH-USD
      arcana bars build time_1h ETH-USD
      arcana bars build dollar_50000 ETH-USD
    """
    builder = _parse_bar_spec(bar_spec, source="coinbase", pair=pair)
    config = _db_config_from_options(host, port, database, user, password)

    click.echo(f"Building {builder.bar_type} bars for {pair}...")

    try:
        with Database(config) as db_conn:
            total = build_bars(builder, db_conn, pair)
    except Exception as exc:
        click.echo(f"Failed to build bars: {exc}", err=True)
        raise SystemExit(1)

    click.echo(f"Done. {total} bars built.")
