"""Arcana CLI — command-line interface for the trading data pipeline."""

from __future__ import annotations

import logging
import os
import sys
from datetime import UTC, datetime

import click

from arcana.config import ArcanaConfig, DatabaseConfig
from arcana.ingestion.coinbase import CoinbaseSource
from arcana.pipeline import (
    DAEMON_INTERVAL,
    ingest_backfill,
    run_daemon,
)
from arcana.storage.database import Database


def _db_config_from_options(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    cfg_db: DatabaseConfig | None = None,
) -> DatabaseConfig:
    """Build DatabaseConfig with fallback: env var > config file > CLI default."""
    cfg_host = cfg_db.host if cfg_db else host
    cfg_port = cfg_db.port if cfg_db else port
    cfg_database = cfg_db.database if cfg_db else database
    cfg_user = cfg_db.user if cfg_db else user
    cfg_password = cfg_db.password if cfg_db else password

    return DatabaseConfig(
        host=os.environ.get("ARCANA_DB_HOST", cfg_host),
        port=int(os.environ.get("ARCANA_DB_PORT", cfg_port)),
        database=os.environ.get("ARCANA_DB_NAME", cfg_database),
        user=os.environ.get("ARCANA_DB_USER", cfg_user),
        password=os.environ.get("ARCANA_DB_PASSWORD", cfg_password),
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
      3. arcana summon ETH-USD                     Start the live trade daemon

    \b
    Bar construction is handled by Sigil (separate package):
      sigil build-all ETH-USD                      Build bars from trade data
      sigil forge ETH-USD                          Start the live bar forge

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
@click.pass_context
def db_init(
    ctx: click.Context, host: str, port: int, database: str, user: str, password: str,
) -> None:
    """Initialize the database schema."""
    arcana_cfg = ctx.obj.get("config") if ctx.obj else None
    cfg_db = arcana_cfg.database if arcana_cfg else None
    config = _db_config_from_options(host, port, database, user, password, cfg_db=cfg_db)
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
@click.pass_context
def ingest(
    ctx: click.Context,
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
    arcana_cfg = ctx.obj.get("config") if ctx.obj else None
    cfg_db = arcana_cfg.database if arcana_cfg else None
    config = _db_config_from_options(host, port, database, user, password, cfg_db=cfg_db)

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
@click.pass_context
def summon(
    ctx: click.Context,
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
    arcana_cfg = ctx.obj.get("config") if ctx.obj else None
    cfg_db = arcana_cfg.database if arcana_cfg else None
    config = _db_config_from_options(host, port, database, user, password, cfg_db=cfg_db)

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
@click.pass_context
def status(
    ctx: click.Context,
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
    arcana_cfg = ctx.obj.get("config") if ctx.obj else None
    cfg_db = arcana_cfg.database if arcana_cfg else None
    config = _db_config_from_options(host, port, database, user, password, cfg_db=cfg_db)

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
