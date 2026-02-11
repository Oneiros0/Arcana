"""Arcana CLI — command-line interface for the ingestion pipeline."""

import logging
import sys
from datetime import datetime, timezone

import click

from arcana.config import ArcanaConfig, DatabaseConfig
from arcana.ingestion.coinbase import CoinbaseSource
from arcana.pipeline import DAEMON_INTERVAL, ingest_backfill, run_daemon
from arcana.storage.database import Database


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
    config = DatabaseConfig(
        host=host, port=port, database=database, user=user, password=password
    )
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
@click.option("--host", default="localhost", help="Database host.")
@click.option("--port", default=5432, type=int, help="Database port.")
@click.option("--database", default="arcana", help="Database name.")
@click.option("--user", default="arcana", help="Database user.")
@click.option("--password", default="", help="Database password.")
def ingest(
    pair: str,
    since: datetime,
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
    config = DatabaseConfig(
        host=host, port=port, database=database, user=user, password=password
    )

    click.echo(f"Ingesting {pair} from {since_utc.date()} to now...")

    with CoinbaseSource() as source, Database(config) as db_conn:
        db_conn.init_schema()
        total = ingest_backfill(source, db_conn, pair, since=since_utc)

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
    config = DatabaseConfig(
        host=host, port=port, database=database, user=user, password=password
    )

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
    config = DatabaseConfig(
        host=host, port=port, database=database, user=user, password=password
    )

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
