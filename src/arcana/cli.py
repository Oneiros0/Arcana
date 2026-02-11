"""Arcana CLI — command-line interface for the ingestion pipeline."""

import logging
import os
import sys
from datetime import datetime, timezone

import click

from arcana.config import ArcanaConfig, DatabaseConfig
from arcana.ingestion.coinbase import CoinbaseSource
from arcana.pipeline import DAEMON_INTERVAL, ingest_backfill, run_daemon
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


# --- Swarm commands ---


@cli.group()
def swarm() -> None:
    """Parallel backfill via Docker Compose."""
    pass


@swarm.command("launch")
@click.argument("pair")
@click.option(
    "--since",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]),
    help="Start date for backfill (e.g. 2022-01-01).",
)
@click.option(
    "--until",
    default=None,
    type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]),
    help="End date for backfill (default: now).",
)
@click.option(
    "--workers",
    default=12,
    type=int,
    help="Number of parallel worker containers.",
)
@click.option("--image", default="arcana:latest", help="Docker image for workers.")
@click.option("--output", default="docker-compose.swarm.yml", help="Output compose file.")
@click.option("--host", default="db", help="Database host (within Docker network).")
@click.option("--port", default=5432, type=int, help="Database port.")
@click.option("--database", default="arcana", help="Database name.")
@click.option("--user", default="arcana", help="Database user.")
@click.option("--password", default="arcana", help="Database password.")
@click.option("--up", is_flag=True, help="Run docker compose up after generating.")
def swarm_launch(
    pair: str,
    since: datetime,
    until: datetime | None,
    workers: int,
    image: str,
    output: str,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    up: bool,
) -> None:
    """Generate a docker-compose file and optionally launch the swarm.

    Example: arcana swarm launch ETH-USD --since 2022-01-01 --workers 24
    """
    from pathlib import Path

    from arcana.swarm import format_worker_summary, generate_compose, write_compose

    since_utc = since.replace(tzinfo=timezone.utc)
    until_utc = until.replace(tzinfo=timezone.utc) if until else datetime.now(timezone.utc)

    # Show the plan
    summary = format_worker_summary(pair, since_utc, until_utc, workers)
    click.echo(summary)
    click.echo()

    # Generate compose file
    compose = generate_compose(
        pair=pair,
        since=since_utc,
        until=until_utc,
        workers=workers,
        db_host=host,
        db_port=port,
        db_name=database,
        db_user=user,
        db_password=password,
        image=image,
    )

    out_path = write_compose(compose, Path(output))
    click.echo(f"Compose file written to: {out_path}")

    if up:
        import subprocess

        click.echo("Starting swarm...")
        result = subprocess.run(
            ["docker", "compose", "-f", str(out_path), "up", "-d"],
            check=False,
        )
        if result.returncode == 0:
            click.echo("Swarm started. Monitor with: arcana swarm status")
        else:
            click.echo("Failed to start swarm. Check Docker output above.", err=True)
            raise SystemExit(1)
    else:
        click.echo()
        click.echo("To start the swarm:")
        click.echo(f"  docker compose -f {output} up -d")
        click.echo()
        click.echo("To monitor progress:")
        click.echo("  arcana swarm status")


@swarm.command("status")
@click.argument("pair", default="ETH-USD")
@click.option("--host", default="localhost", help="Database host.")
@click.option("--port", default=5432, type=int, help="Database port.")
@click.option("--database", default="arcana", help="Database name.")
@click.option("--user", default="arcana", help="Database user.")
@click.option("--password", default="", help="Database password.")
def swarm_status(
    pair: str,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
) -> None:
    """Show ingestion progress for a parallel backfill.

    Example: arcana swarm status ETH-USD
    """
    config = _db_config_from_options(host, port, database, user, password)

    try:
        with Database(config) as db_conn:
            conn = db_conn.connect()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        date_trunc('month', timestamp) AS month,
                        COUNT(*) AS trade_count,
                        MIN(timestamp) AS first_trade,
                        MAX(timestamp) AS last_trade
                    FROM raw_trades
                    WHERE pair = %s
                    GROUP BY month
                    ORDER BY month
                    """,
                    (pair,),
                )
                rows = cur.fetchall()

            total = db_conn.get_trade_count(pair)
            first_ts = db_conn.get_last_timestamp(pair)  # will use for coverage

            click.echo(f"Swarm status: {pair}")
            click.echo(f"  Total trades: {total:,}")
            click.echo()

            if rows:
                click.echo(
                    f"  {'Month':>10s}  {'Trades':>10s}  "
                    f"{'First':>20s}  {'Last':>20s}"
                )
                click.echo(f"  {'─' * 10}  {'─' * 10}  {'─' * 20}  {'─' * 20}")
                for row in rows:
                    month = row[0].strftime("%Y-%m")
                    count = row[1]
                    first = row[2].strftime("%Y-%m-%d %H:%M")
                    last = row[3].strftime("%Y-%m-%d %H:%M")
                    click.echo(f"  {month:>10s}  {count:>10,d}  {first:>20s}  {last:>20s}")
            else:
                click.echo("  No trades found.")

    except Exception as exc:
        click.echo(f"Failed to connect: {exc}", err=True)
        raise SystemExit(1)


@swarm.command("validate")
@click.argument("pair")
@click.option(
    "--since",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]),
    help="Start of expected data range.",
)
@click.option(
    "--until",
    default=None,
    type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]),
    help="End of expected data range (default: now).",
)
@click.option(
    "--gap-threshold",
    default=2,
    type=int,
    help="Flag gaps larger than this many hours (default: 2).",
)
@click.option("--host", default="localhost", help="Database host.")
@click.option("--port", default=5432, type=int, help="Database port.")
@click.option("--database", default="arcana", help="Database name.")
@click.option("--user", default="arcana", help="Database user.")
@click.option("--password", default="", help="Database password.")
def swarm_validate(
    pair: str,
    since: datetime,
    until: datetime | None,
    gap_threshold: int,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
) -> None:
    """Validate trade coverage for a date range and report gaps.

    Example: arcana swarm validate ETH-USD --since 2022-01-01
    """
    from arcana.swarm import validate_coverage

    since_utc = since.replace(tzinfo=timezone.utc)
    until_utc = until.replace(tzinfo=timezone.utc) if until else datetime.now(timezone.utc)
    config = _db_config_from_options(host, port, database, user, password)

    try:
        with Database(config) as db_conn:
            gaps = validate_coverage(
                db_conn,
                pair=pair,
                since=since_utc,
                until=until_utc,
                gap_threshold_hours=gap_threshold,
            )

            total = db_conn.get_trade_count(pair)
            click.echo(f"Validation: {pair} | {since_utc.date()} → {until_utc.date()}")
            click.echo(f"  Total trades: {total:,}")

            if gaps:
                click.echo(f"  Gaps found: {len(gaps)}")
                click.echo()
                for gap in gaps:
                    click.echo(
                        f"  GAP: {gap['start']} → {gap['end']} "
                        f"({gap['hours']:.1f} hours)"
                    )
            else:
                click.echo("  No gaps detected. Coverage is complete.")

    except Exception as exc:
        click.echo(f"Failed to connect: {exc}", err=True)
        raise SystemExit(1)
