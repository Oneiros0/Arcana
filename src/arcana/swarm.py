"""Parallel backfill orchestration via Docker Compose.

Splits a date range into N worker chunks and generates a docker-compose.yml
that runs one ingestion container per chunk. All workers write to the same
TimescaleDB instance using upsert dedup (ON CONFLICT DO NOTHING), so
overlapping boundaries and restarts are safe.
"""

import logging
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


def split_range(
    since: datetime,
    until: datetime,
    workers: int,
) -> list[tuple[datetime, datetime]]:
    """Split a date range into N roughly equal, non-overlapping chunks.

    Args:
        since: Start of the total range (UTC).
        until: End of the total range (UTC).
        workers: Number of chunks to create.

    Returns:
        List of (chunk_start, chunk_end) tuples. Adjacent chunks share
        a boundary — chunk[i].end == chunk[i+1].start — so there is
        no gap and the upsert dedup handles any boundary overlap.

    Raises:
        ValueError: If workers < 1 or since >= until.
    """
    if workers < 1:
        raise ValueError(f"workers must be >= 1, got {workers}")
    if since >= until:
        raise ValueError(f"since ({since}) must be before until ({until})")

    total_seconds = (until - since).total_seconds()
    chunk_seconds = total_seconds / workers

    chunks: list[tuple[datetime, datetime]] = []
    for i in range(workers):
        chunk_start = since + timedelta(seconds=chunk_seconds * i)
        if i == workers - 1:
            # Last chunk always extends to `until` to avoid floating-point gaps
            chunk_end = until
        else:
            chunk_end = since + timedelta(seconds=chunk_seconds * (i + 1))
        chunks.append((chunk_start, chunk_end))

    return chunks


def _worker_label(index: int, start: datetime, end: datetime) -> str:
    """Generate a human-readable service name for a worker."""
    return f"worker-{index:02d}-{start.strftime('%Y%m%d')}-{end.strftime('%Y%m%d')}"


def generate_compose(
    pair: str,
    since: datetime,
    until: datetime,
    workers: int,
    db_host: str = "db",
    db_port: int = 5432,
    db_name: str = "arcana",
    db_user: str = "arcana",
    db_password: str = "arcana",
    image: str = "arcana:latest",
) -> dict:
    """Generate a docker-compose service dict for parallel backfill.

    Args:
        pair: Trading pair, e.g. 'ETH-USD'.
        since: Start of the total backfill range.
        until: End of the total backfill range.
        workers: Number of parallel worker containers.
        db_host: Database hostname (within Docker network).
        db_port: Database port.
        db_name: Database name.
        db_user: Database user.
        db_password: Database password.
        image: Docker image for workers.

    Returns:
        A dict that can be serialized to docker-compose.yml.
    """
    chunks = split_range(since, until, workers)

    services: dict = {
        "db": {
            "image": "timescale/timescaledb:latest-pg16",
            "ports": [f"{db_port}:5432"],
            "environment": {
                "POSTGRES_DB": db_name,
                "POSTGRES_USER": db_user,
                "POSTGRES_PASSWORD": db_password,
            },
            "volumes": ["arcana_data:/var/lib/postgresql/data"],
            "healthcheck": {
                "test": ["CMD-SHELL", f"pg_isready -U {db_user} -d {db_name}"],
                "interval": "5s",
                "timeout": "5s",
                "retries": 10,
            },
        },
    }

    for i, (chunk_start, chunk_end) in enumerate(chunks):
        label = _worker_label(i, chunk_start, chunk_end)
        since_str = chunk_start.strftime("%Y-%m-%dT%H:%M:%S")
        until_str = chunk_end.strftime("%Y-%m-%dT%H:%M:%S")

        services[label] = {
            "image": image,
            "command": [
                "ingest", pair,
                "--since", since_str,
                "--until", until_str,
            ],
            "environment": {
                "ARCANA_DB_HOST": db_host,
                "ARCANA_DB_PORT": str(db_port),
                "ARCANA_DB_NAME": db_name,
                "ARCANA_DB_USER": db_user,
                "ARCANA_DB_PASSWORD": db_password,
            },
            "depends_on": {
                "db": {"condition": "service_healthy"},
            },
            "restart": "on-failure",
            "deploy": {
                "restart_policy": {
                    "condition": "on-failure",
                    "max_attempts": 5,
                    "delay": "30s",
                },
            },
        }

    compose = {
        "services": services,
        "volumes": {"arcana_data": None},
    }

    return compose


def write_compose(
    compose: dict,
    output: Path,
) -> Path:
    """Write a compose dict to a YAML file.

    Returns:
        The path that was written.
    """
    with open(output, "w") as f:
        yaml.dump(compose, f, default_flow_style=False, sort_keys=False)
    return output


def format_worker_summary(
    pair: str,
    since: datetime,
    until: datetime,
    workers: int,
) -> str:
    """Format a human-readable summary of the planned swarm."""
    chunks = split_range(since, until, workers)
    total_days = (until - since).days
    days_per_worker = total_days / workers

    lines = [
        f"Swarm plan: {pair} | {since.date()} → {until.date()} "
        f"({total_days} days)",
        f"Workers: {workers} (~{days_per_worker:.1f} days each)",
        "",
        f"  {'#':>3s}  {'start':>12s}  {'end':>12s}  {'days':>5s}",
        f"  {'─' * 3}  {'─' * 12}  {'─' * 12}  {'─' * 5}",
    ]

    for i, (start, end) in enumerate(chunks):
        chunk_days = (end - start).total_seconds() / 86400
        lines.append(
            f"  {i:>3d}  {start.strftime('%Y-%m-%d'):>12s}  "
            f"{end.strftime('%Y-%m-%d'):>12s}  {chunk_days:>5.1f}"
        )

    return "\n".join(lines)


def validate_coverage(
    db,  # Database instance
    pair: str,
    since: datetime,
    until: datetime,
    source: str = "coinbase",
    gap_threshold_hours: int = 2,
) -> list[dict]:
    """Check for gaps in trade coverage across a date range.

    Queries the database for trade counts per day and identifies
    days with zero trades or suspicious gaps.

    Args:
        db: Database connection.
        pair: Trading pair.
        since: Start of range to validate.
        until: End of range to validate.
        source: Data source name.
        gap_threshold_hours: Flag gaps larger than this many hours.

    Returns:
        List of gap dicts: {"start": datetime, "end": datetime, "hours": float}
    """
    conn = db.connect()
    with conn.cursor() as cur:
        # Get the min/max timestamp per day to detect gaps
        cur.execute(
            """
            SELECT
                date_trunc('day', timestamp) AS day,
                MIN(timestamp) AS first_trade,
                MAX(timestamp) AS last_trade,
                COUNT(*) AS trade_count
            FROM raw_trades
            WHERE pair = %s AND source = %s
              AND timestamp >= %s AND timestamp < %s
            GROUP BY day
            ORDER BY day
            """,
            (pair, source, since, until),
        )
        rows = cur.fetchall()

    if not rows:
        total_hours = (until - since).total_seconds() / 3600
        return [{"start": since, "end": until, "hours": total_hours}]

    gaps: list[dict] = []

    # Check gap between since and first trade
    first_day_start = rows[0][1]  # first_trade of first day
    gap_hours = (first_day_start - since).total_seconds() / 3600
    if gap_hours > gap_threshold_hours:
        gaps.append({"start": since, "end": first_day_start, "hours": gap_hours})

    # Check gaps between consecutive days
    for i in range(len(rows) - 1):
        current_last = rows[i][2]  # last_trade of current day
        next_first = rows[i + 1][1]  # first_trade of next day
        gap_hours = (next_first - current_last).total_seconds() / 3600
        if gap_hours > gap_threshold_hours:
            gaps.append({
                "start": current_last,
                "end": next_first,
                "hours": gap_hours,
            })

    # Check gap between last trade and until
    last_day_end = rows[-1][2]  # last_trade of last day
    gap_hours = (until - last_day_end).total_seconds() / 3600
    if gap_hours > gap_threshold_hours:
        gaps.append({"start": last_day_end, "end": until, "hours": gap_hours})

    return gaps
