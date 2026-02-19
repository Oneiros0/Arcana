"""TimescaleDB connection and schema management."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import TYPE_CHECKING

import psycopg

from arcana.config import DatabaseConfig
from arcana.ingestion.models import Trade

if TYPE_CHECKING:
    from arcana.bars.base import Bar

logger = logging.getLogger(__name__)

# ── Raw trades schema ─────────────────────────────────────────────────────────

RAW_TRADES_SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_trades (
    timestamp    TIMESTAMPTZ   NOT NULL,
    trade_id     TEXT          NOT NULL,
    source       TEXT          NOT NULL,
    pair         TEXT          NOT NULL,
    price        NUMERIC       NOT NULL,
    size         NUMERIC       NOT NULL,
    side         TEXT          NOT NULL,
    UNIQUE (source, trade_id, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_raw_trades_pair_ts
    ON raw_trades (pair, timestamp);
"""

HYPERTABLE_RAW = """
SELECT create_hypertable('raw_trades', 'timestamp', if_not_exists => TRUE);
"""

UPSERT_TRADES = """
INSERT INTO raw_trades (timestamp, trade_id, source, pair, price, size, side)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (source, trade_id, timestamp) DO NOTHING;
"""

# ── Per-pair-per-type bar table helpers ───────────────────────────────────────
#
# Each (bar_type, pair) combination gets its own table with its own
# hypertable and indexes.  E.g. tick_500 bars for ETH-USD live in
# bars_tick_500_eth_usd.  Tables are created lazily on first insert so
# any user-defined threshold + pair is supported without migrations.

_BAR_TYPE_PATTERN = re.compile(r"^[a-z0-9_.]+$")
_PAIR_PATTERN = re.compile(r"^[A-Za-z0-9]+-[A-Za-z0-9]+$")


def _bar_table_name(bar_type: str, pair: str) -> str:
    """Convert a (bar_type, pair) to a safe PostgreSQL table name.

    Examples:
        ('tick_500', 'ETH-USD')    -> 'bars_tick_500_eth_usd'
        ('volume_10.5', 'BTC-USD') -> 'bars_volume_10_5_btc_usd'

    Raises:
        ValueError: If bar_type or pair contain invalid characters.
    """
    if not _BAR_TYPE_PATTERN.match(bar_type):
        raise ValueError(f"Invalid bar_type for table name: {bar_type!r}")
    if not _PAIR_PATTERN.match(pair):
        raise ValueError(f"Invalid pair for table name: {pair!r}")

    pair_norm = pair.lower().replace("-", "_")
    return f"bars_{bar_type.replace('.', '_')}_{pair_norm}"


def _bar_table_schema(table_name: str) -> str:
    """Generate CREATE TABLE + index DDL for a per-type bar table."""
    return f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    time_start    TIMESTAMPTZ   NOT NULL,
    time_end      TIMESTAMPTZ   NOT NULL,
    source        TEXT          NOT NULL,
    pair          TEXT          NOT NULL,
    open          NUMERIC       NOT NULL,
    high          NUMERIC       NOT NULL,
    low           NUMERIC       NOT NULL,
    close         NUMERIC       NOT NULL,
    vwap          NUMERIC       NOT NULL,
    volume        NUMERIC       NOT NULL,
    dollar_volume NUMERIC       NOT NULL,
    tick_count    INTEGER       NOT NULL,
    time_span     INTERVAL      NOT NULL,
    metadata      JSONB
);

CREATE INDEX IF NOT EXISTS idx_{table_name}_pair_ts
    ON {table_name} (source, pair, time_start);
"""


def _bar_table_hypertable(table_name: str) -> str:
    """Generate TimescaleDB hypertable DDL for a bar table."""
    return f"SELECT create_hypertable('{table_name}', 'time_start', if_not_exists => TRUE);"


def _bar_insert_sql(table_name: str) -> str:
    """Generate INSERT SQL for a per-type bar table (14 columns, no bar_type)."""
    return f"""
INSERT INTO {table_name} (
    time_start, time_end, source, pair,
    open, high, low, close, vwap,
    volume, dollar_volume, tick_count, time_span, metadata
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""


# ── Database class ────────────────────────────────────────────────────────────


class Database:
    """Manages TimescaleDB connections, schema, and trade storage."""

    def __init__(self, config: DatabaseConfig) -> None:
        self._config = config
        self._conn: psycopg.Connection | None = None
        self._initialized_bar_tables: set[str] = set()

    def connect(self) -> psycopg.Connection:
        """Open a connection to TimescaleDB."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self._config.dsn)
            logger.info("Connected to database at %s", self._config.host)
        return self._conn

    def init_schema(self) -> None:
        """Create the raw_trades table and convert it to a hypertable.

        Bar tables are created lazily on first insert via _ensure_bar_table().
        Safe to call multiple times — uses IF NOT EXISTS.
        """
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(RAW_TRADES_SCHEMA)
            try:
                cur.execute(HYPERTABLE_RAW)
                logger.info("Hypertables created/verified")
            except psycopg.errors.UndefinedFunction:
                logger.warning(
                    "create_hypertable not available — TimescaleDB extension may not "
                    "be installed. Tables created as regular PostgreSQL tables."
                )
                conn.rollback()
                # Re-create tables since rollback undid them
                with conn.cursor() as cur2:
                    cur2.execute(RAW_TRADES_SCHEMA)
        conn.commit()
        logger.info("Database schema initialized")

    # ── Bar table management ──────────────────────────────────────────────

    def _ensure_bar_table(self, bar_type: str, pair: str) -> str:
        """Ensure the per-pair-per-type bar table exists, creating lazily.

        Returns the table name.
        """
        table_name = _bar_table_name(bar_type, pair)
        if table_name in self._initialized_bar_tables:
            return table_name

        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(_bar_table_schema(table_name))
            try:
                cur.execute(_bar_table_hypertable(table_name))
            except psycopg.errors.UndefinedFunction:
                logger.warning(
                    "create_hypertable not available for %s — "
                    "TimescaleDB extension may not be installed.",
                    table_name,
                )
                conn.rollback()
                with conn.cursor() as cur2:
                    cur2.execute(_bar_table_schema(table_name))
        conn.commit()
        self._initialized_bar_tables.add(table_name)
        logger.debug("Bar table %s initialized", table_name)
        return table_name

    def _table_exists(self, table_name: str) -> bool:
        """Check whether a table exists in the public schema."""
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS ("
                "  SELECT 1 FROM pg_tables "
                "  WHERE schemaname = 'public' AND tablename = %s"
                ")",
                (table_name,),
            )
            row = cur.fetchone()
            return bool(row and row[0])

    def _list_bar_tables(self) -> list[str]:
        """Discover all per-type bar tables in the database."""
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT tablename FROM pg_tables "
                "WHERE schemaname = 'public' AND tablename LIKE 'bars_%' "
                "ORDER BY tablename"
            )
            return [row[0] for row in cur.fetchall()]

    def _count_bars_in_table(self, table_name: str, pair: str | None) -> int:
        """Count rows in a bar table, optionally filtered by pair."""
        conn = self.connect()
        with conn.cursor() as cur:
            if pair:
                cur.execute(
                    f"SELECT COUNT(*) FROM {table_name} WHERE pair = %s",
                    (pair,),
                )
            else:
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            row = cur.fetchone()
            return row[0] if row else 0

    # ── Trade operations ──────────────────────────────────────────────────

    def insert_trades(self, trades: list[Trade]) -> int:
        """Batch upsert trades into raw_trades.

        Uses ON CONFLICT DO NOTHING for idempotent inserts — safe to
        re-run over overlapping time ranges.

        Returns:
            Number of new rows actually inserted.
        """
        if not trades:
            return 0

        conn = self.connect()
        rows_before = self._count_trades(conn)

        with conn.cursor() as cur:
            cur.executemany(
                UPSERT_TRADES,
                [
                    (
                        t.timestamp,
                        t.trade_id,
                        t.source,
                        t.pair,
                        t.price,
                        t.size,
                        t.side,
                    )
                    for t in trades
                ],
            )
        conn.commit()

        rows_after = self._count_trades(conn)
        inserted = rows_after - rows_before
        logger.debug("Inserted %d new trades (batch of %d)", inserted, len(trades))
        return inserted

    def _count_trades(self, conn: psycopg.Connection) -> int:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw_trades")
            row = cur.fetchone()
            return row[0] if row else 0

    def get_last_timestamp(
        self, pair: str, source: str = "coinbase", before: datetime | None = None
    ) -> datetime | None:
        """Get the most recent trade timestamp for a pair.

        Used by the daemon and backfill to know where to resume.
        When *before* is set, only considers trades at or before that
        timestamp — useful for bounded backfill ranges.
        """
        conn = self.connect()
        with conn.cursor() as cur:
            query = "SELECT MAX(timestamp) FROM raw_trades WHERE pair = %s AND source = %s"
            params: list[object] = [pair, source]
            if before is not None:
                query += " AND timestamp <= %s"
                params.append(before)
            cur.execute(query, params)
            row = cur.fetchone()
            return row[0] if row and row[0] else None

    # ── Bar operations ────────────────────────────────────────────────────

    def insert_bars(self, bars: list[Bar]) -> int:
        """Batch insert bars into per-pair-per-type bar tables.

        Each bar is routed to its table based on (bar_type, pair).
        E.g. bars with bar_type='tick_500' and pair='ETH-USD' go into
        the bars_tick_500_eth_usd table.  Tables are created lazily.

        Callers must delete stale bars first (via delete_bars_since) to
        avoid duplicates — this is a plain INSERT with no conflict handling.

        Returns:
            Number of rows inserted.
        """
        import json
        from itertools import groupby

        if not bars:
            return 0

        conn = self.connect()

        # Group bars by (bar_type, pair) for per-table insertion
        def _routing_key(b: Bar) -> tuple[str, str]:
            return (b.bar_type, b.pair)

        sorted_bars = sorted(bars, key=_routing_key)
        for (bar_type, pair), group in groupby(sorted_bars, key=_routing_key):
            table_name = self._ensure_bar_table(bar_type, pair)
            insert_sql = _bar_insert_sql(table_name)
            group_list = list(group)

            with conn.cursor() as cur:
                cur.executemany(
                    insert_sql,
                    [
                        (
                            b.time_start,
                            b.time_end,
                            b.source,
                            b.pair,
                            b.open,
                            b.high,
                            b.low,
                            b.close,
                            b.vwap,
                            b.volume,
                            b.dollar_volume,
                            b.tick_count,
                            b.time_span,
                            json.dumps(b.metadata) if b.metadata else None,
                        )
                        for b in group_list
                    ],
                )
        conn.commit()
        logger.debug("Inserted %d bars", len(bars))
        return len(bars)

    def get_last_bar_time(
        self, bar_type: str, pair: str, source: str = "coinbase"
    ) -> datetime | None:
        """Get the time_end of the most recent bar for a given type/pair.

        Used by bar builders to know where to resume construction.
        """
        table_name = _bar_table_name(bar_type, pair)
        if not self._table_exists(table_name):
            return None

        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT MAX(time_end) FROM {table_name} WHERE pair = %s AND source = %s",
                (pair, source),
            )
            row = cur.fetchone()
            return row[0] if row and row[0] else None

    def delete_bars_since(
        self,
        bar_type: str,
        pair: str,
        since: datetime,
        source: str = "coinbase",
    ) -> int:
        """Delete bars at or after *since* for a given bar type/pair.

        Used before rebuilding bars from a resume point to prevent
        duplicates (since bars use plain INSERT, not upsert).

        Returns:
            Number of rows deleted.
        """
        table_name = _bar_table_name(bar_type, pair)
        if not self._table_exists(table_name):
            return 0

        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {table_name} WHERE pair = %s AND source = %s AND time_start >= %s",
                (pair, source, since),
            )
            deleted = cur.rowcount
        conn.commit()
        logger.debug(
            "Deleted %d %s bars for %s from %s onward",
            deleted,
            bar_type,
            pair,
            since.isoformat(),
        )
        return deleted

    def get_bar_count(self, bar_type: str | None = None, pair: str | None = None) -> int:
        """Get bar count, optionally filtered by type and/or pair.

        When both bar_type and pair are given, queries the specific
        per-pair-per-type table.
        When bar_type is None, discovers all bars_* tables and sums counts.
        """
        if bar_type and pair:
            table_name = _bar_table_name(bar_type, pair)
            if not self._table_exists(table_name):
                return 0
            return self._count_bars_in_table(table_name, pair)
        else:
            tables = self._list_bar_tables()
            return sum(self._count_bars_in_table(t, pair) for t in tables)

    def get_last_bar_metadata(
        self, bar_type: str, pair: str, source: str = "coinbase"
    ) -> dict | None:
        """Get the metadata of the most recent bar for a given type/pair.

        Used by the pipeline to restore EWMA state on daemon restart.
        Returns None if the table doesn't exist or has no bars.
        """
        import json

        table_name = _bar_table_name(bar_type, pair)
        if not self._table_exists(table_name):
            return None

        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT metadata FROM {table_name} "
                "WHERE pair = %s AND source = %s "
                "ORDER BY time_end DESC LIMIT 1",
                (pair, source),
            )
            row = cur.fetchone()
            if row and row[0]:
                # psycopg returns JSONB as dict directly
                return row[0] if isinstance(row[0], dict) else json.loads(row[0])
            return None

    # ── Trade queries ─────────────────────────────────────────────────────

    def get_trades_since(
        self,
        pair: str,
        since: datetime,
        source: str = "coinbase",
        limit: int = 100_000,
        since_trade_id: str | None = None,
    ) -> list[Trade]:
        """Fetch raw trades from the database after a given cursor position.

        Uses a composite cursor (timestamp, trade_id) for deterministic
        pagination.  When multiple trades share the same timestamp, trade_id
        breaks the tie so no trades are skipped between batches.

        Args:
            since_trade_id: If provided, skips trades at ``since`` whose
                trade_id is <= this value.  Pass the trade_id of the last
                trade in the previous batch to guarantee gapless iteration.
        """
        conn = self.connect()
        with conn.cursor() as cur:
            if since_trade_id is not None:
                # Composite cursor: everything strictly after (timestamp, trade_id)
                cur.execute(
                    "SELECT timestamp, trade_id, source, pair, price, size, side "
                    "FROM raw_trades "
                    "WHERE pair = %s AND source = %s "
                    "  AND (timestamp, trade_id) > (%s, %s) "
                    "ORDER BY timestamp ASC, trade_id ASC LIMIT %s",
                    (pair, source, since, since_trade_id, limit),
                )
            else:
                cur.execute(
                    "SELECT timestamp, trade_id, source, pair, price, size, side "
                    "FROM raw_trades "
                    "WHERE pair = %s AND source = %s AND timestamp > %s "
                    "ORDER BY timestamp ASC, trade_id ASC LIMIT %s",
                    (pair, source, since, limit),
                )
            rows = cur.fetchall()

        return [
            Trade(
                timestamp=r[0],
                trade_id=r[1],
                source=r[2],
                pair=r[3],
                price=r[4],
                size=r[5],
                side=r[6],
            )
            for r in rows
        ]

    def get_first_timestamp(self, pair: str, source: str = "coinbase") -> datetime | None:
        """Get the earliest trade timestamp for a pair.

        Used by bar builders to determine where to start construction
        when no bars have been built yet.
        """
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MIN(timestamp) FROM raw_trades WHERE pair = %s AND source = %s",
                (pair, source),
            )
            row = cur.fetchone()
            return row[0] if row and row[0] else None

    def get_trade_count(self, pair: str | None = None) -> int:
        """Get total trade count, optionally filtered by pair."""
        conn = self.connect()
        with conn.cursor() as cur:
            if pair:
                cur.execute("SELECT COUNT(*) FROM raw_trades WHERE pair = %s", (pair,))
            else:
                cur.execute("SELECT COUNT(*) FROM raw_trades")
            row = cur.fetchone()
            return row[0] if row else 0

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()

    def __enter__(self) -> Database:
        self.connect()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
