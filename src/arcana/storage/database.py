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
    from arcana.models import Bar

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
    data_quality TEXT          NOT NULL DEFAULT 'tick',
    UNIQUE (source, trade_id, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_raw_trades_pair_ts
    ON raw_trades (pair, timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_trades_quality
    ON raw_trades (pair, data_quality, timestamp);
"""

# Idempotent migration for databases initialized before data_quality existed.
RAW_TRADES_MIGRATIONS = """
ALTER TABLE raw_trades
    ADD COLUMN IF NOT EXISTS data_quality TEXT NOT NULL DEFAULT 'tick';
CREATE INDEX IF NOT EXISTS idx_raw_trades_quality
    ON raw_trades (pair, data_quality, timestamp);
"""

HYPERTABLE_RAW = """
SELECT create_hypertable('raw_trades', 'timestamp', if_not_exists => TRUE);
"""

UPSERT_TRADES = """
INSERT INTO raw_trades (timestamp, trade_id, source, pair, price, size, side, data_quality)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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


def _parse_bar_table_name(table_name: str) -> tuple[str, str] | None:
    """Parse a bar table name back into (bar_type, pair).

    The pair is always the last two underscore-separated tokens joined
    by a hyphen and uppercased.  Everything between the ``bars_`` prefix
    and the pair suffix is the bar_type.

    Examples:
        'bars_tick_9243_eth_usd'  -> ('tick_9243', 'ETH-USD')
        'bars_tib_20_eth_usd'    -> ('tib_20', 'ETH-USD')
        'bars_volume_3000_btc_usd' -> ('volume_3000', 'BTC-USD')

    Returns None if the table name doesn't follow the expected format.
    """
    if not table_name.startswith("bars_"):
        return None

    parts = table_name[5:].split("_")  # strip 'bars_' prefix
    if len(parts) < 3:
        return None

    # Last two tokens form the pair (e.g., 'eth', 'usd' -> 'ETH-USD')
    pair = f"{parts[-2]}-{parts[-1]}".upper()
    # Everything before the pair tokens is the bar_type
    bar_type = "_".join(parts[:-2])
    if not bar_type:
        return None

    return (bar_type, pair)


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
        # Apply forward-only migrations for pre-existing databases.
        with conn.cursor() as cur:
            cur.execute(RAW_TRADES_MIGRATIONS)
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
                        t.data_quality,
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
        self,
        pair: str,
        source: str = "coinbase",
        before: datetime | None = None,
        data_quality: str | None = None,
    ) -> datetime | None:
        """Get the most recent trade timestamp for a pair.

        Used by the daemon and backfill to know where to resume.
        When *before* is set, only considers trades at or before that
        timestamp — useful for bounded backfill ranges.
        When *data_quality* is set, restricts to rows with that exact tag
        (e.g. 'tick' or 'candle_1m') — used by the candle backfill so its
        resume point is not contaminated by tick rows.
        """
        conn = self.connect()
        with conn.cursor() as cur:
            query = "SELECT MAX(timestamp) FROM raw_trades WHERE pair = %s AND source = %s"
            params: list[object] = [pair, source]
            if before is not None:
                query += " AND timestamp <= %s"
                params.append(before)
            if data_quality is not None:
                query += " AND data_quality = %s"
                params.append(data_quality)
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

    def delete_all_bars(
        self,
        bar_type: str,
        pair: str,
        source: str = "coinbase",
    ) -> int:
        """Delete ALL bars for a given bar type/pair (full rebuild).

        Returns:
            Number of rows deleted.
        """
        table_name = _bar_table_name(bar_type, pair)
        if not self._table_exists(table_name):
            return 0

        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {table_name} WHERE pair = %s AND source = %s",
                (pair, source),
            )
            deleted = cur.rowcount
        conn.commit()
        logger.info(
            "Deleted all %d %s bars for %s (full rebuild)",
            deleted,
            bar_type,
            pair,
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

    # ── Bar queries ────────────────────────────────────────────────────────

    def get_bars(
        self,
        bar_type: str,
        pair: str,
        source: str = "coinbase",
        since: datetime | None = None,
        limit: int = 10_000,
    ) -> list[Bar]:
        """Fetch bars from a per-type table, ordered by time_start.

        Args:
            bar_type: Bar type label, e.g. 'tick_9243'.
            pair: Trading pair, e.g. 'ETH-USD'.
            source: Data source name.
            since: If provided, only fetch bars with time_start > since.
            limit: Maximum number of bars to return.

        Returns:
            List of Bar objects, empty if the table doesn't exist.
        """
        import json as _json

        table_name = _bar_table_name(bar_type, pair)
        if not self._table_exists(table_name):
            return []

        conn = self.connect()
        with conn.cursor() as cur:
            if since is not None:
                cur.execute(
                    f"SELECT time_start, time_end, source, pair, "
                    f"open, high, low, close, vwap, volume, dollar_volume, "
                    f"tick_count, time_span, metadata "
                    f"FROM {table_name} "
                    f"WHERE pair = %s AND source = %s AND time_start > %s "
                    f"ORDER BY time_start ASC LIMIT %s",
                    (pair, source, since, limit),
                )
            else:
                cur.execute(
                    f"SELECT time_start, time_end, source, pair, "
                    f"open, high, low, close, vwap, volume, dollar_volume, "
                    f"tick_count, time_span, metadata "
                    f"FROM {table_name} "
                    f"WHERE pair = %s AND source = %s "
                    f"ORDER BY time_start ASC LIMIT %s",
                    (pair, source, limit),
                )
            rows = cur.fetchall()

        from arcana.models import Bar

        bars: list[Bar] = []
        for r in rows:
            meta = r[13]
            if meta is not None and not isinstance(meta, dict):
                meta = _json.loads(meta)
            bars.append(
                Bar.model_construct(
                    time_start=r[0],
                    time_end=r[1],
                    bar_type=bar_type,
                    source=r[2],
                    pair=r[3],
                    open=r[4],
                    high=r[5],
                    low=r[6],
                    close=r[7],
                    vwap=r[8],
                    volume=r[9],
                    dollar_volume=r[10],
                    tick_count=r[11],
                    time_span=r[12],
                    metadata=meta,
                )
            )
        return bars

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
                    "SELECT timestamp, trade_id, source, pair, price, size, side, data_quality "
                    "FROM raw_trades "
                    "WHERE pair = %s AND source = %s "
                    "  AND (timestamp, trade_id) > (%s, %s) "
                    "ORDER BY timestamp ASC, trade_id ASC LIMIT %s",
                    (pair, source, since, since_trade_id, limit),
                )
            else:
                cur.execute(
                    "SELECT timestamp, trade_id, source, pair, price, size, side, data_quality "
                    "FROM raw_trades "
                    "WHERE pair = %s AND source = %s AND timestamp > %s "
                    "ORDER BY timestamp ASC, trade_id ASC LIMIT %s",
                    (pair, source, since, limit),
                )
            rows = cur.fetchall()

        # model_construct() skips Pydantic validation — safe here because
        # the DB column types (TIMESTAMPTZ, NUMERIC, TEXT) already guarantee
        # the correct Python types via psycopg's type adaptation.
        return [
            Trade.model_construct(
                timestamp=r[0],
                trade_id=r[1],
                source=r[2],
                pair=r[3],
                price=r[4],
                size=r[5],
                side=r[6],
                data_quality=r[7],
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

    def get_dollar_volume_stats(
        self, pair: str, source: str = "coinbase"
    ) -> tuple[float, float] | None:
        """Get total dollar volume and time span in days for a pair.

        Returns:
            (total_dollar_volume, days) or None if no trades exist.
        """
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT SUM(price * size)::float, "
                "EXTRACT(epoch FROM MAX(timestamp) - MIN(timestamp)) / 86400.0 "
                "FROM raw_trades WHERE pair = %s AND source = %s",
                (pair, source),
            )
            row = cur.fetchone()
            if row and row[0] is not None and row[1] is not None and row[1] > 0:
                return (float(row[0]), float(row[1]))
            return None

    def get_trade_volume_stats(
        self, pair: str, source: str = "coinbase"
    ) -> tuple[float, float, float] | None:
        """Get total trade count, total volume, and time span in days.

        Returns:
            (total_trades, total_volume, days) or None if no trades exist.
        """
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*)::float, SUM(size)::float, "
                "EXTRACT(epoch FROM MAX(timestamp) - MIN(timestamp)) / 86400.0 "
                "FROM raw_trades WHERE pair = %s AND source = %s",
                (pair, source),
            )
            row = cur.fetchone()
            if row and row[0] and row[0] > 0 and row[2] and row[2] > 0:
                return (float(row[0]), float(row[1]), float(row[2]))
            return None

    def get_imbalance_stats(
        self, pair: str, source: str = "coinbase"
    ) -> tuple[float, float, float] | None:
        """Compute trade-level statistics from ALL trades for E₀ calibration.

        Uses the full dataset (not a sample) for deterministic, reproducible
        calibration — running the same command on the same data always
        produces the same E₀ regardless of when the command is run.

        Returns:
            (avg_size, avg_dollar_volume, buy_fraction) or None if insufficient data.
        """
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT "
                "  AVG(size)::float, "
                "  AVG(price * size)::float, "
                "  SUM(CASE WHEN side = 'buy' THEN 1 ELSE 0 END)::float / COUNT(*)::float "
                "FROM raw_trades "
                "WHERE pair = %s AND source = %s",
                (pair, source),
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                return (float(row[0]), float(row[1]), float(row[2]))
            return None

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()

    def __enter__(self) -> Database:
        self.connect()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
