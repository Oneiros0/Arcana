"""TimescaleDB connection and schema management."""

import logging
from datetime import datetime

import psycopg
from psycopg.rows import dict_row

from arcana.config import DatabaseConfig
from arcana.ingestion.models import Trade

logger = logging.getLogger(__name__)

RAW_TRADES_SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_trades (
    timestamp    TIMESTAMPTZ   NOT NULL,
    trade_id     TEXT          NOT NULL,
    source       TEXT          NOT NULL,
    pair         TEXT          NOT NULL,
    price        NUMERIC       NOT NULL,
    size         NUMERIC       NOT NULL,
    side         TEXT          NOT NULL,
    UNIQUE (source, trade_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_trades_pair_ts
    ON raw_trades (pair, timestamp);
"""

BARS_SCHEMA = """
CREATE TABLE IF NOT EXISTS bars (
    time_start    TIMESTAMPTZ   NOT NULL,
    time_end      TIMESTAMPTZ   NOT NULL,
    bar_type      TEXT          NOT NULL,
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
    metadata      JSONB,
    UNIQUE (bar_type, source, pair, time_start)
);
"""

HYPERTABLE_RAW = """
SELECT create_hypertable('raw_trades', 'timestamp', if_not_exists => TRUE);
"""

HYPERTABLE_BARS = """
SELECT create_hypertable('bars', 'time_start', if_not_exists => TRUE);
"""

UPSERT_TRADES = """
INSERT INTO raw_trades (timestamp, trade_id, source, pair, price, size, side)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (source, trade_id) DO NOTHING;
"""


class Database:
    """Manages TimescaleDB connections, schema, and trade storage."""

    def __init__(self, config: DatabaseConfig) -> None:
        self._config = config
        self._conn: psycopg.Connection | None = None

    def connect(self) -> psycopg.Connection:
        """Open a connection to TimescaleDB."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self._config.dsn)
            logger.info("Connected to database at %s", self._config.host)
        return self._conn

    def init_schema(self) -> None:
        """Create tables and convert them to hypertables.

        Safe to call multiple times — uses IF NOT EXISTS.
        """
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(RAW_TRADES_SCHEMA)
            cur.execute(BARS_SCHEMA)
            try:
                cur.execute(HYPERTABLE_RAW)
                cur.execute(HYPERTABLE_BARS)
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
                    cur2.execute(BARS_SCHEMA)
        conn.commit()
        logger.info("Database schema initialized")

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

    def get_last_timestamp(self, pair: str, source: str = "coinbase") -> datetime | None:
        """Get the most recent trade timestamp for a pair.

        Used by the daemon to know where to resume ingestion.
        """
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(timestamp) FROM raw_trades WHERE pair = %s AND source = %s",
                (pair, source),
            )
            row = cur.fetchone()
            return row[0] if row and row[0] else None

    def get_trade_count(self, pair: str | None = None) -> int:
        """Get total trade count, optionally filtered by pair."""
        conn = self.connect()
        with conn.cursor() as cur:
            if pair:
                cur.execute(
                    "SELECT COUNT(*) FROM raw_trades WHERE pair = %s", (pair,)
                )
            else:
                cur.execute("SELECT COUNT(*) FROM raw_trades")
            row = cur.fetchone()
            return row[0] if row else 0

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()

    def __enter__(self) -> "Database":
        self.connect()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
