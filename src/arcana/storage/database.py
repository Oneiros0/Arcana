"""TimescaleDB connection and schema management."""

import logging

import psycopg

from arcana.config import DatabaseConfig

logger = logging.getLogger(__name__)

# Raw trades table — will become a TimescaleDB hypertable
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
"""

# Bars table — will become a TimescaleDB hypertable
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


class Database:
    """Manages TimescaleDB connections and schema initialization."""

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

        Safe to call multiple times — uses IF NOT EXISTS and if_not_exists.
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
                    "create_hypertable not available — TimescaleDB extension may not be installed. "
                    "Tables created as regular PostgreSQL tables."
                )
                conn.rollback()
                return
        conn.commit()
        logger.info("Database schema initialized")

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()

    def __enter__(self) -> "Database":
        self.connect()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
