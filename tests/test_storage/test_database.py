"""Tests for database utility functions."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from arcana.config import DatabaseConfig
from arcana.ingestion.models import FutureMetrics
from arcana.storage.database import Database, _bar_table_name


class TestBarTableName:
    """Per-pair-per-type table naming: _bar_table_name(bar_type, pair)."""

    def test_tick_bar(self):
        assert _bar_table_name("tick_500", "ETH-USD") == "bars_tick_500_eth_usd"

    def test_volume_bar(self):
        assert _bar_table_name("volume_100", "ETH-USD") == "bars_volume_100_eth_usd"

    def test_dollar_bar(self):
        assert _bar_table_name("dollar_50000", "BTC-USD") == "bars_dollar_50000_btc_usd"

    def test_time_bar_minutes(self):
        assert _bar_table_name("time_5m", "ETH-USD") == "bars_time_5m_eth_usd"

    def test_time_bar_hours(self):
        assert _bar_table_name("time_1h", "ETH-USD") == "bars_time_1h_eth_usd"

    def test_time_bar_seconds(self):
        assert _bar_table_name("time_30s", "ETH-USD") == "bars_time_30s_eth_usd"

    def test_time_bar_days(self):
        assert _bar_table_name("time_1d", "SOL-USD") == "bars_time_1d_sol_usd"

    def test_decimal_threshold(self):
        assert _bar_table_name("volume_10.5", "ETH-USD") == "bars_volume_10_5_eth_usd"

    def test_rejects_sql_injection_bar_type(self):
        with pytest.raises(ValueError, match="bar_type"):
            _bar_table_name("tick_500; DROP TABLE", "ETH-USD")

    def test_rejects_dash_in_bar_type(self):
        with pytest.raises(ValueError, match="bar_type"):
            _bar_table_name("tick-500", "ETH-USD")

    def test_rejects_uppercase_bar_type(self):
        with pytest.raises(ValueError, match="bar_type"):
            _bar_table_name("TICK_500", "ETH-USD")

    def test_rejects_spaces_bar_type(self):
        with pytest.raises(ValueError, match="bar_type"):
            _bar_table_name("tick 500", "ETH-USD")

    def test_rejects_empty_bar_type(self):
        with pytest.raises(ValueError, match="bar_type"):
            _bar_table_name("", "ETH-USD")

    # ── Pair validation ──────────────────────────────────────────────

    def test_pair_normalization_lowercase(self):
        """Pairs are case-insensitive — always lowered in table name."""
        assert _bar_table_name("tick_500", "Eth-Usd") == "bars_tick_500_eth_usd"

    def test_case_insensitive_pair(self):
        """ETH-USD and eth-usd produce the same table name."""
        assert _bar_table_name("tick_500", "ETH-USD") == _bar_table_name("tick_500", "eth-usd")

    def test_rejects_invalid_pair_no_dash(self):
        with pytest.raises(ValueError, match="pair"):
            _bar_table_name("tick_500", "ETHUSD")

    def test_rejects_invalid_pair_spaces(self):
        with pytest.raises(ValueError, match="pair"):
            _bar_table_name("tick_500", "ETH USD")

    def test_rejects_invalid_pair_sql_injection(self):
        with pytest.raises(ValueError, match="pair"):
            _bar_table_name("tick_500", "ETH-USD; DROP TABLE")

    def test_rejects_empty_pair(self):
        with pytest.raises(ValueError, match="pair"):
            _bar_table_name("tick_500", "")

    # ── Futures product ids (multi-segment pairs) ────────────────────

    def test_accepts_dated_future_id(self):
        """Dated futures ids have three hyphen-separated segments."""
        assert _bar_table_name("tick_500", "BIT-30OCT26-CDE") == "bars_tick_500_bit_30oct26_cde"

    def test_accepts_perp_future_id(self):
        assert _bar_table_name("tick_500", "BTC-PERP-INTX") == "bars_tick_500_btc_perp_intx"

    def test_future_id_case_insensitive(self):
        assert _bar_table_name("tick_500", "bit-30oct26-cde") == _bar_table_name(
            "tick_500", "BIT-30OCT26-CDE"
        )

    def test_future_and_spot_distinct_tables(self):
        assert _bar_table_name("tick_500", "BIT-30OCT26-CDE") != _bar_table_name(
            "tick_500", "ETH-USD"
        )

    def test_still_rejects_trailing_hyphen(self):
        with pytest.raises(ValueError, match="pair"):
            _bar_table_name("tick_500", "BIT-30OCT26-")

    def test_still_rejects_sql_injection_in_future_id(self):
        with pytest.raises(ValueError, match="pair"):
            _bar_table_name("tick_500", "BIT-30OCT26-CDE; DROP TABLE")

    # ── Information-driven bar types ─────────────────────────────────

    def test_imbalance_bar(self):
        assert _bar_table_name("tib_20", "ETH-USD") == "bars_tib_20_eth_usd"

    def test_run_bar(self):
        assert _bar_table_name("trb_10", "BTC-USD") == "bars_trb_10_btc_usd"

    def test_multiple_pairs_different_tables(self):
        """Different pairs for the same bar type produce different tables."""
        eth = _bar_table_name("tick_500", "ETH-USD")
        btc = _bar_table_name("tick_500", "BTC-USD")
        assert eth != btc
        assert eth == "bars_tick_500_eth_usd"
        assert btc == "bars_tick_500_btc_usd"


def _mock_db_with_cursor():
    """Build a Database whose connection yields a controllable mock cursor."""
    db = Database(DatabaseConfig())
    conn = MagicMock()
    conn.closed = False
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    db._conn = conn
    return db, conn, cur


class TestInsertFutureMetrics:
    def _metrics(self, **overrides) -> FutureMetrics:
        defaults = {
            "observed_at": datetime(2026, 10, 3, 19, 0, 0, tzinfo=UTC),
            "pair": "BIT-30OCT26-CDE",
            "underlying": "BTC",
            "contract_expiry_type": "EXPIRING",
            "contract_expiry": datetime(2026, 10, 30, 16, 0, 0, tzinfo=UTC),
            "contract_size": Decimal("0.01"),
            "last_price": Decimal("85170"),
            "volume_24h": Decimal("10747"),
            "open_interest": Decimal("118023"),
            "settlement_price": Decimal("84775"),
        }
        defaults.update(overrides)
        return FutureMetrics(**defaults)

    def test_empty_list_returns_zero(self):
        db, conn, cur = _mock_db_with_cursor()
        assert db.insert_future_metrics([]) == 0
        cur.executemany.assert_not_called()

    def test_maps_columns_in_order(self):
        db, conn, cur = _mock_db_with_cursor()
        # _count_future_metrics is called before and after → 0 then 1
        cur.fetchone.side_effect = [(0,), (1,)]

        inserted = db.insert_future_metrics([self._metrics()])

        assert inserted == 1
        rows = cur.executemany.call_args[0][1]
        assert len(rows) == 1
        row = rows[0]
        # 16 columns in the documented order
        assert len(row) == 16
        assert row[0] == datetime(2026, 10, 3, 19, 0, 0, tzinfo=UTC)  # observed_at
        assert row[1] == "coinbase"  # source default
        assert row[2] == "BIT-30OCT26-CDE"  # pair
        assert row[8] == Decimal("85170")  # last_price
        assert row[10] == Decimal("118023")  # open_interest
        assert row[14] == Decimal("84775")  # settlement_price

    def test_none_metrics_serialize_as_null(self):
        db, conn, cur = _mock_db_with_cursor()
        cur.fetchone.side_effect = [(0,), (1,)]

        m = self._metrics(funding_rate=None, index_price=None, raw=None)
        db.insert_future_metrics([m])

        row = cur.executemany.call_args[0][1][0]
        assert row[11] is None  # funding_rate
        assert row[13] is None  # index_price
        assert row[15] is None  # raw (json.dumps skipped when falsy)

    def test_raw_dict_is_json_serialized(self):
        db, conn, cur = _mock_db_with_cursor()
        cur.fetchone.side_effect = [(0,), (1,)]

        db.insert_future_metrics([self._metrics(raw={"contract_code": "BIT"})])

        row = cur.executemany.call_args[0][1][0]
        assert row[15] == '{"contract_code": "BIT"}'
