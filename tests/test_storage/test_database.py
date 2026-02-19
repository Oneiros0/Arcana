"""Tests for database utility functions."""

import pytest

from arcana.storage.database import _bar_table_name


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
        assert (
            _bar_table_name("tick_500", "ETH-USD")
            == _bar_table_name("tick_500", "eth-usd")
        )

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
