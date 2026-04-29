"""Tests for candle parsing, synthesis, and granularity helpers."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from arcana.ingestion.candles import (
    GRANULARITY_MAP,
    Candle,
    candle_to_trade,
    granularity_api_value,
    granularity_seconds,
    parse_coinbase_candle,
)


class TestGranularity:
    def test_seconds_for_1m(self):
        assert granularity_seconds("1m") == 60

    def test_seconds_for_1h(self):
        assert granularity_seconds("1h") == 3600

    def test_seconds_for_1d(self):
        assert granularity_seconds("1d") == 86400

    def test_api_value_1m(self):
        assert granularity_api_value("1m") == "ONE_MINUTE"

    def test_api_value_5m(self):
        assert granularity_api_value("5m") == "FIVE_MINUTE"

    def test_unknown_granularity_raises(self):
        with pytest.raises(ValueError, match="Unknown granularity"):
            granularity_seconds("3m")
        with pytest.raises(ValueError, match="Unknown granularity"):
            granularity_api_value("3m")

    def test_all_granularities_have_consistent_keys(self):
        for label, (api_value, seconds) in GRANULARITY_MAP.items():
            assert granularity_seconds(label) == seconds
            assert granularity_api_value(label) == api_value


class TestParseCoinbaseCandle:
    def test_basic_parse(self):
        raw = {
            "start": "1700000000",
            "open": "100.0",
            "high": "110.0",
            "low": "95.0",
            "close": "105.0",
            "volume": "12.5",
        }
        candle = parse_coinbase_candle(raw, pair="ETH-USD", granularity="1m", source="coinbase")

        assert candle.start == datetime.fromtimestamp(1700000000, tz=UTC)
        assert candle.open == Decimal("100.0")
        assert candle.high == Decimal("110.0")
        assert candle.low == Decimal("95.0")
        assert candle.close == Decimal("105.0")
        assert candle.volume == Decimal("12.5")
        assert candle.granularity == "1m"
        assert candle.pair == "ETH-USD"
        assert candle.source == "coinbase"

    def test_decimal_precision_preserved(self):
        raw = {
            "start": "1700000000",
            "open": "0.123456789",
            "high": "0.123456789",
            "low": "0.123456789",
            "close": "0.123456789",
            "volume": "0.000000001",
        }
        candle = parse_coinbase_candle(raw, pair="ETH-USD", granularity="1m", source="coinbase")
        assert candle.open == Decimal("0.123456789")
        assert candle.volume == Decimal("0.000000001")


class TestCandleToTrade:
    def _candle(self, **overrides) -> Candle:
        defaults = dict(
            start=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
            open=Decimal("100"),
            high=Decimal("110"),
            low=Decimal("90"),
            close=Decimal("105"),
            volume=Decimal("12.5"),
            granularity="1m",
            pair="ETH-USD",
            source="coinbase",
        )
        defaults.update(overrides)
        return Candle(**defaults)

    def test_price_is_typical_price_hlc_over_3(self):
        candle = self._candle(high=Decimal("110"), low=Decimal("90"), close=Decimal("100"))
        trade = candle_to_trade(candle)
        # HLC/3 = (110 + 90 + 100) / 3 = 100
        assert trade.price == Decimal("100")

    def test_price_uses_hlc_not_open(self):
        # OHLC/4 with these values = (10+100+50+100)/4 = 65
        # HLC/3 with these values = (100+50+100)/3 ≈ 83.33 — must be HLC/3.
        candle = self._candle(
            open=Decimal("10"),
            high=Decimal("100"),
            low=Decimal("50"),
            close=Decimal("100"),
        )
        trade = candle_to_trade(candle)
        expected = (Decimal("100") + Decimal("50") + Decimal("100")) / Decimal(3)
        assert trade.price == expected

    def test_size_is_volume(self):
        candle = self._candle(volume=Decimal("42.5"))
        trade = candle_to_trade(candle)
        assert trade.size == Decimal("42.5")

    def test_side_is_unknown(self):
        candle = self._candle()
        trade = candle_to_trade(candle)
        assert trade.side == "unknown"

    def test_data_quality_tag_includes_granularity(self):
        for gran in ("1m", "5m", "1h"):
            candle = self._candle(granularity=gran)
            trade = candle_to_trade(candle)
            assert trade.data_quality == f"candle_{gran}"

    def test_trade_id_is_stable_for_idempotency(self):
        candle = self._candle()
        trade1 = candle_to_trade(candle)
        trade2 = candle_to_trade(candle)
        assert trade1.trade_id == trade2.trade_id

    def test_trade_id_namespaces_pair_so_no_cross_pair_collision(self):
        eth = candle_to_trade(self._candle(pair="ETH-USD"))
        btc = candle_to_trade(self._candle(pair="BTC-USD"))
        # Same timestamp, different pairs — must NOT collide on the
        # (source, trade_id, timestamp) UNIQUE constraint.
        assert eth.trade_id != btc.trade_id

    def test_trade_id_namespaces_granularity(self):
        a = candle_to_trade(self._candle(granularity="1m"))
        b = candle_to_trade(self._candle(granularity="5m"))
        assert a.trade_id != b.trade_id

    def test_timestamp_matches_candle_start(self):
        ts = datetime(2024, 6, 15, 9, 30, 0, tzinfo=UTC)
        candle = self._candle(start=ts)
        trade = candle_to_trade(candle)
        assert trade.timestamp == ts

    def test_synthesized_trade_sign_is_zero(self):
        """Side='unknown' must yield sign()==0 so tick rule applies downstream
        instead of pretending we know the taker direction."""
        trade = candle_to_trade(self._candle())
        assert trade.sign() == 0
