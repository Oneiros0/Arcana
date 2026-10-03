"""Tests for trade data models."""

from datetime import UTC, datetime
from decimal import Decimal

from arcana.ingestion.models import FutureInstrument, FutureMetrics, Trade


def _make_trade(**overrides) -> Trade:
    defaults = {
        "timestamp": datetime(2026, 2, 10, 14, 30, 0, tzinfo=UTC),
        "trade_id": "12345",
        "source": "coinbase",
        "pair": "ETH-USD",
        "price": Decimal("2845.50"),
        "size": Decimal("1.5"),
        "side": "buy",
    }
    defaults.update(overrides)
    return Trade(**defaults)


class TestTrade:
    def test_dollar_volume(self):
        t = _make_trade(price=Decimal("2000.00"), size=Decimal("0.5"))
        assert t.dollar_volume == Decimal("1000.00")

    def test_dollar_volume_precision(self):
        t = _make_trade(price=Decimal("2845.32"), size=Decimal("0.00345"))
        # Decimal multiplication preserves precision
        assert t.dollar_volume == Decimal("2845.32") * Decimal("0.00345")

    def test_is_buy(self):
        assert _make_trade(side="buy").is_buy is True
        assert _make_trade(side="sell").is_buy is False

    def test_sign(self):
        assert _make_trade(side="buy").sign() == 1
        assert _make_trade(side="sell").sign() == -1

    def test_frozen(self):
        t = _make_trade()
        try:
            t.price = Decimal("9999")  # type: ignore[misc]
            assert False, "Should have raised"
        except Exception:
            pass  # Expected — model is frozen

    def test_fields_stored_correctly(self):
        ts = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)
        t = _make_trade(
            timestamp=ts,
            trade_id="abc-123",
            source="test_exchange",
            pair="BTC-USD",
            price=Decimal("45000.00"),
            size=Decimal("0.001"),
            side="sell",
        )
        assert t.timestamp == ts
        assert t.trade_id == "abc-123"
        assert t.source == "test_exchange"
        assert t.pair == "BTC-USD"
        assert t.price == Decimal("45000.00")
        assert t.size == Decimal("0.001")
        assert t.side == "sell"


class TestFutureInstrument:
    def test_dated_future_full_fields(self):
        inst = FutureInstrument(
            product_id="BIT-30OCT26-CDE",
            underlying="BTC",
            contract_expiry_type="EXPIRING",
            contract_expiry="2026-10-30T16:00:00Z",
            contract_size="0.01",
            venue="FCM",
            display_name="Bitcoin Futures (Oct 2026)",
        )
        assert inst.product_id == "BIT-30OCT26-CDE"
        assert inst.underlying == "BTC"
        assert inst.contract_expiry == datetime(2026, 10, 30, 16, 0, 0, tzinfo=UTC)
        assert inst.contract_size == Decimal("0.01")
        assert inst.venue == "FCM"
        assert inst.source == "coinbase"  # default
        assert inst.trading_disabled is False  # default

    def test_empty_expiry_coerced_to_none(self):
        inst = FutureInstrument(
            product_id="BTC-PERP-INTX",
            underlying="BTC",
            contract_expiry_type="PERPETUAL",
            contract_expiry="",
        )
        assert inst.contract_expiry is None

    def test_null_expiry_coerced_to_none(self):
        inst = FutureInstrument(
            product_id="BTC-PERP-INTX",
            underlying="BTC",
            contract_expiry_type="PERPETUAL",
            contract_expiry=None,
        )
        assert inst.contract_expiry is None

    def test_empty_contract_size_coerced_to_none(self):
        inst = FutureInstrument(
            product_id="BTC-PERP-INTX",
            underlying="BTC",
            contract_expiry_type="PERPETUAL",
            contract_size="",
        )
        assert inst.contract_size is None

    def test_contract_size_decimal_precision(self):
        inst = FutureInstrument(
            product_id="ET-30OCT26-CDE",
            underlying="ETH",
            contract_expiry_type="EXPIRING",
            contract_size="0.10",
        )
        assert inst.contract_size == Decimal("0.10")

    def test_expiry_is_timezone_aware(self):
        inst = FutureInstrument(
            product_id="BIT-30OCT26-CDE",
            underlying="BTC",
            contract_expiry_type="EXPIRING",
            contract_expiry="2026-10-30T16:00:00Z",
        )
        assert inst.contract_expiry is not None
        assert inst.contract_expiry.tzinfo is not None

    def test_frozen(self):
        inst = FutureInstrument(
            product_id="BIT-30OCT26-CDE",
            underlying="BTC",
            contract_expiry_type="EXPIRING",
        )
        try:
            inst.product_id = "x"  # type: ignore[misc]
            assert False, "Should have raised"
        except Exception:
            pass  # Expected — model is frozen


class TestFutureMetrics:
    def _dated(self, **overrides) -> FutureMetrics:
        defaults = {
            "observed_at": datetime(2026, 10, 3, 19, 0, 0, tzinfo=UTC),
            "pair": "BIT-30OCT26-CDE",
            "underlying": "BTC",
            "contract_expiry_type": "EXPIRING",
            "contract_expiry": "2026-10-30T16:00:00Z",
            "contract_size": "0.01",
            "last_price": "85170",
            "volume_24h": "10747",
            "open_interest": "118023",
            "settlement_price": "84775",
            "funding_rate": "",  # dated futures have no funding
            "index_price": "",
            "funding_time": None,
        }
        defaults.update(overrides)
        return FutureMetrics(**defaults)

    def test_dated_future_numeric_fields(self):
        m = self._dated()
        assert m.last_price == Decimal("85170")
        assert m.open_interest == Decimal("118023")
        assert m.settlement_price == Decimal("84775")
        assert m.contract_size == Decimal("0.01")
        assert m.volume_24h == Decimal("10747")

    def test_empty_funding_and_index_coerced_to_none(self):
        m = self._dated()
        assert m.funding_rate is None
        assert m.index_price is None
        assert m.funding_time is None

    def test_contract_expiry_parsed_aware(self):
        m = self._dated()
        assert m.contract_expiry == datetime(2026, 10, 30, 16, 0, 0, tzinfo=UTC)
        assert m.contract_expiry.tzinfo is not None

    def test_perp_funding_present(self):
        m = self._dated(
            contract_expiry_type="PERPETUAL",
            contract_expiry=None,
            settlement_price="",
            funding_rate="0.000059",
            funding_time="2026-10-01T09:00:00Z",
            index_price="83552.26",
        )
        assert m.funding_rate == Decimal("0.000059")
        assert m.funding_time == datetime(2026, 10, 1, 9, 0, 0, tzinfo=UTC)
        assert m.index_price == Decimal("83552.26")
        assert m.settlement_price is None
        assert m.contract_expiry is None

    def test_negative_funding_rate(self):
        m = self._dated(contract_expiry_type="PERPETUAL", funding_rate="-0.0001")
        assert m.funding_rate == Decimal("-0.0001")

    def test_raw_jsonb_passthrough(self):
        m = self._dated(raw={"contract_code": "BIT", "venue": "cde"})
        assert m.raw == {"contract_code": "BIT", "venue": "cde"}

    def test_defaults_when_minimal(self):
        m = FutureMetrics(observed_at=datetime(2026, 1, 1, tzinfo=UTC), pair="X-Y-Z")
        assert m.source == "coinbase"
        assert m.product_type == "FUTURE"
        assert m.open_interest is None
        assert m.raw is None

    def test_frozen(self):
        m = self._dated()
        try:
            m.open_interest = Decimal("1")  # type: ignore[misc]
            assert False, "Should have raised"
        except Exception:
            pass
