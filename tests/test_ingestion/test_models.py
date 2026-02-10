"""Tests for trade data models."""

from datetime import datetime, timezone
from decimal import Decimal

from arcana.ingestion.models import Trade


def _make_trade(**overrides) -> Trade:
    defaults = {
        "timestamp": datetime(2026, 2, 10, 14, 30, 0, tzinfo=timezone.utc),
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
        ts = datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
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
