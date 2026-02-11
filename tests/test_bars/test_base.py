"""Tests for Bar model, Accumulator, and BarBuilder base class."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from arcana.bars.base import Accumulator, Bar, BarBuilder
from arcana.ingestion.models import Trade


def _trade(
    ts_offset: int = 0,
    price: str = "100.00",
    size: str = "1.0",
    side: str = "buy",
) -> Trade:
    """Create a trade at a fixed base time + offset seconds."""
    base = datetime(2026, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
    return Trade(
        timestamp=base + timedelta(seconds=ts_offset),
        trade_id=f"t-{ts_offset}",
        source="test",
        pair="ETH-USD",
        price=Decimal(price),
        size=Decimal(size),
        side=side,
    )


class TestAccumulator:
    def test_single_trade(self):
        acc = Accumulator()
        t = _trade(0, "100.00", "2.5")
        acc.add(t)

        assert acc.tick_count == 1
        assert acc._open == Decimal("100.00")
        assert acc._high == Decimal("100.00")
        assert acc._low == Decimal("100.00")
        assert acc._close == Decimal("100.00")
        assert acc._volume == Decimal("2.5")
        assert acc._dollar_volume == Decimal("250.00")
        assert acc.time_start == t.timestamp
        assert acc.time_end == t.timestamp

    def test_multiple_trades_ohlcv(self):
        acc = Accumulator()
        acc.add(_trade(0, "100.00", "1.0"))
        acc.add(_trade(1, "105.00", "2.0"))
        acc.add(_trade(2, "95.00", "1.5"))
        acc.add(_trade(3, "102.00", "0.5"))

        assert acc.tick_count == 4
        assert acc._open == Decimal("100.00")
        assert acc._high == Decimal("105.00")
        assert acc._low == Decimal("95.00")
        assert acc._close == Decimal("102.00")
        # volume = 1 + 2 + 1.5 + 0.5 = 5.0
        assert acc._volume == Decimal("5.0")
        # dollar_volume = 100 + 210 + 142.5 + 51 = 503.5
        assert acc._dollar_volume == Decimal("503.50")

    def test_to_bar_computes_vwap(self):
        acc = Accumulator()
        # Trade 1: 100 * 1 = 100
        acc.add(_trade(0, "100.00", "1.0"))
        # Trade 2: 200 * 3 = 600
        acc.add(_trade(1, "200.00", "3.0"))
        # VWAP = (100 + 600) / (1 + 3) = 700/4 = 175
        bar = acc.to_bar("test_bar", "test", "ETH-USD")

        assert bar.vwap == Decimal("175")
        assert bar.volume == Decimal("4.0")
        assert bar.tick_count == 2
        assert bar.time_span == timedelta(seconds=1)

    def test_to_bar_empty_raises(self):
        acc = Accumulator()
        with pytest.raises(AssertionError):
            acc.to_bar("test", "test", "ETH-USD")

    def test_to_bar_produces_frozen_model(self):
        acc = Accumulator()
        acc.add(_trade(0))
        bar = acc.to_bar("test", "test", "ETH-USD")
        assert isinstance(bar, Bar)
        with pytest.raises(Exception):
            bar.tick_count = 999  # type: ignore[misc]


class TestBarModel:
    def test_bar_fields(self):
        bar = Bar(
            time_start=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            time_end=datetime(2026, 1, 1, 0, 5, 0, tzinfo=timezone.utc),
            bar_type="time_5m",
            source="coinbase",
            pair="ETH-USD",
            open=Decimal("100"),
            high=Decimal("110"),
            low=Decimal("90"),
            close=Decimal("105"),
            vwap=Decimal("102.5"),
            volume=Decimal("50"),
            dollar_volume=Decimal("5125"),
            tick_count=100,
            time_span=timedelta(minutes=5),
        )
        assert bar.bar_type == "time_5m"
        assert bar.tick_count == 100
        assert bar.time_span == timedelta(minutes=5)
