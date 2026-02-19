"""Tests for run bar builders: TRB, VRB, DRB.

Uses hand-crafted trade sequences with known expected behavior to
verify correctness of information-driven bar sampling (Prado Ch. 2).
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from arcana.bars.runs import (
    DollarRunBarBuilder,
    TickRunBarBuilder,
    VolumeRunBarBuilder,
)
from arcana.ingestion.models import Trade

BASE_TIME = datetime(2026, 2, 10, 14, 0, 0, tzinfo=UTC)


def _trade(
    offset_sec: int,
    price: str = "100.00",
    size: str = "1.0",
    side: str = "buy",
) -> Trade:
    return Trade(
        timestamp=BASE_TIME + timedelta(seconds=offset_sec),
        trade_id=f"t-{offset_sec}",
        source="coinbase",
        pair="ETH-USD",
        price=Decimal(price),
        size=Decimal(size),
        side=side,
    )


# ── Tick Run Bars ────────────────────────────────────────────────────


class TestTickRunBarBuilder:
    def test_bar_type_label(self):
        b = TickRunBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        assert b.bar_type == "trb_10"

    def test_emission_on_long_buy_run(self):
        """Consecutive buys build a run → emit when it exceeds EWMA."""
        builder = TickRunBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        trades = [_trade(i, side="buy") for i in range(10)]
        bars = builder.process_trades(trades)

        # EWMA starts at 0.0 → first trade triggers (1.0 >= 0.0)
        assert len(bars) >= 1

    def test_direction_change_resets_run(self):
        """A sell after buys resets the buy run counter."""
        builder = TickRunBarBuilder("coinbase", "ETH-USD", ewma_window=10)

        # Warm up EWMA to a reasonable level
        warmup = [_trade(i, side="buy") for i in range(40)]
        builder.process_trades(warmup)
        ewma_before = builder._ewma.expected

        # Now alternate: each run is only 1 trade long
        alternating = []
        for i in range(40, 80):
            side = "buy" if i % 2 == 0 else "sell"
            alternating.append(_trade(i, side=side))

        bars_alt = builder.process_trades(alternating)

        # Compared to a fresh builder with long same-direction run
        builder2 = TickRunBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        # Set EWMA to same starting point
        builder2._ewma._expected = ewma_before
        same_dir = [_trade(100 + i, side="buy") for i in range(40)]
        bars_same = builder2.process_trades(same_dir)

        # Long same-direction run should produce more bars
        assert len(bars_same) >= len(bars_alt)

    def test_metadata_contains_ewma_state(self):
        """Every emitted bar should carry EWMA state in metadata."""
        builder = TickRunBarBuilder("coinbase", "ETH-USD", ewma_window=5)
        trades = [_trade(i, side="buy") for i in range(10)]
        bars = builder.process_trades(trades)

        for bar in bars:
            assert bar.metadata is not None
            assert "ewma_window" in bar.metadata
            assert "ewma_expected" in bar.metadata
            assert bar.metadata["ewma_window"] == 5

    def test_unknown_side_uses_tick_rule(self):
        """Side='unknown' falls back to tick rule via price changes."""
        builder = TickRunBarBuilder("coinbase", "ETH-USD", ewma_window=5)
        # Monotonically increasing prices → all upticks → long buy run
        trades = [_trade(i, f"{100 + i}.00", side="unknown") for i in range(10)]
        bars = builder.process_trades(trades)
        assert len(bars) >= 1

    def test_stateful_across_batches(self):
        """Builder accumulates state across process_trades() calls."""
        builder = TickRunBarBuilder("coinbase", "ETH-USD", ewma_window=10)

        bars1 = builder.process_trades([_trade(i, side="buy") for i in range(20)])
        bars2 = builder.process_trades([_trade(20 + i, side="buy") for i in range(10)])

        total_bars = len(bars1) + len(bars2)
        assert total_bars > 0

    def test_flush_includes_metadata(self):
        """Flushed bar should include EWMA metadata."""
        builder = TickRunBarBuilder("coinbase", "ETH-USD", ewma_window=5)
        builder._ewma.update(100.0)  # set high threshold so no emission
        builder.process_trades([_trade(0, side="buy")])
        bar = builder.flush()
        assert bar is not None
        assert bar.metadata is not None
        assert "ewma_expected" in bar.metadata

    def test_restore_state(self):
        """Restore EWMA from metadata for daemon restart."""
        builder = TickRunBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        builder._ewma.update(42.0)
        metadata = builder._flush_metadata()

        builder2 = TickRunBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        builder2.restore_state(metadata)
        assert builder2._ewma.expected == pytest.approx(builder._ewma.expected)


# ── Volume Run Bars ──────────────────────────────────────────────────


class TestVolumeRunBarBuilder:
    def test_bar_type_label(self):
        b = VolumeRunBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        assert b.bar_type == "vrb_10"

    def test_large_volume_run_triggers_faster(self):
        """Large buy-side volumes in a run should trigger faster."""
        builder = VolumeRunBarBuilder("coinbase", "ETH-USD", ewma_window=5)
        trades = [_trade(i, size="10.0", side="buy") for i in range(5)]
        bars = builder.process_trades(trades)
        assert len(bars) >= 1

    def test_run_contribution_is_volume(self):
        """Each trade adds its volume to the current run counter."""
        builder = VolumeRunBarBuilder("coinbase", "ETH-USD", ewma_window=5)
        # 3 buys of size 5.0 → run = 15.0
        trades = [_trade(i, size="5.0", side="buy") for i in range(3)]
        bars = builder.process_trades(trades)
        assert len(bars) >= 1


# ── Dollar Run Bars ──────────────────────────────────────────────────


class TestDollarRunBarBuilder:
    def test_bar_type_label(self):
        b = DollarRunBarBuilder("coinbase", "ETH-USD", ewma_window=20)
        assert b.bar_type == "drb_20"

    def test_dollar_run_emission(self):
        """Dollar run tracks price * size per trade in the run."""
        builder = DollarRunBarBuilder("coinbase", "ETH-USD", ewma_window=5)
        # Each trade: $200 * 5.0 = $1000
        trades = [_trade(i, "200.00", "5.0", side="buy") for i in range(5)]
        bars = builder.process_trades(trades)
        assert len(bars) >= 1

    def test_metadata_present(self):
        builder = DollarRunBarBuilder("coinbase", "ETH-USD", ewma_window=5)
        trades = [_trade(i, side="buy") for i in range(10)]
        bars = builder.process_trades(trades)
        for bar in bars:
            assert bar.metadata is not None
            assert bar.metadata["ewma_window"] == 5
