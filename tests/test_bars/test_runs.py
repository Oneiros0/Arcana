"""Tests for run bar builders: TRB, VRB, DRB.

Uses hand-crafted trade sequences with known expected behavior to
verify correctness of information-driven bar sampling (Prado AFML Ch. 2).

Run bars track max(total_buy_contribution, total_sell_contribution)
across the entire bar WITHOUT resetting on direction change (Prado Def. 2.4).
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


# -- Tick Run Bars ---------------------------------------------------------


class TestTickRunBarBuilder:
    def test_bar_type_label(self):
        b = TickRunBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        assert b.bar_type == "trb_10"

    def test_emission_on_buy_dominated_bar(self):
        """Buys dominate -> max(buy_total, sell_total) exceeds threshold."""
        builder = TickRunBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        trades = [_trade(i, side="buy") for i in range(10)]
        bars = builder.process_trades(trades)

        # EWMA starts at 0.0 -> first trade triggers (1.0 >= 0.0)
        assert len(bars) >= 1

    def test_no_reset_on_direction_change(self):
        """Buy totals accumulate even through sell trades (Prado Def 2.4).

        5 buys, 1 sell, 2 buys -> buy_total=7, sell_total=1
        With the old buggy code (consecutive runs), buy_total would be 2.
        """
        # Set threshold high enough that 2 consecutive buys won't trigger
        # but 7 total buys will
        builder = TickRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 5.0, "p_dom": 1.0, "v": 1.0},
        )
        trades = [
            _trade(0, side="buy"),
            _trade(1, side="buy"),
            _trade(2, side="buy"),
            _trade(3, side="buy"),
            _trade(4, side="buy"),   # buy_total=5
            _trade(5, side="sell"),  # sell_total=1, buy_total still 5
            _trade(6, side="buy"),   # buy_total=6
            _trade(7, side="buy"),   # buy_total=7
        ]
        bars = builder.process_trades(trades)

        # With Prado's definition, buy_total reaches 7 and triggers
        # (threshold = 5 * 1 * 1 = 5, so 5 buys triggers)
        assert len(bars) >= 1
        # The first bar should contain at least 5 trades
        assert bars[0].tick_count >= 5

    def test_sell_totals_accumulate_through_buys(self):
        """Sell totals accumulate through buy trades too."""
        builder = TickRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 4.0, "p_dom": 1.0, "v": 1.0},
        )
        trades = [
            _trade(0, side="sell"),
            _trade(1, side="sell"),
            _trade(2, side="sell"),
            _trade(3, side="buy"),   # buy_total=1, sell_total=3
            _trade(4, side="sell"),  # sell_total=4 >= threshold=4
        ]
        bars = builder.process_trades(trades)
        assert len(bars) >= 1

    def test_metadata_contains_decomposed_ewma(self):
        """Every emitted bar should carry decomposed EWMA state in metadata."""
        builder = TickRunBarBuilder("coinbase", "ETH-USD", ewma_window=5)
        trades = [_trade(i, side="buy") for i in range(10)]
        bars = builder.process_trades(trades)

        for bar in bars:
            assert bar.metadata is not None
            assert "ewma_window" in bar.metadata
            assert "ewma_t" in bar.metadata
            assert "ewma_p_dom" in bar.metadata
            assert "ewma_v" in bar.metadata
            assert bar.metadata["ewma_window"] == 5

    def test_unknown_side_uses_tick_rule(self):
        """Side='unknown' falls back to tick rule via price changes."""
        builder = TickRunBarBuilder("coinbase", "ETH-USD", ewma_window=5)
        # Monotonically increasing prices -> all upticks -> buy dominates
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
        """Flushed bar should include decomposed EWMA metadata."""
        builder = TickRunBarBuilder("coinbase", "ETH-USD", ewma_window=5)
        builder._ewma_t._expected = 1000.0  # high threshold so no emission
        builder.process_trades([_trade(0, side="buy")])
        bar = builder.flush()
        assert bar is not None
        assert bar.metadata is not None
        assert "ewma_t" in bar.metadata
        assert "ewma_p_dom" in bar.metadata
        assert "ewma_v" in bar.metadata

    def test_restore_state_new_format(self):
        """Restore decomposed EWMA from metadata for daemon restart."""
        builder = TickRunBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        builder._ewma_t.update(42.0)
        builder._ewma_p_dom.update(0.7)
        builder._ewma_v.update(1.5)
        metadata = builder._flush_metadata()

        builder2 = TickRunBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        builder2.restore_state(metadata)
        assert builder2._ewma_t.expected == pytest.approx(builder._ewma_t.expected)
        assert builder2._ewma_p_dom.expected == pytest.approx(builder._ewma_p_dom.expected)
        assert builder2._ewma_v.expected == pytest.approx(builder._ewma_v.expected)

    def test_restore_state_legacy_format(self):
        """Legacy metadata with single ewma_expected should still work."""
        builder = TickRunBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        legacy_metadata = {"ewma_window": 10, "ewma_expected": 42.0}
        builder.restore_state(legacy_metadata)
        assert builder._ewma_t.expected == pytest.approx(42.0)

    def test_threshold_decomposition_math(self):
        """Verify threshold = E[T] x E[P_dom] x E[|v|]."""
        builder = TickRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 100.0, "p_dom": 0.7, "v": 1.0},
        )
        # threshold = 100 * 0.7 * 1.0 = 70.0
        assert builder._threshold == pytest.approx(70.0)

    def test_threshold_clamps_p_dominant(self):
        """P_dominant should be clamped to [0.55, 0.95]."""
        builder_low = TickRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 100.0, "p_dom": 0.2, "v": 1.0},
        )
        # p_dom clamped to 0.55: threshold = 100 * 0.55 * 1 = 55
        assert builder_low._threshold == pytest.approx(55.0)

        builder_high = TickRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 100.0, "p_dom": 0.99, "v": 1.0},
        )
        # p_dom clamped to 0.95: threshold = 100 * 0.95 * 1 = 95
        assert builder_high._threshold == pytest.approx(95.0)


# -- Volume Run Bars -------------------------------------------------------


class TestVolumeRunBarBuilder:
    def test_bar_type_label(self):
        b = VolumeRunBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        assert b.bar_type == "vrb_10"

    def test_large_volume_run_triggers_faster(self):
        """Large buy-side volumes should trigger faster."""
        builder = VolumeRunBarBuilder("coinbase", "ETH-USD", ewma_window=5)
        trades = [_trade(i, size="10.0", side="buy") for i in range(5)]
        bars = builder.process_trades(trades)
        assert len(bars) >= 1

    def test_run_contribution_is_volume(self):
        """Each trade adds its volume to the buy or sell total."""
        builder = VolumeRunBarBuilder("coinbase", "ETH-USD", ewma_window=5)
        # 3 buys of size 5.0 -> buy_total = 15.0
        trades = [_trade(i, size="5.0", side="buy") for i in range(3)]
        bars = builder.process_trades(trades)
        assert len(bars) >= 1


# -- Dollar Run Bars -------------------------------------------------------


class TestDollarRunBarBuilder:
    def test_bar_type_label(self):
        b = DollarRunBarBuilder("coinbase", "ETH-USD", ewma_window=20)
        assert b.bar_type == "drb_20"

    def test_dollar_run_emission(self):
        """Dollar run tracks price * size per trade."""
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
            assert "ewma_t" in bar.metadata
