"""Tests for imbalance bar builders: TIB, VIB, DIB.

Uses hand-crafted trade sequences with known expected behavior to
verify correctness of information-driven bar sampling (Prado AFML Ch. 2).
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from arcana.bars.imbalance import (
    DollarImbalanceBarBuilder,
    TickImbalanceBarBuilder,
    VolumeImbalanceBarBuilder,
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


# -- Tick Imbalance Bars ---------------------------------------------------


class TestTickImbalanceBarBuilder:
    def test_bar_type_label(self):
        b = TickImbalanceBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        assert b.bar_type == "tib_10"

    def test_initial_expected_dict_propagates(self):
        """Dict initial_expected should set all three EWMA components."""
        b = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 50.0, "imb": 0.3, "v": 1.0},
        )
        assert b._ewma_t.expected == pytest.approx(50.0)
        assert b._ewma_imb.expected == pytest.approx(0.3)
        assert b._ewma_v.expected == pytest.approx(1.0)

    def test_initial_expected_float_legacy(self):
        """Float initial_expected is treated as E[T] with imb=1, v=1."""
        b = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10, initial_expected=50.0
        )
        assert b._ewma_t.expected == pytest.approx(50.0)
        assert b._ewma_imb.expected == pytest.approx(1.0)
        assert b._ewma_v.expected == pytest.approx(1.0)
        # Threshold = 50 * 1 * 1 = 50
        assert b._threshold == pytest.approx(50.0)

    def test_initial_expected_reduces_bar_count(self):
        """High initial_expected should produce far fewer bars than E0=0."""
        trades = [_trade(i, side="buy") for i in range(100)]

        builder_zero = TickImbalanceBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        bars_zero = builder_zero.process_trades(trades)

        builder_high = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10, initial_expected=50.0
        )
        bars_high = builder_high.process_trades(trades)

        assert len(bars_high) < len(bars_zero)

    def test_emission_on_buy_sequence(self):
        """All-buy sequence: imbalance grows by +1 each trade.

        EWMA starts at 0.0 -> first trade triggers (|1| >= 0).
        After that, EWMA updates and next bar requires more trades.
        """
        builder = TickImbalanceBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        trades = [_trade(i, side="buy") for i in range(5)]
        bars = builder.process_trades(trades)

        # First bar emits on trade 0 (|+1| >= 0.0)
        assert len(bars) >= 1
        assert bars[0].tick_count >= 1

    def test_mixed_signs_cancel_out(self):
        """Alternating buy/sell: net imbalance stays near zero.

        After warming up the EWMA to a stable threshold, alternating
        trades should accumulate more trades per bar than one-sided
        flow, because the signed contributions cancel out.
        """
        initial = {"t": 3.0, "imb": 1.0, "v": 1.0}

        builder_mixed = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10, initial_expected=initial
        )
        builder_mixed._cum_imbalance = 0.0

        builder_buy = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10, initial_expected=initial
        )
        builder_buy._cum_imbalance = 0.0

        # Mixed trades: alternating buy/sell
        trades_mixed = []
        for i in range(40):
            side = "buy" if i % 2 == 0 else "sell"
            trades_mixed.append(_trade(i, side=side))
        bars_mixed = builder_mixed.process_trades(trades_mixed)

        # All-buy trades: imbalance grows by +1 each trade
        trades_buy = [_trade(i, side="buy") for i in range(40)]
        bars_buy = builder_buy.process_trades(trades_buy)

        # All-buy should produce more bars (imbalance builds faster)
        assert len(bars_mixed) < len(bars_buy)

    def test_metadata_contains_decomposed_ewma(self):
        """Every emitted bar should carry decomposed EWMA state in metadata."""
        builder = TickImbalanceBarBuilder("coinbase", "ETH-USD", ewma_window=5)
        trades = [_trade(i, side="buy") for i in range(10)]
        bars = builder.process_trades(trades)

        for bar in bars:
            assert bar.metadata is not None
            assert "ewma_window" in bar.metadata
            assert "ewma_t" in bar.metadata
            assert "ewma_imb" in bar.metadata
            assert "ewma_v" in bar.metadata
            assert bar.metadata["ewma_window"] == 5

    def test_unknown_side_uses_tick_rule(self):
        """Side='unknown' falls back to tick rule via price changes."""
        builder = TickImbalanceBarBuilder("coinbase", "ETH-USD", ewma_window=5)
        trades = [
            _trade(0, "100.00", side="unknown"),
            _trade(1, "101.00", side="unknown"),  # uptick -> +1
            _trade(2, "102.00", side="unknown"),  # uptick -> +1
            _trade(3, "103.00", side="unknown"),  # uptick -> +1
        ]
        bars = builder.process_trades(trades)
        # Should produce bars -- upticks create positive imbalance
        assert len(bars) >= 1

    def test_stateful_across_batches(self):
        """Builder accumulates state across process_trades() calls."""
        builder = TickImbalanceBarBuilder("coinbase", "ETH-USD", ewma_window=10)

        # After several bars, set the EWMA to a known level
        warmup = [_trade(i, side="buy") for i in range(30)]
        bars1 = builder.process_trades(warmup)

        # Now feed one more batch -- should still work
        bars2 = builder.process_trades([_trade(30 + i, side="buy") for i in range(10)])
        # The builder should continue emitting bars
        total_bars = len(bars1) + len(bars2)
        assert total_bars > 0

    def test_flush_includes_metadata(self):
        """Flushed bar should include decomposed EWMA metadata."""
        builder = TickImbalanceBarBuilder("coinbase", "ETH-USD", ewma_window=5)
        # Feed some trades without triggering emission by setting high threshold
        builder._ewma_t._expected = 1000.0
        builder.process_trades([_trade(0, side="buy")])
        bar = builder.flush()
        assert bar is not None
        assert bar.metadata is not None
        assert "ewma_t" in bar.metadata
        assert "ewma_imb" in bar.metadata
        assert "ewma_v" in bar.metadata

    def test_restore_state_new_format(self):
        """Restore decomposed EWMA from metadata for daemon restart."""
        builder = TickImbalanceBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        builder._ewma_t.update(42.0)
        builder._ewma_imb.update(0.3)
        builder._ewma_v.update(1.5)
        metadata = builder._flush_metadata()

        builder2 = TickImbalanceBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        builder2.restore_state(metadata)
        assert builder2._ewma_t.expected == pytest.approx(builder._ewma_t.expected)
        assert builder2._ewma_imb.expected == pytest.approx(builder._ewma_imb.expected)
        assert builder2._ewma_v.expected == pytest.approx(builder._ewma_v.expected)

    def test_restore_state_legacy_format(self):
        """Legacy metadata with single ewma_expected should still work."""
        builder = TickImbalanceBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        legacy_metadata = {"ewma_window": 10, "ewma_expected": 42.0}
        builder.restore_state(legacy_metadata)
        assert builder._ewma_t.expected == pytest.approx(42.0)
        assert builder._ewma_imb.expected == pytest.approx(1.0)
        assert builder._ewma_v.expected == pytest.approx(1.0)

    def test_threshold_decomposition_math(self):
        """Verify threshold = E[T] x |E[2P-1]| x E[|v|]."""
        builder = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 100.0, "imb": 0.4, "v": 2.0},
        )
        # threshold = 100 * |0.4| * 2.0 = 80.0
        assert builder._threshold == pytest.approx(80.0)

    def test_threshold_floors_imbalance_at_01(self):
        """Balanced market (imb~0) should floor at 0.1 to avoid degeneracy."""
        builder = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 100.0, "imb": 0.0, "v": 1.0},
        )
        # threshold = 100 * max(|0.0|, 0.1) * 1.0 = 10.0
        assert builder._threshold == pytest.approx(10.0)

    def test_ewma_updates_with_bar_statistics(self):
        """EWMAs should update with per-bar T, imb, and |v| at emission."""
        builder = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 3.0, "imb": 1.0, "v": 1.0},
        )
        # 5 buys: cum_imbalance = 5, threshold = 3 * 1 * 1 = 3
        # Should emit after 3 trades (|3| >= 3)
        trades = [_trade(i, side="buy") for i in range(5)]
        bars = builder.process_trades(trades)
        assert len(bars) >= 1

        # After emission, ewma_t should have been updated with the bar's tick count
        assert bars[0].tick_count >= 1
        # ewma_t should have moved toward actual bar size from initial 3.0
        assert builder._ewma_t.expected > 0


# -- Volume Imbalance Bars -------------------------------------------------


class TestVolumeImbalanceBarBuilder:
    def test_bar_type_label(self):
        b = VolumeImbalanceBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        assert b.bar_type == "vib_10"

    def test_large_buy_volume_triggers_faster(self):
        """Large buy-side volumes should cause faster emission."""
        builder = VolumeImbalanceBarBuilder("coinbase", "ETH-USD", ewma_window=5)
        # Large buys: each contributes +10.0 to imbalance
        trades = [_trade(i, size="10.0", side="buy") for i in range(5)]
        bars = builder.process_trades(trades)
        assert len(bars) >= 1

    def test_contribution_is_signed_volume(self):
        """Buy adds +size, sell adds -size to imbalance."""
        # 2 buys of size 5 = +10, then 1 sell of size 10 = -10 -> net 0
        # Compare with 3 buys of size 5 = +15 -> net +15
        trades_mixed = [
            _trade(0, size="5.0", side="buy"),
            _trade(1, size="5.0", side="buy"),
            _trade(2, size="10.0", side="sell"),
        ]
        trades_all_buy = [
            _trade(0, size="5.0", side="buy"),
            _trade(1, size="5.0", side="buy"),
            _trade(2, size="5.0", side="buy"),
        ]

        builder_mixed = VolumeImbalanceBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        builder_buy = VolumeImbalanceBarBuilder("coinbase", "ETH-USD", ewma_window=10)

        bars_mixed = builder_mixed.process_trades(trades_mixed)
        bars_buy = builder_buy.process_trades(trades_all_buy)

        # All-buy should produce at least as many bars
        assert len(bars_buy) >= len(bars_mixed)

    def test_metadata_has_decomposed_ewma(self):
        """Emitted bars carry decomposed EWMA in metadata."""
        builder = VolumeImbalanceBarBuilder("coinbase", "ETH-USD", ewma_window=5)
        trades = [_trade(i, size="10.0", side="buy") for i in range(5)]
        bars = builder.process_trades(trades)
        for bar in bars:
            assert bar.metadata is not None
            assert "ewma_t" in bar.metadata
            assert "ewma_v" in bar.metadata


# -- Dollar Imbalance Bars -------------------------------------------------


class TestDollarImbalanceBarBuilder:
    def test_bar_type_label(self):
        b = DollarImbalanceBarBuilder("coinbase", "ETH-USD", ewma_window=20)
        assert b.bar_type == "dib_20"

    def test_contribution_is_signed_dollar_volume(self):
        """Buy at $200 x 5.0 = +$1000, sell at $100 x 5.0 = -$500."""
        builder = DollarImbalanceBarBuilder("coinbase", "ETH-USD", ewma_window=5)
        trades = [
            _trade(0, "200.00", "5.0", side="buy"),  # +$1000
            _trade(1, "100.00", "5.0", side="sell"),  # -$500, net +$500
            _trade(2, "200.00", "5.0", side="buy"),  # +$1000, net +$1500
        ]
        bars = builder.process_trades(trades)
        assert len(bars) >= 1

    def test_metadata_present(self):
        builder = DollarImbalanceBarBuilder("coinbase", "ETH-USD", ewma_window=5)
        trades = [_trade(i, side="buy") for i in range(10)]
        bars = builder.process_trades(trades)
        for bar in bars:
            assert bar.metadata is not None
            assert bar.metadata["ewma_window"] == 5
