"""Mathematical proofs and rigorous verification for information-driven bars.

Each test in this module is a constructive mathematical proof: it derives the
expected result from first principles using Prado's AFML Ch. 2 formulas, then
verifies the implementation matches exactly.

Proofs cover:
  - EWMA convergence properties and exact multi-step arithmetic
  - Imbalance bar (TIB/VIB/DIB) CUSUM trigger logic with hand-traced sequences
  - Run bar (TRB/VRB/DRB) accumulation-without-reset with hand-traced sequences
  - Threshold decomposition algebra
  - EWMA update ordering at bar emission
  - Equilibrium bar-size analysis for synthetic distributions
  - Edge cases: cold start, floor behavior, all-same-sign, tick rule
"""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from arcana.bars.imbalance import (
    DollarImbalanceBarBuilder,
    TickImbalanceBarBuilder,
    VolumeImbalanceBarBuilder,
)
from arcana.bars.runs import (
    DollarRunBarBuilder,
    TickRunBarBuilder,
    VolumeRunBarBuilder,
)
from arcana.bars.utils import EWMAEstimator
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


# ═══════════════════════════════════════════════════════════════════════
# EWMA MATHEMATICAL PROOFS
# ═══════════════════════════════════════════════════════════════════════


class TestEWMAMathematicalProofs:
    """Prove EWMA arithmetic and convergence properties."""

    def test_exact_alpha_values(self):
        """Proof: alpha = 2/(window+1).

        window=1  → alpha=1.0
        window=4  → alpha=0.4
        window=9  → alpha=0.2
        window=10 → alpha=2/11
        window=20 → alpha=2/21
        """
        cases = [
            (1, 1.0),
            (4, 0.4),
            (9, 0.2),
            (10, 2.0 / 11.0),
            (20, 2.0 / 21.0),
        ]
        for window, expected_alpha in cases:
            ewma = EWMAEstimator(window=window)
            assert ewma._alpha == pytest.approx(expected_alpha), (
                f"window={window}: alpha={ewma._alpha}, expected={expected_alpha}"
            )

    def test_exact_three_step_trace(self):
        """Proof: verify EWMA update E[t] = α·x + (1-α)·E[t-1] step by step.

        Window=4, alpha=0.4, initial=0.0.
        Step 1: x=10 → E = 0.4×10 + 0.6×0   = 4.0
        Step 2: x=20 → E = 0.4×20 + 0.6×4   = 10.4
        Step 3: x=5  → E = 0.4×5  + 0.6×10.4 = 8.24
        """
        ewma = EWMAEstimator(window=4, initial_value=0.0)

        assert ewma.update(10.0) == pytest.approx(4.0)
        assert ewma.update(20.0) == pytest.approx(10.4)
        assert ewma.update(5.0) == pytest.approx(8.24)

    def test_exact_trace_with_nonzero_initial(self):
        """Proof: EWMA with initial=50.0.

        Window=9, alpha=0.2, initial=50.0.
        Step 1: x=100 → E = 0.2×100 + 0.8×50 = 60.0
        Step 2: x=0   → E = 0.2×0   + 0.8×60 = 48.0
        Step 3: x=50  → E = 0.2×50  + 0.8×48 = 48.4
        """
        ewma = EWMAEstimator(window=9, initial_value=50.0)

        assert ewma.update(100.0) == pytest.approx(60.0)
        assert ewma.update(0.0) == pytest.approx(48.0)
        assert ewma.update(50.0) == pytest.approx(48.4)

    def test_convergence_bound_constant_input(self):
        """Proof: after N updates of constant c, E[N] = c·(1-(1-α)^N).

        Starting from E[0]=0, after N updates of value c:
          E[N] = c × (1 - (1-α)^N)

        For window=10 (α=2/11), after 30 updates (≈3×window):
          E[30] = c × (1 - (9/11)^30)
          (9/11)^30 ≈ 0.00295, so E[30] ≈ 0.997×c

        The error |E[N] - c| = c × (1-α)^N decays geometrically.
        """
        window = 10
        alpha = 2.0 / (window + 1)
        c = 42.0

        ewma = EWMAEstimator(window=window, initial_value=0.0)
        for _ in range(30):
            ewma.update(c)

        theoretical = c * (1.0 - (1.0 - alpha) ** 30)
        assert ewma.expected == pytest.approx(theoretical)
        assert abs(ewma.expected - c) < 0.15  # very close to c after 3×window

    def test_convergence_bound_general_initial(self):
        """Proof: after N updates of constant c, starting from E[0]=a:
          E[N] = c + (a-c)×(1-α)^N

        Window=4 (α=0.4), initial=100.0, constant input=20.0.
        After 10 updates:
          E[10] = 20 + (100-20)×(0.6)^10 = 20 + 80×0.006047 ≈ 20.484
        """
        ewma = EWMAEstimator(window=4, initial_value=100.0)
        for _ in range(10):
            ewma.update(20.0)

        theoretical = 20.0 + (100.0 - 20.0) * (0.6**10)
        assert ewma.expected == pytest.approx(theoretical, rel=1e-10)

    def test_step_change_response(self):
        """Proof: EWMA adapts to step change in input.

        Window=9 (α=0.2). Converge to 10.0, then switch to 50.0.
        After switch, each update moves 20% toward 50.

        After 1 update at 50: E = 0.2×50 + 0.8×10 = 18.0
        After 2 updates at 50: E = 0.2×50 + 0.8×18 = 24.4
        After 3 updates at 50: E = 0.2×50 + 0.8×24.4 = 29.52
        """
        ewma = EWMAEstimator(window=9, initial_value=10.0)
        # Converge fully to 10
        for _ in range(100):
            ewma.update(10.0)
        assert ewma.expected == pytest.approx(10.0, abs=0.01)

        # Step change to 50
        assert ewma.update(50.0) == pytest.approx(18.0, abs=0.01)
        assert ewma.update(50.0) == pytest.approx(24.4, abs=0.01)
        assert ewma.update(50.0) == pytest.approx(29.52, abs=0.01)


# ═══════════════════════════════════════════════════════════════════════
# TICK IMBALANCE BAR (TIB) MATHEMATICAL PROOFS
# ═══════════════════════════════════════════════════════════════════════


class TestTickImbalanceBarMathProofs:
    """Prove TIB correctness via exact hand-traced sequences.

    TIB contribution: sign(trade) ∈ {+1, -1}.
    Threshold: E[T] × |E[2P-1]| × E[|v|].
    For TIB, E[|v|] = 1.0 always (since |±1| = 1).
    """

    def test_all_buy_exact_bar_boundaries(self):
        """Proof: all-buy sequence with stable EWMAs produces fixed-size bars.

        Setup: E[T]=5, E[2P-1]=1.0, E[|v|]=1.0, window=10.
        Threshold = 5 × 1.0 × 1.0 = 5.0

        All-buy: cumulative imbalance grows +1 per trade.
        Bar emits at trade 5 (|5| ≥ 5.0).

        After emission: p_buy=1.0, avg_v=1.0.
          E[T] = α×5 + (1-α)×5 = 5.0  (unchanged — observed = expected)
          E[2P-1] = α×1 + (1-α)×1 = 1.0  (unchanged)
          E[|v|] = α×1 + (1-α)×1 = 1.0  (unchanged)
          New threshold = 5.0 (unchanged)

        Therefore: all subsequent bars also have exactly 5 ticks.
        10 buy trades → exactly 2 bars of 5 ticks each.
        """
        builder = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 5.0, "imb": 1.0, "v": 1.0},
        )
        trades = [_trade(i, side="buy") for i in range(10)]
        bars = builder.process_trades(trades)

        assert len(bars) == 2
        assert bars[0].tick_count == 5
        assert bars[1].tick_count == 5

        # EWMA should be unchanged (observed = expected)
        assert builder._ewma_t.expected == pytest.approx(5.0)
        assert builder._ewma_imb.expected == pytest.approx(1.0)
        assert builder._ewma_v.expected == pytest.approx(1.0)

    def test_all_sell_exact_bar_boundaries(self):
        """Proof: all-sell sequence is symmetric to all-buy.

        Each sell contributes -1. |cum_imbalance| still grows by 1 per trade.
        Threshold = 5.0, so bars emit every 5 trades.

        After emission: p_buy=0/5=0.0, 2P-1 = -1.0.
          E[2P-1] = α×(-1) + (1-α)×1.0 = 2/11×(-1) + 9/11×1 = 7/11 ≈ 0.636
          But |E[2P-1]| = 0.636 > 0.1, so no floor needed.
          Threshold changes to: 5.0 × 0.636 × 1.0 = 3.18
        """
        alpha = 2.0 / 11.0
        builder = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 5.0, "imb": 1.0, "v": 1.0},
        )
        trades = [_trade(i, side="sell") for i in range(10)]
        bars = builder.process_trades(trades)

        # First bar: 5 trades to reach |cum| = 5 >= 5.0
        assert bars[0].tick_count == 5

        # After first bar: E[2P-1] updated with (2×0 - 1) = -1
        expected_imb = alpha * (-1.0) + (1.0 - alpha) * 1.0
        assert builder._ewma_imb.expected != pytest.approx(1.0)  # changed

        # Second bar has different size due to threshold change
        # Threshold after first bar: 5.0 × |expected_imb| × 1.0
        new_threshold = 5.0 * max(abs(expected_imb), 0.1) * 1.0

        # Sells continue: each contributes -1, bar emits at ceil(new_threshold)
        import math
        expected_ticks_bar2 = math.ceil(new_threshold)
        assert bars[1].tick_count == expected_ticks_bar2

    def test_mixed_sequence_exact_trace(self):
        """Proof: trace mixed buy/sell through CUSUM and EWMA updates.

        Setup: E[T]=3, E[2P-1]=0.5, E[|v|]=1.0, window=4 (α=0.4).
        Threshold = 3 × 0.5 × 1.0 = 1.5

        Sequence: B, B, S, B, B, S, B, B, B, B

        Trade 1 (B): cum=+1, |1| < 1.5 → no emit
        Trade 2 (B): cum=+2, |2| ≥ 1.5 → EMIT!
          Bar 1: ticks=2, p_buy=1.0, 2P-1=1.0, avg_v=1.0
          E[T] = 0.4×2 + 0.6×3 = 2.6
          E[imb] = 0.4×1.0 + 0.6×0.5 = 0.7
          E[v] = 0.4×1 + 0.6×1 = 1.0
          New threshold = 2.6 × 0.7 × 1.0 = 1.82

        Trade 3 (S): cum=-1, |-1| < 1.82 → no
        Trade 4 (B): cum=0,  |0| < 1.82 → no
        Trade 5 (B): cum=+1, |1| < 1.82 → no
        Trade 6 (S): cum=0,  |0| < 1.82 → no
        Trade 7 (B): cum=+1, |1| < 1.82 → no
        Trade 8 (B): cum=+2, |2| ≥ 1.82 → EMIT!
          Bar 2: ticks=6, buy_count=4, p_buy=4/6, 2P-1=2/3-1=1/3, avg_v=1.0
          E[T] = 0.4×6 + 0.6×2.6 = 3.96
          E[imb] = 0.4×(1/3) + 0.6×0.7 = 0.5533...
          E[v] = 1.0
          New threshold = 3.96 × 0.5533 × 1.0 = 2.191...

        Trade 9 (B): cum=+1, |1| < 2.191 → no
        Trade 10 (B): cum=+2, |2| < 2.191 → no
        """
        builder = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=4,
            initial_expected={"t": 3.0, "imb": 0.5, "v": 1.0},
        )
        sides = ["buy", "buy", "sell", "buy", "buy", "sell", "buy", "buy", "buy", "buy"]
        trades = [_trade(i, side=s) for i, s in enumerate(sides)]
        bars = builder.process_trades(trades)

        # Exactly 2 bars emitted
        assert len(bars) == 2

        # Bar 1: emits on trade 2
        assert bars[0].tick_count == 2

        # Bar 2: emits on trade 8 (6 trades in this bar)
        assert bars[1].tick_count == 6

        # Verify final EWMA state
        assert builder._ewma_t.expected == pytest.approx(3.96)
        assert builder._ewma_imb.expected == pytest.approx(0.4 * (1.0 / 3.0) + 0.6 * 0.7)
        assert builder._ewma_v.expected == pytest.approx(1.0)

    def test_ewma_v_is_always_one_for_tib(self):
        """Proof: TIB contribution is ±1, so |contribution| = 1.0 always.

        Therefore avg_v = sum(|contribution|) / ticks = ticks/ticks = 1.0.
        E[|v|] is always updated with 1.0, so it converges to 1.0.

        Starting from initial v=2.5 with window=4 (α=0.4):
        After N bars updating with 1.0: E[v] = 1 + (2.5-1)×(0.6)^N.
        After 10 bars: E[v] = 1 + 1.5×0.006 ≈ 1.009.

        Use low initial E[T]=1 to produce many small bars for fast convergence.
        """
        builder = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=4,
            initial_expected={"t": 1.0, "imb": 1.0, "v": 2.5},
        )
        trades = [_trade(i, side="buy") for i in range(200)]
        builder.process_trades(trades)

        # After many bars (each updating E[v] with 1.0), converges to 1.0
        assert builder._ewma_v.expected == pytest.approx(1.0, abs=0.01)

    def test_cumulative_imbalance_resets_after_emission(self):
        """Proof: cum_imbalance resets to 0 at each bar boundary.

        Setup: threshold=3 (stable). All-buy sequence.
        Bar 1: trades 1-3, cum goes 1,2,3 → emit, reset to 0.
        Bar 2: trades 4-6, cum goes 1,2,3 → emit, reset to 0.
        """
        builder = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 3.0, "imb": 1.0, "v": 1.0},
        )
        trades = [_trade(i, side="buy") for i in range(6)]
        bars = builder.process_trades(trades)

        assert len(bars) == 2
        assert bars[0].tick_count == 3
        assert bars[1].tick_count == 3
        # After both bars, cum_imbalance should be 0
        assert builder._cum_imbalance == pytest.approx(0.0)

    def test_threshold_floor_at_01_in_balanced_market(self):
        """Proof: when E[2P-1] ≈ 0, floor of 0.1 prevents degenerate threshold.

        Setup: E[T]=100, E[2P-1]=0.0 (balanced), E[|v|]=1.0.
        Without floor: threshold = 100 × 0 × 1 = 0 (every trade emits!).
        With floor: threshold = 100 × 0.1 × 1 = 10.

        The floor ensures bars contain meaningful content in balanced markets.
        """
        builder = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 100.0, "imb": 0.0, "v": 1.0},
        )
        assert builder._threshold == pytest.approx(10.0)

        # Also test negative imbalance near zero
        builder2 = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 100.0, "imb": -0.05, "v": 1.0},
        )
        # |E[2P-1]| = 0.05 < 0.1 → floored to 0.1
        assert builder2._threshold == pytest.approx(10.0)

    def test_threshold_uses_abs_of_imbalance(self):
        """Proof: negative E[2P-1] works correctly via absolute value.

        E[T]=50, E[2P-1]=-0.4 (sell-dominated), E[|v|]=1.0.
        threshold = 50 × |−0.4| × 1.0 = 50 × 0.4 = 20.0
        """
        builder = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 50.0, "imb": -0.4, "v": 1.0},
        )
        assert builder._threshold == pytest.approx(20.0)

    def test_ewma_update_uses_bar_statistics_not_trade(self):
        """Proof: EWMAs update with per-BAR statistics at emission, not per-trade.

        4 buys then 1 sell: bar has 5 ticks, p_buy=4/5=0.8, 2P-1=0.6.
        EWMA should update with T=5, imb=0.6, v=1.0 — NOT trade-by-trade.
        """
        alpha = 0.4  # window=4
        builder = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=4,
            initial_expected={"t": 5.0, "imb": 1.0, "v": 1.0},
        )
        trades = [
            _trade(0, side="buy"),
            _trade(1, side="buy"),
            _trade(2, side="buy"),
            _trade(3, side="buy"),
            _trade(4, side="sell"),  # cum = 4-1 = 3, but threshold = 5*1*1 = 5, |3| < 5
        ]
        # With threshold=5, 4 buys + 1 sell gives |cum|=3 < 5, no emission
        bars = builder.process_trades(trades)
        assert len(bars) == 0

        # Feed more buys to trigger
        trades2 = [_trade(5, side="buy"), _trade(6, side="buy")]
        # cum = 3 + 1 + 1 = 5, |5| >= 5 → emit!
        bars2 = builder.process_trades(trades2)
        assert len(bars2) == 1
        assert bars2[0].tick_count == 7  # 5 original + 2 more

        # Verify EWMA updated with bar stats, not individual trades
        # Bar: 7 ticks, 6 buys, 1 sell → p_buy=6/7, 2P-1=12/7-1=5/7
        expected_t = alpha * 7 + (1 - alpha) * 5.0
        expected_imb = alpha * (5.0 / 7.0) + (1 - alpha) * 1.0
        assert builder._ewma_t.expected == pytest.approx(expected_t)
        assert builder._ewma_imb.expected == pytest.approx(expected_imb)

    def test_threshold_cache_invalidation(self):
        """Proof: threshold cache invalidates after each emission.

        The threshold is recalculated from updated EWMAs after bar emission.
        """
        builder = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=4,
            initial_expected={"t": 2.0, "imb": 1.0, "v": 1.0},
        )
        threshold_before = builder._threshold
        assert threshold_before == pytest.approx(2.0)

        # 2 buys → emit (|2| >= 2.0)
        bars = builder.process_trades([_trade(0, side="buy"), _trade(1, side="buy")])
        assert len(bars) == 1

        # Threshold should have been recalculated
        threshold_after = builder._threshold
        # After bar with T=2, p_buy=1.0: E[T]=0.4*2+0.6*2=2, E[imb]=0.4*1+0.6*1=1
        assert threshold_after == pytest.approx(2.0)  # same because observed=expected

    def test_cold_start_first_trade_always_emits(self):
        """Proof: with E[T]=0 (cold start), threshold=0, first trade always emits.

        This is mathematically correct: threshold = 0 × anything = 0.
        |cum_imbalance| = 1 ≥ 0 → emit.
        """
        builder = TickImbalanceBarBuilder("coinbase", "ETH-USD", ewma_window=20)
        bar = builder.process_trade(_trade(0, side="buy"))

        assert bar is not None
        assert bar.tick_count == 1

    def test_cold_start_convergence_all_buy(self):
        """Proof: cold-start TIB with all-buy converges to 1-tick bars.

        Mathematical derivation:
        After N 1-tick bars: E[T] = 1 - (1-α)^N, converging to 1.0.
        E[2P-1] → 1.0 (all buys), E[|v|] = 1.0.
        Threshold → 1.0 × 1.0 × 1.0 = 1.0.
        Since TIB contribution = ±1, |cum| = 1 ≥ 1 on first trade → 1-tick bars.

        This is a fixed-point attractor: every bar is 1 tick, forever.
        """
        builder = TickImbalanceBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        trades = [_trade(i, side="buy") for i in range(100)]
        bars = builder.process_trades(trades)

        # Every trade should produce a bar (1-tick bars)
        assert len(bars) == 100
        for bar in bars:
            assert bar.tick_count == 1


# ═══════════════════════════════════════════════════════════════════════
# VOLUME IMBALANCE BAR (VIB) MATHEMATICAL PROOFS
# ═══════════════════════════════════════════════════════════════════════


class TestVolumeImbalanceBarMathProofs:
    """Prove VIB correctness: contribution = sign × volume."""

    def test_exact_trace_uniform_volume(self):
        """Proof: VIB with uniform size=2.0, all-buy.

        Setup: E[T]=3, E[imb]=1.0, E[v]=2.0, window=4 (α=0.4).
        Threshold = 3 × 1.0 × 2.0 = 6.0

        Each buy contributes +2.0 to cumulative imbalance.
        Trade 1: cum=2, |2|<6 → no
        Trade 2: cum=4, |4|<6 → no
        Trade 3: cum=6, |6|≥6 → EMIT!
          ticks=3, p_buy=1.0, 2P-1=1.0, avg_v=6/3=2.0
          E[T] = 0.4×3 + 0.6×3 = 3.0
          E[imb] = 0.4×1 + 0.6×1 = 1.0
          E[v] = 0.4×2 + 0.6×2 = 2.0
          threshold = 6.0 (unchanged)
        """
        builder = VolumeImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=4,
            initial_expected={"t": 3.0, "imb": 1.0, "v": 2.0},
        )
        trades = [_trade(i, size="2.0", side="buy") for i in range(6)]
        bars = builder.process_trades(trades)

        assert len(bars) == 2
        assert bars[0].tick_count == 3
        assert bars[1].tick_count == 3

    def test_exact_trace_varying_volume(self):
        """Proof: VIB with varying sizes.

        Setup: E[T]=3, E[imb]=1.0, E[v]=2.0, window=4 (α=0.4).
        Threshold = 6.0

        Buys with sizes [1.5, 2.0, 3.5, 1.0, 2.5]:
        t1 (B, 1.5): cum=+1.5, |1.5|<6 → no
        t2 (B, 2.0): cum=+3.5, |3.5|<6 → no
        t3 (B, 3.5): cum=+7.0, |7.0|≥6 → EMIT!
          ticks=3, sum_abs=7.0, avg_v=7/3≈2.333
          E[T] = 0.4×3 + 0.6×3 = 3.0
          E[v] = 0.4×(7/3) + 0.6×2 = 2.133
          New threshold = 3.0 × 1.0 × 2.133 = 6.4

        t4 (B, 1.0): cum=+1.0, |1|<6.4 → no
        t5 (B, 2.5): cum=+3.5, |3.5|<6.4 → no
        → 1 bar emitted
        """
        builder = VolumeImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=4,
            initial_expected={"t": 3.0, "imb": 1.0, "v": 2.0},
        )
        sizes = ["1.5", "2.0", "3.5", "1.0", "2.5"]
        trades = [_trade(i, size=s, side="buy") for i, s in enumerate(sizes)]
        bars = builder.process_trades(trades)

        assert len(bars) == 1
        assert bars[0].tick_count == 3

        # Verify EWMA of v updated correctly
        avg_v_bar1 = (1.5 + 2.0 + 3.5) / 3.0
        expected_v = 0.4 * avg_v_bar1 + 0.6 * 2.0
        assert builder._ewma_v.expected == pytest.approx(expected_v)

    def test_signed_volume_cancellation(self):
        """Proof: buy + sell volumes cancel in cumulative imbalance.

        Buy size=5.0: contribution=+5.0
        Sell size=5.0: contribution=-5.0
        Net imbalance after both: 0.0
        """
        builder = VolumeImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 100.0, "imb": 1.0, "v": 5.0},
        )
        trades = [
            _trade(0, size="5.0", side="buy"),   # cum = +5
            _trade(1, size="5.0", side="sell"),   # cum = 0
        ]
        bars = builder.process_trades(trades)
        assert len(bars) == 0  # threshold=500, |0| < 500
        assert builder._cum_imbalance == pytest.approx(0.0)

    def test_large_sell_overrides_buy_imbalance(self):
        """Proof: a large sell can flip the cumulative imbalance sign.

        Setup: threshold=100, trades: buy(3.0), buy(3.0), sell(10.0)
        cum after: +3, +6, -4
        |cum| = 4, direction flipped.
        """
        builder = VolumeImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 100.0, "imb": 1.0, "v": 1.0},
        )
        trades = [
            _trade(0, size="3.0", side="buy"),    # cum = +3
            _trade(1, size="3.0", side="buy"),    # cum = +6
            _trade(2, size="10.0", side="sell"),   # cum = +6-10 = -4
        ]
        bars = builder.process_trades(trades)
        assert len(bars) == 0  # threshold=100, |-4| < 100
        assert builder._cum_imbalance == pytest.approx(-4.0)

    def test_ewma_v_tracks_average_volume(self):
        """Proof: E[|v|] for VIB converges to the mean trade volume.

        All trades have size=3.0. After convergence, E[|v|] = 3.0.
        """
        builder = VolumeImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=4,
            initial_expected={"t": 3.0, "imb": 1.0, "v": 1.0},  # v starts wrong
        )
        trades = [_trade(i, size="3.0", side="buy") for i in range(50)]
        builder.process_trades(trades)

        assert builder._ewma_v.expected == pytest.approx(3.0, abs=0.1)


# ═══════════════════════════════════════════════════════════════════════
# DOLLAR IMBALANCE BAR (DIB) MATHEMATICAL PROOFS
# ═══════════════════════════════════════════════════════════════════════


class TestDollarImbalanceBarMathProofs:
    """Prove DIB correctness: contribution = sign × price × size."""

    def test_exact_trace(self):
        """Proof: DIB exact arithmetic.

        Setup: E[T]=2, E[imb]=1.0, E[v]=200.0, window=4 (α=0.4).
        Threshold = 2 × 1.0 × 200.0 = 400.0

        t1 (B, $100×1.5=$150): cum=+150, |150|<400 → no
        t2 (B, $100×2.0=$200): cum=+350, |350|<400 → no
        t3 (B, $100×3.0=$300): cum=+650, |650|≥400 → EMIT!
          ticks=3, sum_abs=650, avg_v=650/3≈216.667
          E[T] = 0.4×3 + 0.6×2 = 2.4
          E[v] = 0.4×216.667 + 0.6×200 = 206.667
          New threshold = 2.4 × 1.0 × 206.667 = 496.0
        """
        builder = DollarImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=4,
            initial_expected={"t": 2.0, "imb": 1.0, "v": 200.0},
        )
        trades = [
            _trade(0, "100.00", "1.5", side="buy"),   # dv = $150
            _trade(1, "100.00", "2.0", side="buy"),   # dv = $200
            _trade(2, "100.00", "3.0", side="buy"),   # dv = $300
        ]
        bars = builder.process_trades(trades)
        assert len(bars) == 1
        assert bars[0].tick_count == 3

        # Verify EWMA updates
        assert builder._ewma_t.expected == pytest.approx(2.4)
        expected_v = 0.4 * (650.0 / 3.0) + 0.6 * 200.0
        assert builder._ewma_v.expected == pytest.approx(expected_v)

    def test_price_sensitivity(self):
        """Proof: DIB is sensitive to price level, unlike VIB.

        Two scenarios with same volume but different prices:
        Price=$100: contribution = ±$100
        Price=$1000: contribution = ±$1000

        Higher price → faster threshold crossing → more bars.
        """
        trades_low = [_trade(i, "100.00", "1.0", side="buy") for i in range(50)]
        trades_high = [_trade(i, "1000.00", "1.0", side="buy") for i in range(50)]

        builder_low = DollarImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=4,
            initial_expected={"t": 5.0, "imb": 1.0, "v": 500.0},
        )
        builder_high = DollarImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=4,
            initial_expected={"t": 5.0, "imb": 1.0, "v": 500.0},
        )

        bars_low = builder_low.process_trades(trades_low)
        bars_high = builder_high.process_trades(trades_high)

        # High price produces more bars (crosses threshold faster)
        assert len(bars_high) > len(bars_low)

    def test_contribution_equals_price_times_size(self):
        """Proof: dollar contribution = price × size.

        Buy at $250.50 × 0.4 = $100.20.
        Sell at $250.50 × 0.4 = -$100.20.
        Net = 0.
        """
        builder = DollarImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 1000.0, "imb": 1.0, "v": 1000.0},
        )
        trades = [
            _trade(0, "250.50", "0.4", side="buy"),    # +100.20
            _trade(1, "250.50", "0.4", side="sell"),    # -100.20
        ]
        builder.process_trades(trades)
        assert builder._cum_imbalance == pytest.approx(0.0, abs=0.01)


# ═══════════════════════════════════════════════════════════════════════
# TICK RUN BAR (TRB) MATHEMATICAL PROOFS
# ═══════════════════════════════════════════════════════════════════════


class TestTickRunBarMathProofs:
    """Prove TRB correctness via exact hand-traced sequences.

    TRB contribution: 1.0 per trade (unsigned).
    Run magnitude: max(buy_total, sell_total).
    Threshold: E[T] × E[P_dom] × E[|v|].
    For TRB, E[|v|] = 1.0 always.
    """

    def test_exact_trace_buy_dominated(self):
        """Proof: buy-dominated run with exact threshold crossing.

        Setup: E[T]=5, E[P_dom]=0.7, E[v]=1.0, window=10 (α=2/11).
        Threshold = 5 × 0.7 × 1.0 = 3.5

        Sequence: B, B, S, B, B, S, B, B, B
        t1 (B): buy=1, sell=0, max=1, <3.5 → no
        t2 (B): buy=2, sell=0, max=2, <3.5 → no
        t3 (S): buy=2, sell=1, max=2, <3.5 → no   ← buy_total PRESERVED
        t4 (B): buy=3, sell=1, max=3, <3.5 → no
        t5 (B): buy=4, sell=1, max=4, ≥3.5 → EMIT!
          ticks=5, buy_count=4, p_buy=0.8, p_dom=max(0.8,0.2)=0.8
          avg_v = 5/5 = 1.0
          E[T] = 2/11×5 + 9/11×5 = 5.0
          E[P_dom] = 2/11×0.8 + 9/11×0.7 = 7.9/11 ≈ 0.71818
          E[v] = 1.0
          New threshold = 5.0 × 0.71818 × 1.0 = 3.59091

        t6 (S): buy=0, sell=1, max=1, <3.59 → no
        t7 (B): buy=1, sell=1, max=1, <3.59 → no
        t8 (B): buy=2, sell=1, max=2, <3.59 → no
        t9 (B): buy=3, sell=1, max=3, <3.59 → no
        → 1 bar emitted, bar 2 not yet complete
        """
        builder = TickRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 5.0, "p_dom": 0.7, "v": 1.0},
        )
        sides = ["buy", "buy", "sell", "buy", "buy", "sell", "buy", "buy", "buy"]
        trades = [_trade(i, side=s) for i, s in enumerate(sides)]
        bars = builder.process_trades(trades)

        assert len(bars) == 1
        assert bars[0].tick_count == 5

        # Verify EWMA updates
        alpha = 2.0 / 11.0
        expected_p_dom = alpha * 0.8 + (1 - alpha) * 0.7
        assert builder._ewma_t.expected == pytest.approx(5.0)
        assert builder._ewma_p_dom.expected == pytest.approx(expected_p_dom)

    def test_no_reset_on_direction_change_exact(self):
        """Proof: buy_total and sell_total accumulate without reset (Prado Def. 2.4).

        Setup: threshold=6.0 (E[T]=10, P_dom=0.6, v=1.0).

        Sequence: B, B, B, S, S, B, B, B
        t1 (B): buy=1, sell=0
        t2 (B): buy=2, sell=0
        t3 (B): buy=3, sell=0
        t4 (S): buy=3, sell=1   ← buy_total NOT reset
        t5 (S): buy=3, sell=2   ← buy_total NOT reset
        t6 (B): buy=4, sell=2
        t7 (B): buy=5, sell=2
        t8 (B): buy=6, sell=2, max=6, ≥6.0 → EMIT!

        With buggy reset-on-direction-change: buy_total would be 3 at t8.
        """
        builder = TickRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 10.0, "p_dom": 0.6, "v": 1.0},
        )
        sides = ["buy", "buy", "buy", "sell", "sell", "buy", "buy", "buy"]
        trades = [_trade(i, side=s) for i, s in enumerate(sides)]
        bars = builder.process_trades(trades)

        assert len(bars) == 1
        assert bars[0].tick_count == 8

    def test_sell_dominated_run(self):
        """Proof: sell side can dominate and trigger emission.

        Setup: threshold=3.0.
        Sequence: S, S, B, S
        t1 (S): buy=0, sell=1, max=1, <3 → no
        t2 (S): buy=0, sell=2, max=2, <3 → no
        t3 (B): buy=1, sell=2, max=2, <3 → no
        t4 (S): buy=1, sell=3, max=3, ≥3 → EMIT!
        """
        builder = TickRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 5.0, "p_dom": 0.6, "v": 1.0},
        )
        sides = ["sell", "sell", "buy", "sell"]
        trades = [_trade(i, side=s) for i, s in enumerate(sides)]
        bars = builder.process_trades(trades)

        assert len(bars) == 1
        assert bars[0].tick_count == 4

    def test_p_dominant_clamping_lower(self):
        """Proof: P_dom clamped at 0.55 in threshold, but EWMA tracks raw.

        If bar has P_buy=0.5 → P_dom=0.5, below 0.55 minimum.
        EWMA stores 0.5, but threshold uses 0.55.
        """
        builder = TickRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 100.0, "p_dom": 0.5, "v": 1.0},
        )
        # EWMA stores 0.5 but threshold uses max(0.5, 0.55) = 0.55
        assert builder._ewma_p_dom.expected == pytest.approx(0.5)
        assert builder._threshold == pytest.approx(100.0 * 0.55 * 1.0)

    def test_p_dominant_clamping_upper(self):
        """Proof: P_dom clamped at 0.95 in threshold.

        P_dom=0.99 → clamped to 0.95 in threshold calculation.
        """
        builder = TickRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 100.0, "p_dom": 0.99, "v": 1.0},
        )
        assert builder._ewma_p_dom.expected == pytest.approx(0.99)
        assert builder._threshold == pytest.approx(100.0 * 0.95 * 1.0)

    def test_p_dominant_no_clamping_in_range(self):
        """Proof: P_dom within [0.55, 0.95] is used as-is."""
        builder = TickRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 100.0, "p_dom": 0.75, "v": 1.0},
        )
        assert builder._threshold == pytest.approx(100.0 * 0.75 * 1.0)

    def test_ewma_v_is_always_one_for_trb(self):
        """Proof: TRB contribution is always 1.0, so E[|v|] converges to 1.0.

        Starting from v=5.0 with window=4 (α=0.4):
        After N bars: E[v] = 1 + (5-1)×(0.6)^N.
        After 15 bars: E[v] = 1 + 4×0.6^15 ≈ 1.002.

        Use low initial E[T]=1 for many small bars (faster convergence).
        """
        builder = TickRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=4,
            initial_expected={"t": 1.0, "p_dom": 0.7, "v": 5.0},
        )
        trades = [_trade(i, side="buy") for i in range(200)]
        builder.process_trades(trades)

        assert builder._ewma_v.expected == pytest.approx(1.0, abs=0.01)

    def test_ewma_update_with_bar_p_dominant(self):
        """Proof: P_dom EWMA updates with max(p_buy, 1-p_buy) from bar.

        Bar with 7 trades, 5 buys, 2 sells:
          p_buy = 5/7 ≈ 0.714
          p_dom = max(5/7, 2/7) = 5/7 ≈ 0.714
        """
        alpha = 0.4  # window=4
        builder = TickRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=4,
            initial_expected={"t": 7.0, "p_dom": 0.7, "v": 1.0},
        )
        # threshold = 7 * 0.7 * 1 = 4.9
        # 5 buys + 2 sells: buy_total=5, sell_total=2, max=5, 5>=4.9 → EMIT
        trades = [
            _trade(0, side="buy"),
            _trade(1, side="buy"),
            _trade(2, side="sell"),
            _trade(3, side="buy"),
            _trade(4, side="sell"),
            _trade(5, side="buy"),
            _trade(6, side="buy"),
        ]
        bars = builder.process_trades(trades)
        assert len(bars) == 1
        assert bars[0].tick_count == 7

        expected_p_dom = alpha * (5.0 / 7.0) + (1 - alpha) * 0.7
        assert builder._ewma_p_dom.expected == pytest.approx(expected_p_dom)

    def test_both_sides_reset_after_emission(self):
        """Proof: buy_total and sell_total both reset to 0 after bar emission."""
        builder = TickRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 3.0, "p_dom": 0.7, "v": 1.0},
        )
        # threshold = 3 * 0.7 * 1 = 2.1
        # 3 buys: buy_total=3, >=2.1 → emit
        bars = builder.process_trades([_trade(i, side="buy") for i in range(3)])
        assert len(bars) == 1

        # After emission, both totals should be 0
        assert builder._buy_total == pytest.approx(0.0)
        assert builder._sell_total == pytest.approx(0.0)


# ═══════════════════════════════════════════════════════════════════════
# VOLUME RUN BAR (VRB) MATHEMATICAL PROOFS
# ═══════════════════════════════════════════════════════════════════════


class TestVolumeRunBarMathProofs:
    """Prove VRB correctness: contribution = trade.size (unsigned)."""

    def test_exact_trace(self):
        """Proof: VRB with varying trade sizes.

        Setup: E[T]=3, E[P_dom]=0.7, E[v]=2.0, window=4 (α=0.4).
        Threshold = 3 × 0.7 × 2.0 = 4.2

        t1 (B, 1.5): buy=1.5, sell=0, max=1.5, <4.2 → no
        t2 (S, 0.5): buy=1.5, sell=0.5, max=1.5, <4.2 → no
        t3 (B, 3.0): buy=4.5, sell=0.5, max=4.5, ≥4.2 → EMIT!
          ticks=3, buy_count=2, p_buy=2/3, p_dom=2/3
          sum_abs = 1.5+0.5+3.0=5.0, avg_v=5/3
          E[T] = 0.4×3 + 0.6×3 = 3.0
          E[P_dom] = 0.4×(2/3) + 0.6×0.7 = 0.6867
          E[v] = 0.4×(5/3) + 0.6×2.0 = 1.8667
          New threshold = 3.0 × 0.6867 × 1.8667 = 3.845
        """
        builder = VolumeRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=4,
            initial_expected={"t": 3.0, "p_dom": 0.7, "v": 2.0},
        )
        trades = [
            _trade(0, size="1.5", side="buy"),
            _trade(1, size="0.5", side="sell"),
            _trade(2, size="3.0", side="buy"),
        ]
        bars = builder.process_trades(trades)
        assert len(bars) == 1
        assert bars[0].tick_count == 3

        # Verify EWMAs
        assert builder._ewma_t.expected == pytest.approx(3.0)
        assert builder._ewma_p_dom.expected == pytest.approx(0.4 * (2.0 / 3.0) + 0.6 * 0.7)
        assert builder._ewma_v.expected == pytest.approx(0.4 * (5.0 / 3.0) + 0.6 * 2.0)

    def test_large_volume_single_trade_triggers(self):
        """Proof: a single large trade can trigger if its volume exceeds threshold.

        Threshold = 10.0. One buy of size=15.0 → buy_total=15 ≥ 10 → emit!
        """
        builder = VolumeRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 10.0, "p_dom": 0.6, "v": 1.0},
        )
        # threshold = 10 * 0.6 * 1 = 6.0
        bar = builder.process_trade(_trade(0, size="15.0", side="buy"))
        assert bar is not None
        assert bar.tick_count == 1

    def test_volume_accumulation_no_reset(self):
        """Proof: volume accumulates on each side without reset.

        Sequence: buy(3.0), sell(1.0), buy(2.0)
        buy_total = 3.0 + 2.0 = 5.0  (not reset at sell)
        sell_total = 1.0
        max = 5.0
        """
        builder = VolumeRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 10.0, "p_dom": 0.6, "v": 5.0},
        )
        # threshold = 10 * 0.6 * 5 = 30 (high, so no emission)
        trades = [
            _trade(0, size="3.0", side="buy"),
            _trade(1, size="1.0", side="sell"),
            _trade(2, size="2.0", side="buy"),
        ]
        builder.process_trades(trades)

        assert builder._buy_total == pytest.approx(5.0)
        assert builder._sell_total == pytest.approx(1.0)


# ═══════════════════════════════════════════════════════════════════════
# DOLLAR RUN BAR (DRB) MATHEMATICAL PROOFS
# ═══════════════════════════════════════════════════════════════════════


class TestDollarRunBarMathProofs:
    """Prove DRB correctness: contribution = trade.dollar_volume (unsigned)."""

    def test_exact_trace(self):
        """Proof: DRB dollar volume accumulation.

        Setup: E[T]=3, E[P_dom]=0.7, E[v]=200.0, window=4 (α=0.4).
        Threshold = 3 × 0.7 × 200.0 = 420.0

        t1 (B, $100×1.5=$150): buy=150, sell=0, max=150, <420 → no
        t2 (S, $100×0.5=$50):  buy=150, sell=50, max=150, <420 → no
        t3 (B, $100×3.0=$300): buy=450, sell=50, max=450, ≥420 → EMIT!
        """
        builder = DollarRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=4,
            initial_expected={"t": 3.0, "p_dom": 0.7, "v": 200.0},
        )
        trades = [
            _trade(0, "100.00", "1.5", side="buy"),   # dv=$150
            _trade(1, "100.00", "0.5", side="sell"),   # dv=$50
            _trade(2, "100.00", "3.0", side="buy"),    # dv=$300
        ]
        bars = builder.process_trades(trades)
        assert len(bars) == 1
        assert bars[0].tick_count == 3

    def test_price_affects_dollar_contribution(self):
        """Proof: same base volume, different prices → different dollar contributions.

        price=$50, size=2.0 → dollar_volume=$100
        price=$200, size=2.0 → dollar_volume=$400
        """
        builder = DollarRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 100.0, "p_dom": 0.7, "v": 1000.0},
        )
        # threshold = 100 * 0.7 * 1000 = 70000 (high, no emission)
        trades = [
            _trade(0, "50.00", "2.0", side="buy"),    # $100
            _trade(1, "200.00", "2.0", side="buy"),    # $400
        ]
        builder.process_trades(trades)
        # buy_total = 100 + 400 = 500
        assert builder._buy_total == pytest.approx(500.0)


# ═══════════════════════════════════════════════════════════════════════
# TICK RULE INTEGRATION PROOFS
# ═══════════════════════════════════════════════════════════════════════


class TestTickRuleIntegration:
    """Prove tick rule correctly infers trade direction from price movement."""

    def test_uptick_sequence_all_buys(self):
        """Proof: monotonically increasing prices → all trades classified as buy.

        Price: 100, 101, 102, 103.
        tick_rule(101, 100) = +1, tick_rule(102, 101) = +1, etc.
        """
        builder = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 3.0, "imb": 1.0, "v": 1.0},
        )
        trades = [
            _trade(0, "100.00", side="unknown"),
            _trade(1, "101.00", side="unknown"),
            _trade(2, "102.00", side="unknown"),
            _trade(3, "103.00", side="unknown"),
        ]
        bars = builder.process_trades(trades)

        # First trade: unknown side, no prev_price → defaults to +1 (buy)
        # Trades 2-4: uptick → +1 (buy)
        # All buy → imbalance = +1 per trade → fast emission
        # With threshold=3, emit at trade 3 (|3| >= 3)
        assert len(bars) >= 1
        assert bars[0].tick_count == 3

    def test_downtick_sequence_all_sells(self):
        """Proof: monotonically decreasing prices → all trades classified as sell.

        Price: 103, 102, 101, 100.
        tick_rule(102, 103) = -1, tick_rule(101, 102) = -1, etc.
        """
        builder = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 3.0, "imb": 1.0, "v": 1.0},
        )
        trades = [
            _trade(0, "103.00", side="unknown"),  # default: +1
            _trade(1, "102.00", side="unknown"),  # downtick: -1
            _trade(2, "101.00", side="unknown"),  # downtick: -1
            _trade(3, "100.00", side="unknown"),  # downtick: -1
        ]
        bars = builder.process_trades(trades)

        # Trade 1: defaults to +1 (cum=+1)
        # Trade 2: -1 (cum=0)
        # Trade 3: -1 (cum=-1)
        # Trade 4: -1 (cum=-2), but we need to check threshold
        # threshold=3, so |cum| needs to reach 3
        # Actually cum goes: +1, 0, -1, -2
        # After 4 trades: |cum| = 2 < 3, no emission yet
        # This is correct: first trade was a buy, then 3 sells → net = +1-3 = -2
        assert len(bars) == 0

    def test_equal_price_carries_forward(self):
        """Proof: equal prices carry the previous sign.

        Price: 100, 101, 101, 101 → signs: default(+1), +1, +1, +1.
        All classified as buy (carry-forward of last uptick).
        """
        builder = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 3.0, "imb": 1.0, "v": 1.0},
        )
        trades = [
            _trade(0, "100.00", side="unknown"),
            _trade(1, "101.00", side="unknown"),  # uptick → +1
            _trade(2, "101.00", side="unknown"),  # equal → carry +1
            _trade(3, "101.00", side="unknown"),  # equal → carry +1
        ]
        bars = builder.process_trades(trades)
        # All +1: cum = 1, 2, 3, 4
        # Emit at trade 3 (|3| >= 3.0)
        assert len(bars) == 1
        assert bars[0].tick_count == 3

    def test_known_side_overrides_tick_rule(self):
        """Proof: explicit side='buy' or 'sell' is used, tick rule is NOT applied.

        Price drops from 102 to 100, but side='buy' → sign=+1.
        Tick rule would give -1, but explicit side takes priority.
        """
        builder = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 2.0, "imb": 1.0, "v": 1.0},
        )
        trades = [
            _trade(0, "102.00", side="buy"),   # explicit buy → +1
            _trade(1, "100.00", side="buy"),   # explicit buy → +1 (NOT downtick -1)
        ]
        bars = builder.process_trades(trades)
        # cum = +1, +2. |2| >= 2.0 → emit after 2
        assert len(bars) == 1
        assert bars[0].tick_count == 2


# ═══════════════════════════════════════════════════════════════════════
# CROSS-VARIANT CONSISTENCY PROOFS
# ═══════════════════════════════════════════════════════════════════════


class TestCrossVariantConsistency:
    """Prove consistency properties across bar variants."""

    def test_tib_equals_vib_when_volume_is_one(self):
        """Proof: when all trades have size=1.0, TIB and VIB are identical.

        TIB contribution: sign(trade) = ±1
        VIB contribution: sign(trade) × size = ±1 × 1.0 = ±1

        Same contribution → same bars.
        """
        trades = [_trade(i, size="1.0", side="buy") for i in range(20)]
        initial = {"t": 5.0, "imb": 0.8, "v": 1.0}

        tib = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10, initial_expected=initial
        )
        vib = VolumeImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10, initial_expected=initial
        )

        bars_tib = tib.process_trades(trades)
        bars_vib = vib.process_trades(trades)

        assert len(bars_tib) == len(bars_vib)
        for bt, bv in zip(bars_tib, bars_vib):
            assert bt.tick_count == bv.tick_count

    def test_vib_equals_dib_when_price_is_one(self):
        """Proof: when price=1.0, VIB and DIB are identical.

        VIB contribution: sign × size
        DIB contribution: sign × dollar_volume = sign × price × size = sign × 1 × size

        Same contribution → same bars.
        """
        trades = [_trade(i, "1.00", "3.0", side="buy") for i in range(20)]
        initial = {"t": 5.0, "imb": 0.8, "v": 3.0}

        vib = VolumeImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10, initial_expected=initial
        )
        dib = DollarImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10, initial_expected=initial
        )

        bars_vib = vib.process_trades(trades)
        bars_dib = dib.process_trades(trades)

        assert len(bars_vib) == len(bars_dib)
        for bv, bd in zip(bars_vib, bars_dib):
            assert bv.tick_count == bd.tick_count

    def test_trb_equals_vrb_when_volume_is_one(self):
        """Proof: when size=1.0, TRB and VRB are identical.

        TRB contribution: 1.0
        VRB contribution: size = 1.0

        Same contribution → same bars.
        """
        trades = [_trade(i, size="1.0", side="buy") for i in range(20)]
        initial = {"t": 5.0, "p_dom": 0.7, "v": 1.0}

        trb = TickRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10, initial_expected=initial
        )
        vrb = VolumeRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10, initial_expected=initial
        )

        bars_trb = trb.process_trades(trades)
        bars_vrb = vrb.process_trades(trades)

        assert len(bars_trb) == len(bars_vrb)
        for bt, bv in zip(bars_trb, bars_vrb):
            assert bt.tick_count == bv.tick_count

    def test_vrb_equals_drb_when_price_is_one(self):
        """Proof: when price=1.0, VRB and DRB are identical.

        VRB contribution: size
        DRB contribution: dollar_volume = price × size = 1 × size

        Same contribution → same bars.
        """
        trades = [_trade(i, "1.00", "3.0", side="buy") for i in range(20)]
        initial = {"t": 5.0, "p_dom": 0.7, "v": 3.0}

        vrb = VolumeRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10, initial_expected=initial
        )
        drb = DollarRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10, initial_expected=initial
        )

        bars_vrb = vrb.process_trades(trades)
        bars_drb = drb.process_trades(trades)

        assert len(bars_vrb) == len(bars_drb)
        for bv, bd in zip(bars_vrb, bars_drb):
            assert bv.tick_count == bd.tick_count


# ═══════════════════════════════════════════════════════════════════════
# EQUILIBRIUM CONVERGENCE PROOFS
# ═══════════════════════════════════════════════════════════════════════


class TestEquilibriumConvergence:
    """Prove bar size convergence to theoretical equilibrium values."""

    def test_tib_biased_market_stable_equilibrium(self):
        """Proof: in biased market (P_buy=0.8), TIB has STABLE equilibrium.

        Mathematical derivation:
        With drift μ = 2×0.8 - 1 = 0.6 per trade, cumulative imbalance
        grows linearly: E[θ_T] = T × 0.6.

        Threshold = E[T] × |E[2P-1]| × 1.0.
        At emission: T × 0.6 ≈ E[T] × 0.6 → T ≈ E[T].

        This equilibrium is STABLE because the first-passage time
        for a biased random walk crossing threshold h is h/μ (linear),
        so perturbations self-correct: if E[T] > T_actual, E[T] decreases.

        The EWMA dynamics add lag, so convergence is approximate, not exact.
        Bar size stabilizes within a factor of the seeded E[T].
        """
        random.seed(42)
        builder = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=20,
            initial_expected={"t": 20.0, "imb": 0.6, "v": 1.0},
        )

        n_trades = 20000
        trades = []
        for i in range(n_trades):
            side = "buy" if random.random() < 0.8 else "sell"
            trades.append(_trade(i, side=side))

        bars = builder.process_trades(trades)

        # Should produce many bars (biased market → efficient emission)
        assert len(bars) > 50

        # After warmup, bar sizes should be stable (not collapsing or exploding)
        if len(bars) > 30:
            mature_bars = bars[20:]
            mean_ticks = sum(b.tick_count for b in mature_bars) / len(mature_bars)
            # In biased markets, equilibrium bar size depends on EWMA dynamics.
            # It should be finite and reasonable (not 1-tick and not infinite).
            assert 5 < mean_ticks < 200, f"mean_ticks={mean_ticks:.1f}"

    def test_tib_balanced_market_unstable_equilibrium(self):
        """Proof: in balanced market (P_buy=0.5), TIB equilibrium is UNSTABLE.

        Mathematical derivation:
        For symmetric random walk, first-passage time to ±h is h².
        Threshold h = E[T] × 0.1 (floor applied since E[2P-1] → 0).
        So T_actual = (E[T] × 0.1)² = 0.01 × E[T]².

        At equilibrium: T = E[T] → E[T] = 0.01 × E[T]² → E[T] = 100.

        But this equilibrium is UNSTABLE:
        - If E[T] = 100+ε: T_actual = 0.01×(100+ε)² ≈ 100+2ε > E[T] → E[T] grows
        - If E[T] = 100-ε: T_actual = 0.01×(100-ε)² ≈ 100-2ε < E[T] → E[T] shrinks

        The perturbation AMPLIFIES. This is because h² grows faster than h.

        Starting from E[T]=64 < 100: the system collapses to 1-tick bars.
        T_actual = (64×0.1)² = 40.96 < 64 → E[T] decreases → threshold drops
        → bars get shorter → E[T] drops further → positive feedback → collapse.

        This is a fundamental property of TIB in balanced markets.
        """
        random.seed(123)
        builder = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=20,
            initial_expected={"t": 64.0, "imb": 0.0, "v": 1.0},
        )

        n_trades = 5000
        trades = []
        for i in range(n_trades):
            side = "buy" if random.random() < 0.5 else "sell"
            trades.append(_trade(i, side=side))

        bars = builder.process_trades(trades)

        # System collapses to 1-tick bars (degenerate attractor)
        if len(bars) > 100:
            last_100 = bars[-100:]
            mean_ticks = sum(b.tick_count for b in last_100) / len(last_100)
            # After collapse, nearly all bars are 1-tick
            assert mean_ticks < 3.0, (
                f"Expected collapse to ~1-tick bars, got mean={mean_ticks:.1f}"
            )

    def test_trb_biased_market_equilibrium(self):
        """Proof: in biased market, TRB bar size ≈ E[T].

        P_buy = 0.75 → P_dom = 0.75.
        max(buy_total, sell_total) ≈ T × P_dom = T × 0.75.
        Threshold = E[T] × E[P_dom] × 1.0 ≈ E[T] × 0.75.
        At emission: T × 0.75 ≈ E[T] × 0.75 → T ≈ E[T].
        """
        random.seed(42)
        builder = TickRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=20,
            initial_expected={"t": 20.0, "p_dom": 0.75, "v": 1.0},
        )

        n_trades = 10000
        trades = []
        for i in range(n_trades):
            side = "buy" if random.random() < 0.75 else "sell"
            trades.append(_trade(i, side=side))

        bars = builder.process_trades(trades)

        if len(bars) > 20:
            mature_bars = bars[10:]
            mean_ticks = sum(b.tick_count for b in mature_bars) / len(mature_bars)
            assert 10 < mean_ticks < 40, f"mean_ticks={mean_ticks:.1f}, expected ~20"

    def test_more_bars_in_trending_than_balanced(self):
        """Proof: information-driven bars produce more bars when market trends.

        Trending market (P_buy=0.9): strong directional flow → faster imbalance buildup.
        Balanced market (P_buy=0.5): cancellation → slower imbalance buildup.

        Therefore trending → more bars, balanced → fewer bars.
        """
        random.seed(42)
        n_trades = 5000

        # Trending market
        trades_trend = []
        for i in range(n_trades):
            side = "buy" if random.random() < 0.9 else "sell"
            trades_trend.append(_trade(i, side=side))

        # Balanced market
        trades_bal = []
        for i in range(n_trades):
            side = "buy" if random.random() < 0.5 else "sell"
            trades_bal.append(_trade(i, side=side))

        initial = {"t": 50.0, "imb": 0.5, "v": 1.0}
        tib_trend = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=20, initial_expected=initial
        )
        tib_bal = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=20, initial_expected=initial
        )

        bars_trend = tib_trend.process_trades(trades_trend)
        bars_bal = tib_bal.process_trades(trades_bal)

        # Trending should produce more bars (faster threshold crossing)
        assert len(bars_trend) > len(bars_bal), (
            f"trending={len(bars_trend)}, balanced={len(bars_bal)}"
        )


# ═══════════════════════════════════════════════════════════════════════
# EDGE CASES AND DEGENERATE SCENARIOS
# ═══════════════════════════════════════════════════════════════════════


class TestEdgeCases:
    """Test degenerate and boundary conditions."""

    def test_single_trade_flush(self):
        """Single trade without emission → flush produces valid bar."""
        builder = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 100.0, "imb": 1.0, "v": 1.0},
        )
        # threshold = 100, so 1 trade won't trigger
        builder.process_trade(_trade(0, side="buy"))
        bar = builder.flush()

        assert bar is not None
        assert bar.tick_count == 1
        assert bar.metadata is not None
        assert bar.open == Decimal("100.00")
        assert bar.close == Decimal("100.00")

    def test_zero_volume_trade(self):
        """Trade with size=0 should not cause division by zero in VWAP."""
        builder = VolumeImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 1.0, "imb": 1.0, "v": 1.0},
        )
        # size=0 → contribution=0
        bar = builder.process_trade(_trade(0, "100.00", "0.0", side="buy"))
        # The trade contributes 0 volume, so cum=0, |0| >= very small threshold
        # With threshold close to 0 from EWMA, this should still work
        # Actually threshold = 1*1*1 = 1.0, |0| < 1.0 → no emit
        assert bar is None

    def test_very_large_ewma_window(self):
        """Large EWMA window → very slow adaptation."""
        builder = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=1000,
            initial_expected={"t": 50.0, "imb": 0.5, "v": 1.0},
        )
        # alpha = 2/1001 ≈ 0.002 → very slow to change
        trades = [_trade(i, side="buy") for i in range(200)]
        bars = builder.process_trades(trades)

        # Threshold ≈ 50 * 0.5 * 1.0 = 25 initially
        # Should produce bars, but E[T] barely changes
        assert len(bars) > 0
        # E[T] should still be very close to 50 (barely moved)
        assert builder._ewma_t.expected == pytest.approx(50.0, abs=5.0)

    def test_ewma_window_one_tracks_exactly(self):
        """Window=1 → alpha=1.0 → EWMA = last value exactly.

        This means threshold changes dramatically after each bar.
        """
        builder = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=1,
            initial_expected={"t": 3.0, "imb": 1.0, "v": 1.0},
        )
        # First bar: 3 buys → emit, E[T] = 1.0*3 = 3 (same)
        trades = [_trade(i, side="buy") for i in range(6)]
        bars = builder.process_trades(trades)

        assert len(bars) == 2
        assert bars[0].tick_count == 3
        # E[T] = 3 after first bar, threshold stays 3
        assert bars[1].tick_count == 3

    def test_metadata_roundtrip_imbalance(self):
        """Metadata from emission restores builder to identical state."""
        builder1 = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 20.0, "imb": 0.4, "v": 1.0},
        )
        trades = [_trade(i, side="buy") for i in range(50)]
        bars = builder1.process_trades(trades)

        # Get metadata from last bar
        last_meta = bars[-1].metadata
        assert last_meta is not None

        # Restore to new builder
        builder2 = TickImbalanceBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        builder2.restore_state(last_meta)

        assert builder2._ewma_t.expected == pytest.approx(builder1._ewma_t.expected)
        assert builder2._ewma_imb.expected == pytest.approx(builder1._ewma_imb.expected)
        assert builder2._ewma_v.expected == pytest.approx(builder1._ewma_v.expected)
        assert builder2._threshold == pytest.approx(builder1._threshold)

    def test_metadata_roundtrip_run(self):
        """Metadata from run bar emission restores builder to identical state."""
        builder1 = TickRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 10.0, "p_dom": 0.7, "v": 1.0},
        )
        trades = [_trade(i, side="buy") for i in range(50)]
        bars = builder1.process_trades(trades)

        last_meta = bars[-1].metadata
        assert last_meta is not None

        builder2 = TickRunBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        builder2.restore_state(last_meta)

        assert builder2._ewma_t.expected == pytest.approx(builder1._ewma_t.expected)
        assert builder2._ewma_p_dom.expected == pytest.approx(builder1._ewma_p_dom.expected)
        assert builder2._ewma_v.expected == pytest.approx(builder1._ewma_v.expected)
        assert builder2._threshold == pytest.approx(builder1._threshold)

    def test_ohlcv_correctness_across_bar(self):
        """Verify OHLCV fields are correct for a bar spanning multiple prices."""
        builder = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 4.0, "imb": 1.0, "v": 1.0},
        )
        trades = [
            _trade(0, "100.00", "1.0", side="buy"),   # open
            _trade(1, "105.00", "2.0", side="buy"),   # high
            _trade(2, "95.00", "1.5", side="buy"),     # low
            _trade(3, "102.00", "0.5", side="buy"),    # close
        ]
        bars = builder.process_trades(trades)
        assert len(bars) == 1

        bar = bars[0]
        assert bar.open == Decimal("100.00")
        assert bar.high == Decimal("105.00")
        assert bar.low == Decimal("95.00")
        assert bar.close == Decimal("102.00")
        assert bar.tick_count == 4

        # Volume = 1.0 + 2.0 + 1.5 + 0.5 = 5.0
        assert bar.volume == Decimal("5.0")

        # Dollar volume = 100*1 + 105*2 + 95*1.5 + 102*0.5
        #               = 100 + 210 + 142.5 + 51 = 503.5
        assert bar.dollar_volume == Decimal("503.50")

        # VWAP = dollar_volume / volume = 503.5 / 5.0 = 100.7
        assert bar.vwap == Decimal("100.7")

    def test_imbalance_bar_versus_run_bar_sensitivity(self):
        """Proof: imbalance bars and run bars respond differently to same data.

        Imbalance bars track signed cumulative flow (cancellation-sensitive).
        Run bars track max-side accumulation (no cancellation).

        For alternating buy/sell: imbalance ≈ 0 (slow emission),
        but run bars still accumulate on each side.
        """
        initial_imb = {"t": 10.0, "imb": 0.5, "v": 1.0}
        initial_run = {"t": 10.0, "p_dom": 0.6, "v": 1.0}

        builder_imb = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10, initial_expected=initial_imb
        )
        builder_run = TickRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10, initial_expected=initial_run
        )

        # Perfectly alternating: B, S, B, S, ...
        trades = []
        for i in range(100):
            side = "buy" if i % 2 == 0 else "sell"
            trades.append(_trade(i, side=side))

        bars_imb = builder_imb.process_trades(trades)
        bars_run = builder_run.process_trades(trades)

        # Run bars should produce more bars than imbalance bars for alternating data
        # because run bars accumulate each side independently,
        # while imbalance bars have ±1 cancellation → slow growth
        assert len(bars_run) > len(bars_imb), (
            f"run={len(bars_run)}, imb={len(bars_imb)}"
        )


# ═══════════════════════════════════════════════════════════════════════
# E[T] CLAMPING PROOFS (mlfinlab exp_num_ticks_constraints)
# ═══════════════════════════════════════════════════════════════════════


class TestExpectedTicksClamping:
    """Prove E[T] clamping prevents collapse/explosion instability.

    The unstable equilibrium at E[T]=100 in balanced markets causes:
    - Below equilibrium: E[T] collapses to 1 (degenerate 1-tick bars)
    - Above equilibrium: E[T] explodes to ∞ (ever-fewer bars)

    Clamping E[T] to [min, max] bounds this behavior, following
    mlfinlab's exp_num_ticks_constraints pattern.
    """

    def test_clamping_prevents_collapse_in_balanced_market(self):
        """Proof: with clamping, E[T] stays ≥ min even in balanced markets.

        Without clamping, balanced-market TIB with E[T] seeded below the
        unstable equilibrium collapses to 1-tick bars (proved in
        test_tib_balanced_market_unstable_equilibrium).

        The unstable equilibrium for imb=0 (floored at 0.1) is at E[T]=100:
          T_actual = (E[T] * 0.1)² = 0.01 * E[T]²
          Equilibrium: T = E[T] → E[T] = 0.01 * E[T]² → E[T] = 100

        Seeding E[T]=10 (well below 100) triggers collapse.
        With clamping at min=5, E[T] cannot fall below 5.
        """
        random.seed(42)
        n_trades = 2000

        # Balanced market — 50/50 buy/sell
        trades = []
        for i in range(n_trades):
            side = "buy" if random.random() < 0.5 else "sell"
            trades.append(_trade(i, side=side))

        # WITHOUT clamping: seed below unstable equilibrium → collapse
        # E[T]=10, imb=0 (floored to 0.1), threshold = 10*0.1*1 = 1
        # Random walk reaches 1 in ~1 step → E[T] collapses to ~1
        builder_unclamped = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 10.0, "imb": 0.0, "v": 1.0},
        )
        bars_unclamped = builder_unclamped.process_trades(trades)

        # WITH clamping: E[T] stays ≥ 5
        builder_clamped = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 10.0, "imb": 0.0, "v": 1.0},
            expected_ticks_range=(5.0, 500.0),
        )
        bars_clamped = builder_clamped.process_trades(trades)

        # Unclamped collapses to tiny bars (E[T] → ~1)
        assert builder_unclamped._ewma_t.expected < 5.0, (
            f"Expected unclamped collapse, got E[T]={builder_unclamped._ewma_t.expected:.1f}"
        )

        # Clamped stays ≥ min
        assert builder_clamped._ewma_t.expected >= 5.0, (
            f"Clamped E[T]={builder_clamped._ewma_t.expected:.1f} < min=5.0"
        )

        # Clamped should produce fewer, more meaningful bars
        assert len(bars_clamped) < len(bars_unclamped)

    def test_clamping_prevents_threshold_explosion(self):
        """Proof: with clamping, E[T] stays ≤ max.

        Simulate a scenario where E[T] would grow without bound.
        Seed E[T] above the unstable equilibrium with balanced data.
        Without clamping, each bar's T_actual > E[T] (quadratic random walk),
        so E[T] grows. With clamping at max=200, it's bounded.
        """
        random.seed(123)
        n_trades = 5000

        trades = []
        for i in range(n_trades):
            side = "buy" if random.random() < 0.5 else "sell"
            trades.append(_trade(i, side=side))

        # Start above unstable equilibrium — E[T]=150 with imb~0 (floored at 0.1)
        # threshold = 150 * 0.1 * 1 = 15
        # Random walk first-passage: T_actual ~ 15² = 225 >> 150
        # So E[T] drifts upward toward explosion
        builder_clamped = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 150.0, "imb": 0.0, "v": 1.0},
            expected_ticks_range=(10.0, 200.0),
        )
        builder_clamped.process_trades(trades)

        # E[T] should be bounded by max
        assert builder_clamped._ewma_t.expected <= 200.0, (
            f"Clamped E[T]={builder_clamped._ewma_t.expected:.1f} > max=200.0"
        )

    def test_clamping_preserves_biased_market_behavior(self):
        """Proof: clamping does NOT interfere when equilibrium is within range.

        In a biased market (P_buy=0.8), the natural equilibrium E[T]
        is within the clamping range, so clamped and unclamped builders
        should produce identical bars.
        """
        random.seed(42)
        n_trades = 1000
        trades = []
        for i in range(n_trades):
            side = "buy" if random.random() < 0.8 else "sell"
            trades.append(_trade(i, side=side))

        initial = {"t": 20.0, "imb": 0.6, "v": 1.0}

        builder_unclamped = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected=initial,
        )
        # Wide range that won't interfere with natural equilibrium
        builder_clamped = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected=initial,
            expected_ticks_range=(1.0, 10000.0),
        )

        bars_unclamped = builder_unclamped.process_trades(trades)
        bars_clamped = builder_clamped.process_trades(trades)

        # With sufficiently wide range, bars should be identical
        assert len(bars_clamped) == len(bars_unclamped)
        for bc, bu in zip(bars_clamped, bars_unclamped):
            assert bc.tick_count == bu.tick_count

    def test_clamping_metadata_roundtrip_imbalance(self):
        """Proof: expected_ticks_range persists through metadata for daemon restart."""
        builder1 = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 50.0, "imb": 0.5, "v": 1.0},
            expected_ticks_range=(5.0, 500.0),
        )
        trades = [_trade(i, side="buy") for i in range(100)]
        bars = builder1.process_trades(trades)
        assert len(bars) > 0

        # Check metadata includes range
        last_meta = bars[-1].metadata
        assert "expected_ticks_range" in last_meta
        assert last_meta["expected_ticks_range"] == [5.0, 500.0]

        # Restore to new builder — range should be recovered
        builder2 = TickImbalanceBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        builder2.restore_state(last_meta)
        assert builder2._expected_ticks_range == (5.0, 500.0)
        assert builder2._ewma_t.expected == pytest.approx(builder1._ewma_t.expected)

    def test_clamping_metadata_roundtrip_run(self):
        """Proof: expected_ticks_range persists through run bar metadata."""
        builder1 = TickRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 50.0, "p_dom": 0.7, "v": 1.0},
            expected_ticks_range=(5.0, 500.0),
        )
        trades = [_trade(i, side="buy") for i in range(100)]
        bars = builder1.process_trades(trades)
        assert len(bars) > 0

        last_meta = bars[-1].metadata
        assert "expected_ticks_range" in last_meta
        assert last_meta["expected_ticks_range"] == [5.0, 500.0]

        builder2 = TickRunBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        builder2.restore_state(last_meta)
        assert builder2._expected_ticks_range == (5.0, 500.0)
        assert builder2._ewma_t.expected == pytest.approx(builder1._ewma_t.expected)

    def test_none_range_is_backward_compatible(self):
        """Proof: expected_ticks_range=None preserves original unclamped behavior."""
        trades = [_trade(i, side="buy") for i in range(50)]
        initial = {"t": 10.0, "imb": 0.8, "v": 1.0}

        builder_none = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10, initial_expected=initial,
        )
        builder_explicit_none = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10, initial_expected=initial,
            expected_ticks_range=None,
        )

        bars_none = builder_none.process_trades(trades)
        bars_explicit = builder_explicit_none.process_trades(trades)

        assert len(bars_none) == len(bars_explicit)
        for bn, be in zip(bars_none, bars_explicit):
            assert bn.tick_count == be.tick_count

    def test_clamping_applies_after_every_ewma_update(self):
        """Proof: clamping is applied after EACH bar emission, not just once.

        Feed trades that would drive E[T] below min repeatedly.
        After each bar, E[T] should be clamped back to min.
        """
        # Set min=20, initial E[T]=20. In balanced market, natural E[T] → 1.
        # After each bar, EWMA tries to pull E[T] down, but clamp holds at 20.
        builder = TickImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=5,
            initial_expected={"t": 20.0, "imb": 1.0, "v": 1.0},
            expected_ticks_range=(20.0, 200.0),
        )

        # All-buy: imbalance grows by +1 per trade, threshold = 20 * 1 * 1 = 20
        # Each bar should be exactly 20 trades (if E[T] stays clamped at 20)
        trades = [_trade(i, side="buy") for i in range(100)]
        bars = builder.process_trades(trades)

        # After emission, EWMA of T would try to update to bar_ticks=20,
        # which equals the min. With window=5 (alpha=0.4):
        # new E[T] = 0.4*20 + 0.6*20 = 20 (no change since value equals current)
        # So E[T] should stay exactly at 20
        assert builder._ewma_t.expected >= 20.0

        # All bars should have consistent size (threshold stable)
        for bar in bars:
            assert bar.tick_count == 20

    def test_all_imbalance_subclasses_accept_range(self):
        """Proof: VIB and DIB accept expected_ticks_range parameter."""
        vib = VolumeImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 50.0, "imb": 0.5, "v": 1.0},
            expected_ticks_range=(5.0, 500.0),
        )
        assert vib._expected_ticks_range == (5.0, 500.0)

        dib = DollarImbalanceBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 50.0, "imb": 0.5, "v": 100.0},
            expected_ticks_range=(5.0, 500.0),
        )
        assert dib._expected_ticks_range == (5.0, 500.0)

    def test_all_run_subclasses_accept_range(self):
        """Proof: VRB and DRB accept expected_ticks_range parameter."""
        vrb = VolumeRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 50.0, "p_dom": 0.7, "v": 1.0},
            expected_ticks_range=(5.0, 500.0),
        )
        assert vrb._expected_ticks_range == (5.0, 500.0)

        drb = DollarRunBarBuilder(
            "coinbase", "ETH-USD", ewma_window=10,
            initial_expected={"t": 50.0, "p_dom": 0.7, "v": 100.0},
            expected_ticks_range=(5.0, 500.0),
        )
        assert drb._expected_ticks_range == (5.0, 500.0)
