"""Information-driven run bar builders (Prado AFML Ch. 2, Def. 2.4).

Run bars sample when one side's cumulative contribution dominates the
other within a bar.  Per Prado:

  theta_T = max( sum_{t: b_t=1} v_t,  sum_{t: b_t=-1} v_t )

Buy and sell totals accumulate across the ENTIRE bar without resetting
on direction change.  A bar emits when theta_T exceeds an adaptive
threshold decomposed as: E[T] x E[P_dominant] x E[|v|].

Three variants weight contributions differently:
  - Tick Run (TRB): count of 1 per trade
  - Volume Run (VRB): cumulative volume
  - Dollar Run (DRB): cumulative dollar volume
"""

from __future__ import annotations

from abc import abstractmethod
from decimal import Decimal

from arcana.bars.base import Bar, BarBuilder
from arcana.bars.utils import EWMAEstimator, tick_rule
from arcana.ingestion.models import Trade


class _RunBarBuilder(BarBuilder):
    """Private base for all run bar builders.

    Tracks cumulative buy and sell contributions across the bar.
    Contributions accumulate for the ENTIRE bar without resetting on
    direction change (Prado AFML Def. 2.4).

    A bar emits when max(buy_total, sell_total) >= EWMA threshold,
    decomposed as: E[T] x E[P_dominant] x E[|v|].

    E[T] can be clamped to a [min, max] range to prevent the well-known
    instability in balanced markets where the threshold either collapses
    or explodes.  See mlfinlab's exp_num_ticks_constraints for the same fix.
    """

    _MIN_P_DOMINANT = 0.55
    _MAX_P_DOMINANT = 0.95

    def __init__(
        self,
        source: str,
        pair: str,
        ewma_window: int,
        initial_expected: dict | float | None = None,
        expected_ticks_range: tuple[float, float] | None = None,
    ) -> None:
        super().__init__(source, pair)

        # E[T] clamping range — prevents collapse/explosion feedback loop
        self._expected_ticks_range = expected_ticks_range

        # Decompose initial expected into three EWMA components
        if isinstance(initial_expected, dict):
            init_t = initial_expected.get("t", 0.0)
            init_p = initial_expected.get("p_dom", 0.6)
            init_v = initial_expected.get("v", 1.0)
        elif isinstance(initial_expected, (int, float)) and initial_expected > 0:
            # Legacy single-value: treat as composite threshold.
            # Self-corrects within ~window bars.
            init_t = float(initial_expected)
            init_p = 1.0
            init_v = 1.0
        else:
            init_t = 0.0
            init_p = 0.6
            init_v = 1.0

        self._ewma_t = EWMAEstimator(window=ewma_window, initial_value=init_t)
        self._ewma_p_dom = EWMAEstimator(window=ewma_window, initial_value=init_p)
        self._ewma_v = EWMAEstimator(window=ewma_window, initial_value=init_v)

        self._buy_total: float = 0.0
        self._sell_total: float = 0.0
        self._buy_count: int = 0
        self._sum_abs_contrib: float = 0.0
        self._prev_price: Decimal | None = None
        self._prev_sign: int = 1
        self._cached_threshold: float | None = None

    # -- Threshold --------------------------------------------------------

    @property
    def _threshold(self) -> float:
        """Adaptive threshold: E[T] x E[P_dominant] x E[|v|].

        Clamps P_dominant to [0.55, 0.95] for numerical stability.

        Cached because the EWMAs only change on bar emission; avoids
        recomputing on every trade.
        """
        if self._cached_threshold is None:
            p_dom = min(
                max(self._ewma_p_dom.expected, self._MIN_P_DOMINANT),
                self._MAX_P_DOMINANT,
            )
            self._cached_threshold = self._ewma_t.expected * p_dom * self._ewma_v.expected
        return self._cached_threshold

    # -- Subclass contract ------------------------------------------------

    @abstractmethod
    def _run_contribution(self, trade: Trade) -> float:
        """Unsigned contribution of this trade to the current run."""
        ...

    # -- Trade direction resolution ---------------------------------------

    def _resolve_sign(self, trade: Trade) -> int:
        """Get trade sign, falling back to tick rule if side is unknown."""
        sign = trade.sign()
        if sign == 0 and self._prev_price is not None:
            sign = tick_rule(trade.price, self._prev_price, self._prev_sign)
        if self._prev_price is not None and sign != 0:
            self._prev_sign = sign
        self._prev_price = trade.price
        return sign if sign != 0 else self._prev_sign

    # -- Core logic -------------------------------------------------------

    def process_trade(self, trade: Trade) -> Bar | None:
        sign = self._resolve_sign(trade)
        self._acc.add(trade)
        contribution = self._run_contribution(trade)

        # Accumulate buy and sell totals for the entire bar (NO reset)
        if sign > 0:
            self._buy_total += contribution
            self._buy_count += 1
        else:
            self._sell_total += contribution
        self._sum_abs_contrib += contribution

        max_side = max(self._buy_total, self._sell_total)

        threshold = self._threshold
        if max_side >= threshold and self._acc.tick_count > 0:
            # Update decomposed EWMAs from this bar's observed statistics
            bar_ticks = self._acc.tick_count
            p_buy = self._buy_count / bar_ticks
            p_dom = max(p_buy, 1.0 - p_buy)
            avg_v = self._sum_abs_contrib / bar_ticks

            self._ewma_t.update(float(bar_ticks))
            self._ewma_p_dom.update(p_dom)
            self._ewma_v.update(avg_v)

            # Clamp E[T] to prevent collapse/explosion feedback loop
            if self._expected_ticks_range is not None:
                lo, hi = self._expected_ticks_range
                self._ewma_t._expected = max(lo, min(hi, self._ewma_t._expected))

            self._cached_threshold = None  # invalidate — EWMAs changed

            metadata = self._flush_metadata()
            self._buy_total = 0.0
            self._sell_total = 0.0
            self._buy_count = 0
            self._sum_abs_contrib = 0.0
            return self._emit_and_reset(metadata=metadata)

        return None

    # -- Metadata hooks ---------------------------------------------------

    def _flush_metadata(self) -> dict:
        meta = {
            "ewma_window": self._ewma_t.window,
            "ewma_t": self._ewma_t.expected,
            "ewma_p_dom": self._ewma_p_dom.expected,
            "ewma_v": self._ewma_v.expected,
        }
        if self._expected_ticks_range is not None:
            meta["expected_ticks_range"] = list(self._expected_ticks_range)
        return meta

    def restore_state(self, metadata: dict) -> None:
        """Restore EWMA state from bar metadata (daemon restart).

        Handles both the new decomposed format and legacy single-EWMA format.
        """
        window = metadata.get("ewma_window", self._ewma_t.window)

        if "ewma_t" in metadata:
            # New decomposed format
            self._ewma_t = EWMAEstimator(
                window=window, initial_value=metadata["ewma_t"]
            )
            self._ewma_p_dom = EWMAEstimator(
                window=window, initial_value=metadata.get("ewma_p_dom", 0.6)
            )
            self._ewma_v = EWMAEstimator(
                window=window, initial_value=metadata.get("ewma_v", 1.0)
            )
        elif "ewma_expected" in metadata:
            # Legacy single-value format
            self._ewma_t = EWMAEstimator(
                window=window, initial_value=metadata["ewma_expected"]
            )
            self._ewma_p_dom = EWMAEstimator(window=window, initial_value=1.0)
            self._ewma_v = EWMAEstimator(window=window, initial_value=1.0)

        if "expected_ticks_range" in metadata:
            r = metadata["expected_ticks_range"]
            self._expected_ticks_range = (float(r[0]), float(r[1]))

        self._cached_threshold = None  # invalidate — EWMAs replaced


# -- Concrete run builders ------------------------------------------------


class TickRunBarBuilder(_RunBarBuilder):
    """Tick Run Bar (TRB): count of 1 per trade.

    Emits when one side's total trade count dominates the other within
    the bar by exceeding the EWMA threshold.  Pure count-based.
    """

    def __init__(
        self,
        source: str,
        pair: str,
        ewma_window: int,
        initial_expected: dict | float | None = None,
        expected_ticks_range: tuple[float, float] | None = None,
    ) -> None:
        super().__init__(source, pair, ewma_window, initial_expected, expected_ticks_range)
        self._ewma_window = ewma_window

    @property
    def bar_type(self) -> str:
        return f"trb_{self._ewma_window}"

    def _run_contribution(self, trade: Trade) -> float:
        return 1.0


class VolumeRunBarBuilder(_RunBarBuilder):
    """Volume Run Bar (VRB): cumulative volume per side.

    Emits when one side's total volume dominates the other within the
    bar by exceeding the EWMA threshold.  Large trades contribute more.
    """

    def __init__(
        self,
        source: str,
        pair: str,
        ewma_window: int,
        initial_expected: dict | float | None = None,
        expected_ticks_range: tuple[float, float] | None = None,
    ) -> None:
        super().__init__(source, pair, ewma_window, initial_expected, expected_ticks_range)
        self._ewma_window = ewma_window

    @property
    def bar_type(self) -> str:
        return f"vrb_{self._ewma_window}"

    def _run_contribution(self, trade: Trade) -> float:
        return float(trade.size)


class DollarRunBarBuilder(_RunBarBuilder):
    """Dollar Run Bar (DRB): cumulative dollar volume per side.

    Emits when one side's total dollar volume dominates the other within
    the bar by exceeding the EWMA threshold.  The most economically
    significant variant -- normalizes for both trade size and price.
    """

    def __init__(
        self,
        source: str,
        pair: str,
        ewma_window: int,
        initial_expected: dict | float | None = None,
        expected_ticks_range: tuple[float, float] | None = None,
    ) -> None:
        super().__init__(source, pair, ewma_window, initial_expected, expected_ticks_range)
        self._ewma_window = ewma_window

    @property
    def bar_type(self) -> str:
        return f"drb_{self._ewma_window}"

    def _run_contribution(self, trade: Trade) -> float:
        return float(trade.dollar_volume)
