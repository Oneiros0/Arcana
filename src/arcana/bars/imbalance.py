"""Information-driven imbalance bar builders (Prado AFML Ch. 2, Def. 2.3).

Imbalance bars sample when the order-flow imbalance exceeds an adaptive
threshold.  This makes them sensitive to informed trading -- the bars
"speed up" when smart money is moving the market.

Per Prado, the threshold is decomposed as:
  E[T] x |E[2P[b=1]-1]| x E[|v|]
where each factor is tracked by a separate EWMA for faster adaptation
to market regime changes.

Three variants weight the imbalance differently:
  - Tick Imbalance (TIB): +/-1 per trade
  - Volume Imbalance (VIB): +/-volume per trade
  - Dollar Imbalance (DIB): +/-dollar_volume per trade
"""

from __future__ import annotations

from abc import abstractmethod
from decimal import Decimal

from arcana.bars.base import Bar, BarBuilder
from arcana.bars.utils import EWMAEstimator, tick_rule
from arcana.ingestion.models import Trade


class _ImbalanceBarBuilder(BarBuilder):
    """Private base for all imbalance bar builders.

    Tracks cumulative imbalance (signed sum of per-trade contributions)
    and emits a bar when |cumulative_imbalance| >= adaptive threshold.

    The threshold is decomposed per Prado AFML Def. 2.3:
      threshold = E[T] x |E[2P-1]| x E[|v|]
    Each factor is tracked by a separate EWMA, enabling faster adaptation
    to market regime changes than a monolithic E[|theta|] tracker.

    E[T] can be clamped to a [min, max] range to prevent the well-known
    instability in balanced markets where the threshold either collapses
    (E[T] → 1, producing 1-tick bars) or explodes (E[T] → ∞, producing
    ever-fewer bars).  See mlfinlab's exp_num_ticks_constraints for the
    same fix.
    """

    # Minimum |E[2P-1]| to prevent degenerate bars in balanced markets
    _MIN_DIRECTIONAL_BIAS = 0.1

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
            init_imb = initial_expected.get("imb", 0.0)
            init_v = initial_expected.get("v", 1.0)
        elif isinstance(initial_expected, (int, float)) and initial_expected > 0:
            # Legacy single-value: treat as composite threshold.
            # Decompose as: threshold ~ T, with imb=1 and v=1.
            # Self-corrects within ~window bars.
            init_t = float(initial_expected)
            init_imb = 1.0
            init_v = 1.0
        else:
            init_t = 0.0
            init_imb = 0.0
            init_v = 1.0

        self._ewma_t = EWMAEstimator(window=ewma_window, initial_value=init_t)
        self._ewma_imb = EWMAEstimator(window=ewma_window, initial_value=init_imb)
        self._ewma_v = EWMAEstimator(window=ewma_window, initial_value=init_v)

        self._cum_imbalance: float = 0.0
        self._buy_count: int = 0
        self._sum_abs_contrib: float = 0.0
        self._prev_price: Decimal | None = None
        self._prev_sign: int = 1  # default to +1 until first tick rule fires
        self._cached_threshold: float | None = None

    # -- Threshold --------------------------------------------------------

    @property
    def _threshold(self) -> float:
        """Adaptive threshold: E[T] x |E[2P-1]| x E[|v|].

        Floors |E[2P-1]| at 0.1 to prevent degenerate zero-threshold
        in balanced markets (Prado's recommendation).

        Cached because the EWMAs only change on bar emission; avoids
        recomputing 3 multiplications on every trade.
        """
        if self._cached_threshold is None:
            imb = max(abs(self._ewma_imb.expected), self._MIN_DIRECTIONAL_BIAS)
            self._cached_threshold = self._ewma_t.expected * imb * self._ewma_v.expected
        return self._cached_threshold

    # -- Subclass contract ------------------------------------------------

    @abstractmethod
    def _imbalance_contribution(self, trade: Trade, sign: int) -> float:
        """Signed contribution of this trade to cumulative imbalance."""
        ...

    # -- Trade direction resolution ---------------------------------------

    def _resolve_sign(self, trade: Trade) -> int:
        """Get trade sign, falling back to tick rule if side is unknown."""
        sign = trade.sign()
        if sign == 0 and self._prev_price is not None:
            sign = tick_rule(trade.price, self._prev_price, self._prev_sign)
        # Update tick rule state
        if self._prev_price is not None and sign != 0:
            self._prev_sign = sign
        self._prev_price = trade.price
        return sign if sign != 0 else self._prev_sign

    # -- Core logic -------------------------------------------------------

    def process_trade(self, trade: Trade) -> Bar | None:
        sign = self._resolve_sign(trade)
        self._acc.add(trade)
        contribution = self._imbalance_contribution(trade, sign)
        self._cum_imbalance += contribution

        # Track per-bar statistics for decomposed EWMA updates
        if sign > 0:
            self._buy_count += 1
        self._sum_abs_contrib += abs(contribution)

        # Emit when imbalance exceeds adaptive threshold
        threshold = self._threshold
        if abs(self._cum_imbalance) >= threshold and self._acc.tick_count > 0:
            # Update decomposed EWMAs from this bar's observed statistics
            bar_ticks = self._acc.tick_count
            p_buy = self._buy_count / bar_ticks
            self._ewma_t.update(float(bar_ticks))
            self._ewma_imb.update(2.0 * p_buy - 1.0)
            self._ewma_v.update(self._sum_abs_contrib / bar_ticks)

            # Clamp E[T] to prevent collapse/explosion feedback loop
            if self._expected_ticks_range is not None:
                lo, hi = self._expected_ticks_range
                self._ewma_t._expected = max(lo, min(hi, self._ewma_t._expected))

            self._cached_threshold = None  # invalidate — EWMAs changed

            metadata = self._flush_metadata()
            self._cum_imbalance = 0.0
            self._buy_count = 0
            self._sum_abs_contrib = 0.0
            return self._emit_and_reset(metadata=metadata)

        return None

    # -- Metadata hooks ---------------------------------------------------

    def _flush_metadata(self) -> dict:
        meta = {
            "ewma_window": self._ewma_t.window,
            "ewma_t": self._ewma_t.expected,
            "ewma_imb": self._ewma_imb.expected,
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
            self._ewma_imb = EWMAEstimator(
                window=window, initial_value=metadata.get("ewma_imb", 0.0)
            )
            self._ewma_v = EWMAEstimator(
                window=window, initial_value=metadata.get("ewma_v", 1.0)
            )
        elif "ewma_expected" in metadata:
            # Legacy single-value format — use as ewma_t, self-corrects
            self._ewma_t = EWMAEstimator(
                window=window, initial_value=metadata["ewma_expected"]
            )
            self._ewma_imb = EWMAEstimator(window=window, initial_value=1.0)
            self._ewma_v = EWMAEstimator(window=window, initial_value=1.0)

        if "expected_ticks_range" in metadata and self._expected_ticks_range is None:
            r = metadata["expected_ticks_range"]
            self._expected_ticks_range = (float(r[0]), float(r[1]))

        self._cached_threshold = None  # invalidate — EWMAs replaced


# -- Concrete imbalance builders ------------------------------------------


class TickImbalanceBarBuilder(_ImbalanceBarBuilder):
    """Tick Imbalance Bar (TIB): +/-1 per trade.

    Emits when the signed tick count exceeds the EWMA threshold.
    Pure count-based -- insensitive to trade size or price.
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
        return f"tib_{self._ewma_window}"

    def _imbalance_contribution(self, trade: Trade, sign: int) -> float:
        return float(sign)


class VolumeImbalanceBarBuilder(_ImbalanceBarBuilder):
    """Volume Imbalance Bar (VIB): +/-volume per trade.

    Emits when the signed volume imbalance exceeds the EWMA threshold.
    Sensitive to trade size but not price level.
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
        return f"vib_{self._ewma_window}"

    def _imbalance_contribution(self, trade: Trade, sign: int) -> float:
        return sign * float(trade.size)


class DollarImbalanceBarBuilder(_ImbalanceBarBuilder):
    """Dollar Imbalance Bar (DIB): +/-dollar_volume per trade.

    Emits when the signed dollar-volume imbalance exceeds the EWMA
    threshold.  The most economically meaningful variant -- normalizes
    for both trade size and price level.
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
        return f"dib_{self._ewma_window}"

    def _imbalance_contribution(self, trade: Trade, sign: int) -> float:
        return sign * float(trade.dollar_volume)
