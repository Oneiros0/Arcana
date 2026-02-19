"""Information-driven imbalance bar builders (Prado Ch. 2).

Imbalance bars sample when the order-flow imbalance exceeds an adaptive
(EWMA) threshold.  This makes them sensitive to informed trading — the
bars "speed up" when smart money is moving the market.

Three variants weight the imbalance differently:
  - Tick Imbalance (TIB): ±1 per trade
  - Volume Imbalance (VIB): ±volume per trade
  - Dollar Imbalance (DIB): ±dollar_volume per trade
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
    and emits a bar when |cumulative_imbalance| >= EWMA expected value.

    The EWMA adapts the threshold over time based on observed imbalance
    magnitudes — bars become harder to emit in balanced markets and
    easier to emit during directional flow.
    """

    def __init__(self, source: str, pair: str, ewma_window: int) -> None:
        super().__init__(source, pair)
        self._ewma = EWMAEstimator(window=ewma_window)
        self._cum_imbalance: float = 0.0
        self._prev_price: Decimal | None = None
        self._prev_sign: int = 1  # default to +1 until first tick rule fires

    # ── Subclass contract ────────────────────────────────────────────

    @abstractmethod
    def _imbalance_contribution(self, trade: Trade, sign: int) -> float:
        """Signed contribution of this trade to cumulative imbalance."""
        ...

    # ── Trade direction resolution ───────────────────────────────────

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

    # ── Core logic ───────────────────────────────────────────────────

    def process_trade(self, trade: Trade) -> Bar | None:
        sign = self._resolve_sign(trade)
        self._acc.add(trade)
        self._cum_imbalance += self._imbalance_contribution(trade, sign)

        # Emit when imbalance exceeds adaptive threshold
        if abs(self._cum_imbalance) >= self._ewma.expected and self._acc.tick_count > 0:
            imbalance_magnitude = abs(self._cum_imbalance)
            self._ewma.update(imbalance_magnitude)
            metadata = self._flush_metadata()
            self._cum_imbalance = 0.0
            return self._emit_and_reset(metadata=metadata)

        return None

    # ── Metadata hooks ───────────────────────────────────────────────

    def _flush_metadata(self) -> dict:
        return self._ewma.to_dict()

    def restore_state(self, metadata: dict) -> None:
        """Restore EWMA state from bar metadata (daemon restart)."""
        self._ewma = EWMAEstimator.from_dict(metadata)


# ── Concrete imbalance builders ──────────────────────────────────────────────


class TickImbalanceBarBuilder(_ImbalanceBarBuilder):
    """Tick Imbalance Bar (TIB): ±1 per trade.

    Emits when the signed tick count exceeds the EWMA threshold.
    Pure count-based — insensitive to trade size or price.
    """

    def __init__(self, source: str, pair: str, ewma_window: int) -> None:
        super().__init__(source, pair, ewma_window)
        self._ewma_window = ewma_window

    @property
    def bar_type(self) -> str:
        return f"tib_{self._ewma_window}"

    def _imbalance_contribution(self, trade: Trade, sign: int) -> float:
        return float(sign)


class VolumeImbalanceBarBuilder(_ImbalanceBarBuilder):
    """Volume Imbalance Bar (VIB): ±volume per trade.

    Emits when the signed volume imbalance exceeds the EWMA threshold.
    Sensitive to trade size but not price level.
    """

    def __init__(self, source: str, pair: str, ewma_window: int) -> None:
        super().__init__(source, pair, ewma_window)
        self._ewma_window = ewma_window

    @property
    def bar_type(self) -> str:
        return f"vib_{self._ewma_window}"

    def _imbalance_contribution(self, trade: Trade, sign: int) -> float:
        return sign * float(trade.size)


class DollarImbalanceBarBuilder(_ImbalanceBarBuilder):
    """Dollar Imbalance Bar (DIB): ±dollar_volume per trade.

    Emits when the signed dollar-volume imbalance exceeds the EWMA
    threshold.  The most economically meaningful variant — normalizes
    for both trade size and price level.
    """

    def __init__(self, source: str, pair: str, ewma_window: int) -> None:
        super().__init__(source, pair, ewma_window)
        self._ewma_window = ewma_window

    @property
    def bar_type(self) -> str:
        return f"dib_{self._ewma_window}"

    def _imbalance_contribution(self, trade: Trade, sign: int) -> float:
        return sign * float(trade.dollar_volume)
