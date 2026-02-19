"""Information-driven run bar builders (Prado Ch. 2).

Run bars sample when the longest consecutive same-direction run exceeds
an adaptive (EWMA) threshold.  They detect sustained directional
pressure — a long buy run suggests informed buying, and vice versa.

Three variants weight the run differently:
  - Tick Run (TRB): count of 1 per trade
  - Volume Run (VRB): cumulative volume in the run
  - Dollar Run (DRB): cumulative dollar volume in the run
"""

from __future__ import annotations

from abc import abstractmethod
from decimal import Decimal

from arcana.bars.base import Bar, BarBuilder
from arcana.bars.utils import EWMAEstimator, tick_rule
from arcana.ingestion.models import Trade


class _RunBarBuilder(BarBuilder):
    """Private base for all run bar builders.

    Tracks buy-run and sell-run counters.  On each trade, the matching
    direction's counter grows; if the direction changes, the opposite
    counter resets.  A bar emits when max(buy_run, sell_run) >= EWMA
    expected value.
    """

    def __init__(self, source: str, pair: str, ewma_window: int) -> None:
        super().__init__(source, pair)
        self._ewma = EWMAEstimator(window=ewma_window)
        self._buy_run: float = 0.0
        self._sell_run: float = 0.0
        self._prev_price: Decimal | None = None
        self._prev_sign: int = 1

    # ── Subclass contract ────────────────────────────────────────────

    @abstractmethod
    def _run_contribution(self, trade: Trade) -> float:
        """Unsigned contribution of this trade to the current run."""
        ...

    # ── Trade direction resolution ───────────────────────────────────

    def _resolve_sign(self, trade: Trade) -> int:
        """Get trade sign, falling back to tick rule if side is unknown."""
        sign = trade.sign()
        if sign == 0 and self._prev_price is not None:
            sign = tick_rule(trade.price, self._prev_price, self._prev_sign)
        if self._prev_price is not None and sign != 0:
            self._prev_sign = sign
        self._prev_price = trade.price
        return sign if sign != 0 else self._prev_sign

    # ── Core logic ───────────────────────────────────────────────────

    def process_trade(self, trade: Trade) -> Bar | None:
        sign = self._resolve_sign(trade)
        self._acc.add(trade)
        contribution = self._run_contribution(trade)

        if sign >= 0:
            self._buy_run += contribution
            self._sell_run = 0.0  # reset opposite run
        else:
            self._sell_run += contribution
            self._buy_run = 0.0  # reset opposite run

        max_run = max(self._buy_run, self._sell_run)

        if max_run >= self._ewma.expected and self._acc.tick_count > 0:
            self._ewma.update(max_run)
            metadata = self._flush_metadata()
            self._buy_run = 0.0
            self._sell_run = 0.0
            return self._emit_and_reset(metadata=metadata)

        return None

    # ── Metadata hooks ───────────────────────────────────────────────

    def _flush_metadata(self) -> dict:
        return self._ewma.to_dict()

    def restore_state(self, metadata: dict) -> None:
        """Restore EWMA state from bar metadata (daemon restart)."""
        self._ewma = EWMAEstimator.from_dict(metadata)


# ── Concrete run builders ────────────────────────────────────────────────────


class TickRunBarBuilder(_RunBarBuilder):
    """Tick Run Bar (TRB): count of 1 per trade.

    Emits when a consecutive run of same-direction trades exceeds the
    EWMA threshold.  Pure count-based run detection.
    """

    def __init__(self, source: str, pair: str, ewma_window: int) -> None:
        super().__init__(source, pair, ewma_window)
        self._ewma_window = ewma_window

    @property
    def bar_type(self) -> str:
        return f"trb_{self._ewma_window}"

    def _run_contribution(self, trade: Trade) -> float:
        return 1.0


class VolumeRunBarBuilder(_RunBarBuilder):
    """Volume Run Bar (VRB): cumulative volume in the run.

    Emits when the volume accumulated during a same-direction run
    exceeds the EWMA threshold.  Large trades contribute more.
    """

    def __init__(self, source: str, pair: str, ewma_window: int) -> None:
        super().__init__(source, pair, ewma_window)
        self._ewma_window = ewma_window

    @property
    def bar_type(self) -> str:
        return f"vrb_{self._ewma_window}"

    def _run_contribution(self, trade: Trade) -> float:
        return float(trade.size)


class DollarRunBarBuilder(_RunBarBuilder):
    """Dollar Run Bar (DRB): cumulative dollar volume in the run.

    Emits when the dollar volume accumulated during a same-direction
    run exceeds the EWMA threshold.  The most economically significant
    variant — normalizes for both trade size and price.
    """

    def __init__(self, source: str, pair: str, ewma_window: int) -> None:
        super().__init__(source, pair, ewma_window)
        self._ewma_window = ewma_window

    @property
    def bar_type(self) -> str:
        return f"drb_{self._ewma_window}"

    def _run_contribution(self, trade: Trade) -> float:
        return float(trade.dollar_volume)
