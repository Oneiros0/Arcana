"""Bar model, accumulator, and abstract builder.

Every bar type — time, tick, volume, dollar, imbalance, run — shares the
same output schema (OHLCV + auxiliary info) and accumulation logic. This
module provides those common building blocks.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from decimal import Decimal

from pydantic import BaseModel, Field

from arcana.ingestion.models import Trade


class Bar(BaseModel):
    """A single completed bar with OHLCV and auxiliary data.

    The bar_type field is used in-memory for routing to the correct
    per-type table (e.g. bars_tick_500) but is not stored as a column.
    """

    time_start: datetime = Field(description="Timestamp of the first trade in the bar")
    time_end: datetime = Field(description="Timestamp of the last trade in the bar")
    bar_type: str = Field(description="Bar type label, e.g. 'tick_500', 'time_5m'")
    source: str = Field(description="Data source, e.g. 'coinbase'")
    pair: str = Field(description="Trading pair, e.g. 'ETH-USD'")
    open: Decimal = Field(description="Price of the first trade")
    high: Decimal = Field(description="Highest price in the bar")
    low: Decimal = Field(description="Lowest price in the bar")
    close: Decimal = Field(description="Price of the last trade")
    vwap: Decimal = Field(description="Volume-weighted average price")
    volume: Decimal = Field(description="Total volume in base currency")
    dollar_volume: Decimal = Field(description="Total volume in quote currency")
    tick_count: int = Field(description="Number of trades in the bar")
    time_span: timedelta = Field(description="Duration from first to last trade")
    metadata: dict | None = Field(default=None, description="Bar-specific extra info")

    model_config = {"frozen": True}


class Accumulator:
    """Tracks running OHLCV state while building a single bar.

    Feed trades in via add(). When the bar is complete, call to_bar()
    to produce the Bar and then create a fresh Accumulator.
    """

    __slots__ = (
        "time_start",
        "time_end",
        "_open",
        "_high",
        "_low",
        "_close",
        "_volume",
        "_dollar_volume",
        "_price_x_volume",
        "tick_count",
    )

    def __init__(self) -> None:
        self.time_start: datetime | None = None
        self.time_end: datetime | None = None
        self._open: Decimal | None = None
        self._high: Decimal | None = None
        self._low: Decimal | None = None
        self._close: Decimal | None = None
        self._volume: Decimal = Decimal("0")
        self._dollar_volume: Decimal = Decimal("0")
        self._price_x_volume: Decimal = Decimal("0")
        self.tick_count: int = 0

    def add(self, trade: Trade) -> None:
        """Incorporate a trade into the running accumulation."""
        if self.tick_count == 0:
            self.time_start = trade.timestamp
            self._open = trade.price
            self._high = trade.price
            self._low = trade.price
        else:
            assert self._high is not None and self._low is not None
            self._high = max(self._high, trade.price)
            self._low = min(self._low, trade.price)

        self.time_end = trade.timestamp
        self._close = trade.price
        self._volume += trade.size
        self._dollar_volume += trade.dollar_volume
        self._price_x_volume += trade.price * trade.size
        self.tick_count += 1

    def to_bar(
        self,
        bar_type: str,
        source: str,
        pair: str,
        metadata: dict | None = None,
    ) -> Bar:
        """Produce a completed Bar from the accumulated state.

        Args:
            metadata: Optional bar-specific extra info (e.g. EWMA state
                for information-driven bars).  Stored in the JSONB column.
        """
        assert self.tick_count > 0, "Cannot create bar from empty accumulator"
        assert self.time_start is not None and self.time_end is not None
        assert self._open is not None and self._close is not None
        assert self._high is not None and self._low is not None

        vwap = self._price_x_volume / self._volume if self._volume > 0 else self._close

        return Bar(
            time_start=self.time_start,
            time_end=self.time_end,
            bar_type=bar_type,
            source=source,
            pair=pair,
            open=self._open,
            high=self._high,
            low=self._low,
            close=self._close,
            vwap=vwap,
            volume=self._volume,
            dollar_volume=self._dollar_volume,
            tick_count=self.tick_count,
            time_span=self.time_end - self.time_start,
            metadata=metadata,
        )


class BarBuilder(ABC):
    """Abstract base class for all bar builders.

    Subclasses implement process_trade() with their specific emission logic.
    The builder is stateful — it accumulates trades across process_trades()
    calls, which is essential for daemon mode where trades arrive in batches.
    """

    def __init__(self, source: str, pair: str) -> None:
        self._source = source
        self._pair = pair
        self._acc = Accumulator()

    @property
    @abstractmethod
    def bar_type(self) -> str:
        """Label for this bar type, e.g. 'tick_500', 'time_5m'."""
        ...

    @abstractmethod
    def process_trade(self, trade: Trade) -> Bar | None:
        """Process one trade. Returns a completed bar if threshold is met.

        The trade that triggers emission is included in the completed bar.
        For time bars, the trade that starts a new bucket emits the
        previous bucket first.
        """
        ...

    def process_trades(self, trades: list[Trade]) -> list[Bar]:
        """Process a batch of trades, collecting completed bars.

        Trades must be in ascending timestamp order.
        """
        bars: list[Bar] = []
        for trade in trades:
            bar = self.process_trade(trade)
            if bar is not None:
                bars.append(bar)
        return bars

    def flush(self) -> Bar | None:
        """Emit the current in-progress bar (at end of data or shutdown).

        Returns None if no trades have been accumulated.
        Includes flush metadata from subclass hook (e.g. EWMA state).
        """
        if self._acc.tick_count > 0:
            metadata = self._flush_metadata()
            bar = self._acc.to_bar(
                self.bar_type,
                self._source,
                self._pair,
                metadata=metadata,
            )
            self._acc = Accumulator()
            return bar
        return None

    def _emit_and_reset(self, metadata: dict | None = None) -> Bar:
        """Emit the current bar and start a fresh accumulator."""
        bar = self._acc.to_bar(
            self.bar_type,
            self._source,
            self._pair,
            metadata=metadata,
        )
        self._acc = Accumulator()
        return bar

    def _flush_metadata(self) -> dict | None:
        """Hook for subclasses to attach metadata on flush.

        Information-driven builders override this to persist EWMA state
        so that daemon restarts can resume without a cold start.

        Returns None by default (standard builders have no metadata).
        """
        return None

    def restore_state(self, metadata: dict) -> None:
        """Restore builder state from bar metadata (e.g. on daemon restart).

        Information-driven builders override this to restore their EWMA
        estimator from the last emitted bar's metadata.

        No-op in the base class and standard builders.
        """
