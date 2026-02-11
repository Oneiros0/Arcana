"""Standard bar builders: time, tick, volume, dollar.

These sample based on a fixed threshold of activity (Prado Ch. 2).
All share the same OHLCV+auxiliary output; they differ only in what
triggers a new bar.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from arcana.bars.base import Accumulator, Bar, BarBuilder
from arcana.ingestion.models import Trade


class TickBarBuilder(BarBuilder):
    """Emit a bar every N trades.

    Removes time-dependent oversampling of quiet periods — each bar
    contains the same number of ticks regardless of wall-clock time.
    """

    def __init__(self, source: str, pair: str, threshold: int) -> None:
        super().__init__(source, pair)
        self._threshold = threshold

    @property
    def bar_type(self) -> str:
        return f"tick_{self._threshold}"

    def process_trade(self, trade: Trade) -> Bar | None:
        self._acc.add(trade)
        if self._acc.tick_count >= self._threshold:
            return self._emit_and_reset()
        return None


class VolumeBarBuilder(BarBuilder):
    """Emit a bar every V units of base-currency volume.

    Samples proportional to market activity — busy periods produce
    more bars, quiet periods fewer.
    """

    def __init__(self, source: str, pair: str, threshold: Decimal) -> None:
        super().__init__(source, pair)
        self._threshold = Decimal(str(threshold))

    @property
    def bar_type(self) -> str:
        return f"volume_{self._threshold}"

    def process_trade(self, trade: Trade) -> Bar | None:
        self._acc.add(trade)
        if self._acc._volume >= self._threshold:
            return self._emit_and_reset()
        return None


class DollarBarBuilder(BarBuilder):
    """Emit a bar every D dollars of notional volume.

    Prado's preferred standard bar — normalizes for price changes
    over time, so the economic significance of each bar is constant.
    """

    def __init__(self, source: str, pair: str, threshold: Decimal) -> None:
        super().__init__(source, pair)
        self._threshold = Decimal(str(threshold))

    @property
    def bar_type(self) -> str:
        return f"dollar_{self._threshold}"

    def process_trade(self, trade: Trade) -> Bar | None:
        self._acc.add(trade)
        if self._acc._dollar_volume >= self._threshold:
            return self._emit_and_reset()
        return None


class TimeBarBuilder(BarBuilder):
    """Emit a bar at fixed time intervals (1m, 5m, 15m, 1h, etc.).

    Bars are aligned to clock boundaries — a 5m bar covers :00-:05,
    :05-:10, etc. When a trade arrives in a new time bucket, the
    previous bucket's bar is emitted first.

    Empty periods (no trades) do not produce bars.
    """

    def __init__(self, source: str, pair: str, interval: timedelta) -> None:
        super().__init__(source, pair)
        self._interval = interval
        self._interval_seconds = interval.total_seconds()
        self._current_bucket_end: datetime | None = None

    @property
    def bar_type(self) -> str:
        secs = self._interval.total_seconds()
        if secs < 60:
            return f"time_{int(secs)}s"
        elif secs < 3600:
            return f"time_{int(secs // 60)}m"
        elif secs < 86400:
            return f"time_{int(secs // 3600)}h"
        else:
            return f"time_{int(secs // 86400)}d"

    def _bucket_end(self, ts: datetime) -> datetime:
        """Compute the end of the time bucket that contains ts."""
        epoch = ts.timestamp()
        bucket_start_epoch = (epoch // self._interval_seconds) * self._interval_seconds
        bucket_end_epoch = bucket_start_epoch + self._interval_seconds
        return datetime.fromtimestamp(bucket_end_epoch, tz=timezone.utc)

    def process_trade(self, trade: Trade) -> Bar | None:
        bucket_end = self._bucket_end(trade.timestamp)
        result: Bar | None = None

        # If this trade falls in a new bucket, emit the previous one
        if (
            self._current_bucket_end is not None
            and bucket_end != self._current_bucket_end
            and self._acc.tick_count > 0
        ):
            result = self._emit_and_reset()

        self._current_bucket_end = bucket_end
        self._acc.add(trade)
        return result
