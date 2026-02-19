"""Tests for standard bar builders: tick, volume, dollar, time.

Each test uses hand-crafted trade sequences with known expected outputs
to verify correctness — per Prado's emphasis on exact bar math.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from arcana.bars.standard import (
    DollarBarBuilder,
    TickBarBuilder,
    TimeBarBuilder,
    VolumeBarBuilder,
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


# ── Tick Bars ──────────────────────────────────────────────────────────


class TestTickBarBuilder:
    def test_bar_type_label(self):
        b = TickBarBuilder("coinbase", "ETH-USD", threshold=500)
        assert b.bar_type == "tick_500"

    def test_emits_after_n_trades(self):
        builder = TickBarBuilder("coinbase", "ETH-USD", threshold=3)
        trades = [_trade(i, "100.00", "1.0") for i in range(3)]
        bars = builder.process_trades(trades)

        assert len(bars) == 1
        assert bars[0].tick_count == 3
        assert bars[0].bar_type == "tick_3"

    def test_multiple_bars(self):
        builder = TickBarBuilder("coinbase", "ETH-USD", threshold=2)
        trades = [_trade(i) for i in range(7)]
        bars = builder.process_trades(trades)

        # 7 trades / 2 per bar = 3 full bars, 1 trade leftover
        assert len(bars) == 3
        for bar in bars:
            assert bar.tick_count == 2

        # Flush the remaining trade
        final = builder.flush()
        assert final is not None
        assert final.tick_count == 1

    def test_ohlcv_correctness(self):
        builder = TickBarBuilder("coinbase", "ETH-USD", threshold=4)
        trades = [
            _trade(0, "100.00", "1.0"),  # open
            _trade(1, "110.00", "2.0"),  # high
            _trade(2, "90.00", "0.5"),  # low
            _trade(3, "105.00", "1.5"),  # close
        ]
        bars = builder.process_trades(trades)

        assert len(bars) == 1
        bar = bars[0]
        assert bar.open == Decimal("100.00")
        assert bar.high == Decimal("110.00")
        assert bar.low == Decimal("90.00")
        assert bar.close == Decimal("105.00")
        # volume = 1 + 2 + 0.5 + 1.5 = 5.0
        assert bar.volume == Decimal("5.0")
        # dollar_volume = 100 + 220 + 45 + 157.5 = 522.5
        assert bar.dollar_volume == Decimal("522.50")
        # VWAP = 522.5 / 5 = 104.5
        assert bar.vwap == Decimal("104.5")
        assert bar.time_span == timedelta(seconds=3)

    def test_no_emission_under_threshold(self):
        builder = TickBarBuilder("coinbase", "ETH-USD", threshold=10)
        trades = [_trade(i) for i in range(5)]
        bars = builder.process_trades(trades)
        assert len(bars) == 0

    def test_flush_empty_returns_none(self):
        builder = TickBarBuilder("coinbase", "ETH-USD", threshold=5)
        assert builder.flush() is None

    def test_stateful_across_batches(self):
        builder = TickBarBuilder("coinbase", "ETH-USD", threshold=3)

        # Batch 1: 2 trades — no bar yet
        bars1 = builder.process_trades([_trade(0), _trade(1)])
        assert len(bars1) == 0

        # Batch 2: 1 trade — completes the bar
        bars2 = builder.process_trades([_trade(2)])
        assert len(bars2) == 1
        assert bars2[0].tick_count == 3


# ── Volume Bars ────────────────────────────────────────────────────────


class TestVolumeBarBuilder:
    def test_bar_type_label(self):
        b = VolumeBarBuilder("coinbase", "ETH-USD", threshold="10.0")
        assert b.bar_type == "volume_10.0"

    def test_emits_at_volume_threshold(self):
        builder = VolumeBarBuilder("coinbase", "ETH-USD", threshold="5.0")
        trades = [
            _trade(0, "100", "2.0"),  # cumulative: 2.0
            _trade(1, "100", "1.5"),  # cumulative: 3.5
            _trade(2, "100", "2.0"),  # cumulative: 5.5 >= 5.0 → emit
            _trade(3, "100", "1.0"),  # new bar starts
        ]
        bars = builder.process_trades(trades)

        assert len(bars) == 1
        assert bars[0].volume == Decimal("5.5")
        assert bars[0].tick_count == 3

    def test_large_trade_triggers_immediately(self):
        builder = VolumeBarBuilder("coinbase", "ETH-USD", threshold="10.0")
        trades = [_trade(0, "100", "15.0")]  # single trade exceeds threshold
        bars = builder.process_trades(trades)

        assert len(bars) == 1
        assert bars[0].volume == Decimal("15.0")
        assert bars[0].tick_count == 1

    def test_multiple_volume_bars(self):
        builder = VolumeBarBuilder("coinbase", "ETH-USD", threshold="3.0")
        trades = [
            _trade(0, "100", "2.0"),  # bar 1: 2.0
            _trade(1, "100", "2.0"),  # bar 1: 4.0 >= 3 → emit
            _trade(2, "100", "1.0"),  # bar 2: 1.0
            _trade(3, "100", "1.0"),  # bar 2: 2.0
            _trade(4, "100", "1.5"),  # bar 2: 3.5 >= 3 → emit
        ]
        bars = builder.process_trades(trades)

        assert len(bars) == 2
        assert bars[0].volume == Decimal("4.0")
        assert bars[1].volume == Decimal("3.5")


# ── Dollar Bars ────────────────────────────────────────────────────────


class TestDollarBarBuilder:
    def test_bar_type_label(self):
        b = DollarBarBuilder("coinbase", "ETH-USD", threshold="50000")
        assert b.bar_type == "dollar_50000"

    def test_emits_at_dollar_threshold(self):
        builder = DollarBarBuilder("coinbase", "ETH-USD", threshold="500")
        trades = [
            _trade(0, "100", "2.0"),  # $200
            _trade(1, "100", "1.5"),  # $350
            _trade(2, "100", "2.0"),  # $550 >= $500 → emit
        ]
        bars = builder.process_trades(trades)

        assert len(bars) == 1
        assert bars[0].dollar_volume == Decimal("550.0")

    def test_price_variation_affects_bar_boundary(self):
        """Higher prices fill the dollar threshold faster."""
        builder = DollarBarBuilder("coinbase", "ETH-USD", threshold="1000")
        trades = [
            _trade(0, "200", "2.0"),  # $400
            _trade(1, "300", "1.0"),  # $700
            _trade(2, "400", "1.0"),  # $1100 >= $1000 → emit
        ]
        bars = builder.process_trades(trades)

        assert len(bars) == 1
        assert bars[0].dollar_volume == Decimal("1100.0")
        assert bars[0].tick_count == 3


# ── Time Bars ──────────────────────────────────────────────────────────


class TestTimeBarBuilder:
    def test_bar_type_labels(self):
        assert TimeBarBuilder("c", "P", timedelta(seconds=30)).bar_type == "time_30s"
        assert TimeBarBuilder("c", "P", timedelta(minutes=1)).bar_type == "time_1m"
        assert TimeBarBuilder("c", "P", timedelta(minutes=5)).bar_type == "time_5m"
        assert TimeBarBuilder("c", "P", timedelta(hours=1)).bar_type == "time_1h"
        assert TimeBarBuilder("c", "P", timedelta(days=1)).bar_type == "time_1d"

    def test_emits_on_bucket_change(self):
        """Trades in one 5m bucket, then a trade in the next bucket."""
        builder = TimeBarBuilder("coinbase", "ETH-USD", timedelta(minutes=5))

        # All in the 14:00-14:05 bucket
        trades_bucket_1 = [
            _trade(0, "100", "1.0"),  # 14:00:00
            _trade(60, "101", "1.0"),  # 14:01:00
            _trade(120, "102", "1.0"),  # 14:02:00
        ]
        bars = builder.process_trades(trades_bucket_1)
        assert len(bars) == 0  # no emission yet — bucket not closed

        # Trade in 14:05-14:10 bucket triggers emission of 14:00-14:05
        bar_list = builder.process_trades([_trade(300, "103", "1.0")])
        assert len(bar_list) == 1
        bar = bar_list[0]
        assert bar.tick_count == 3
        assert bar.open == Decimal("100")
        assert bar.close == Decimal("102")

    def test_multiple_buckets(self):
        builder = TimeBarBuilder("coinbase", "ETH-USD", timedelta(minutes=5))
        trades = [
            _trade(0, "100", "1.0"),  # 14:00 bucket
            _trade(60, "101", "1.0"),  # 14:00 bucket
            _trade(300, "102", "1.0"),  # 14:05 bucket → emits 14:00
            _trade(600, "103", "1.0"),  # 14:10 bucket → emits 14:05
            _trade(900, "104", "1.0"),  # 14:15 bucket → emits 14:10
        ]
        bars = builder.process_trades(trades)

        # 3 bars emitted (14:00, 14:05, 14:10); 14:15 still accumulating
        assert len(bars) == 3
        assert bars[0].tick_count == 2  # 14:00 had 2 trades
        assert bars[1].tick_count == 1  # 14:05 had 1 trade
        assert bars[2].tick_count == 1  # 14:10 had 1 trade

    def test_flush_emits_current_bucket(self):
        builder = TimeBarBuilder("coinbase", "ETH-USD", timedelta(minutes=5))
        builder.process_trades([_trade(0, "100", "1.0"), _trade(1, "101", "1.0")])
        bar = builder.flush()
        assert bar is not None
        assert bar.tick_count == 2

    def test_empty_gaps_produce_no_bars(self):
        """A gap from 14:00 to 14:30 should not produce empty bars."""
        builder = TimeBarBuilder("coinbase", "ETH-USD", timedelta(minutes=5))
        trades = [
            _trade(0, "100", "1.0"),  # 14:00 bucket
            _trade(1800, "105", "1.0"),  # 14:30 bucket → emits 14:00 only
        ]
        bars = builder.process_trades(trades)

        assert len(bars) == 1  # only the 14:00 bar, not empty 14:05..14:25
        assert bars[0].tick_count == 1

    def test_time_alignment(self):
        """Bars should align to clock boundaries, not trade arrival times."""
        builder = TimeBarBuilder("coinbase", "ETH-USD", timedelta(minutes=5))
        # Trade at 14:03:45 — belongs to 14:00-14:05 bucket
        t1 = _trade(225, "100", "1.0")  # 14:03:45
        # Trade at 14:07:30 — belongs to 14:05-14:10 bucket
        t2 = _trade(450, "101", "1.0")  # 14:07:30

        bars = builder.process_trades([t1, t2])
        assert len(bars) == 1  # 14:00 bucket emitted
        assert bars[0].time_start == t1.timestamp
        assert bars[0].time_end == t1.timestamp  # only 1 trade in that bucket

    def test_stateful_across_batches(self):
        builder = TimeBarBuilder("coinbase", "ETH-USD", timedelta(minutes=5))

        # Batch 1: trades in 14:00 bucket
        bars1 = builder.process_trades([_trade(0), _trade(60)])
        assert len(bars1) == 0

        # Batch 2: trade in 14:05 bucket, emits 14:00
        bars2 = builder.process_trades([_trade(300)])
        assert len(bars2) == 1
        assert bars2[0].tick_count == 2  # from batch 1
