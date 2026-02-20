"""Tests for the ingestion pipeline."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from arcana.bars.standard import TickBarBuilder
from arcana.ingestion.coinbase import CoinbaseSource
from arcana.ingestion.models import Trade
from arcana.pipeline import (
    _format_eta,
    build_bars,
    calibrate_dollar_threshold,
    calibrate_info_bar_initial_expected,
    calibrate_tick_threshold,
    calibrate_volume_threshold,
    ingest_backfill,
    run_daemon,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _make_trades(count: int, start_ts: datetime | None = None) -> list[Trade]:
    """Generate a list of fake trades for testing."""
    base = start_ts or datetime(2026, 2, 10, 12, 0, 0, tzinfo=UTC)
    trades = []
    for i in range(count):
        trades.append(
            Trade(
                timestamp=base + timedelta(seconds=i),
                trade_id=f"test-{i:06d}",
                source="coinbase",
                pair="ETH-USD",
                price=Decimal("2845.50"),
                size=Decimal("0.1"),
                side="buy" if i % 2 == 0 else "sell",
            )
        )
    return trades


class TestIngestBackfill:
    @patch("arcana.pipeline.GracefulShutdown")
    @patch("arcana.pipeline.time_mod.sleep")
    def test_backfill_stores_trades(self, mock_sleep, mock_shutdown):
        """Backfill should fetch trades and store them in the database."""
        mock_shutdown.return_value.should_stop = False
        trades = _make_trades(10)

        source = MagicMock(spec=CoinbaseSource)
        source.name = "coinbase"
        source.fetch_all_trades.return_value = trades
        source._client = True  # just needs to be truthy

        db = MagicMock()
        db.get_last_timestamp.return_value = None
        db.insert_trades.return_value = 10

        since = datetime(2026, 2, 10, 12, 0, 0, tzinfo=UTC)
        # Use a window large enough to cover since→now in a single window
        total = ingest_backfill(
            source,
            db,
            "ETH-USD",
            since,
            window=timedelta(days=7),
        )

        assert total == 10
        assert db.insert_trades.called
        assert source.fetch_all_trades.called

    @patch("arcana.pipeline.GracefulShutdown")
    @patch("arcana.pipeline.time_mod.sleep")
    def test_backfill_resumes_from_last_timestamp(self, mock_sleep, mock_shutdown):
        """If data exists, backfill should resume from last stored timestamp."""
        mock_shutdown.return_value.should_stop = False

        source = MagicMock(spec=CoinbaseSource)
        source.name = "coinbase"
        source.fetch_all_trades.return_value = []
        source._client = True

        last = datetime(2026, 2, 10, 14, 0, 0, tzinfo=UTC)
        db = MagicMock()
        db.get_last_timestamp.return_value = last
        db.insert_trades.return_value = 0

        since = datetime(2026, 2, 10, 12, 0, 0, tzinfo=UTC)
        ingest_backfill(source, db, "ETH-USD", since)

        # The first fetch_trades call should start from last, not since
        first_call = source.fetch_all_trades.call_args_list[0]
        assert first_call.kwargs["start"] == last

    @patch("arcana.pipeline.GracefulShutdown")
    @patch("arcana.pipeline.time_mod.sleep")
    def test_backfill_resume_scoped_to_until(self, mock_sleep, mock_shutdown):
        """Resume query should be bounded by `until` so backfill
        doesn't see trades outside the requested range."""
        mock_shutdown.return_value.should_stop = False

        source = MagicMock(spec=CoinbaseSource)
        source.name = "coinbase"
        source.fetch_all_trades.return_value = _make_trades(5)
        source._client = True

        until = datetime(2026, 2, 4, 0, 0, 0, tzinfo=UTC)

        db = MagicMock()
        # Simulate: global max is Feb 12 (from another worker), but
        # within this worker's range the max is Feb 3
        db.get_last_timestamp.return_value = datetime(2026, 2, 3, 12, 0, 0, tzinfo=UTC)
        db.insert_trades.return_value = 5

        since = datetime(2026, 2, 3, 0, 0, 0, tzinfo=UTC)
        ingest_backfill(source, db, "ETH-USD", since, until=until)

        # Verify get_last_timestamp was called with before=until
        db.get_last_timestamp.assert_called_once_with("ETH-USD", "coinbase", before=until)

    @patch("arcana.pipeline.GracefulShutdown")
    @patch("arcana.pipeline.time_mod.sleep")
    def test_backfill_checkpoints_in_batches(self, mock_sleep, mock_shutdown):
        """Trades should be committed in batches of BATCH_SIZE."""
        mock_shutdown.return_value.should_stop = False

        # Return enough trades to trigger at least one checkpoint
        trades = _make_trades(600)

        source = MagicMock(spec=CoinbaseSource)
        source.name = "coinbase"
        source.fetch_all_trades.return_value = trades
        source._client = True

        db = MagicMock()
        db.get_last_timestamp.return_value = None
        db.insert_trades.return_value = 600

        since = datetime(2026, 2, 10, 12, 0, 0, tzinfo=UTC)
        ingest_backfill(source, db, "ETH-USD", since, window=timedelta(minutes=30))

        # insert_trades should be called (at least once for final flush)
        assert db.insert_trades.called

    @patch("arcana.pipeline.GracefulShutdown")
    @patch("arcana.pipeline.time_mod.sleep")
    def test_backfill_graceful_shutdown(self, mock_sleep, mock_shutdown):
        """On shutdown signal, should commit buffer and stop."""
        shutdown_instance = MagicMock()
        # Stop after first window
        shutdown_instance.should_stop = False

        call_count = 0

        def toggle_shutdown(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count >= 1:
                shutdown_instance.should_stop = True
            return _make_trades(5)

        mock_shutdown.return_value = shutdown_instance

        source = MagicMock(spec=CoinbaseSource)
        source.name = "coinbase"
        source.fetch_all_trades.side_effect = toggle_shutdown
        source._client = True

        db = MagicMock()
        db.get_last_timestamp.return_value = None
        db.insert_trades.return_value = 5

        since = datetime(2026, 2, 10, 12, 0, 0, tzinfo=UTC)
        ingest_backfill(source, db, "ETH-USD", since)

        # Should have committed the buffer before stopping
        assert db.insert_trades.called


class TestBuildBars:
    @patch("arcana.pipeline.GracefulShutdown")
    def test_build_bars_processes_trades(self, mock_shutdown):
        """build_bars should fetch trades, run builder, and store bars."""
        mock_shutdown.return_value.should_stop = False
        trades = _make_trades(10)

        db = MagicMock()
        db.get_last_bar_time.return_value = None
        db.get_first_timestamp.return_value = trades[0].timestamp
        db.get_trades_since.return_value = trades
        db.insert_bars.return_value = 1

        builder = TickBarBuilder("coinbase", "ETH-USD", threshold=5)
        total = build_bars(builder, db, "ETH-USD")

        # 10 trades / 5 threshold = 2 full bars + possible flush
        assert total >= 2
        assert db.insert_bars.called

    @patch("arcana.pipeline.GracefulShutdown")
    def test_build_bars_resumes_from_last_bar(self, mock_shutdown):
        """If bars exist, should resume from last bar's time_end."""
        mock_shutdown.return_value.should_stop = False

        last_bar_time = datetime(2026, 2, 10, 14, 0, 0, tzinfo=UTC)

        db = MagicMock()
        db.get_last_bar_time.return_value = last_bar_time
        db.get_last_bar_metadata.return_value = None
        db.delete_bars_since.return_value = 0
        db.get_trades_since.return_value = []

        builder = TickBarBuilder("coinbase", "ETH-USD", threshold=5)
        build_bars(builder, db, "ETH-USD")

        # Should delete stale bars from resume point before rebuilding
        db.delete_bars_since.assert_called_once_with(
            "tick_5",
            "ETH-USD",
            last_bar_time,
            "coinbase",
        )
        # Should query trades starting from last bar time, not first trade
        db.get_trades_since.assert_called_once_with(
            "ETH-USD",
            last_bar_time,
            "coinbase",
            limit=100_000,
            since_trade_id=None,
        )
        db.get_first_timestamp.assert_not_called()

    @patch("arcana.pipeline.GracefulShutdown")
    def test_build_bars_no_trades_returns_zero(self, mock_shutdown):
        """If no trades exist, should return 0 and not crash."""
        mock_shutdown.return_value.should_stop = False

        db = MagicMock()
        db.get_last_bar_time.return_value = None
        db.get_first_timestamp.return_value = None

        builder = TickBarBuilder("coinbase", "ETH-USD", threshold=5)
        total = build_bars(builder, db, "ETH-USD")

        assert total == 0

    @patch("arcana.pipeline.TRADE_BATCH", 10)
    @patch("arcana.pipeline.GracefulShutdown")
    def test_build_bars_paginates(self, mock_shutdown):
        """Should loop through batches until trades are exhausted."""
        mock_shutdown.return_value.should_stop = False

        # First batch exactly at TRADE_BATCH (10) triggers pagination
        batch1 = _make_trades(10)
        batch2 = _make_trades(5, start_ts=datetime(2026, 2, 10, 12, 0, 10, tzinfo=UTC))

        db = MagicMock()
        db.get_last_bar_time.return_value = None
        db.get_first_timestamp.return_value = batch1[0].timestamp
        db.get_trades_since.side_effect = [batch1, batch2]
        db.insert_bars.return_value = 1

        builder = TickBarBuilder("coinbase", "ETH-USD", threshold=5)
        build_bars(builder, db, "ETH-USD")

        # Should have called get_trades_since twice (batch1 at limit, batch2 under)
        assert db.get_trades_since.call_count == 2

    @patch("arcana.pipeline.GracefulShutdown")
    def test_build_bars_flushes_partial(self, mock_shutdown):
        """Should flush partial bar at end of data."""
        mock_shutdown.return_value.should_stop = False

        # 3 trades with threshold=5 means no full bar, but flush should emit one
        trades = _make_trades(3)

        db = MagicMock()
        db.get_last_bar_time.return_value = None
        db.get_first_timestamp.return_value = trades[0].timestamp
        db.get_trades_since.return_value = trades
        db.insert_bars.return_value = 1

        builder = TickBarBuilder("coinbase", "ETH-USD", threshold=5)
        total = build_bars(builder, db, "ETH-USD")

        # The flush should produce 1 bar
        assert total == 1
        assert db.insert_bars.called

    @patch("arcana.pipeline.GracefulShutdown")
    def test_build_bars_rebuild_deletes_existing(self, mock_shutdown):
        """rebuild=True should delete all existing bars before building."""
        mock_shutdown.return_value.should_stop = False
        trades = _make_trades(10)

        db = MagicMock()
        db.delete_all_bars.return_value = 500
        db.get_last_bar_time.return_value = None
        db.get_first_timestamp.return_value = trades[0].timestamp
        db.get_trades_since.return_value = trades
        db.insert_bars.return_value = 1

        builder = TickBarBuilder("coinbase", "ETH-USD", threshold=5)
        total = build_bars(builder, db, "ETH-USD", rebuild=True)

        # Should have deleted existing bars
        db.delete_all_bars.assert_called_once_with("tick_5", "ETH-USD", "coinbase")
        # After delete, last_bar_time returns None → fresh build
        assert total >= 2
        assert db.insert_bars.called

    @patch("arcana.pipeline.GracefulShutdown")
    def test_build_bars_rebuild_false_no_delete(self, mock_shutdown):
        """rebuild=False should NOT call delete_all_bars."""
        mock_shutdown.return_value.should_stop = False
        trades = _make_trades(10)

        db = MagicMock()
        db.get_last_bar_time.return_value = None
        db.get_first_timestamp.return_value = trades[0].timestamp
        db.get_trades_since.return_value = trades
        db.insert_bars.return_value = 1

        builder = TickBarBuilder("coinbase", "ETH-USD", threshold=5)
        build_bars(builder, db, "ETH-USD", rebuild=False)

        db.delete_all_bars.assert_not_called()

    @patch("arcana.pipeline.GracefulShutdown")
    def test_build_bars_restores_ewma_state(self, mock_shutdown):
        """When resuming, should restore EWMA state from last bar metadata."""
        mock_shutdown.return_value.should_stop = False

        from arcana.bars.imbalance import TickImbalanceBarBuilder

        last_bar_time = datetime(2026, 2, 10, 14, 0, 0, tzinfo=UTC)
        saved_metadata = {"ewma_window": 10, "ewma_expected": 42.5}

        db = MagicMock()
        db.get_last_bar_time.return_value = last_bar_time
        db.get_last_bar_metadata.return_value = saved_metadata
        db.delete_bars_since.return_value = 0
        db.get_trades_since.return_value = []

        builder = TickImbalanceBarBuilder("coinbase", "ETH-USD", ewma_window=10)
        build_bars(builder, db, "ETH-USD")

        # EWMA should have been restored from metadata
        assert builder._ewma.expected == pytest.approx(42.5)
        assert builder._ewma.window == 10


class TestRunDaemon:
    @patch("arcana.pipeline.GracefulShutdown")
    def test_daemon_raises_when_no_data(self, mock_shutdown):
        """Daemon should raise RuntimeError if no trades exist."""
        mock_shutdown.return_value.should_stop = False

        source = MagicMock(spec=CoinbaseSource)
        source.name = "coinbase"

        db = MagicMock()
        db.get_last_timestamp.return_value = None

        with pytest.raises(RuntimeError, match="No trades found"):
            run_daemon(source, db, "ETH-USD")


class TestCalibrateDollarThreshold:
    def test_calibrate_basic(self):
        """Should compute threshold from dollar volume and time span."""
        db = MagicMock()
        # $10M over 10 days, target 50 bars/day → 500 total bars → $20,000 per bar
        db.get_dollar_volume_stats.return_value = (10_000_000.0, 10.0)

        threshold = calibrate_dollar_threshold(db, "ETH-USD", bars_per_day=50)

        # raw = 10M / 500 = 20,000 → rounded to 20,000
        assert threshold == 20_000

    def test_calibrate_rounds_to_clean_value(self):
        """Threshold should be rounded to nearest significant digit."""
        db = MagicMock()
        # $50.9B over 94.7 days, target 50 bars/day
        db.get_dollar_volume_stats.return_value = (50_900_000_000.0, 94.7)

        threshold = calibrate_dollar_threshold(db, "ETH-USD", bars_per_day=50)

        # raw = 50.9B / 4735 ≈ $10,749,736 → rounded to 10,000,000 or 11,000,000
        assert threshold >= 10_000_000
        assert threshold <= 11_000_000

    def test_calibrate_custom_bars_per_day(self):
        """Should respect bars_per_day parameter."""
        db = MagicMock()
        db.get_dollar_volume_stats.return_value = (10_000_000.0, 10.0)

        t50 = calibrate_dollar_threshold(db, "ETH-USD", bars_per_day=50)
        t100 = calibrate_dollar_threshold(db, "ETH-USD", bars_per_day=100)

        # More bars/day → smaller threshold
        assert t100 < t50

    def test_calibrate_no_data_raises(self):
        """Should raise ValueError when no trade data exists."""
        db = MagicMock()
        db.get_dollar_volume_stats.return_value = None

        with pytest.raises(ValueError, match="No trade data"):
            calibrate_dollar_threshold(db, "ETH-USD")


class TestCalibrateTickThreshold:
    def test_basic(self):
        """threshold = total_trades / (days * bars_per_day)."""
        db = MagicMock()
        # 500,000 trades over 100 days, 50 bars/day → 100 ticks/bar
        db.get_trade_volume_stats.return_value = (500_000.0, 50_000.0, 100.0)
        threshold = calibrate_tick_threshold(db, "ETH-USD", bars_per_day=50)
        assert threshold == 100

    def test_rounds_to_integer(self):
        db = MagicMock()
        db.get_trade_volume_stats.return_value = (501_000.0, 50_000.0, 100.0)
        threshold = calibrate_tick_threshold(db, "ETH-USD", bars_per_day=50)
        assert isinstance(threshold, int)
        assert threshold == 100  # 501000/5000 = 100.2, rounds to 100

    def test_minimum_one(self):
        db = MagicMock()
        db.get_trade_volume_stats.return_value = (10.0, 1.0, 100.0)
        threshold = calibrate_tick_threshold(db, "ETH-USD", bars_per_day=50)
        assert threshold >= 1

    def test_no_data_raises(self):
        db = MagicMock()
        db.get_trade_volume_stats.return_value = None
        with pytest.raises(ValueError, match="No trade data"):
            calibrate_tick_threshold(db, "ETH-USD")


class TestCalibrateVolumeThreshold:
    def test_basic(self):
        db = MagicMock()
        # 100,000 volume over 100 days, 50 bars/day → 20 units/bar
        db.get_trade_volume_stats.return_value = (500_000.0, 100_000.0, 100.0)
        threshold = calibrate_volume_threshold(db, "ETH-USD", bars_per_day=50)
        assert threshold == 20

    def test_no_data_raises(self):
        db = MagicMock()
        db.get_trade_volume_stats.return_value = None
        with pytest.raises(ValueError, match="No trade data"):
            calibrate_volume_threshold(db, "ETH-USD")


class TestCalibrateInfoBarInitialExpected:
    def test_tib_balanced_market(self):
        """TIB in balanced market (P=0.5): E0 = E[T] * 0.1 * 1.0."""
        db = MagicMock()
        db.get_trade_volume_stats.return_value = (500_000.0, 50_000.0, 100.0)
        db.get_imbalance_stats.return_value = (0.1, 285.0, 0.50)  # balanced

        e0 = calibrate_info_bar_initial_expected(db, "ETH-USD", "tib", bars_per_day=50)
        # E[T] = 500000/(100*50) = 100, bias = max(|2*0.5-1|, 0.1) = 0.1, contrib = 1.0
        assert e0 == pytest.approx(100 * 0.1 * 1.0)

    def test_tib_directional_market(self):
        """TIB with P[buy]=0.6: E0 = E[T] * 0.2 * 1.0."""
        db = MagicMock()
        db.get_trade_volume_stats.return_value = (500_000.0, 50_000.0, 100.0)
        db.get_imbalance_stats.return_value = (0.1, 285.0, 0.60)

        e0 = calibrate_info_bar_initial_expected(db, "ETH-USD", "tib", bars_per_day=50)
        # bias = |2*0.6 - 1| = 0.2
        assert e0 == pytest.approx(100 * 0.2 * 1.0)

    def test_vib(self):
        """VIB: contribution = avg_size."""
        db = MagicMock()
        db.get_trade_volume_stats.return_value = (500_000.0, 50_000.0, 100.0)
        db.get_imbalance_stats.return_value = (0.1, 285.0, 0.55)

        e0 = calibrate_info_bar_initial_expected(db, "ETH-USD", "vib", bars_per_day=50)
        # E[T]=100, bias=|2*0.55-1|=0.1, contrib=0.1
        assert e0 == pytest.approx(100 * 0.1 * 0.1)

    def test_dib(self):
        """DIB: contribution = avg_dollar_volume."""
        db = MagicMock()
        db.get_trade_volume_stats.return_value = (500_000.0, 50_000.0, 100.0)
        db.get_imbalance_stats.return_value = (0.1, 285.0, 0.55)

        e0 = calibrate_info_bar_initial_expected(db, "ETH-USD", "dib", bars_per_day=50)
        assert e0 == pytest.approx(100 * 0.1 * 285.0)

    def test_trb_uses_geometric_run_length(self):
        """TRB E₀ = p_same / (1-p_same) × E[|c|], where c=1.0 for ticks."""
        db = MagicMock()
        db.get_trade_volume_stats.return_value = (500_000.0, 50_000.0, 100.0)
        db.get_imbalance_stats.return_value = (0.1, 285.0, 0.60)

        e0 = calibrate_info_bar_initial_expected(db, "ETH-USD", "trb", bars_per_day=50)
        # P[buy]=0.60 → p_same=0.60, E[run]=0.60/0.40=1.5, contrib=1.0
        assert e0 == pytest.approx(1.5 * 1.0)

    def test_trb_differs_from_tib(self):
        """Run bars use geometric run length, not imbalance formula."""
        db = MagicMock()
        db.get_trade_volume_stats.return_value = (500_000.0, 50_000.0, 100.0)
        db.get_imbalance_stats.return_value = (0.1, 285.0, 0.60)

        e0_tib = calibrate_info_bar_initial_expected(db, "ETH-USD", "tib")
        e0_trb = calibrate_info_bar_initial_expected(db, "ETH-USD", "trb")
        # TIB: E[T]*bias*c = 100*0.2*1.0 = 20.0
        # TRB: p_same/(1-p_same)*c = 0.6/0.4*1.0 = 1.5
        assert e0_tib != e0_trb
        assert e0_tib == pytest.approx(20.0)
        assert e0_trb == pytest.approx(1.5)

    def test_vrb_uses_volume_contribution(self):
        """VRB E₀ = p_same / (1-p_same) × avg_size."""
        db = MagicMock()
        db.get_trade_volume_stats.return_value = (500_000.0, 50_000.0, 100.0)
        db.get_imbalance_stats.return_value = (0.1, 285.0, 0.55)

        e0 = calibrate_info_bar_initial_expected(db, "ETH-USD", "vrb", bars_per_day=50)
        # P[buy]=0.55 → p_same=0.55, E[run]=0.55/0.45≈1.222, contrib=0.1
        assert e0 == pytest.approx(0.55 / 0.45 * 0.1)

    def test_drb_uses_dollar_contribution(self):
        """DRB E₀ = p_same / (1-p_same) × avg_dollar."""
        db = MagicMock()
        db.get_trade_volume_stats.return_value = (500_000.0, 50_000.0, 100.0)
        db.get_imbalance_stats.return_value = (0.1, 285.0, 0.55)

        e0 = calibrate_info_bar_initial_expected(db, "ETH-USD", "drb", bars_per_day=50)
        # p_same=0.55, E[run]=0.55/0.45≈1.222, contrib=285.0
        assert e0 == pytest.approx(0.55 / 0.45 * 285.0)

    def test_run_bar_p_same_clamped_low(self):
        """p_same is floored at 0.55 for stability."""
        db = MagicMock()
        db.get_trade_volume_stats.return_value = (500_000.0, 50_000.0, 100.0)
        db.get_imbalance_stats.return_value = (0.1, 285.0, 0.50)  # balanced

        e0 = calibrate_info_bar_initial_expected(db, "ETH-USD", "trb")
        # P[buy]=0.50 → p_same=max(0.50,0.50)=0.50, clamped to 0.55
        # E[run]=0.55/0.45≈1.222, contrib=1.0
        assert e0 == pytest.approx(0.55 / 0.45 * 1.0)

    def test_run_bar_p_same_clamped_high(self):
        """p_same is capped at 0.95 to prevent degenerate thresholds."""
        db = MagicMock()
        db.get_trade_volume_stats.return_value = (500_000.0, 50_000.0, 100.0)
        db.get_imbalance_stats.return_value = (0.1, 285.0, 0.99)  # extreme

        e0 = calibrate_info_bar_initial_expected(db, "ETH-USD", "trb")
        # P[buy]=0.99 → p_same=0.99, clamped to 0.95
        # E[run]=0.95/0.05=19.0, contrib=1.0
        assert e0 == pytest.approx(19.0)

    def test_direction_bias_floor(self):
        """When P=0.5 exactly, floor at 0.1 prevents degenerate zero."""
        db = MagicMock()
        db.get_trade_volume_stats.return_value = (500_000.0, 50_000.0, 100.0)
        db.get_imbalance_stats.return_value = (0.1, 285.0, 0.500)

        e0 = calibrate_info_bar_initial_expected(db, "ETH-USD", "tib")
        assert e0 > 0  # should not be zero

    def test_unknown_bar_kind_raises(self):
        db = MagicMock()
        db.get_trade_volume_stats.return_value = (500_000.0, 50_000.0, 100.0)
        db.get_imbalance_stats.return_value = (0.1, 285.0, 0.50)
        with pytest.raises(ValueError, match="Unknown bar kind"):
            calibrate_info_bar_initial_expected(db, "ETH-USD", "xyz")

    def test_no_trade_data_raises(self):
        db = MagicMock()
        db.get_trade_volume_stats.return_value = None
        with pytest.raises(ValueError, match="No trade data"):
            calibrate_info_bar_initial_expected(db, "ETH-USD", "tib")

    def test_no_imbalance_data_raises(self):
        db = MagicMock()
        db.get_trade_volume_stats.return_value = (500_000.0, 50_000.0, 100.0)
        db.get_imbalance_stats.return_value = None
        with pytest.raises(ValueError, match="Insufficient trade data"):
            calibrate_info_bar_initial_expected(db, "ETH-USD", "tib")


class TestFormatEta:
    def test_seconds(self):
        assert _format_eta(45) == "45s"

    def test_minutes(self):
        assert _format_eta(150) == "2.5m"

    def test_hours(self):
        assert _format_eta(7500) == "2h 5m"
