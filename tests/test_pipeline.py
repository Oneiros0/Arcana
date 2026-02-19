"""Tests for the ingestion pipeline."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from arcana.bars.standard import TickBarBuilder
from arcana.ingestion.coinbase import CoinbaseSource
from arcana.ingestion.models import Trade
from arcana.pipeline import _format_eta, build_bars, ingest_backfill, run_daemon

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


class TestFormatEta:
    def test_seconds(self):
        assert _format_eta(45) == "45s"

    def test_minutes(self):
        assert _format_eta(150) == "2.5m"

    def test_hours(self):
        assert _format_eta(7500) == "2h 5m"
