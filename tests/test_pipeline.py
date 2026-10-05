"""Tests for the ingestion pipeline."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from arcana.ingestion.candles import Candle
from arcana.ingestion.coinbase import CoinbaseSource
from arcana.ingestion.models import FutureMetrics, Trade
from arcana.pipeline import (
    _format_eta,
    backfill_candles,
    ingest_backfill,
    poll_metrics,
    run_daemon,
    snapshot_metrics_once,
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


def _make_candles(
    count: int, start_ts: datetime | None = None, granularity: str = "1m"
) -> list[Candle]:
    """Generate a list of fake candles for testing."""
    base = start_ts or datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    bucket = timedelta(minutes=1)
    return [
        Candle(
            start=base + bucket * i,
            open=Decimal("100"),
            high=Decimal("110"),
            low=Decimal("90"),
            close=Decimal("105"),
            volume=Decimal("1.0"),
            granularity=granularity,
            pair="ETH-USD",
            source="coinbase",
        )
        for i in range(count)
    ]


class TestBackfillCandles:
    @patch("arcana.pipeline.GracefulShutdown")
    @patch("arcana.pipeline.time_mod.sleep")
    def test_synthesizes_and_inserts_with_quality_tag(self, mock_sleep, mock_shutdown):
        mock_shutdown.return_value.should_stop = False
        candles = _make_candles(5)

        source = MagicMock(spec=CoinbaseSource)
        source.name = "coinbase"
        source.fetch_candles.return_value = candles

        db = MagicMock()
        db.get_last_timestamp.return_value = None
        db.insert_trades.side_effect = lambda batch: len(batch)

        since = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        until = datetime(2024, 1, 1, 0, 5, 0, tzinfo=UTC)
        total = backfill_candles(source, db, "ETH-USD", since=since, until=until, granularity="1m")

        assert total == 5
        # Inserted trades all carry the candle_1m tag
        inserted = db.insert_trades.call_args[0][0]
        assert all(t.data_quality == "candle_1m" for t in inserted)
        assert all(t.side == "unknown" for t in inserted)

    @patch("arcana.pipeline.GracefulShutdown")
    @patch("arcana.pipeline.time_mod.sleep")
    def test_resume_filters_by_data_quality(self, mock_sleep, mock_shutdown):
        """Resume must only consider candle rows, not tick rows in the same range."""
        mock_shutdown.return_value.should_stop = False

        source = MagicMock(spec=CoinbaseSource)
        source.name = "coinbase"
        source.fetch_candles.return_value = []

        db = MagicMock()
        db.get_last_timestamp.return_value = None
        db.insert_trades.return_value = 0

        since = datetime(2024, 1, 1, tzinfo=UTC)
        until = datetime(2024, 1, 1, 0, 30, 0, tzinfo=UTC)
        backfill_candles(source, db, "ETH-USD", since=since, until=until, granularity="1m")

        db.get_last_timestamp.assert_called_once_with(
            "ETH-USD", "coinbase", before=until, data_quality="candle_1m"
        )

    @patch("arcana.pipeline.GracefulShutdown")
    @patch("arcana.pipeline.time_mod.sleep")
    def test_resume_advances_past_last_candle(self, mock_sleep, mock_shutdown):
        """Resume from last_ts + one bucket so we don't refetch the boundary candle."""
        mock_shutdown.return_value.should_stop = False

        source = MagicMock(spec=CoinbaseSource)
        source.name = "coinbase"
        source.fetch_candles.return_value = []

        last = datetime(2024, 1, 1, 0, 10, 0, tzinfo=UTC)
        db = MagicMock()
        db.get_last_timestamp.return_value = last
        db.insert_trades.return_value = 0

        since = datetime(2024, 1, 1, tzinfo=UTC)
        until = datetime(2024, 1, 1, 0, 30, 0, tzinfo=UTC)
        backfill_candles(source, db, "ETH-USD", since=since, until=until, granularity="1m")

        first_call = source.fetch_candles.call_args_list[0]
        # First fetch should start one minute past `last`, not at `last`
        assert first_call.kwargs["start"] == last + timedelta(minutes=1)

    @patch("arcana.pipeline.GracefulShutdown")
    @patch("arcana.pipeline.time_mod.sleep")
    def test_no_op_when_range_already_covered(self, mock_sleep, mock_shutdown):
        mock_shutdown.return_value.should_stop = False

        source = MagicMock(spec=CoinbaseSource)
        source.name = "coinbase"

        db = MagicMock()
        # Simulate prior backfill already reached the requested end
        db.get_last_timestamp.return_value = datetime(2024, 1, 1, 0, 30, 0, tzinfo=UTC)

        since = datetime(2024, 1, 1, tzinfo=UTC)
        until = datetime(2024, 1, 1, 0, 30, 0, tzinfo=UTC)
        total = backfill_candles(source, db, "ETH-USD", since=since, until=until, granularity="1m")

        assert total == 0
        source.fetch_candles.assert_not_called()


def _make_metrics(pair: str, observed: datetime | None = None) -> FutureMetrics:
    return FutureMetrics(
        observed_at=observed or datetime(2026, 10, 3, 19, 0, 0, tzinfo=UTC),
        pair=pair,
        underlying="BTC",
        contract_expiry_type="EXPIRING",
        last_price=Decimal("85170"),
        open_interest=Decimal("118023"),
        settlement_price=Decimal("84775"),
    )


class TestSnapshotMetricsOnce:
    def test_snapshots_each_pair(self):
        source = MagicMock(spec=CoinbaseSource)
        source.fetch_future_metrics.side_effect = lambda p: _make_metrics(p)
        db = MagicMock()
        db.insert_future_metrics.return_value = 2

        inserted = snapshot_metrics_once(source, db, ["BIT-30OCT26-CDE", "ET-30OCT26-CDE"])

        assert inserted == 2
        assert source.fetch_future_metrics.call_count == 2
        stored = db.insert_future_metrics.call_args[0][0]
        assert {m.pair for m in stored} == {"BIT-30OCT26-CDE", "ET-30OCT26-CDE"}

    def test_one_pair_failure_does_not_abort_others(self):
        source = MagicMock(spec=CoinbaseSource)

        def _fetch(p: str) -> FutureMetrics:
            if p == "BAD":
                raise RuntimeError("boom")
            return _make_metrics(p)

        source.fetch_future_metrics.side_effect = _fetch
        db = MagicMock()
        db.insert_future_metrics.return_value = 1

        inserted = snapshot_metrics_once(source, db, ["BAD", "BIT-30OCT26-CDE"])

        assert inserted == 1
        stored = db.insert_future_metrics.call_args[0][0]
        assert [m.pair for m in stored] == ["BIT-30OCT26-CDE"]

    def test_all_failures_insert_not_called(self):
        source = MagicMock(spec=CoinbaseSource)
        source.fetch_future_metrics.side_effect = RuntimeError("down")
        db = MagicMock()

        inserted = snapshot_metrics_once(source, db, ["BIT-30OCT26-CDE"])

        assert inserted == 0
        db.insert_future_metrics.assert_not_called()


class TestPollMetrics:
    @patch("arcana.pipeline.GracefulShutdown")
    @patch("arcana.pipeline.time_mod.sleep")
    def test_runs_max_cycles_then_stops(self, mock_sleep, mock_shutdown):
        mock_shutdown.return_value.should_stop = False
        source = MagicMock(spec=CoinbaseSource)
        source.fetch_future_metrics.side_effect = lambda p: _make_metrics(p)
        db = MagicMock()
        db.insert_future_metrics.return_value = 1

        total = poll_metrics(source, db, ["BIT-30OCT26-CDE"], interval=60, max_cycles=3)

        assert total == 3
        assert db.insert_future_metrics.call_count == 3

    @patch("arcana.pipeline.GracefulShutdown")
    @patch("arcana.pipeline.time_mod.sleep")
    def test_stops_on_shutdown_flag(self, mock_sleep, mock_shutdown):
        # should_stop True from the start → zero cycles
        mock_shutdown.return_value.should_stop = True
        source = MagicMock(spec=CoinbaseSource)
        db = MagicMock()

        total = poll_metrics(source, db, ["BIT-30OCT26-CDE"], interval=60)

        assert total == 0
        source.fetch_future_metrics.assert_not_called()

    @patch("arcana.pipeline.GracefulShutdown")
    @patch("arcana.pipeline.time_mod.sleep")
    def test_cycle_exception_is_swallowed(self, mock_sleep, mock_shutdown):
        mock_shutdown.return_value.should_stop = False
        source = MagicMock(spec=CoinbaseSource)
        db = MagicMock()
        # snapshot_metrics_once catches per-pair errors, but guard the loop too:
        db.insert_future_metrics.side_effect = RuntimeError("db down")
        source.fetch_future_metrics.side_effect = lambda p: _make_metrics(p)

        # Should not raise despite the DB error each cycle.
        total = poll_metrics(source, db, ["BIT-30OCT26-CDE"], interval=1, max_cycles=2)
        assert total == 0
