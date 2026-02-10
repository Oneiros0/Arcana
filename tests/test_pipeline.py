"""Tests for the ingestion pipeline."""

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from arcana.ingestion.coinbase import CoinbaseSource
from arcana.ingestion.models import Trade
from arcana.pipeline import BATCH_SIZE, _format_eta, ingest_backfill

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _make_trades(count: int, start_ts: datetime | None = None) -> list[Trade]:
    """Generate a list of fake trades for testing."""
    base = start_ts or datetime(2026, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
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
        source.fetch_trades.return_value = trades
        source._client = True  # just needs to be truthy

        db = MagicMock()
        db.get_last_timestamp.return_value = None
        db.insert_trades.return_value = 10

        since = datetime(2026, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
        total = ingest_backfill(source, db, "ETH-USD", since)

        assert total == 10
        assert db.insert_trades.called
        assert source.fetch_trades.called

    @patch("arcana.pipeline.GracefulShutdown")
    @patch("arcana.pipeline.time_mod.sleep")
    def test_backfill_resumes_from_last_timestamp(self, mock_sleep, mock_shutdown):
        """If data exists, backfill should resume from last stored timestamp."""
        mock_shutdown.return_value.should_stop = False

        source = MagicMock(spec=CoinbaseSource)
        source.name = "coinbase"
        source.fetch_trades.return_value = []
        source._client = True

        last = datetime(2026, 2, 10, 14, 0, 0, tzinfo=timezone.utc)
        db = MagicMock()
        db.get_last_timestamp.return_value = last
        db.insert_trades.return_value = 0

        since = datetime(2026, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
        ingest_backfill(source, db, "ETH-USD", since)

        # The first fetch_trades call should start from last, not since
        first_call = source.fetch_trades.call_args_list[0]
        assert first_call.kwargs["start"] == last

    @patch("arcana.pipeline.GracefulShutdown")
    @patch("arcana.pipeline.time_mod.sleep")
    def test_backfill_checkpoints_in_batches(self, mock_sleep, mock_shutdown):
        """Trades should be committed in batches of BATCH_SIZE."""
        mock_shutdown.return_value.should_stop = False

        # Return enough trades to trigger at least one checkpoint
        trades = _make_trades(600)

        source = MagicMock(spec=CoinbaseSource)
        source.name = "coinbase"
        source.fetch_trades.return_value = trades
        source._client = True

        db = MagicMock()
        db.get_last_timestamp.return_value = None
        db.insert_trades.return_value = 600

        since = datetime(2026, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
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
        source.fetch_trades.side_effect = toggle_shutdown
        source._client = True

        db = MagicMock()
        db.get_last_timestamp.return_value = None
        db.insert_trades.return_value = 5

        since = datetime(2026, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
        total = ingest_backfill(source, db, "ETH-USD", since)

        # Should have committed the buffer before stopping
        assert db.insert_trades.called


class TestFormatEta:
    def test_seconds(self):
        assert _format_eta(45) == "45s"

    def test_minutes(self):
        assert _format_eta(150) == "2.5m"

    def test_hours(self):
        assert _format_eta(7500) == "2h 5m"
