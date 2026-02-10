"""Tests for Coinbase Advanced Trade API client."""

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

from arcana.ingestion.coinbase import CoinbaseSource

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


def _load_fixture() -> dict:
    return json.loads((FIXTURES_DIR / "sample_advanced_trade_response.json").read_text())


def _mock_response(data: dict, status_code: int = 200) -> httpx.Response:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = data
    resp.raise_for_status.return_value = None
    return resp


class TestCoinbaseSource:
    def test_name(self):
        source = CoinbaseSource()
        assert source.name == "coinbase"

    def test_parse_trade(self):
        source = CoinbaseSource()
        raw = {
            "trade_id": "a1b2c3d4-0001",
            "product_id": "ETH-USD",
            "price": "2845.32",
            "size": "0.5",
            "time": "2026-02-10T14:30:01.123Z",
            "side": "BUY",
            "exchange": "COINBASE",
        }
        trade = source._parse_trade(raw, "ETH-USD")

        assert trade.source == "coinbase"
        assert trade.pair == "ETH-USD"
        assert trade.price == Decimal("2845.32")
        assert trade.size == Decimal("0.5")
        assert trade.side == "buy"  # lowercased from "BUY"
        assert trade.trade_id == "a1b2c3d4-0001"
        assert trade.timestamp.year == 2026

    def test_parse_trade_sell_side(self):
        source = CoinbaseSource()
        raw = {
            "trade_id": "x",
            "product_id": "ETH-USD",
            "price": "100",
            "size": "1",
            "time": "2026-01-01T00:00:00Z",
            "side": "SELL",
            "exchange": "COINBASE",
        }
        trade = source._parse_trade(raw, "ETH-USD")
        assert trade.side == "sell"

    def test_fetch_trades_returns_sorted(self):
        fixture = _load_fixture()

        source = CoinbaseSource()
        source._client = MagicMock()
        source._client.get.return_value = _mock_response(fixture)

        trades = source.fetch_trades("ETH-USD")

        assert len(trades) == 20
        for i in range(1, len(trades)):
            assert trades[i].timestamp >= trades[i - 1].timestamp

    def test_fetch_trades_passes_time_params(self):
        fixture = _load_fixture()

        source = CoinbaseSource()
        source._client = MagicMock()
        source._client.get.return_value = _mock_response(fixture)

        start = datetime(2026, 2, 10, 14, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 2, 10, 15, 0, 0, tzinfo=timezone.utc)
        source.fetch_trades("ETH-USD", start=start, end=end, limit=100)

        call_args = source._client.get.call_args
        params = call_args.kwargs.get("params") or call_args[1].get("params")
        assert params["start"] == str(int(start.timestamp()))
        assert params["end"] == str(int(end.timestamp()))
        assert params["limit"] == 100

    def test_fetch_trades_all_have_required_fields(self):
        fixture = _load_fixture()

        source = CoinbaseSource()
        source._client = MagicMock()
        source._client.get.return_value = _mock_response(fixture)

        trades = source.fetch_trades("ETH-USD")

        for trade in trades:
            assert trade.source == "coinbase"
            assert trade.pair == "ETH-USD"
            assert trade.price > 0
            assert trade.size > 0
            assert trade.side in ("buy", "sell")
            assert trade.timestamp.tzinfo is not None

    def test_fetch_trades_empty_response(self):
        source = CoinbaseSource()
        source._client = MagicMock()
        source._client.get.return_value = _mock_response({"trades": []})

        trades = source.fetch_trades("ETH-USD")
        assert trades == []

    def test_fetch_trades_missing_trades_key(self):
        source = CoinbaseSource()
        source._client = MagicMock()
        source._client.get.return_value = _mock_response({})

        trades = source.fetch_trades("ETH-USD")
        assert trades == []

    @patch("arcana.ingestion.coinbase.time_mod.sleep")
    def test_fetch_trades_window(self, mock_sleep):
        """fetch_trades_window should walk forward through time."""
        fixture = _load_fixture()

        source = CoinbaseSource()
        source._client = MagicMock()
        source._client.get.return_value = _mock_response(fixture)

        start = datetime(2026, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 2, 10, 15, 0, 0, tzinfo=timezone.utc)
        window = timedelta(hours=1)

        trades = source.fetch_trades_window("ETH-USD", start, end, window)

        # 3 hours = 3 windows, each returns 20 trades
        assert source._client.get.call_count == 3
        assert len(trades) == 60  # 20 * 3
        # All sorted ascending
        for i in range(1, len(trades)):
            assert trades[i].timestamp >= trades[i - 1].timestamp

    @patch("arcana.ingestion.coinbase.time_mod.sleep")
    def test_request_with_retry_succeeds_after_failure(self, mock_sleep):
        source = CoinbaseSource()
        source._client = MagicMock()

        fail_resp = MagicMock(spec=httpx.Response)
        fail_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "500", request=MagicMock(), response=fail_resp
        )
        ok_resp = _mock_response({"trades": []})

        source._client.get.side_effect = [fail_resp, ok_resp]

        result = source._request_with_retry("/test", {})
        assert result == ok_resp
        assert source._client.get.call_count == 2

    def test_context_manager(self):
        with CoinbaseSource() as source:
            assert source.name == "coinbase"
