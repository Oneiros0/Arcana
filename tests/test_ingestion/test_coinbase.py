"""Tests for Coinbase Exchange API client."""

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

from arcana.ingestion.coinbase import CoinbaseSource

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


def _load_exchange_fixture() -> list[dict]:
    return json.loads((FIXTURES_DIR / "sample_exchange_trades.json").read_text())


def _mock_response(data: list[dict], status_code: int = 200) -> httpx.Response:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = data
    resp.raise_for_status.return_value = None
    return resp


class TestCoinbaseSource:
    def test_name(self):
        source = CoinbaseSource()
        assert source.name == "coinbase"

    def test_invert_side(self):
        source = CoinbaseSource()
        assert source._invert_side("sell") == "buy"
        assert source._invert_side("buy") == "sell"
        assert source._invert_side("SELL") == "buy"
        assert source._invert_side("BUY") == "sell"

    def test_parse_trade(self):
        source = CoinbaseSource()
        raw = {
            "time": "2026-02-10T14:30:01.123Z",
            "trade_id": 98000050,
            "price": "2845.32",
            "size": "0.5",
            "side": "sell",
        }
        trade = source._parse_trade(raw, "ETH-USD")

        assert trade.source == "coinbase"
        assert trade.pair == "ETH-USD"
        assert trade.price == Decimal("2845.32")
        assert trade.size == Decimal("0.5")
        assert trade.side == "buy"  # inverted from maker "sell"
        assert trade.trade_id == "98000050"
        assert trade.timestamp.year == 2026

    @patch("arcana.ingestion.coinbase.time.sleep")
    def test_fetch_trades_returns_sorted(self, mock_sleep):
        """Trades should come back sorted ascending by timestamp."""
        fixture = _load_exchange_fixture()

        source = CoinbaseSource()
        source._client = MagicMock()
        source._client.get.return_value = _mock_response(fixture)

        trades = source.fetch_trades("ETH-USD", limit=20)

        assert len(trades) == 20
        # Verify ascending timestamp order
        for i in range(1, len(trades)):
            assert trades[i].timestamp >= trades[i - 1].timestamp

    @patch("arcana.ingestion.coinbase.time.sleep")
    def test_fetch_trades_respects_limit(self, mock_sleep):
        fixture = _load_exchange_fixture()

        source = CoinbaseSource()
        source._client = MagicMock()
        source._client.get.return_value = _mock_response(fixture)

        trades = source.fetch_trades("ETH-USD", limit=5)
        assert len(trades) == 5

    @patch("arcana.ingestion.coinbase.time.sleep")
    def test_fetch_trades_all_have_required_fields(self, mock_sleep):
        fixture = _load_exchange_fixture()

        source = CoinbaseSource()
        source._client = MagicMock()
        source._client.get.return_value = _mock_response(fixture)

        trades = source.fetch_trades("ETH-USD", limit=10)

        for trade in trades:
            assert trade.source == "coinbase"
            assert trade.pair == "ETH-USD"
            assert trade.price > 0
            assert trade.size > 0
            assert trade.side in ("buy", "sell")
            assert trade.timestamp.tzinfo is not None

    @patch("arcana.ingestion.coinbase.time.sleep")
    def test_fetch_trades_empty_response(self, mock_sleep):
        source = CoinbaseSource()
        source._client = MagicMock()
        source._client.get.return_value = _mock_response([])

        trades = source.fetch_trades("ETH-USD", limit=100)
        assert trades == []

    def test_context_manager(self):
        with CoinbaseSource() as source:
            assert source.name == "coinbase"
