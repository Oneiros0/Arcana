"""Tests for Coinbase Advanced Trade API client."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx

from arcana.ingestion.coinbase import DEFAULT_LIMIT, CoinbaseSource

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


def _load_fixture() -> dict:
    return json.loads((FIXTURES_DIR / "sample_advanced_trade_response.json").read_text())


def _mock_response(data: dict, status_code: int = 200) -> httpx.Response:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = data
    resp.raise_for_status.return_value = None
    return resp


def _make_raw_trades(
    count: int,
    start_time: str = "2026-02-10T14:00:00Z",
    prefix: str = "test",
) -> list[dict]:
    """Generate raw API-format trade dicts for testing."""
    base = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
    return [
        {
            "trade_id": f"{prefix}-{i:06d}",
            "product_id": "ETH-USD",
            "price": "2845.50",
            "size": "0.1",
            "time": (base + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "side": "BUY" if i % 2 == 0 else "SELL",
            "exchange": "COINBASE",
        }
        for i in range(count)
    ]


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

        start = datetime(2026, 2, 10, 14, 0, 0, tzinfo=UTC)
        end = datetime(2026, 2, 10, 15, 0, 0, tzinfo=UTC)
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


class TestFetchAllTrades:
    """Tests for backward sequential pagination logic."""

    @patch("arcana.ingestion.coinbase.time_mod.sleep")
    def test_single_page_under_limit(self, mock_sleep):
        """When API returns fewer than DEFAULT_LIMIT, no pagination needed."""
        raw = _make_raw_trades(50)
        source = CoinbaseSource()
        source._client = MagicMock()
        source._client.get.return_value = _mock_response({"trades": raw})

        start = datetime(2026, 2, 10, 14, 0, 0, tzinfo=UTC)
        end = datetime(2026, 2, 10, 15, 0, 0, tzinfo=UTC)

        trades = source.fetch_all_trades("ETH-USD", start, end)

        assert len(trades) == 50
        assert source._client.get.call_count == 1

    @patch("arcana.ingestion.coinbase.time_mod.sleep")
    def test_pages_backward_when_at_limit(self, mock_sleep):
        """When API returns DEFAULT_LIMIT, should page backward for more."""
        # Page 1: newest trades at limit (14:55:00+)
        page1 = _make_raw_trades(DEFAULT_LIMIT, "2026-02-10T14:55:00Z", prefix="p1")
        # Page 2: older trades, under limit = done
        page2 = _make_raw_trades(200, "2026-02-10T14:50:00Z", prefix="p2")

        source = CoinbaseSource()
        source._client = MagicMock()
        source._client.get.side_effect = [
            _mock_response({"trades": page1}),
            _mock_response({"trades": page2}),
        ]

        start = datetime(2026, 2, 10, 14, 0, 0, tzinfo=UTC)
        end = datetime(2026, 2, 10, 15, 0, 0, tzinfo=UTC)

        trades = source.fetch_all_trades("ETH-USD", start, end)

        assert source._client.get.call_count == 2
        assert len(trades) == DEFAULT_LIMIT + 200

    @patch("arcana.ingestion.coinbase.time_mod.sleep")
    def test_multiple_pages(self, mock_sleep):
        """Pages backward through 3 pages to collect all trades."""
        page1 = _make_raw_trades(DEFAULT_LIMIT, "2026-02-10T14:50:00Z", prefix="p1")
        page2 = _make_raw_trades(DEFAULT_LIMIT, "2026-02-10T14:40:00Z", prefix="p2")
        page3 = _make_raw_trades(150, "2026-02-10T14:30:00Z", prefix="p3")

        source = CoinbaseSource()
        source._client = MagicMock()
        source._client.get.side_effect = [
            _mock_response({"trades": page1}),
            _mock_response({"trades": page2}),
            _mock_response({"trades": page3}),
        ]

        start = datetime(2026, 2, 10, 14, 0, 0, tzinfo=UTC)
        end = datetime(2026, 2, 10, 15, 0, 0, tzinfo=UTC)

        trades = source.fetch_all_trades("ETH-USD", start, end)

        assert source._client.get.call_count == 3
        assert len(trades) == DEFAULT_LIMIT * 2 + 150
        # All sorted ascending
        for i in range(1, len(trades)):
            assert trades[i].timestamp >= trades[i - 1].timestamp

    @patch("arcana.ingestion.coinbase.time_mod.sleep")
    def test_deduplicates_boundary_trades(self, mock_sleep):
        """Trades at page boundaries are deduplicated by trade_id."""
        # 10 trades at the boundary appear in both pages (same prefix)
        boundary = _make_raw_trades(10, "2026-02-10T14:50:00Z", prefix="shared")
        newer = _make_raw_trades(DEFAULT_LIMIT - 10, "2026-02-10T14:50:10Z", prefix="p1")
        older = _make_raw_trades(80, "2026-02-10T14:40:00Z", prefix="p2")

        source = CoinbaseSource()
        source._client = MagicMock()
        source._client.get.side_effect = [
            _mock_response({"trades": newer + boundary}),  # page 1: at limit
            _mock_response({"trades": boundary + older}),   # page 2: overlap + older
        ]

        start = datetime(2026, 2, 10, 14, 0, 0, tzinfo=UTC)
        end = datetime(2026, 2, 10, 15, 0, 0, tzinfo=UTC)

        trades = source.fetch_all_trades("ETH-USD", start, end)

        # (DEFAULT_LIMIT - 10) newer + 10 shared + 80 older = unique
        assert len(trades) == DEFAULT_LIMIT - 10 + 10 + 80
        trade_ids = [t.trade_id for t in trades]
        assert len(trade_ids) == len(set(trade_ids))

    @patch("arcana.ingestion.coinbase.time_mod.sleep")
    def test_stops_on_no_progress(self, mock_sleep):
        """Stops if all returned trades are duplicates (no new data)."""
        # Trades in the middle of the window — not at the start boundary,
        # so backward pagination will try another page
        same_trades = _make_raw_trades(DEFAULT_LIMIT, "2026-02-10T14:30:00Z")

        source = CoinbaseSource()
        source._client = MagicMock()
        # Always returns the same trades — second page is all dupes
        source._client.get.return_value = _mock_response({"trades": same_trades})

        start = datetime(2026, 2, 10, 14, 0, 0, tzinfo=UTC)
        end = datetime(2026, 2, 10, 15, 0, 0, tzinfo=UTC)

        trades = source.fetch_all_trades("ETH-USD", start, end)

        assert len(trades) == DEFAULT_LIMIT
        # Should stop after 2 calls (first gets data, second is all dupes)
        assert source._client.get.call_count == 2

    @patch("arcana.ingestion.coinbase.time_mod.sleep")
    def test_results_sorted_ascending(self, mock_sleep):
        """Output is always sorted ascending by timestamp."""
        # Pages come in reverse chronological order but output should be ascending
        page1 = _make_raw_trades(DEFAULT_LIMIT, "2026-02-10T14:50:00Z", prefix="p1")
        page2 = _make_raw_trades(100, "2026-02-10T14:00:00Z", prefix="p2")

        source = CoinbaseSource()
        source._client = MagicMock()
        source._client.get.side_effect = [
            _mock_response({"trades": page1}),
            _mock_response({"trades": page2}),
        ]

        start = datetime(2026, 2, 10, 14, 0, 0, tzinfo=UTC)
        end = datetime(2026, 2, 10, 15, 0, 0, tzinfo=UTC)

        trades = source.fetch_all_trades("ETH-USD", start, end)

        for i in range(1, len(trades)):
            assert trades[i].timestamp >= trades[i - 1].timestamp
