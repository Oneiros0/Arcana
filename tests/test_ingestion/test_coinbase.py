"""Tests for Coinbase Advanced Trade API client."""

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

from arcana.ingestion.coinbase import CoinbaseSource, DEFAULT_LIMIT

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

        # 3 hours = 3 windows, each returns 20 trades (under limit, no subdivision)
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


class TestFetchAllTrades:
    """Tests for the binary-subdivision pagination logic."""

    @patch("arcana.ingestion.coinbase.time_mod.sleep")
    def test_no_subdivision_when_under_limit(self, mock_sleep):
        """When API returns fewer than DEFAULT_LIMIT, no subdivision occurs."""
        raw = _make_raw_trades(50)
        source = CoinbaseSource()
        source._client = MagicMock()
        source._client.get.return_value = _mock_response({"trades": raw})

        start = datetime(2026, 2, 10, 14, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 2, 10, 15, 0, 0, tzinfo=timezone.utc)

        trades = source.fetch_all_trades("ETH-USD", start, end)

        assert len(trades) == 50
        assert source._client.get.call_count == 1  # single call, no subdivision

    @patch("arcana.ingestion.coinbase.time_mod.sleep")
    def test_subdivides_when_at_limit(self, mock_sleep):
        """When API returns exactly DEFAULT_LIMIT, window is split in half."""
        # First call: returns DEFAULT_LIMIT trades → triggers subdivision
        full_batch = _make_raw_trades(DEFAULT_LIMIT, "2026-02-10T14:00:00Z", prefix="full")
        # Left half: returns under limit → no further subdivision
        left_batch = _make_raw_trades(100, "2026-02-10T14:00:00Z", prefix="left")
        # Right half: returns under limit → no further subdivision
        right_batch = _make_raw_trades(120, "2026-02-10T14:30:00Z", prefix="right")

        source = CoinbaseSource()
        source._client = MagicMock()
        source._client.get.side_effect = [
            _mock_response({"trades": full_batch}),
            _mock_response({"trades": left_batch}),
            _mock_response({"trades": right_batch}),
        ]

        start = datetime(2026, 2, 10, 14, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 2, 10, 15, 0, 0, tzinfo=timezone.utc)

        trades = source.fetch_all_trades("ETH-USD", start, end)

        # 3 API calls: initial full → left half → right half
        assert source._client.get.call_count == 3
        # Trades from both halves are merged (deduped by trade_id)
        assert len(trades) == 220  # 100 + 120 (unique prefixes, no overlap)

    @patch("arcana.ingestion.coinbase.time_mod.sleep")
    def test_deduplicates_boundary_trades(self, mock_sleep):
        """Trades appearing in both halves are deduplicated by trade_id."""
        # Full batch → triggers subdivision
        full_batch = _make_raw_trades(DEFAULT_LIMIT, prefix="full")
        # Both halves share 10 trades at the boundary (same prefix = same trade_ids)
        shared_trades = _make_raw_trades(10, "2026-02-10T14:29:50Z", prefix="shared")
        left_unique = _make_raw_trades(80, "2026-02-10T14:00:00Z", prefix="left")
        right_unique = _make_raw_trades(90, "2026-02-10T14:30:00Z", prefix="right")

        source = CoinbaseSource()
        source._client = MagicMock()
        source._client.get.side_effect = [
            _mock_response({"trades": full_batch}),
            _mock_response({"trades": left_unique + shared_trades}),
            _mock_response({"trades": shared_trades + right_unique}),
        ]

        start = datetime(2026, 2, 10, 14, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 2, 10, 15, 0, 0, tzinfo=timezone.utc)

        trades = source.fetch_all_trades("ETH-USD", start, end)

        # 80 left + 10 shared + 90 right = 180 unique trades
        assert len(trades) == 180
        # Verify no duplicate trade_ids
        trade_ids = [t.trade_id for t in trades]
        assert len(trade_ids) == len(set(trade_ids))

    @patch("arcana.ingestion.coinbase.time_mod.sleep")
    def test_recursive_subdivision_depth(self, mock_sleep):
        """Can subdivide multiple levels deep for very busy periods."""
        # Level 0: full → subdivide
        full_0 = _make_raw_trades(DEFAULT_LIMIT, "2026-02-10T14:00:00Z", prefix="f0")
        # Level 1 left: full → subdivide again
        full_1_left = _make_raw_trades(DEFAULT_LIMIT, "2026-02-10T14:00:00Z", prefix="f1l")
        # Level 1 right: under limit
        partial_1_right = _make_raw_trades(100, "2026-02-10T14:30:00Z", prefix="p1r")
        # Level 2 left-left: under limit
        partial_2_ll = _make_raw_trades(120, "2026-02-10T14:00:00Z", prefix="p2ll")
        # Level 2 left-right: under limit
        partial_2_lr = _make_raw_trades(130, "2026-02-10T14:15:00Z", prefix="p2lr")

        source = CoinbaseSource()
        source._client = MagicMock()
        source._client.get.side_effect = [
            _mock_response({"trades": full_0}),        # depth=0: full window
            _mock_response({"trades": full_1_left}),    # depth=1: left half
            _mock_response({"trades": partial_2_ll}),   # depth=2: left-left quarter
            _mock_response({"trades": partial_2_lr}),   # depth=2: left-right quarter
            _mock_response({"trades": partial_1_right}),  # depth=1: right half
        ]

        start = datetime(2026, 2, 10, 14, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 2, 10, 15, 0, 0, tzinfo=timezone.utc)

        trades = source.fetch_all_trades("ETH-USD", start, end)

        assert source._client.get.call_count == 5
        # All trades deduped and sorted
        assert len(trades) == 350  # 120 + 130 + 100
        for i in range(1, len(trades)):
            assert trades[i].timestamp >= trades[i - 1].timestamp

    @patch("arcana.ingestion.coinbase.time_mod.sleep")
    def test_respects_max_depth(self, mock_sleep):
        """Should stop subdividing at MAX_DEPTH and return what it has."""
        # Always return exactly DEFAULT_LIMIT trades to force maximum recursion
        always_full = _make_raw_trades(DEFAULT_LIMIT)

        source = CoinbaseSource()
        source._client = MagicMock()
        source._client.get.return_value = _mock_response({"trades": always_full})

        start = datetime(2026, 2, 10, 14, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 2, 10, 15, 0, 0, tzinfo=timezone.utc)

        trades = source.fetch_all_trades("ETH-USD", start, end)

        # Should not recurse forever — bounded by MAX_DEPTH (10)
        # At depth 10: 2^10 = 1024 leaf calls + internal calls
        # But with dedup all returning same trade_ids, result is just DEFAULT_LIMIT
        assert len(trades) == DEFAULT_LIMIT
        assert source._client.get.call_count > 1  # subdivision happened

    @patch("arcana.ingestion.coinbase.time_mod.sleep")
    def test_results_sorted_ascending(self, mock_sleep):
        """Output from fetch_all_trades is always sorted ascending by timestamp."""
        full_batch = _make_raw_trades(DEFAULT_LIMIT, "2026-02-10T14:00:00Z", prefix="full")
        left = _make_raw_trades(50, "2026-02-10T14:00:00Z", prefix="left")
        right = _make_raw_trades(60, "2026-02-10T14:30:00Z", prefix="right")

        source = CoinbaseSource()
        source._client = MagicMock()
        source._client.get.side_effect = [
            _mock_response({"trades": full_batch}),
            _mock_response({"trades": left}),
            _mock_response({"trades": right}),
        ]

        start = datetime(2026, 2, 10, 14, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 2, 10, 15, 0, 0, tzinfo=timezone.utc)

        trades = source.fetch_all_trades("ETH-USD", start, end)

        for i in range(1, len(trades)):
            assert trades[i].timestamp >= trades[i - 1].timestamp
