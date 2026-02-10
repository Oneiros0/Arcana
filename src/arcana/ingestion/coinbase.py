"""Coinbase Advanced Trade API client for fetching raw trade data.

Uses the public Advanced Trade API (no authentication required):
  GET /api/v3/brokerage/market/products/{product_id}/ticker

Key advantages over the Exchange API:
  - Time-window queries via start/end UNIX timestamps
  - Forward pagination (walk forward through time)
  - Taker side reported directly (no inversion needed)
  - 10 req/s rate limit (public), 30 req/s (authenticated)
"""

import logging
import time as time_mod
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import httpx

from arcana.ingestion.base import DataSource
from arcana.ingestion.models import Trade

logger = logging.getLogger(__name__)

BASE_URL = "https://api.coinbase.com"
API_PREFIX = "/api/v3/brokerage/market"
DEFAULT_LIMIT = 300
RATE_LIMIT_DELAY = 0.12  # ~8 req/s with margin (limit is 10)
MAX_RETRIES = 4
RETRY_BACKOFF = [2, 4, 8, 16]


class CoinbaseSource(DataSource):
    """Fetches trade data from the Coinbase Advanced Trade API.

    Uses the public /market/ endpoints — no API key required.
    Trades are queried by time window (start/end UNIX timestamps),
    making both backfill and incremental ingestion straightforward.

    The 'side' field from this API is the taker side ("BUY"/"SELL"),
    which is the convention needed for Prado's tick rule.
    """

    def __init__(self, base_url: str = BASE_URL) -> None:
        self._base_url = base_url
        self._client = httpx.Client(
            base_url=base_url,
            headers={"Content-Type": "application/json"},
            timeout=30.0,
        )

    @property
    def name(self) -> str:
        return "coinbase"

    def _parse_trade(self, raw: dict, pair: str) -> Trade:
        """Parse a raw Advanced Trade API trade dict into a Trade model."""
        return Trade(
            timestamp=datetime.fromisoformat(raw["time"].replace("Z", "+00:00")),
            trade_id=str(raw["trade_id"]),
            source=self.name,
            pair=pair,
            price=Decimal(raw["price"]),
            size=Decimal(raw["size"]),
            side=raw["side"].lower(),  # API returns "BUY"/"SELL" → "buy"/"sell"
        )

    def _request_with_retry(self, endpoint: str, params: dict) -> httpx.Response:
        """Make an HTTP GET request with exponential backoff on failure."""
        for attempt in range(MAX_RETRIES + 1):
            try:
                response = self._client.get(endpoint, params=params)
                response.raise_for_status()
                return response
            except (httpx.HTTPStatusError, httpx.TransportError) as exc:
                if attempt == MAX_RETRIES:
                    raise
                wait = RETRY_BACKOFF[attempt]
                logger.warning(
                    "Request failed (attempt %d/%d): %s. Retrying in %ds...",
                    attempt + 1,
                    MAX_RETRIES,
                    exc,
                    wait,
                )
                time_mod.sleep(wait)
        raise RuntimeError("Unreachable")  # pragma: no cover

    def fetch_trades(
        self,
        pair: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = DEFAULT_LIMIT,
    ) -> list[Trade]:
        """Fetch trades for a pair within a time window (single API call).

        This may not return all trades if the window contains more than
        `limit` trades. Use fetch_all_trades() for complete data.

        Args:
            pair: Trading pair, e.g. 'ETH-USD'.
            start: Start of time window (UTC). None means no lower bound.
            end: End of time window (UTC). None means now.
            limit: Number of trades to request per API call.

        Returns:
            List of Trade objects, ordered by timestamp ascending.
        """
        endpoint = f"{API_PREFIX}/products/{pair}/ticker"
        params: dict[str, str | int] = {"limit": limit}

        if start is not None:
            params["start"] = str(int(start.timestamp()))
        if end is not None:
            params["end"] = str(int(end.timestamp()))

        response = self._request_with_retry(endpoint, params)
        data = response.json()

        raw_trades = data.get("trades", [])
        trades = [self._parse_trade(raw, pair) for raw in raw_trades]
        return sorted(trades, key=lambda t: t.timestamp)

    def fetch_all_trades(
        self,
        pair: str,
        start: datetime,
        end: datetime,
    ) -> list[Trade]:
        """Fetch ALL trades in a time window, paginating automatically.

        Uses binary subdivision: if a single API call hits the per-request
        limit (300 trades), the window is split in half and each half is
        fetched recursively. This adapts to any traffic volume — busy
        hours get subdivided into smaller windows until all trades fit.

        Args:
            pair: Trading pair, e.g. 'ETH-USD'.
            start: Start of time window (UTC).
            end: End of time window (UTC).

        Returns:
            Complete list of Trade objects in the range, ascending.
        """
        return self._fetch_with_subdivision(pair, start, end, depth=0)

    def _fetch_with_subdivision(
        self,
        pair: str,
        start: datetime,
        end: datetime,
        depth: int,
    ) -> list[Trade]:
        """Recursively subdivide time windows to fetch all trades."""
        MAX_DEPTH = 10  # ~3.5s minimum window from 1-hour start

        trades = self.fetch_trades(pair=pair, start=start, end=end)

        if len(trades) < DEFAULT_LIMIT or depth >= MAX_DEPTH:
            if depth >= MAX_DEPTH and len(trades) >= DEFAULT_LIMIT:
                logger.warning(
                    "Pagination depth limit reached for %s [%s → %s]: "
                    "%d trades returned, some may be missing",
                    pair,
                    start.isoformat(),
                    end.isoformat(),
                    len(trades),
                )
            return trades

        # Window is full — split in half and recurse
        mid = start + (end - start) / 2
        logger.debug(
            "Subdividing window [%s → %s] at depth %d (got %d trades at limit)",
            start.strftime("%H:%M:%S"),
            end.strftime("%H:%M:%S"),
            depth,
            len(trades),
        )

        time_mod.sleep(RATE_LIMIT_DELAY)
        left = self._fetch_with_subdivision(pair, start, mid, depth + 1)
        time_mod.sleep(RATE_LIMIT_DELAY)
        right = self._fetch_with_subdivision(pair, mid, end, depth + 1)

        # Deduplicate by trade_id (boundary trades may appear in both halves)
        seen: set[str] = set()
        result: list[Trade] = []
        for trade in left + right:
            if trade.trade_id not in seen:
                seen.add(trade.trade_id)
                result.append(trade)

        return sorted(result, key=lambda t: t.timestamp)

    def fetch_trades_window(
        self,
        pair: str,
        start: datetime,
        end: datetime,
        window: timedelta = timedelta(hours=1),
    ) -> list[Trade]:
        """Fetch all trades in a range by walking forward through time windows.

        This is the primary method for bulk backfill. It splits the range
        into windows and fetches each one with full pagination, yielding
        a complete set of trades.

        Args:
            pair: Trading pair, e.g. 'ETH-USD'.
            start: Backfill start time (UTC).
            end: Backfill end time (UTC).
            window: Size of each time window to query.

        Returns:
            List of all Trade objects in the range, ascending by timestamp.
        """
        all_trades: list[Trade] = []
        current = start
        total_windows = max(1, int((end - start) / window) + 1)
        completed = 0

        while current < end:
            window_end = min(current + window, end)

            trades = self.fetch_all_trades(
                pair=pair,
                start=current,
                end=window_end,
            )
            all_trades.extend(trades)
            completed += 1

            logger.info(
                "Window %d/%d: %s → %s | %d trades (total: %d)",
                completed,
                total_windows,
                current.strftime("%Y-%m-%d %H:%M"),
                window_end.strftime("%Y-%m-%d %H:%M"),
                len(trades),
                len(all_trades),
            )

            current = window_end
            time_mod.sleep(RATE_LIMIT_DELAY)

        return sorted(all_trades, key=lambda t: t.timestamp)

    def get_supported_pairs(self) -> list[str]:
        """Fetch all available trading pairs from Coinbase."""
        endpoint = f"{API_PREFIX}/products"
        response = self._request_with_retry(endpoint, {})
        products = response.json().get("products", [])
        return [p["product_id"] for p in products if not p.get("is_disabled", False)]

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "CoinbaseSource":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
