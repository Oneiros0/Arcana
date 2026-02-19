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
import os
import time as time_mod
from datetime import UTC, datetime
from decimal import Decimal

import httpx

from arcana.ingestion.base import DataSource
from arcana.ingestion.models import Trade

logger = logging.getLogger(__name__)

BASE_URL = "https://api.coinbase.com"
API_PREFIX = "/api/v3/brokerage/market"
DEFAULT_LIMIT = 1000  # API accepts up to 1000; 2500+ returns 500 errors
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
        # Configurable via ARCANA_RATE_DELAY env var (default ~8 req/s)
        self._rate_delay = float(os.environ.get("ARCANA_RATE_DELAY", RATE_LIMIT_DELAY))

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
        """Fetch ALL trades in a time window using backward sequential pagination.

        The Coinbase API returns the most recent trades first. When a single
        call hits the 1000-trade limit, we page backward by moving `end` to
        before the earliest returned trade and fetching again. Each API call
        produces useful data — no wasted probe calls.

        For a window with 25K trades, this makes ~25 calls (25000/1000).

        Args:
            pair: Trading pair, e.g. 'ETH-USD'.
            start: Start of time window (UTC).
            end: End of time window (UTC).

        Returns:
            Complete list of Trade objects in the range, ascending.
        """
        all_trades: list[Trade] = []
        seen_ids: set[str] = set()
        current_end = end
        pages = 0

        while True:
            trades = self.fetch_trades(pair=pair, start=start, end=current_end)
            pages += 1

            # Dedup against already-fetched trades
            new_trades = [t for t in trades if t.trade_id not in seen_ids]
            seen_ids.update(t.trade_id for t in new_trades)
            all_trades.extend(new_trades)

            # Under limit means we got everything in the remaining range
            if len(trades) < DEFAULT_LIMIT:
                break

            if not new_trades:
                logger.warning(
                    "No new trades on page %d for %s [%s → %s] — "
                    "possible duplicate cluster at boundary",
                    pages,
                    pair,
                    start.isoformat(),
                    current_end.isoformat(),
                )
                break

            # Move end backward past the earliest trade we received
            earliest_ts = min(t.timestamp for t in trades)
            earliest_unix = int(earliest_ts.timestamp())
            start_unix = int(start.timestamp())

            if earliest_unix <= start_unix:
                break  # Reached the start boundary

            current_end = datetime.fromtimestamp(earliest_unix, tz=UTC)
            time_mod.sleep(self._rate_delay)

        if pages > 1:
            logger.debug(
                "Paginated %d pages for [%s → %s]: %d trades",
                pages,
                start.strftime("%Y-%m-%d %H:%M"),
                end.strftime("%Y-%m-%d %H:%M"),
                len(all_trades),
            )

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
