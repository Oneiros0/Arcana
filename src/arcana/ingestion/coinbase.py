"""Coinbase Exchange API client for fetching raw trade data.

Uses the public Exchange API (api.exchange.coinbase.com) which provides:
- Individual trade-level data without authentication
- Integer trade IDs for reliable cursor-based pagination
- 3 req/s rate limit (public), up to 6 burst
"""

import logging
import time
from datetime import datetime, timezone
from decimal import Decimal

import httpx

from arcana.ingestion.base import DataSource
from arcana.ingestion.models import Trade

logger = logging.getLogger(__name__)

EXCHANGE_BASE_URL = "https://api.exchange.coinbase.com"
DEFAULT_PAGE_SIZE = 100  # Coinbase max per page
RATE_LIMIT_DELAY = 0.35  # ~3 req/s with margin


class CoinbaseSource(DataSource):
    """Fetches trade data from the Coinbase Exchange API.

    The Exchange API (formerly Coinbase Pro) provides public market data
    with cursor-based pagination using integer trade IDs, which makes it
    ideal for reliable historical backfill.

    Note on 'side': The Exchange API reports the *maker* side. We invert
    it to get the taker side, which is the convention for trade sign in
    Prado's bar construction.
    """

    def __init__(self, base_url: str = EXCHANGE_BASE_URL) -> None:
        self._base_url = base_url
        self._client = httpx.Client(
            base_url=base_url,
            headers={"Content-Type": "application/json"},
            timeout=30.0,
        )

    @property
    def name(self) -> str:
        return "coinbase"

    def _invert_side(self, maker_side: str) -> str:
        """Exchange API reports maker side; invert to get taker side."""
        return "buy" if maker_side.lower() == "sell" else "sell"

    def _parse_trade(self, raw: dict, pair: str) -> Trade:
        """Parse a raw JSON trade dict into a Trade model."""
        return Trade(
            timestamp=datetime.fromisoformat(raw["time"].replace("Z", "+00:00")),
            trade_id=str(raw["trade_id"]),
            source=self.name,
            pair=pair,
            price=Decimal(raw["price"]),
            size=Decimal(raw["size"]),
            side=self._invert_side(raw["side"]),
        )

    def fetch_trades(
        self,
        pair: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[Trade]:
        """Fetch trades from Coinbase Exchange API.

        Uses cursor-based pagination with the 'after' parameter to page
        backward through trades (newest to oldest). Returns results
        sorted ascending by timestamp.

        Args:
            pair: Trading pair, e.g. 'ETH-USD'.
            start: If provided, stop fetching once we reach trades before this time.
            end: If provided, skip trades after this time.
            limit: Maximum total trades to return.
        """
        all_trades: list[Trade] = []
        cursor: str | None = None
        fetched = 0

        while fetched < limit:
            page_size = min(DEFAULT_PAGE_SIZE, limit - fetched)
            params: dict[str, str | int] = {"limit": page_size}
            if cursor is not None:
                params["after"] = cursor

            response = self._client.get(f"/products/{pair}/trades", params=params)
            response.raise_for_status()

            raw_trades = response.json()
            if not raw_trades:
                break

            # Parse the page — Exchange API returns newest first
            for raw in raw_trades:
                trade = self._parse_trade(raw, pair)

                if end and trade.timestamp >= end:
                    continue
                if start and trade.timestamp < start:
                    # We've gone past our window, stop entirely
                    return sorted(all_trades, key=lambda t: t.timestamp)

                all_trades.append(trade)
                fetched += 1

                if fetched >= limit:
                    break

            # Cursor for next (older) page — use the last trade_id
            cursor = str(raw_trades[-1]["trade_id"])

            # Respect rate limits
            time.sleep(RATE_LIMIT_DELAY)

            logger.debug(
                "Fetched page: %d trades (total: %d, cursor: %s)",
                len(raw_trades),
                fetched,
                cursor,
            )

        return sorted(all_trades, key=lambda t: t.timestamp)

    def fetch_recent_trades(self, pair: str, limit: int = 100) -> list[Trade]:
        """Convenience method to fetch the most recent trades."""
        return self.fetch_trades(pair=pair, limit=limit)

    def get_supported_pairs(self) -> list[str]:
        """Fetch all available trading pairs from Coinbase."""
        response = self._client.get("/products")
        response.raise_for_status()
        products = response.json()
        return [p["id"] for p in products if p.get("status") == "online"]

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "CoinbaseSource":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
