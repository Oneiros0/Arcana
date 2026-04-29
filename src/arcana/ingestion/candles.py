"""Historical OHLCV candle fetching and synthesis.

Coinbase's tick-level history is bounded — for older periods only candles
are available via REST. This module models those candles and synthesizes
each one as a single `Trade` so the existing storage and bar-build pipelines
continue to work, while tagging the row's `data_quality` so downstream
consumers can tell candle-derived data apart from real ticks.

Synthesis convention: one Trade per candle, priced at HLC/3 (the typical
price), sized at the candle's volume, side='unknown', timestamped at the
candle's start. We deliberately do NOT fan a candle out to four O/H/L/C
trades — the inter-tick timestamps would be fictional and would mislead
tick/imbalance/run bar builders. The lossiness is explicit; the flag is
the contract that prevents models from training across the boundary.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from pydantic import BaseModel, Field

from arcana.ingestion.models import Trade

# CLI-friendly granularity → Coinbase Advanced Trade API enum + seconds.
GRANULARITY_MAP: dict[str, tuple[str, int]] = {
    "1m": ("ONE_MINUTE", 60),
    "5m": ("FIVE_MINUTE", 300),
    "15m": ("FIFTEEN_MINUTE", 900),
    "30m": ("THIRTY_MINUTE", 1800),
    "1h": ("ONE_HOUR", 3600),
    "2h": ("TWO_HOUR", 7200),
    "6h": ("SIX_HOUR", 21600),
    "1d": ("ONE_DAY", 86400),
}


def granularity_seconds(granularity: str) -> int:
    """Return the bucket size in seconds for a CLI granularity label."""
    if granularity not in GRANULARITY_MAP:
        raise ValueError(
            f"Unknown granularity {granularity!r}; "
            f"expected one of {sorted(GRANULARITY_MAP)}"
        )
    return GRANULARITY_MAP[granularity][1]


def granularity_api_value(granularity: str) -> str:
    """Return the Coinbase API enum value for a CLI granularity label."""
    if granularity not in GRANULARITY_MAP:
        raise ValueError(
            f"Unknown granularity {granularity!r}; "
            f"expected one of {sorted(GRANULARITY_MAP)}"
        )
    return GRANULARITY_MAP[granularity][0]


class Candle(BaseModel):
    """A single OHLCV bucket fetched from a REST candles endpoint."""

    start: datetime = Field(description="Bucket start time in UTC")
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    granularity: str = Field(description="CLI label, e.g. '1m'")
    pair: str
    source: str

    model_config = {"frozen": True}


def candle_to_trade(candle: Candle) -> Trade:
    """Synthesize a single Trade representing a candle.

    Price is the typical price (H+L+C)/3 — a standard close-to-VWAP proxy
    when raw VWAP is unavailable. Side is 'unknown' so the tick rule will
    skip these rows when imbalance/run bars apply directional logic.
    """
    typical_price = (candle.high + candle.low + candle.close) / Decimal(3)
    ts_unix = int(candle.start.timestamp())
    return Trade(
        timestamp=candle.start,
        trade_id=f"candle:{candle.granularity}:{candle.pair}:{ts_unix}",
        source=candle.source,
        pair=candle.pair,
        price=typical_price,
        size=candle.volume,
        side="unknown",
        data_quality=f"candle_{candle.granularity}",
    )


def parse_coinbase_candle(raw: dict, pair: str, granularity: str, source: str) -> Candle:
    """Parse a raw Coinbase Advanced Trade API candle dict into a Candle."""
    start_unix = int(raw["start"])
    return Candle(
        start=datetime.fromtimestamp(start_unix, tz=UTC),
        open=Decimal(raw["open"]),
        high=Decimal(raw["high"]),
        low=Decimal(raw["low"]),
        close=Decimal(raw["close"]),
        volume=Decimal(raw["volume"]),
        granularity=granularity,
        pair=pair,
        source=source,
    )
