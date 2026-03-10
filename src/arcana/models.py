"""Shared data models for the Arcana ecosystem."""

from datetime import datetime, timedelta
from decimal import Decimal

from pydantic import BaseModel, Field


class Bar(BaseModel):
    """A single completed bar with OHLCV and auxiliary data.

    The bar_type field is used in-memory for routing to the correct
    per-type table (e.g. bars_tick_500) but is not stored as a column.
    """

    time_start: datetime = Field(description="Timestamp of the first trade in the bar")
    time_end: datetime = Field(description="Timestamp of the last trade in the bar")
    bar_type: str = Field(description="Bar type label, e.g. 'tick_500', 'time_5m'")
    source: str = Field(description="Data source, e.g. 'coinbase'")
    pair: str = Field(description="Trading pair, e.g. 'ETH-USD'")
    open: Decimal = Field(description="Price of the first trade")
    high: Decimal = Field(description="Highest price in the bar")
    low: Decimal = Field(description="Lowest price in the bar")
    close: Decimal = Field(description="Price of the last trade")
    vwap: Decimal = Field(description="Volume-weighted average price")
    volume: Decimal = Field(description="Total volume in base currency")
    dollar_volume: Decimal = Field(description="Total volume in quote currency")
    tick_count: int = Field(description="Number of trades in the bar")
    time_span: timedelta = Field(description="Duration from first to last trade")
    metadata: dict | None = Field(default=None, description="Bar-specific extra info")

    model_config = {"frozen": True}
