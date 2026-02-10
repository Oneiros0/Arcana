"""Trade data models."""

from datetime import datetime, timezone
from decimal import Decimal

from pydantic import BaseModel, Field


class Trade(BaseModel):
    """A single executed trade from an exchange.

    All prices and sizes are stored as Decimal to avoid floating-point
    precision issues — critical for financial data.
    """

    timestamp: datetime = Field(description="Execution time in UTC")
    trade_id: str = Field(description="Exchange-specific trade identifier")
    source: str = Field(description="Data source name, e.g. 'coinbase'")
    pair: str = Field(description="Trading pair, e.g. 'ETH-USD'")
    price: Decimal = Field(description="Execution price in quote currency")
    size: Decimal = Field(description="Execution size in base currency")
    side: str = Field(description="Taker side: 'buy' or 'sell'")

    @property
    def dollar_volume(self) -> Decimal:
        """Price * size — the dollar value of this trade."""
        return self.price * self.size

    @property
    def is_buy(self) -> bool:
        return self.side == "buy"

    def sign(self) -> int:
        """Trade sign for tick rule: +1 for buy, -1 for sell."""
        return 1 if self.is_buy else -1

    model_config = {"frozen": True}
