"""Trade data models."""

from datetime import datetime
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
    side: str = Field(description="Taker side: 'buy', 'sell', or 'unknown'")

    @property
    def dollar_volume(self) -> Decimal:
        """Price * size — the dollar value of this trade."""
        return self.price * self.size

    @property
    def is_buy(self) -> bool:
        return self.side == "buy"

    def sign(self) -> int:
        """Trade sign: +1 for buy, -1 for sell, 0 for unknown.

        When side is 'unknown', returns 0 to signal that downstream
        consumers (e.g. imbalance bar builders) should apply the tick
        rule to infer direction from price movement.
        """
        if self.side == "buy":
            return 1
        elif self.side == "sell":
            return -1
        return 0

    model_config = {"frozen": True}
