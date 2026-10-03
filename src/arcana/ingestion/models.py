"""Trade data models."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field, field_validator


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
    data_quality: str = Field(
        default="tick",
        description=(
            "Provenance tag. 'tick' = real exchange-reported trade. "
            "'candle_<gran>' = synthesized from an OHLCV candle (e.g. 'candle_1m'). "
            "Bars built across data_quality boundaries are NOT comparable — "
            "downstream consumers must split or filter on this field."
        ),
    )

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


class FutureInstrument(BaseModel):
    """A tradeable futures contract discovered from the products endpoint.

    Captures the identifying + reference metadata for a Coinbase futures
    product (dated or perpetual-style). Price/trade series are ingested via
    the normal trade/candle path keyed on ``product_id``; the time-varying
    risk metrics (funding, open interest, settlement, index) are captured
    separately — see FutureMetrics / the metrics poller.
    """

    product_id: str = Field(description="Full product id, e.g. 'BIT-30OCT26-CDE'")
    source: str = Field(default="coinbase", description="Data source name")
    underlying: str = Field(description="Underlying asset, e.g. 'BTC' (contract_root_unit)")
    contract_expiry_type: str = Field(description="'EXPIRING' or 'PERPETUAL'")
    contract_expiry: datetime | None = Field(
        default=None, description="Settlement time in UTC; None for true perpetuals"
    )
    contract_size: Decimal | None = Field(
        default=None, description="Units of underlying per contract, e.g. 0.01 BTC"
    )
    venue: str = Field(default="", description="Product venue, e.g. 'FCM' or 'INTX'")
    display_name: str = Field(default="", description="Human-readable contract name")
    trading_disabled: bool = Field(
        default=False, description="True when the contract is frozen / not tradeable"
    )

    @field_validator("contract_expiry", mode="before")
    @classmethod
    def _parse_expiry(cls, v: object) -> object:
        """Coerce empty string / 'null' expiry to None; parse ISO 8601 Z form."""
        if v is None or v == "":
            return None
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        return v

    @field_validator("contract_size", mode="before")
    @classmethod
    def _parse_size(cls, v: object) -> object:
        """Coerce empty string to None so missing sizes don't fail validation."""
        if v is None or v == "":
            return None
        return v

    model_config = {"frozen": True}


def _empty_to_none(v: object) -> object:
    """Coerce empty string / 'null' sentinels to None; pass through otherwise."""
    if v is None or v == "":
        return None
    return v


def _parse_iso_or_none(v: object) -> object:
    """Parse an ISO 8601 (Z) timestamp to aware datetime; empty/None → None."""
    if v is None or v == "":
        return None
    if isinstance(v, str):
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    return v


class FutureMetrics(BaseModel):
    """A point-in-time snapshot of a futures contract's risk metrics.

    The Coinbase public API exposes funding/open-interest/index/settlement only
    as *current* values on the product object — there is no historical series —
    so these are captured by polling and stored as time-stamped snapshots. Every
    metric is optional: dated futures carry no funding/index, perps carry no
    settlement, and the INTX perps are frozen. ``raw`` keeps the full
    ``future_product_details`` dict for forward-compatibility.
    """

    observed_at: datetime = Field(description="When this snapshot was polled (UTC)")
    source: str = Field(default="coinbase", description="Data source name")
    pair: str = Field(description="Product id, e.g. 'BIT-30OCT26-CDE'")
    product_type: str = Field(default="FUTURE", description="e.g. 'FUTURE'")
    contract_expiry_type: str = Field(default="", description="'EXPIRING' or 'PERPETUAL'")
    underlying: str = Field(default="", description="contract_root_unit, e.g. 'BTC'")

    contract_expiry: datetime | None = Field(default=None)
    contract_size: Decimal | None = Field(default=None)
    last_price: Decimal | None = Field(default=None, description="Top-level last price")
    volume_24h: Decimal | None = Field(default=None)
    open_interest: Decimal | None = Field(default=None)
    funding_rate: Decimal | None = Field(default=None)
    funding_time: datetime | None = Field(default=None)
    index_price: Decimal | None = Field(default=None)
    settlement_price: Decimal | None = Field(default=None)

    raw: dict | None = Field(
        default=None, description="Full future_product_details dict (JSONB escape hatch)"
    )

    @field_validator(
        "contract_size",
        "last_price",
        "volume_24h",
        "open_interest",
        "funding_rate",
        "index_price",
        "settlement_price",
        mode="before",
    )
    @classmethod
    def _coerce_numeric(cls, v: object) -> object:
        return _empty_to_none(v)

    @field_validator("contract_expiry", "funding_time", mode="before")
    @classmethod
    def _coerce_time(cls, v: object) -> object:
        return _parse_iso_or_none(v)

    model_config = {"frozen": True}
