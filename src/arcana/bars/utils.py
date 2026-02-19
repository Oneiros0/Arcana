"""Shared utilities for information-driven bar builders.

Provides the EWMA estimator (for adaptive thresholds) and the tick rule
(for inferring trade direction from price movement).  These implement the
building blocks described in Prado, *Advances in Financial Machine
Learning*, Ch. 2.
"""

from __future__ import annotations

from decimal import Decimal


class EWMAEstimator:
    """Exponentially Weighted Moving Average for adaptive bar thresholds.

    The EWMA tracks a running expected value that adapts over time.
    Imbalance and run bar builders use it to set their emission threshold:
    when the accumulated imbalance (or run length) exceeds the EWMA
    estimate, a new bar is emitted.

    Uses float arithmetic — this is a statistical estimate, not a
    financial precision calculation.
    """

    __slots__ = ("_window", "_alpha", "_expected")

    def __init__(self, window: int, initial_value: float = 0.0) -> None:
        if window < 1:
            raise ValueError(f"EWMA window must be >= 1, got {window}")
        self._window = window
        self._alpha = 2.0 / (window + 1)
        self._expected = initial_value

    @property
    def expected(self) -> float:
        """Current EWMA estimate."""
        return self._expected

    @property
    def window(self) -> int:
        return self._window

    def update(self, value: float) -> float:
        """Incorporate a new observation and return the updated estimate.

        E[t] = alpha * value + (1 - alpha) * E[t-1]
        """
        self._expected = self._alpha * value + (1.0 - self._alpha) * self._expected
        return self._expected

    def to_dict(self) -> dict:
        """Serialize to a dict for storage in bar metadata JSONB."""
        return {
            "ewma_window": self._window,
            "ewma_expected": self._expected,
        }

    @classmethod
    def from_dict(cls, data: dict) -> EWMAEstimator:
        """Restore from a serialized dict (e.g. bar metadata on restart)."""
        estimator = cls(window=data["ewma_window"])
        estimator._expected = float(data["ewma_expected"])
        return estimator

    def __repr__(self) -> str:
        return (
            f"EWMAEstimator(window={self._window}, "
            f"alpha={self._alpha:.4f}, expected={self._expected:.6f})"
        )


def tick_rule(
    price: Decimal | float,
    prev_price: Decimal | float,
    prev_sign: int,
) -> int:
    """Infer trade direction from price movement (the tick rule).

    Returns:
        +1 if price > prev_price (uptick),
        -1 if price < prev_price (downtick),
        prev_sign if price == prev_price (carry forward).

    This is the standard tick rule from market microstructure:
    when side information is unavailable, the direction of the last
    price change is used as a proxy for trade aggressor.
    """
    if price > prev_price:
        return 1
    elif price < prev_price:
        return -1
    return prev_sign
