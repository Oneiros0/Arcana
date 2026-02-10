"""Abstract base class for data sources."""

from abc import ABC, abstractmethod
from datetime import datetime

from arcana.ingestion.models import Trade


class DataSource(ABC):
    """Interface that all exchange data sources must implement."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Short identifier for this source, e.g. 'coinbase'."""
        ...

    @abstractmethod
    def fetch_trades(
        self,
        pair: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[Trade]:
        """Fetch trades for a given pair within an optional time window.

        Args:
            pair: Trading pair, e.g. 'ETH-USD'.
            start: Inclusive start time (UTC). None means 'as early as possible'.
            end: Exclusive end time (UTC). None means 'up to now'.
            limit: Maximum number of trades to return per call.

        Returns:
            List of Trade objects, ordered by timestamp ascending.
        """
        ...

    @abstractmethod
    def get_supported_pairs(self) -> list[str]:
        """Return a list of trading pairs available from this source."""
        ...
