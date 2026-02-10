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

        This is a single-request method — may not return all trades if the
        source has a per-request limit.

        Args:
            pair: Trading pair, e.g. 'ETH-USD'.
            start: Inclusive start time (UTC). None means 'as early as possible'.
            end: Exclusive end time (UTC). None means 'up to now'.
            limit: Maximum number of trades to return per call.

        Returns:
            List of Trade objects, ordered by timestamp ascending.
        """
        ...

    def fetch_all_trades(
        self,
        pair: str,
        start: datetime,
        end: datetime,
    ) -> list[Trade]:
        """Fetch ALL trades in a time window, handling pagination automatically.

        Subclasses should override this if the source has per-request limits
        that require multiple calls to retrieve complete data.

        Args:
            pair: Trading pair, e.g. 'ETH-USD'.
            start: Start of time window (UTC).
            end: End of time window (UTC).

        Returns:
            List of all Trade objects in the range, ascending by timestamp.
        """
        return self.fetch_trades(pair=pair, start=start, end=end)

    @abstractmethod
    def get_supported_pairs(self) -> list[str]:
        """Return a list of trading pairs available from this source."""
        ...
