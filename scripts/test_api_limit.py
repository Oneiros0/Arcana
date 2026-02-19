"""Test what limit values the Coinbase Advanced Trade API actually accepts.

Run from your venv:
    python scripts/test_api_limit.py

Tests limit=5000, 2500, 1000, 300 against a known busy hour
and reports how many trades each returns.
"""

import time
from datetime import UTC, datetime

from arcana.ingestion.coinbase import CoinbaseSource

# A busy trading hour — Feb 5 2025, 14:00-15:00 UTC
START = datetime(2025, 2, 5, 14, 0, 0, tzinfo=UTC)
END = datetime(2025, 2, 5, 15, 0, 0, tzinfo=UTC)
LIMITS = [5000, 2500, 1000, 300]


def main() -> None:
    source = CoinbaseSource()

    print("Testing Coinbase API limit parameter")
    print(f"Window: {START.isoformat()} → {END.isoformat()}")
    print(f"{'limit':>8s} │ {'returned':>8s} │ notes")
    print(f"{'─' * 8} │ {'─' * 8} │ {'─' * 40}")

    results = []
    for limit in LIMITS:
        try:
            trades = source.fetch_trades("ETH-USD", start=START, end=END, limit=limit)
            count = len(trades)
            if count == limit:
                note = "← at limit (may be capped)"
            elif count < limit:
                note = "← under limit (all trades in window)"
            else:
                note = "← exceeded limit??"
            print(f"{limit:>8d} │ {count:>8d} │ {note}")
            results.append((limit, count))
        except Exception as e:
            print(f"{limit:>8d} │ {'ERROR':>8s} │ {e}")
            results.append((limit, -1))

        time.sleep(0.15)  # rate limit

    # Analysis
    print()
    counts = [c for _, c in results if c > 0]
    if len(set(counts)) == 1:
        print(f"All limits returned {counts[0]} trades — API caps at {counts[0]}.")
    elif counts and max(counts) > 300:
        best = max(counts)
        best_limit = [lim for lim, c in results if c == best][0]
        print(f"Higher limits work! Best: limit={best_limit} → {best} trades.")
        print(f"This is {best / 300:.1f}x more trades per request than limit=300.")
    else:
        print("Could not determine if higher limits are supported.")


if __name__ == "__main__":
    main()
