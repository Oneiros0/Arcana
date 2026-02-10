#!/usr/bin/env python3
"""Exploration script — fetch trades from Coinbase and analyze the response.

Run: python scripts/explore_coinbase.py [--live]

Without --live, uses sample fixture data (works offline).
With --live, hits the real Coinbase Exchange API.
"""

import json
import statistics
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

# Add src to path for development
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from arcana.ingestion.coinbase import CoinbaseSource
from arcana.ingestion.models import Trade

PAIR = "ETH-USD"
FIXTURES_DIR = Path(__file__).parent.parent / "tests" / "fixtures"


def section(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}\n")


def load_sample_trades() -> list[Trade]:
    """Load trades from the sample Exchange API fixture."""
    raw_path = FIXTURES_DIR / "sample_exchange_trades.json"
    raw_trades = json.loads(raw_path.read_text())

    trades = []
    for raw in raw_trades:
        # Exchange API reports maker side — invert for taker side
        taker_side = "buy" if raw["side"] == "sell" else "sell"
        trades.append(
            Trade(
                timestamp=datetime.fromisoformat(raw["time"].replace("Z", "+00:00")),
                trade_id=str(raw["trade_id"]),
                source="coinbase",
                pair=PAIR,
                price=Decimal(raw["price"]),
                size=Decimal(raw["size"]),
                side=taker_side,
            )
        )
    return sorted(trades, key=lambda t: t.timestamp)


def show_raw_api_shapes() -> None:
    """Show both API response formats from fixture files."""
    section("1. RAW API RESPONSE SHAPES")

    # --- Exchange API (api.exchange.coinbase.com) ---
    print("EXCHANGE API: GET /products/ETH-USD/trades")
    print("  Returns: JSON array (flat list)\n")

    exchange_path = FIXTURES_DIR / "sample_exchange_trades.json"
    exchange_raw = json.loads(exchange_path.read_text())

    print(f"  Response type: list, length: {len(exchange_raw)}")
    print(f"  First trade:")
    print(json.dumps(exchange_raw[0], indent=4))
    print(f"\n  Fields: {list(exchange_raw[0].keys())}")
    print(f"  Field types:")
    for k, v in exchange_raw[0].items():
        print(f"    {k}: {type(v).__name__} = {v!r}")

    # --- Advanced Trade API ---
    print(f"\n{'-' * 40}\n")
    print("ADVANCED TRADE API: GET /api/v3/brokerage/market/products/ETH-USD/ticker")
    print("  Returns: JSON object with 'trades' array + bid/ask\n")

    adv_path = FIXTURES_DIR / "sample_advanced_trade_response.json"
    adv_raw = json.loads(adv_path.read_text())

    print(f"  Response type: dict, top-level keys: {list(adv_raw.keys())}")
    print(f"  Trades count: {len(adv_raw['trades'])}")
    print(f"  First trade:")
    print(json.dumps(adv_raw["trades"][0], indent=4))
    print(f"\n  Trade fields: {list(adv_raw['trades'][0].keys())}")
    print(f"  Extra fields: best_bid={adv_raw['best_bid']}, best_ask={adv_raw['best_ask']}")

    section("2. KEY DIFFERENCES BETWEEN APIS")

    print("""  Exchange API (recommended for backfill):
    - Flat JSON array response
    - trade_id is INTEGER (monotonically increasing — great for pagination)
    - side is LOWERCASE ("buy"/"sell") and means MAKER side
    - No product_id/exchange fields in response
    - Cursor pagination via CB-BEFORE/CB-AFTER headers
    - Rate limit: 3 req/s (public)

  Advanced Trade API (recommended for recent data):
    - Wrapped in {"trades": [...], "best_bid": ..., "best_ask": ...}
    - trade_id is UUID string
    - side is UPPERCASE ("BUY"/"SELL") and means TAKER side
    - Includes product_id and exchange fields
    - Time-window pagination via start/end UNIX timestamps
    - Rate limit: 10 req/s (public), 30 req/s (authenticated)

  IMPORTANT for bar construction:
    - Exchange API 'side' = maker side, so we INVERT it for taker side
    - Advanced Trade API 'side' = taker side, use directly
    - All prices/sizes are STRINGS — must parse to Decimal""")


def analyze_trades(trades: list[Trade], source_label: str) -> None:
    """Compute statistics on a list of parsed trades."""
    section(f"3. PARSED TRADES ({len(trades)} trades — {source_label})")

    if not trades:
        print("No trades!")
        return

    print(f"Time range: {trades[0].timestamp} -> {trades[-1].timestamp}")

    span = (trades[-1].timestamp - trades[0].timestamp).total_seconds()
    print(f"Time span: {span:.1f} seconds ({span / 60:.1f} minutes)")

    print(f"\nFirst 5 trades:")
    for t in trades[:5]:
        print(
            f"  {t.timestamp.isoformat()} | {t.side:4s} | "
            f"price={t.price} | size={t.size} | "
            f"dollar_vol=${t.dollar_volume:.2f}"
        )

    section("4. PRICE STATISTICS")

    prices = [float(t.price) for t in trades]
    sizes = [float(t.size) for t in trades]
    dollar_vols = [float(t.dollar_volume) for t in trades]

    print(f"Price range: ${min(prices):.2f} — ${max(prices):.2f}")
    print(f"Price mean:  ${statistics.mean(prices):.2f}")
    if len(prices) > 1:
        print(f"Price stdev: ${statistics.stdev(prices):.4f}")

    section("5. SIZE (VOLUME) STATISTICS")

    print(f"Size range:  {min(sizes):.8f} — {max(sizes):.8f} ETH")
    print(f"Size mean:   {statistics.mean(sizes):.8f} ETH")
    print(f"Size median: {statistics.median(sizes):.8f} ETH")
    print(f"Total volume: {sum(sizes):.4f} ETH")

    section("6. DOLLAR VOLUME STATISTICS")

    print(f"Dollar vol range:  ${min(dollar_vols):.2f} — ${max(dollar_vols):.2f}")
    print(f"Dollar vol mean:   ${statistics.mean(dollar_vols):.2f}")
    print(f"Dollar vol median: ${statistics.median(dollar_vols):.2f}")
    print(f"Total dollar vol:  ${sum(dollar_vols):,.2f}")

    section("7. TRADE FREQUENCY")

    if span > 0:
        trades_per_sec = len(trades) / span
        trades_per_min = trades_per_sec * 60
        print(f"Trades/second: {trades_per_sec:.2f}")
        print(f"Trades/minute: {trades_per_min:.1f}")
        print(f"Volume/minute: {sum(sizes) / (span / 60):.4f} ETH")
        print(f"Dollar vol/minute: ${sum(dollar_vols) / (span / 60):,.2f}")
    else:
        print("Span too short to compute frequency")

    section("8. SIDE DISTRIBUTION & IMBALANCE")

    buys = [t for t in trades if t.is_buy]
    sells = [t for t in trades if not t.is_buy]
    print(f"Buys:  {len(buys)} ({100 * len(buys) / len(trades):.1f}%)")
    print(f"Sells: {len(sells)} ({100 * len(sells) / len(trades):.1f}%)")

    buy_vol = sum(float(t.size) for t in buys)
    sell_vol = sum(float(t.size) for t in sells)
    print(f"\nBuy volume:  {buy_vol:.4f} ETH")
    print(f"Sell volume: {sell_vol:.4f} ETH")
    print(f"Volume imbalance: {buy_vol - sell_vol:+.4f} ETH")

    # Tick imbalance (sum of signs)
    tick_imbalance = sum(t.sign() for t in trades)
    print(f"\nTick imbalance (sum of signs): {tick_imbalance:+d}")
    print(f"  (positive = net buying pressure, negative = net selling pressure)")

    section("9. INTER-TRADE TIME ANALYSIS")

    if len(trades) > 1:
        deltas = []
        for i in range(1, len(trades)):
            dt = (trades[i].timestamp - trades[i - 1].timestamp).total_seconds()
            deltas.append(dt)

        print(f"Inter-trade time range: {min(deltas):.3f}s — {max(deltas):.3f}s")
        print(f"Inter-trade time mean:  {statistics.mean(deltas):.3f}s")
        print(f"Inter-trade time median: {statistics.median(deltas):.3f}s")

        zero_deltas = sum(1 for d in deltas if d == 0)
        print(
            f"Simultaneous trades (dt=0): {zero_deltas} "
            f"({100 * zero_deltas / len(deltas):.1f}%)"
        )

    section("10. BAR THRESHOLD SUGGESTIONS")

    if span > 0:
        trades_per_min = len(trades) / (span / 60)
        est_daily_trades = trades_per_min * 60 * 24
        est_daily_volume = sum(sizes) / (span / 60) * 60 * 24
        est_daily_dollar = sum(dollar_vols) / (span / 60) * 60 * 24

        print("Extrapolated daily estimates:")
        print(f"  Trades/day:  ~{est_daily_trades:,.0f}")
        print(f"  Volume/day:  ~{est_daily_volume:,.0f} ETH")
        print(f"  Dollar/day:  ~${est_daily_dollar:,.0f}")
        print()
        print("Suggested thresholds for ~50 bars/day:")
        print(f"  Tick bars:   {max(1, int(est_daily_trades / 50))} trades/bar")
        print(f"  Volume bars: {max(1, int(est_daily_volume / 50))} ETH/bar")
        print(f"  Dollar bars: ${max(1, int(est_daily_dollar / 50)):,}/bar")
    else:
        print("Need a longer time span to estimate thresholds.")

    print("\nNOTE: Run with --live against real API for accurate estimates.")
    print("Sample fixture data has limited time span and trade count.")


if __name__ == "__main__":
    live_mode = "--live" in sys.argv

    print("Arcana — Coinbase API Exploration")
    print(f"Pair: {PAIR}")
    print(f"Mode: {'LIVE (hitting real API)' if live_mode else 'SAMPLE (using fixture data)'}")

    show_raw_api_shapes()

    if live_mode:
        with CoinbaseSource() as source:
            trades = source.fetch_trades(pair=PAIR, limit=500)
        analyze_trades(trades, "live Coinbase API")
    else:
        trades = load_sample_trades()
        analyze_trades(trades, "sample fixture data")

    print(f"\n{'=' * 60}")
    print("  Done.")
    print(f"{'=' * 60}")
