#!/usr/bin/env python3
"""Exploration script — fetch trades from Coinbase and analyze the response.

Run: python scripts/explore_coinbase.py [--live]

Without --live, uses sample fixture data (works offline).
With --live, hits the real Coinbase Advanced Trade API (no auth needed).
"""

import json
import statistics
import sys
from datetime import datetime
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
    """Load trades from the Advanced Trade API fixture."""
    raw_path = FIXTURES_DIR / "sample_advanced_trade_response.json"
    data = json.loads(raw_path.read_text())

    trades = []
    for raw in data["trades"]:
        trades.append(
            Trade(
                timestamp=datetime.fromisoformat(raw["time"].replace("Z", "+00:00")),
                trade_id=str(raw["trade_id"]),
                source="coinbase",
                pair=PAIR,
                price=Decimal(raw["price"]),
                size=Decimal(raw["size"]),
                side=raw["side"].lower(),  # API returns "BUY"/"SELL"
            )
        )
    return sorted(trades, key=lambda t: t.timestamp)


def show_raw_api_shape() -> None:
    """Show the Advanced Trade API response format from fixture."""
    section("1. RAW API RESPONSE SHAPE")

    print("ADVANCED TRADE API (public, no auth):")
    print("  GET /api/v3/brokerage/market/products/ETH-USD/ticker")
    print("  Params: limit (required), start/end (optional, UNIX timestamps)\n")

    raw_path = FIXTURES_DIR / "sample_advanced_trade_response.json"
    data = json.loads(raw_path.read_text())

    print("  Response type: dict")
    print(f"  Top-level keys: {list(data.keys())}")
    print(f"  Trades count: {len(data['trades'])}")
    print(f"  best_bid: {data['best_bid']}, best_ask: {data['best_ask']}")
    print("\n  First trade:")
    print(json.dumps(data["trades"][0], indent=4))
    print(f"\n  Trade fields: {list(data['trades'][0].keys())}")
    print("  Field types:")
    for k, v in data["trades"][0].items():
        print(f"    {k}: {type(v).__name__} = {v!r}")

    print("\n  Key notes:")
    print("    - side is TAKER side ('BUY'/'SELL') — use directly, no inversion")
    print("    - trade_id is UUID string")
    print("    - All prices/sizes are strings — parse with Decimal")
    print("    - start/end params are UNIX timestamps for time-window queries")
    print("    - Forward pagination: walk start/end windows through time")


def analyze_trades(trades: list[Trade], source_label: str) -> None:
    """Compute statistics on a list of parsed trades."""
    section(f"2. PARSED TRADES ({len(trades)} trades — {source_label})")

    if not trades:
        print("No trades!")
        return

    print(f"Time range: {trades[0].timestamp} -> {trades[-1].timestamp}")

    span = (trades[-1].timestamp - trades[0].timestamp).total_seconds()
    print(f"Time span: {span:.1f} seconds ({span / 60:.1f} minutes)")

    print("\nFirst 5 trades:")
    for t in trades[:5]:
        print(
            f"  {t.timestamp.isoformat()} | {t.side:4s} | "
            f"price={t.price} | size={t.size} | "
            f"dollar_vol=${t.dollar_volume:.2f}"
        )

    section("3. PRICE STATISTICS")

    prices = [float(t.price) for t in trades]
    sizes = [float(t.size) for t in trades]
    dollar_vols = [float(t.dollar_volume) for t in trades]

    print(f"Price range: ${min(prices):.2f} — ${max(prices):.2f}")
    print(f"Price mean:  ${statistics.mean(prices):.2f}")
    if len(prices) > 1:
        print(f"Price stdev: ${statistics.stdev(prices):.4f}")

    section("4. SIZE (VOLUME) STATISTICS")

    print(f"Size range:  {min(sizes):.8f} — {max(sizes):.8f} ETH")
    print(f"Size mean:   {statistics.mean(sizes):.8f} ETH")
    print(f"Size median: {statistics.median(sizes):.8f} ETH")
    print(f"Total volume: {sum(sizes):.4f} ETH")

    section("5. DOLLAR VOLUME STATISTICS")

    print(f"Dollar vol range:  ${min(dollar_vols):.2f} — ${max(dollar_vols):.2f}")
    print(f"Dollar vol mean:   ${statistics.mean(dollar_vols):.2f}")
    print(f"Dollar vol median: ${statistics.median(dollar_vols):.2f}")
    print(f"Total dollar vol:  ${sum(dollar_vols):,.2f}")

    section("6. TRADE FREQUENCY")

    if span > 0:
        trades_per_sec = len(trades) / span
        trades_per_min = trades_per_sec * 60
        print(f"Trades/second: {trades_per_sec:.2f}")
        print(f"Trades/minute: {trades_per_min:.1f}")
        print(f"Volume/minute: {sum(sizes) / (span / 60):.4f} ETH")
        print(f"Dollar vol/minute: ${sum(dollar_vols) / (span / 60):,.2f}")
    else:
        print("Span too short to compute frequency")

    section("7. SIDE DISTRIBUTION & IMBALANCE")

    buys = [t for t in trades if t.is_buy]
    sells = [t for t in trades if not t.is_buy]
    print(f"Buys:  {len(buys)} ({100 * len(buys) / len(trades):.1f}%)")
    print(f"Sells: {len(sells)} ({100 * len(sells) / len(trades):.1f}%)")

    buy_vol = sum(float(t.size) for t in buys)
    sell_vol = sum(float(t.size) for t in sells)
    print(f"\nBuy volume:  {buy_vol:.4f} ETH")
    print(f"Sell volume: {sell_vol:.4f} ETH")
    print(f"Volume imbalance: {buy_vol - sell_vol:+.4f} ETH")

    tick_imbalance = sum(t.sign() for t in trades)
    print(f"\nTick imbalance (sum of signs): {tick_imbalance:+d}")
    print("  (positive = net buying pressure, negative = net selling pressure)")

    section("8. INTER-TRADE TIME ANALYSIS")

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

    section("9. BAR THRESHOLD SUGGESTIONS")

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

    print("Arcana — Coinbase Advanced Trade API Exploration")
    print(f"Pair: {PAIR}")
    print(f"Mode: {'LIVE (hitting real API)' if live_mode else 'SAMPLE (using fixture data)'}")

    show_raw_api_shape()

    if live_mode:
        with CoinbaseSource() as source:
            trades = source.fetch_trades(pair=PAIR, limit=300)
        analyze_trades(trades, "live Coinbase API")
    else:
        trades = load_sample_trades()
        analyze_trades(trades, "sample fixture data")

    print(f"\n{'=' * 60}")
    print("  Done.")
    print(f"{'=' * 60}")
