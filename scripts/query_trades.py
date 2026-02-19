#!/usr/bin/env python3
"""Query raw trades from the database and analyze them.

Run: python scripts/query_trades.py [--pair ETH-USD] [--limit 1000]

This script connects to the arcana database, pulls raw trade data,
and displays summary statistics — a dry run for what bar builders
will consume.
"""

import argparse
import statistics
import sys
from pathlib import Path

import psycopg

# Add src to path for development
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from arcana.config import DatabaseConfig


def connect(args: argparse.Namespace) -> psycopg.Connection:
    config = DatabaseConfig(
        host=args.host, port=args.port, database=args.database,
        user=args.user, password=args.password,
    )
    return psycopg.connect(config.dsn)


def section(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Query raw trades from arcana database")
    parser.add_argument("--pair", default="ETH-USD", help="Trading pair")
    parser.add_argument("--limit", type=int, default=5000, help="Max trades to pull")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--database", default="arcana")
    parser.add_argument("--user", default="arcana")
    parser.add_argument("--password", default="")
    args = parser.parse_args()

    conn = connect(args)

    # --- Overview ---
    section("1. DATABASE OVERVIEW")

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw_trades WHERE pair = %s", (args.pair,))
        total_count = cur.fetchone()[0]

        cur.execute(
            "SELECT MIN(timestamp), MAX(timestamp) FROM raw_trades WHERE pair = %s",
            (args.pair,),
        )
        row = cur.fetchone()
        min_ts, max_ts = row[0], row[1]

        cur.execute(
            "SELECT COUNT(DISTINCT pair) FROM raw_trades",
        )
        pair_count = cur.fetchone()[0]

    print(f"Pair:          {args.pair}")
    print(f"Total trades:  {total_count:,}")
    print(f"Distinct pairs in DB: {pair_count}")
    if min_ts and max_ts:
        span = (max_ts - min_ts).total_seconds()
        print(f"Time range:    {min_ts} → {max_ts}")
        print(f"Time span:     {span / 3600:.1f} hours ({span / 86400:.1f} days)")
    else:
        print("No trades found.")
        return

    # --- Pull a sample ---
    section(f"2. SAMPLE TRADES (newest {args.limit:,})")

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT timestamp, trade_id, price, size, side
            FROM raw_trades
            WHERE pair = %s
            ORDER BY timestamp DESC
            LIMIT %s
            """,
            (args.pair, args.limit),
        )
        rows = cur.fetchall()

    # Reverse to chronological order
    rows.reverse()

    print(f"Pulled {len(rows):,} trades\n")
    print("First 10:")
    print(f"  {'timestamp':<32} {'side':>4}  {'price':>12}  {'size':>14}  {'dollar_vol':>14}")
    print(f"  {'-'*32} {'-'*4}  {'-'*12}  {'-'*14}  {'-'*14}")
    for ts, tid, price, size, side in rows[:10]:
        dv = price * size
        print(f"  {str(ts):<32} {side:>4}  {price:>12.2f}  {size:>14.8f}  {dv:>14.2f}")

    print("\nLast 10:")
    print(f"  {'timestamp':<32} {'side':>4}  {'price':>12}  {'size':>14}  {'dollar_vol':>14}")
    print(f"  {'-'*32} {'-'*4}  {'-'*12}  {'-'*14}  {'-'*14}")
    for ts, tid, price, size, side in rows[-10:]:
        dv = price * size
        print(f"  {str(ts):<32} {side:>4}  {price:>12.2f}  {size:>14.8f}  {dv:>14.2f}")

    # --- Statistics ---
    prices = [float(r[2]) for r in rows]
    sizes = [float(r[3]) for r in rows]
    dollar_vols = [float(r[2] * r[3]) for r in rows]

    section("3. PRICE STATISTICS")
    print(f"Range:   ${min(prices):.2f} — ${max(prices):.2f}")
    print(f"Mean:    ${statistics.mean(prices):.2f}")
    print(f"Median:  ${statistics.median(prices):.2f}")
    if len(prices) > 1:
        print(f"Stdev:   ${statistics.stdev(prices):.4f}")

    section("4. VOLUME STATISTICS")
    print(f"Range:   {min(sizes):.8f} — {max(sizes):.8f} ETH")
    print(f"Mean:    {statistics.mean(sizes):.8f} ETH")
    print(f"Median:  {statistics.median(sizes):.8f} ETH")
    print(f"Total:   {sum(sizes):,.4f} ETH")

    section("5. DOLLAR VOLUME")
    print(f"Range:   ${min(dollar_vols):.2f} — ${max(dollar_vols):,.2f}")
    print(f"Mean:    ${statistics.mean(dollar_vols):,.2f}")
    print(f"Total:   ${sum(dollar_vols):,.2f}")

    section("6. SIDE DISTRIBUTION")
    buys = sum(1 for r in rows if r[4] == "buy")
    sells = len(rows) - buys
    buy_vol = sum(float(r[3]) for r in rows if r[4] == "buy")
    sell_vol = sum(float(r[3]) for r in rows if r[4] == "sell")

    print(f"Buys:    {buys:,} ({100 * buys / len(rows):.1f}%)")
    print(f"Sells:   {sells:,} ({100 * sells / len(rows):.1f}%)")
    print(f"Buy vol: {buy_vol:,.4f} ETH")
    print(f"Sell vol:{sell_vol:,.4f} ETH")
    print(f"Imbalance: {buy_vol - sell_vol:+,.4f} ETH")

    tick_imbalance = sum(1 if r[4] == "buy" else -1 for r in rows)
    print(f"Tick imbalance: {tick_imbalance:+d}")

    section("7. TRADE FREQUENCY")
    if len(rows) > 1:
        deltas = []
        for i in range(1, len(rows)):
            dt = (rows[i][0] - rows[i - 1][0]).total_seconds()
            deltas.append(dt)

        total_span = (rows[-1][0] - rows[0][0]).total_seconds()
        print(f"Span:    {total_span / 3600:.2f} hours")
        if total_span > 0:
            print(f"Rate:    {len(rows) / (total_span / 60):.1f} trades/min")
        print("Inter-trade time:")
        print(f"  Min:    {min(deltas):.3f}s")
        print(f"  Mean:   {statistics.mean(deltas):.3f}s")
        print(f"  Median: {statistics.median(deltas):.3f}s")
        print(f"  Max:    {max(deltas):.3f}s")

    # --- Data quality ---
    section("8. DATA QUALITY")

    with conn.cursor() as cur:
        # Check for duplicate trade_ids
        cur.execute(
            """
            SELECT trade_id, COUNT(*) as cnt
            FROM raw_trades WHERE pair = %s
            GROUP BY trade_id HAVING COUNT(*) > 1
            LIMIT 5
            """,
            (args.pair,),
        )
        dupes = cur.fetchall()

        # Check for gaps > 5 minutes
        cur.execute(
            """
            SELECT a.timestamp, b.timestamp,
                   EXTRACT(EPOCH FROM (b.timestamp - a.timestamp)) as gap_seconds
            FROM (
                SELECT timestamp, ROW_NUMBER() OVER (ORDER BY timestamp) as rn
                FROM raw_trades WHERE pair = %s
            ) a
            JOIN (
                SELECT timestamp, ROW_NUMBER() OVER (ORDER BY timestamp) as rn
                FROM raw_trades WHERE pair = %s
            ) b ON b.rn = a.rn + 1
            WHERE EXTRACT(EPOCH FROM (b.timestamp - a.timestamp)) > 300
            ORDER BY gap_seconds DESC
            LIMIT 10
            """,
            (args.pair, args.pair),
        )
        gaps = cur.fetchall()

    print(f"Duplicate trade_ids: {len(dupes)}")
    if dupes:
        for tid, cnt in dupes:
            print(f"  {tid}: {cnt} occurrences")

    print(f"Gaps > 5 minutes:    {len(gaps)}")
    if gaps:
        for ts_a, ts_b, gap_s in gaps[:5]:
            print(f"  {ts_a} → {ts_b} ({gap_s / 60:.1f} min)")

    section("9. READY FOR BAR CONSTRUCTION")
    print(f"This dataset has {total_count:,} trades spanning {span / 3600:.1f} hours.")
    print(f"Sample pulled: {len(rows):,} trades for analysis above.")
    if total_span > 0:
        rate = len(rows) / (total_span / 60)
        est_daily = rate * 60 * 24
        print(f"\nEstimated daily volume: ~{est_daily:,.0f} trades/day")
        print("\nSuggested bar thresholds (~50 bars/day):")
        print(f"  Tick bars:   {max(1, int(est_daily / 50)):,} trades/bar")
        est_vol_daily = sum(sizes) / (total_span / 86400)
        est_dollar_daily = sum(dollar_vols) / (total_span / 86400)
        print(f"  Volume bars: {est_vol_daily / 50:,.2f} ETH/bar")
        print(f"  Dollar bars: ${est_dollar_daily / 50:,.0f}/bar")

    conn.close()
    print(f"\n{'=' * 60}")
    print("  Done.")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
