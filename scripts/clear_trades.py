#!/usr/bin/env python3
"""Delete raw trades from the database.

Usage:
  python scripts/clear_trades.py                     # preview: show count per pair
  python scripts/clear_trades.py --pair ETH-USD      # delete all ETH-USD trades
  python scripts/clear_trades.py --all               # delete ALL trades
  python scripts/clear_trades.py --pair ETH-USD --yes  # skip confirmation prompt
"""

import argparse
import sys
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from arcana.config import DatabaseConfig


def connect(args: argparse.Namespace) -> psycopg.Connection:
    config = DatabaseConfig(
        host=args.host, port=args.port, database=args.database,
        user=args.user, password=args.password,
    )
    return psycopg.connect(config.dsn)


def show_summary(conn: psycopg.Connection) -> None:
    """Print trade counts per pair."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pair, COUNT(*), MIN(timestamp), MAX(timestamp) "
            "FROM raw_trades GROUP BY pair ORDER BY pair"
        )
        rows = cur.fetchall()

    if not rows:
        print("No trades in database.")
        return

    print(f"\n{'pair':<12} {'count':>10}  {'earliest':<26} {'latest':<26}")
    print(f"{'-'*12} {'-'*10}  {'-'*26} {'-'*26}")
    total = 0
    for pair, count, min_ts, max_ts in rows:
        print(f"{pair:<12} {count:>10,}  {str(min_ts):<26} {str(max_ts):<26}")
        total += count
    print(f"\n{'Total':<12} {total:>10,}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Delete raw trades from arcana database")
    parser.add_argument("--pair", help="Delete trades for this pair only (e.g. ETH-USD)")
    parser.add_argument("--all", action="store_true", help="Delete ALL trades")
    parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--database", default="arcana")
    parser.add_argument("--user", default="arcana")
    parser.add_argument("--password", default="")
    args = parser.parse_args()

    conn = connect(args)

    # No flags → just show summary
    if not args.pair and not args.all:
        print("Current database contents:")
        show_summary(conn)
        print("\nTo delete, run with --pair ETH-USD or --all")
        conn.close()
        return

    # Show what will be deleted
    with conn.cursor() as cur:
        if args.pair:
            cur.execute(
                "SELECT COUNT(*) FROM raw_trades WHERE pair = %s", (args.pair,)
            )
        else:
            cur.execute("SELECT COUNT(*) FROM raw_trades")
        count = cur.fetchone()[0]

    if count == 0:
        target = f"pair={args.pair}" if args.pair else "all pairs"
        print(f"No trades to delete ({target}).")
        conn.close()
        return

    target = f"pair={args.pair}" if args.pair else "ALL pairs"
    print(f"\nAbout to delete {count:,} trades ({target}).")

    if not args.yes:
        answer = input("Are you sure? [y/N] ").strip().lower()
        if answer != "y":
            print("Aborted.")
            conn.close()
            return

    # Delete
    with conn.cursor() as cur:
        if args.pair:
            cur.execute("DELETE FROM raw_trades WHERE pair = %s", (args.pair,))
        else:
            cur.execute("DELETE FROM raw_trades")
        deleted = cur.rowcount
    conn.commit()

    print(f"Deleted {deleted:,} trades.")

    # Show what's left
    show_summary(conn)
    conn.close()


if __name__ == "__main__":
    main()
