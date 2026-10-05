#!/usr/bin/env python3
"""Arcana derivatives + spot intake runner (unattended, resumable).

Backfills one or more Coinbase products — spot pairs and/or futures contracts —
from a start date to the present, in retryable day-chunks, and optionally starts
the forward-only futures metrics poller afterward.

Designed to run remotely/overnight:
  - Resumes automatically (idempotent; skips already-covered chunks)
  - Per-chunk retry with backoff
  - Rate-limited so concurrent-friendly against the 10 req/s public cap
  - Full logging to a timestamped file
  - Graceful Ctrl+C

This is the ingestion layer only — no bar construction (that is Sigil's job).

Usage:
    # Defaults: BTC-USD + BIP-20DEC30-CDE, 4 months back, tick intake
    python scripts/derivatives_ingest.py

    # Custom products and window
    python scripts/derivatives_ingest.py --pairs BTC-USD BIP-20DEC30-CDE \
        --since 2026-06-04

    # Faster spot history via candles instead of ticks
    python scripts/derivatives_ingest.py --candles

    # After backfill, keep a metrics snapshot poller running for the futures legs
    python scripts/derivatives_ingest.py --metrics

Prerequisites:
    - TimescaleDB reachable via ARCANA_DB_* (defaults match the arcana-tsdb container).
    - `arcana` CLI installed from THIS branch (`pip install -e .`) — the metrics /
      list-futures commands only exist here. Plain tick/candle ingest works with any
      recent arcana.

Output:
    logs/derivatives_YYYY-MM-DD_HHMMSS.log
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

# Force UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ── Defaults ────────────────────────────────────────────────────────────────
DEFAULT_PAIRS = ["BTC-USD", "BIP-20DEC30-CDE"]  # spot + perpetual-style BTC future
DEFAULT_MONTHS_BACK = 4
CHUNK_DAYS = 10  # days per backfill chunk (keeps each arcana call well under 2h)
MAX_RETRIES = 5
RETRY_DELAY = 30  # seconds between retries
RATE_DELAY = "0.25"  # ARCANA_RATE_DELAY — concurrent-safe under the 10 req/s cap
METRICS_INTERVAL = 300  # seconds between futures metric snapshots

# DB credentials (match the docker container; override via real env vars)
DB_ENV = {
    "ARCANA_DB_HOST": os.environ.get("ARCANA_DB_HOST", "localhost"),
    "ARCANA_DB_PORT": os.environ.get("ARCANA_DB_PORT", "5432"),
    "ARCANA_DB_NAME": os.environ.get("ARCANA_DB_NAME", "arcana"),
    "ARCANA_DB_USER": os.environ.get("ARCANA_DB_USER", "arcana"),
    "ARCANA_DB_PASSWORD": os.environ.get("ARCANA_DB_PASSWORD", "arcana"),
}

# ── Logging ───────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / f"derivatives_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.log"

failures: list[dict] = []


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def log_section(title: str) -> None:
    log("")
    log("=" * 70)
    log(f"  {title}")
    log("=" * 70)


def _run_cmd(cmd: list[str], timeout: int = 7200) -> tuple[int, str]:
    """Run a CLI command with DB env vars + rate delay, return (exit_code, output)."""
    env = {**os.environ, **DB_ENV, "ARCANA_RATE_DELAY": RATE_DELAY}
    log(f"  $ {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)
        if result.stderr:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                for line in result.stderr.strip().splitlines():
                    f.write(f"    {line}\n")
        return result.returncode, result.stdout + result.stderr
    except subprocess.TimeoutExpired:
        return -1, f"TIMEOUT after {timeout}s"


def run_arcana(*args: str, timeout: int = 7200) -> tuple[int, str]:
    return _run_cmd(["arcana", "--log-level", "WARNING", *args], timeout=timeout)


def run_metrics_poller(futures: list[str]) -> None:
    """Run the futures metrics poller, streaming its output (runs until Ctrl+C)."""
    env = {**os.environ, **DB_ENV, "ARCANA_RATE_DELAY": RATE_DELAY}
    cmd = [
        "arcana",
        "--log-level",
        "INFO",
        "metrics",
        *futures,
        "--interval",
        str(METRICS_INTERVAL),
    ]
    log(f"  $ {' '.join(cmd)}")
    # No capture / no timeout: the poller streams and handles its own shutdown.
    subprocess.run(cmd, env=env, check=False)


def psql_int(query: str) -> int:
    """Count helper via the arcana-tsdb container; returns 0 if unavailable."""
    cmd = [
        "docker",
        "exec",
        "arcana-tsdb",
        "psql",
        "-U",
        DB_ENV["ARCANA_DB_USER"],
        "-d",
        DB_ENV["ARCANA_DB_NAME"],
        "-t",
        "-A",
        "-c",
        query,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        val = result.stdout.strip()
        return int(val) if val else 0
    except Exception:
        return 0  # counting is informational — never fatal


def record_failure(phase: str, detail: str) -> None:
    failures.append({"time": datetime.now().isoformat(), "phase": phase, "detail": detail})
    log(f"  FAILURE: {phase} — {detail}")


def generate_chunks(start: datetime, end: datetime) -> list[tuple[str, str, str]]:
    chunks = []
    current = start
    while current < end:
        chunk_end = min(current + timedelta(days=CHUNK_DAYS), end)
        label = f"{current.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}"
        chunks.append(
            (
                current.strftime("%Y-%m-%dT%H:%M:%S"),
                chunk_end.strftime("%Y-%m-%dT%H:%M:%S"),
                label,
            )
        )
        current = chunk_end
    return chunks


def ingest_chunk(pair: str, since: str, until: str, label: str, candles: bool) -> bool:
    """Ingest one chunk for one pair with retry. Returns success."""
    subcmd = "backfill-candles" if candles else "ingest"
    for attempt in range(1, MAX_RETRIES + 1):
        before = psql_int(f"SELECT COUNT(*) FROM raw_trades WHERE pair = '{pair}';")
        args = [subcmd, pair, "--since", since, "--until", until]
        if candles:
            args += ["--granularity", "1m"]
        rc, out = run_arcana(*args, timeout=7200)
        after = psql_int(f"SELECT COUNT(*) FROM raw_trades WHERE pair = '{pair}';")
        new = after - before

        if rc == 0:
            log(f"    +{new:,} rows ({label})")
            return True

        snippet = out[-200:] if out else ""
        log(f"    attempt {attempt}/{MAX_RETRIES} failed (exit {rc}): {snippet}")
        if attempt < MAX_RETRIES:
            if new > 0:
                log(f"    partial progress +{new:,} (resumable)")
            time.sleep(RETRY_DELAY)

    record_failure(f"{subcmd} {pair} {label}", f"failed after {MAX_RETRIES} attempts")
    return False


def backfill_pair(pair: str, start: datetime, end: datetime, candles: bool) -> None:
    chunks = generate_chunks(start, end)
    log(
        f"  {pair}: {len(chunks)} chunk(s), {start.strftime('%Y-%m-%d')} -> "
        f"{end.strftime('%Y-%m-%d')}"
    )
    for i, (since, until, label) in enumerate(chunks, 1):
        # Skip chunks already fully covered (resume support).
        chunk_end = datetime.fromisoformat(until).replace(tzinfo=UTC)
        last = psql_int(
            f"SELECT EXTRACT(EPOCH FROM MAX(timestamp))::bigint "
            f"FROM raw_trades WHERE pair = '{pair}';"
        )
        if last and datetime.fromtimestamp(last, tz=UTC) >= chunk_end:
            log(f"  [{pair} {i}/{len(chunks)}] {label} — skip (covered)")
            continue
        log(f"  [{pair} {i}/{len(chunks)}] {label}")
        t0 = time.time()
        ingest_chunk(pair, since, until, label, candles)
        total = psql_int(f"SELECT COUNT(*) FROM raw_trades WHERE pair = '{pair}';")
        log(f"    chunk done in {time.time() - t0:.0f}s | {pair} total: {total:,}")


def is_future(pair: str) -> bool:
    """Heuristic: futures product ids have >1 hyphen (e.g. BIP-20DEC30-CDE)."""
    return pair.count("-") > 1


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Arcana derivatives + spot intake runner.")
    p.add_argument("--pairs", nargs="+", default=DEFAULT_PAIRS, help="Products to ingest.")
    p.add_argument("--since", default=None, help="Start date YYYY-MM-DD (default: 4 months ago).")
    p.add_argument("--until", default=None, help="End date YYYY-MM-DD (default: now).")
    p.add_argument("--candles", action="store_true", help="Use candle backfill instead of ticks.")
    p.add_argument(
        "--metrics",
        action="store_true",
        help="After backfill, run the futures metrics poller for the futures legs.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    script_start = time.time()

    now = datetime.now(UTC)
    start = (
        datetime.fromisoformat(args.since).replace(tzinfo=UTC)
        if args.since
        else now - timedelta(days=DEFAULT_MONTHS_BACK * 30)
    )
    end = datetime.fromisoformat(args.until).replace(tzinfo=UTC) if args.until else now

    log_section("ARCANA DERIVATIVES + SPOT INTAKE")
    log(f"Pairs:     {', '.join(args.pairs)}")
    log(f"Window:    {start.strftime('%Y-%m-%d')} -> {end.strftime('%Y-%m-%d')}")
    log(f"Mode:      {'candles(1m)' if args.candles else 'ticks'}")
    log(f"Rate:      ARCANA_RATE_DELAY={RATE_DELAY}")
    log(f"Log file:  {LOG_FILE}")

    # ── 1. Prerequisites ──────────────────────────────────────────────
    log_section("1. PREREQUISITES")
    rc, _ = run_arcana("db", "init", timeout=120)
    if rc != 0:
        log("  Schema init failed — is the database reachable? Exiting.")
        return 1
    log("  Schema: OK")

    # ── 2. Backfill each pair ─────────────────────────────────────────
    log_section("2. BACKFILL")
    for pair in args.pairs:
        backfill_pair(pair, start, end, args.candles)

    # ── 3. Optional: futures metrics poller ───────────────────────────
    futures = [p for p in args.pairs if is_future(p)]
    if args.metrics and futures:
        log_section("3. METRICS POLLER (runs until interrupted)")
        log(f"  Polling {', '.join(futures)} every {METRICS_INTERVAL}s")
        run_metrics_poller(futures)
    elif args.metrics:
        log("  --metrics set but no futures legs in --pairs; skipping poller.")

    # ── Summary ───────────────────────────────────────────────────────
    log_section("SUMMARY")
    log(f"  Runtime:   {(time.time() - script_start) / 3600:.2f}h")
    for pair in args.pairs:
        total = psql_int(f"SELECT COUNT(*) FROM raw_trades WHERE pair = '{pair}';")
        log(f"  {pair}: {total:,} rows")
    log(f"  Failures:  {len(failures)}")
    for f in failures:
        log(f"    [{f['time']}] {f['phase']}: {f['detail']}")
    if failures:
        log("  (Intake is resumable — re-run to fill gaps.)")
    log(f"  Full log: {LOG_FILE}")
    return 1 if failures else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log("\nInterrupted. Stored data is safe — re-run to resume.")
        sys.exit(0)
