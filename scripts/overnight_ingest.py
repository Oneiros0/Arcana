#!/usr/bin/env python3
"""Arcana Overnight Ingestion Script.

Backfills ETH-USD from 6 months ago to present, then starts the daemon.
Designed to run unattended overnight with:
  - Auto-resume on crash (month-by-month chunks)
  - Full logging to timestamped file
  - Failure tracking and summary
  - Graceful shutdown on Ctrl+C

Usage:
    python scripts/overnight_ingest.py

Output:
    logs/overnight_YYYY-MM-DD_HHMMSS.log
"""

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

# ── Configuration ─────────────────────────────────────────────────────────
PAIR = "ETH-USD"
MONTHS_BACK = 6
CHUNK_DAYS = 10  # days per backfill chunk (keep under 2h timeout)
DAEMON_INTERVAL = 900  # 15 minutes
MAX_RETRIES = 5
RETRY_DELAY = 30  # seconds between retries

# DB credentials (match docker container)
DB_ENV = {
    "ARCANA_DB_HOST": "localhost",
    "ARCANA_DB_PORT": "5432",
    "ARCANA_DB_NAME": "arcana",
    "ARCANA_DB_USER": "arcana",
    "ARCANA_DB_PASSWORD": "arcana",
}

# ── Logging ───────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / f"overnight_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.log"

# Track failures
failures: list[dict] = []


def log(msg: str) -> None:
    """Print and write to log file."""
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


def run_arcana(*args: str, timeout: int = 7200) -> tuple[int, str]:
    """Run arcana CLI with DB env vars, return (exit_code, output)."""
    env = {**os.environ, **DB_ENV}
    cmd = ["arcana", "--log-level", "INFO"] + list(args)
    log(f"  $ {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
        # Log output to file (not to stdout to reduce noise)
        if result.stderr:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                for line in result.stderr.strip().splitlines():
                    f.write(f"    {line}\n")
        return result.returncode, result.stdout + result.stderr
    except subprocess.TimeoutExpired:
        return -1, f"TIMEOUT after {timeout}s"


def psql(query: str) -> str:
    """Run a psql query and return result."""
    cmd = [
        "docker",
        "exec",
        "arcana-tsdb",
        "psql",
        "-U",
        "arcana",
        "-d",
        "arcana",
        "-t",
        "-A",
        "-c",
        query,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    return result.stdout.strip()


def psql_int(query: str) -> int:
    """Run a psql query and return int result."""
    val = psql(query)
    return int(val) if val else 0


def record_failure(phase: str, detail: str) -> None:
    """Record a failure for the summary."""
    entry = {
        "time": datetime.now().isoformat(),
        "phase": phase,
        "detail": detail,
    }
    failures.append(entry)
    log(f"  FAILURE: {phase} — {detail}")


def ingest_chunk(since: str, until: str, chunk_label: str) -> tuple[bool, int]:
    """Ingest a single time chunk with retry logic.

    Returns (success, trades_inserted).
    """
    for attempt in range(1, MAX_RETRIES + 1):
        # Check what we have before
        count_before = psql_int(f"SELECT COUNT(*) FROM raw_trades WHERE pair = '{PAIR}';")

        log(f"  Attempt {attempt}/{MAX_RETRIES} for {chunk_label}")
        rc, out = run_arcana(
            "ingest",
            PAIR,
            "--since",
            since,
            "--until",
            until,
            timeout=7200,  # 2 hours per chunk max
        )

        count_after = psql_int(f"SELECT COUNT(*) FROM raw_trades WHERE pair = '{PAIR}';")
        new_trades = count_after - count_before

        if rc == 0:
            log(f"  +{new_trades:,} trades ({chunk_label})")
            return True, new_trades

        # Failed — log and retry
        error_snippet = out[-300:] if len(out) > 300 else out
        log(f"  Attempt {attempt} failed (exit {rc}): {error_snippet[:200]}")

        if attempt < MAX_RETRIES:
            # Still got some trades? That's progress — resumable.
            if new_trades > 0:
                log(
                    f"  Partial progress: +{new_trades:,} trades saved "
                    "(will resume from last trade)"
                )
            log(f"  Retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)

    # All retries exhausted
    record_failure(f"ingest {chunk_label}", f"failed after {MAX_RETRIES} attempts")
    return False, 0


def generate_chunks(start: datetime, end: datetime) -> list[tuple[str, str, str]]:
    """Generate (since, until, label) tuples for backfill chunks."""
    chunks = []
    current = start
    while current < end:
        chunk_end = min(current + timedelta(days=CHUNK_DAYS), end)
        since_str = current.strftime("%Y-%m-%dT%H:%M:%S")
        until_str = chunk_end.strftime("%Y-%m-%dT%H:%M:%S")
        label = f"{current.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}"
        chunks.append((since_str, until_str, label))
        current = chunk_end
    return chunks


def main() -> int:
    script_start = time.time()

    log_section("ARCANA OVERNIGHT INGESTION")
    log(f"Pair:           {PAIR}")
    log(f"Backfill:       {MONTHS_BACK} months")
    log(f"Log file:       {LOG_FILE}")
    log(f"Max retries:    {MAX_RETRIES} per chunk")
    log(f"Started:        {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # ── 1. Verify prerequisites ───────────────────────────────────────
    log_section("1. PREREQUISITES")

    # Check Docker/TimescaleDB
    try:
        psql("SELECT 1;")
        log("  TimescaleDB: OK")
    except Exception as e:
        log(f"  TimescaleDB: FAILED ({e})")
        log("  Cannot proceed without database. Exiting.")
        return 1

    # Check existing data — resume if present, fresh start only if empty
    existing_trades = 0
    try:
        existing_trades = psql_int(f"SELECT COUNT(*) FROM raw_trades WHERE pair = '{PAIR}';")
    except Exception:
        pass  # Table doesn't exist yet

    if existing_trades > 0:
        last_ts = psql(f"SELECT MAX(timestamp)::text FROM raw_trades WHERE pair = '{PAIR}';")
        log(f"  Existing data found: {existing_trades:,} trades (through {last_ts})")
        log("  Resuming from where we left off.")
    else:
        log("  No existing data — fresh start.")

    # Ensure schema exists (idempotent — won't drop existing tables)
    rc, _ = run_arcana("db", "init")
    if rc == 0:
        log("  Schema: OK")
    else:
        log("  Schema init failed!")
        return 1

    # ── 2. Monthly backfill ───────────────────────────────────────────
    log_section(f"2. BACKFILL ({CHUNK_DAYS}-day chunks)")

    now = datetime.now(UTC)
    start = now - timedelta(days=MONTHS_BACK * 30)
    chunks = generate_chunks(start, now)

    log(f"  {len(chunks)} chunks to process ({CHUNK_DAYS} days each)")
    log(f"  {start.strftime('%Y-%m-%d')} -> {now.strftime('%Y-%m-%d')}")
    log("")

    total_ingested = 0
    chunks_ok = 0
    chunks_failed = 0

    for i, (since, until, label) in enumerate(chunks, 1):
        # Skip chunks that are already fully covered
        chunk_end_dt = datetime.fromisoformat(until).replace(tzinfo=UTC)
        try:
            last_ts_str = psql(
                f"SELECT MAX(timestamp)::text FROM raw_trades WHERE pair = '{PAIR}';"
            )
            if last_ts_str and last_ts_str != "":
                # Parse the timestamp (psql returns ISO format)
                last_ts_str = last_ts_str.strip()
                if "+" in last_ts_str:
                    last_dt = datetime.fromisoformat(last_ts_str)
                else:
                    last_dt = datetime.fromisoformat(last_ts_str).replace(tzinfo=UTC)
                if last_dt >= chunk_end_dt:
                    log(f"  [{i}/{len(chunks)}] {label} — SKIPPED (already ingested)")
                    chunks_ok += 1
                    continue
        except Exception:
            pass  # Can't check, just proceed normally

        log(f"  [{i}/{len(chunks)}] {label}")
        t0 = time.time()
        ok, count = ingest_chunk(since, until, label)
        elapsed = time.time() - t0

        total_ingested += count
        if ok:
            chunks_ok += 1
        else:
            chunks_failed += 1

        # Running total
        total_trades = psql_int(f"SELECT COUNT(*) FROM raw_trades WHERE pair = '{PAIR}';")
        log(
            f"  Chunk done in {elapsed:.0f}s | "
            f"Total: {total_trades:,} trades | "
            f"Chunks: {chunks_ok} ok, {chunks_failed} failed"
        )
        log("")

    # ── 3. Build bar types from config ───────────────────────────────
    # Load bar specs from arcana.toml (auto-calibrated thresholds)
    try:
        from arcana.config import ArcanaConfig

        cfg = ArcanaConfig.find_and_load()
    except Exception:
        cfg = None

    if cfg and cfg.bars:
        bar_specs = [b.spec for b in cfg.bars if b.enabled]
    else:
        # Fallback: auto-calibrated defaults
        bar_specs = [
            "tick_auto",
            "volume_auto",
            "dollar_auto",
            "time_5m",
            "time_1h",
            "tib_10",
            "vib_10",
            "dib_10",
            "trb_10",
            "vrb_10",
            "drb_10",
        ]

    log_section(f"3. BAR CONSTRUCTION ({len(bar_specs)} types)")

    for spec in bar_specs:
        log(f"  Building {spec}...")
        t0 = time.time()

        for attempt in range(1, MAX_RETRIES + 1):
            rc, out = run_arcana("bars", "build", spec, PAIR, timeout=7200)
            if rc == 0:
                break
            log(f"  Attempt {attempt} failed for {spec}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

        elapsed = time.time() - t0
        if rc == 0:
            # Count bars — for auto specs, get table name from CLI output
            pair_norm = PAIR.lower().replace("-", "_")
            # Auto-calibrated specs resolve to e.g. tick_3847 at build time.
            # The CLI output contains "Building tick_3847 bars for ETH-USD..."
            # Parse the resolved bar_type from the output.
            resolved = spec
            for line in out.splitlines():
                if line.startswith("Building ") and " bars for " in line:
                    resolved = line.split("Building ")[1].split(" bars for ")[0]
                    break
                elif line.startswith("Auto-calibrated: "):
                    resolved = line.split("Auto-calibrated: ")[1].split(" (")[0]
                    break
            tbl = f"bars_{resolved.replace('.', '_')}_{pair_norm}"
            try:
                bar_count = psql_int(f"SELECT COUNT(*) FROM {tbl};")
                log(f"  {resolved}: {bar_count:,} bars in {elapsed:.0f}s")
            except Exception:
                log(f"  {spec}: completed in {elapsed:.0f}s (could not count bars)")
        else:
            record_failure(f"bars build {spec}", f"exit {rc}")
            log(f"  {spec}: FAILED after {MAX_RETRIES} attempts")

    # ── 4. Start daemon ───────────────────────────────────────────────
    log_section("4. DAEMON (runs until interrupted)")

    total_before = psql_int(f"SELECT COUNT(*) FROM raw_trades WHERE pair = '{PAIR}';")
    log(f"  Trades before daemon: {total_before:,}")
    log(f"  Interval: {DAEMON_INTERVAL}s ({DAEMON_INTERVAL // 60} min)")
    log("  Press Ctrl+C to stop")
    log("")

    daemon_start = time.time()
    daemon_cycles = 0

    while True:
        daemon_cycles += 1
        try:
            rc, out = run_arcana(
                "ingest",
                PAIR,
                "--since",
                (datetime.now(UTC) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S"),
                timeout=300,
            )

            current = psql_int(f"SELECT COUNT(*) FROM raw_trades WHERE pair = '{PAIR}';")
            new = current - total_before
            uptime = time.time() - daemon_start
            log(
                f"  Cycle {daemon_cycles} | "
                f"Trades: {current:,} (+{new:,}) | "
                f"Uptime: {uptime / 3600:.1f}h"
            )

        except KeyboardInterrupt:
            log("  Ctrl+C received — shutting down daemon")
            break
        except Exception as e:
            record_failure(f"daemon cycle {daemon_cycles}", str(e))
            log(f"  Cycle {daemon_cycles} error: {e}")

        # Sleep in small increments to catch Ctrl+C
        try:
            for _ in range(DAEMON_INTERVAL):
                time.sleep(1)
        except KeyboardInterrupt:
            log("  Ctrl+C received — shutting down daemon")
            break

    # ── Summary ───────────────────────────────────────────────────────
    log_section("SUMMARY")

    elapsed_total = time.time() - script_start
    final_trades = psql_int(f"SELECT COUNT(*) FROM raw_trades WHERE pair = '{PAIR}';")
    trade_range = psql(
        f"SELECT MIN(timestamp)::text || ' to ' || MAX(timestamp)::text "
        f"FROM raw_trades WHERE pair = '{PAIR}';"
    )

    log(f"  Total runtime:     {elapsed_total / 3600:.1f}h")
    log(f"  Trades:            {final_trades:,}")
    log(f"  Range:             {trade_range}")
    log(f"  Backfill chunks:   {chunks_ok} ok, {chunks_failed} failed")
    log(f"  Daemon cycles:     {daemon_cycles}")
    log(f"  Failures:          {len(failures)}")

    if failures:
        log("")
        log("  FAILURE LOG:")
        for f in failures:
            log(f"    [{f['time']}] {f['phase']}: {f['detail']}")

    log("")
    if not failures:
        log("  RESULT: ALL OPERATIONS COMPLETED SUCCESSFULLY")
    else:
        log(f"  RESULT: COMPLETED WITH {len(failures)} FAILURE(S)")
        log("  (Ingestion is resumable — re-run to fill gaps)")

    log(f"\n  Full log: {LOG_FILE}")
    return 1 if failures else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log("\nScript interrupted. Data saved is safe — re-run to resume.")
        sys.exit(0)
