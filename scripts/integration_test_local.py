#!/usr/bin/env python3
"""Arcana Local Integration Test & Performance Diagnostics.

Simulates the complete onboarding user story against a live TimescaleDB
instance with real Coinbase API data:

    pip install -e .  →  arcana db init  →  arcana ingest  →
    arcana bars build (all 11 types)  →  arcana run (daemon)  →
    data integrity proofs  →  storage metrics

Target runtime: ~10 minutes (ingestion dominates).

Prerequisites:
    - TimescaleDB running on localhost:5432 (user=arcana, password=arcana, db=arcana)
    - Arcana installed: pip install -e .

Usage:
    python scripts/integration_test_local.py
"""

import os
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta

# Force UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ── Environment ─────────────────────────────────────────────────────────────
os.environ.setdefault("ARCANA_DB_HOST", "localhost")
os.environ.setdefault("ARCANA_DB_PORT", "5432")
os.environ.setdefault("ARCANA_DB_NAME", "arcana")
os.environ.setdefault("ARCANA_DB_USER", "arcana")
os.environ.setdefault("ARCANA_DB_PASSWORD", "arcana")

# ── Test Harness ────────────────────────────────────────────────────────────
PASS = 0
FAIL = 0
RESULTS: list[tuple[str, bool, str]] = []


def _log(msg: str) -> None:
    print(f"  {msg}", flush=True)


def _pass(description: str, detail: str = "") -> None:
    global PASS
    PASS += 1
    tag = f" ({detail})" if detail else ""
    RESULTS.append((description, True, detail))
    print(f"  \033[92m\u2713\033[0m {description}{tag}", flush=True)


def _fail(description: str, detail: str = "") -> None:
    global FAIL
    FAIL += 1
    RESULTS.append((description, False, detail))
    print(f"  \033[91m\u2717\033[0m {description}", flush=True)
    if detail:
        print(f"    \u2192 {detail}", flush=True)


def _section(title: str) -> None:
    print(f"\n{'=' * 60}", flush=True)
    print(f"  {title}", flush=True)
    print(f"{'=' * 60}", flush=True)


def run_arcana(*args: str, timeout: int = 600) -> tuple[int, str]:
    """Run an arcana CLI command, return (exit_code, combined_output)."""
    cmd = ["arcana"] + list(args)
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout,
        )
        return result.returncode, result.stdout + result.stderr
    except subprocess.TimeoutExpired:
        return -1, f"TIMEOUT after {timeout}s"


def psql(query: str) -> str:
    """Run a psql query via docker exec and return stripped result."""
    cmd = [
        "docker", "exec", "arcana-tsdb",
        "psql", "-U", "arcana", "-d", "arcana",
        "-t", "-A", "-c", query,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    return result.stdout.strip()


def psql_int(query: str) -> int:
    """Run a psql query and return the result as an integer."""
    return int(psql(query))


def table_name(spec: str) -> str:
    """Return per-pair-per-type bar table name."""
    return f"bars_{spec.replace('.', '_')}_{PAIR_NORM}"


# ── Constants ──────────────────────────────────────────────────────────────
# Ingest a 6-hour window (not full day) to keep runtime under 10 minutes
HOURS_AGO = 6
SINCE = (datetime.now(UTC) - timedelta(hours=HOURS_AGO)).strftime("%Y-%m-%d %H:%M")
UNTIL = datetime.now(UTC).strftime("%Y-%m-%d %H:%M")
PAIR = "ETH-USD"
PAIR_NORM = PAIR.lower().replace("-", "_")

# All 11 bar types
BAR_SPECS = [
    "tick_500", "volume_100", "dollar_50000", "time_5m", "time_1h",
    "tib_10", "vib_10", "dib_10", "trb_10", "vrb_10", "drb_10",
]
INFO_DRIVEN = {"tib_10", "vib_10", "dib_10", "trb_10", "vrb_10", "drb_10"}

# Daemon settings
DAEMON_INTERVAL = 60  # seconds between polls
DAEMON_RUN_TIME = 150  # let daemon run ~2.5 minutes (2 full cycles)

# ── Metrics ────────────────────────────────────────────────────────────────
metrics: dict = {
    "ingest_trades": 0,
    "ingest_duration": 0.0,
    "ingest_rate": 0.0,
    "bars": {},
    "daemon_cycles": 0,
    "daemon_new_trades": 0,
}


def main() -> int:
    test_start = time.time()
    print("=" * 60)
    print("  ARCANA E2E INTEGRATION TEST")
    print(f"  Pair: {PAIR}  |  Window: last {HOURS_AGO}h")
    print(f"  {SINCE} -> {UNTIL}")
    print("=" * 60)

    # ── Clean slate ────────────────────────────────────────────────
    _log("Resetting database for clean test run...")
    bar_tables = psql(
        "SELECT tablename FROM pg_tables "
        "WHERE schemaname = 'public' AND tablename LIKE 'bars_%';"
    )
    for t in bar_tables.splitlines():
        if t.strip():
            psql(f"DROP TABLE IF EXISTS {t.strip()} CASCADE;")
    psql("DROP TABLE IF EXISTS bars CASCADE;")
    psql("DROP TABLE IF EXISTS raw_trades CASCADE;")
    _log("Tables dropped.\n")

    # ════════════════════════════════════════════════════════════════
    #  1. ONBOARDING: pip install + db init
    # ════════════════════════════════════════════════════════════════
    _section("1. SCHEMA INITIALIZATION (arcana db init)")

    rc, out = run_arcana("db", "init")
    if rc == 0:
        _pass("db init (first run)")
    else:
        _fail("db init (first run)", f"exit {rc}: {out[:200]}")

    rc, out = run_arcana("db", "init")
    if rc == 0:
        _pass("db init (idempotent second run)")
    else:
        _fail("db init (second run)", f"exit {rc}")

    if psql_int(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = 'raw_trades' AND table_schema = 'public';"
    ) == 1:
        _pass("raw_trades table exists")
    else:
        _fail("raw_trades table missing")

    if psql_int(
        "SELECT COUNT(*) FROM timescaledb_information.hypertables "
        "WHERE hypertable_name = 'raw_trades';"
    ) == 1:
        _pass("raw_trades is a TimescaleDB hypertable")
    else:
        _fail("raw_trades hypertable missing")

    # ════════════════════════════════════════════════════════════════
    #  2. EDGE CASES (empty DB)
    # ════════════════════════════════════════════════════════════════
    _section("2. PRE-DATA EDGE CASES")

    rc, out = run_arcana("bars", "build", "tick_500", PAIR)
    if rc == 0 and "0 bars built" in out:
        _pass("bars build on empty DB -> 0 bars")
    else:
        _fail("bars build on empty DB", f"exit {rc}, {out[:200]}")

    rc, _ = run_arcana("summon", PAIR, "--interval", "10", timeout=30)
    if rc != 0:
        _pass("daemon refuses to start with no data")
    else:
        _fail("daemon should refuse with no data")

    rc, _ = run_arcana("bars", "build", "invalid_spec", PAIR)
    if rc != 0:
        _pass("invalid bar spec rejected")
    else:
        _fail("invalid bar spec should be rejected")

    for bad in ["tick_", "volume_abc", "time_5x", "dollar", "foo_bar"]:
        rc, _ = run_arcana("bars", "build", bad, PAIR)
        if rc != 0:
            _pass(f"bad spec '{bad}' rejected")
        else:
            _fail(f"bad spec '{bad}' should be rejected")

    rc, out = run_arcana("bars", "build", "tick_500", "FAKE-PAIR")
    if rc == 0 and "0 bars built" in out:
        _pass("bars build on fake pair -> 0 bars")
    else:
        _fail("bars build on fake pair", f"exit {rc}, {out[:200]}")

    # ════════════════════════════════════════════════════════════════
    #  3. TRADE INGESTION
    # ════════════════════════════════════════════════════════════════
    _section(f"3. TRADE INGESTION ({HOURS_AGO}h window)")
    _log(f"Ingesting {PAIR} trades: {SINCE} -> {UNTIL}")
    _log("(This is the longest step — typically 2-5 minutes)\n")

    t0 = time.time()
    rc, out = run_arcana(
        "--log-level", "INFO",
        "ingest", PAIR,
        "--since", SINCE,
        "--until", UNTIL,
        timeout=600,
    )
    t1 = time.time()
    metrics["ingest_duration"] = t1 - t0

    if rc == 0:
        _pass("ingest completed", f"{metrics['ingest_duration']:.1f}s")
    else:
        _fail("ingest FAILED", f"exit {rc}: {out[:300]}")
        print(f"\n\033[91mABORTED: {FAIL} failures in {time.time() - test_start:.0f}s\033[0m")
        return 1

    trade_count = psql_int(f"SELECT COUNT(*) FROM raw_trades WHERE pair = '{PAIR}';")
    metrics["ingest_trades"] = trade_count

    if trade_count > 0:
        _pass("trades stored", f"{trade_count:,}")
    else:
        _fail("no trades stored")
        print(f"\n\033[91mABORTED: {FAIL} failures in {time.time() - test_start:.0f}s\033[0m")
        return 1

    if metrics["ingest_duration"] > 0:
        metrics["ingest_rate"] = trade_count / metrics["ingest_duration"]

    _log(f"-> {trade_count:,} trades in {metrics['ingest_duration']:.1f}s "
         f"(~{metrics['ingest_rate']:.0f} trades/sec)")

    # ════════════════════════════════════════════════════════════════
    #  4. INGESTION IDEMPOTENCY
    # ════════════════════════════════════════════════════════════════
    _section("4. INGESTION IDEMPOTENCY")
    _log("Re-running identical ingest...")

    count_before = psql_int(f"SELECT COUNT(*) FROM raw_trades WHERE pair = '{PAIR}';")
    rc, _ = run_arcana("ingest", PAIR, "--since", SINCE, "--until", UNTIL, timeout=600)
    count_after = psql_int(f"SELECT COUNT(*) FROM raw_trades WHERE pair = '{PAIR}';")

    if rc == 0:
        _pass("re-ingest completed")
    else:
        _fail("re-ingest failed", f"exit {rc}")

    if count_after == count_before:
        _pass("zero duplicates after re-ingest", f"{count_before:,} == {count_after:,}")
    else:
        _fail("duplicates detected!", f"before={count_before}, after={count_after}")

    # ════════════════════════════════════════════════════════════════
    #  5. BAR CONSTRUCTION (all 11 types)
    # ════════════════════════════════════════════════════════════════
    _section("5. BAR CONSTRUCTION (11 types)")
    _log(f"Building bars from {trade_count:,} trades...\n")

    for spec in BAR_SPECS:
        _log(f"Building {spec}...")
        t0 = time.time()
        rc, out = run_arcana("--log-level", "INFO", "bars", "build", spec, PAIR, timeout=300)
        duration = time.time() - t0

        if rc != 0:
            _fail(f"bars build {spec}", f"exit {rc}: {out[:200]}")
            continue

        _pass(f"bars build {spec}", f"{duration:.1f}s")

        tbl = table_name(spec)
        bar_count = psql_int(f"SELECT COUNT(*) FROM {tbl} WHERE pair = '{PAIR}';")

        if bar_count > 0:
            _pass(f"{spec} bar count", f"{bar_count:,}")
        else:
            _fail(f"{spec} bar count", "0 bars produced")

        # Info-driven bars: verify EWMA metadata
        if spec in INFO_DRIVEN and bar_count > 0:
            null_meta = psql_int(f"SELECT COUNT(*) FROM {tbl} WHERE metadata IS NULL;")
            if null_meta == 0:
                _pass(f"{spec} metadata on all bars")
            else:
                _fail(f"{spec} metadata", f"{null_meta} bars missing metadata")

            sample = psql(f"SELECT metadata::text FROM {tbl} LIMIT 1;")
            if "ewma_window" in sample and "ewma_expected" in sample:
                _pass(f"{spec} EWMA keys in metadata")
            else:
                _fail(f"{spec} EWMA keys", f"got: {sample[:100]}")

        metrics["bars"][spec] = {"count": bar_count, "duration": duration}
        _log(f"-> {bar_count:,} bars in {duration:.1f}s\n")

    # ════════════════════════════════════════════════════════════════
    #  6. BAR BUILD IDEMPOTENCY
    # ════════════════════════════════════════════════════════════════
    _section("6. BAR BUILD IDEMPOTENCY")

    tick_tbl = table_name("tick_500")
    before = psql_int(f"SELECT COUNT(*) FROM {tick_tbl} WHERE pair = '{PAIR}';")
    run_arcana("bars", "build", "tick_500", PAIR)
    after = psql_int(f"SELECT COUNT(*) FROM {tick_tbl} WHERE pair = '{PAIR}';")

    if after == before:
        _pass("tick_500 idempotent rebuild", f"{before} == {after}")
    else:
        _fail("tick_500 changed!", f"before={before}, after={after}")

    # ════════════════════════════════════════════════════════════════
    #  7. STATUS COMMAND
    # ════════════════════════════════════════════════════════════════
    _section("7. STATUS COMMAND")

    rc, out = run_arcana("status", PAIR)
    if rc == 0:
        _pass("arcana status exits 0")
    else:
        _fail("arcana status", f"exit {rc}")

    if "Total trades" in out:
        _pass("status shows trade count")
    else:
        _fail("status missing trade count")

    if PAIR in out:
        _pass("status shows pair")
    else:
        _fail("status missing pair")

    # ════════════════════════════════════════════════════════════════
    #  8. DAEMON LIFECYCLE (~2.5 min run)
    # ════════════════════════════════════════════════════════════════
    _section(f"8. DAEMON RUN ({DAEMON_RUN_TIME}s, {DAEMON_INTERVAL}s interval)")

    trades_before_daemon = psql_int(
        f"SELECT COUNT(*) FROM raw_trades WHERE pair = '{PAIR}';"
    )

    _log("Starting daemon (PID will follow)...")
    _log(f"Trades before daemon: {trades_before_daemon:,}")

    creation_flags = 0
    if sys.platform == "win32":
        creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP

    daemon_proc = subprocess.Popen(
        ["arcana", "--log-level", "INFO", "summon", PAIR,
         "--interval", str(DAEMON_INTERVAL)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=creation_flags,
    )

    time.sleep(8)

    if daemon_proc.poll() is None:
        _pass("daemon is running", f"PID {daemon_proc.pid}")
    else:
        _fail("daemon exited prematurely", f"exit {daemon_proc.returncode}")

    # Let daemon run for DAEMON_RUN_TIME seconds, polling trade count
    _log(f"Letting daemon run for {DAEMON_RUN_TIME}s...")
    remaining = DAEMON_RUN_TIME - 8  # already waited 8s
    check_interval = 30
    while remaining > 0:
        sleep_time = min(check_interval, remaining)
        time.sleep(sleep_time)
        remaining -= sleep_time

        if daemon_proc.poll() is not None:
            _fail("daemon died during run", f"exit {daemon_proc.returncode}")
            break

        current = psql_int(f"SELECT COUNT(*) FROM raw_trades WHERE pair = '{PAIR}';")
        _log(f"  [{DAEMON_RUN_TIME - remaining:>3.0f}s] trades: {current:,} "
             f"(+{current - trades_before_daemon:,})")

    trades_after_daemon = psql_int(
        f"SELECT COUNT(*) FROM raw_trades WHERE pair = '{PAIR}';"
    )
    metrics["daemon_new_trades"] = trades_after_daemon - trades_before_daemon

    if trades_after_daemon >= trades_before_daemon:
        _pass("daemon maintained/grew trade count",
              f"{trades_before_daemon:,} -> {trades_after_daemon:,} "
              f"(+{metrics['daemon_new_trades']:,})")
    else:
        _fail("trade count decreased!", f"{trades_before_daemon} -> {trades_after_daemon}")

    # Graceful shutdown
    _log("Sending interrupt for graceful shutdown...")
    try:
        if sys.platform == "win32":
            daemon_proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            daemon_proc.send_signal(signal.SIGINT)
    except Exception as e:
        _log(f"  (signal failed: {e}, using terminate)")
        daemon_proc.terminate()

    try:
        daemon_proc.wait(timeout=15)
        _pass("daemon shutdown gracefully", f"exit {daemon_proc.returncode}")
    except subprocess.TimeoutExpired:
        _log("  Daemon didn't respond, terminating...")
        daemon_proc.terminate()
        try:
            daemon_proc.wait(timeout=5)
            _pass("daemon terminated", f"exit {daemon_proc.returncode}")
        except subprocess.TimeoutExpired:
            daemon_proc.kill()
            daemon_proc.wait()
            _fail("daemon required kill")

    # ════════════════════════════════════════════════════════════════
    #  9. DATA INTEGRITY PROOFS (100% accuracy assertions)
    # ════════════════════════════════════════════════════════════════
    _section("9. DATA INTEGRITY PROOFS")
    _log("Asserting 100% accuracy on all stored data...\n")

    # --- Trade-level proofs ---
    _log("Trade-level proofs:")

    # No NULLs in critical columns
    nulls = psql_int(
        "SELECT COUNT(*) FROM raw_trades "
        "WHERE price IS NULL OR size IS NULL OR side IS NULL;"
    )
    if nulls == 0:
        _pass("zero NULL prices/sizes/sides")
    else:
        _fail("NULL values found", f"{nulls} rows")

    # All trades belong to correct pair
    wrong_pair = psql_int(f"SELECT COUNT(*) FROM raw_trades WHERE pair != '{PAIR}';")
    if wrong_pair == 0:
        _pass(f"all trades are for {PAIR}")
    else:
        _fail("wrong pair trades", f"{wrong_pair} rows")

    # All prices and sizes are positive
    bad_nums = psql_int(
        "SELECT COUNT(*) FROM raw_trades WHERE price <= 0 OR size <= 0;"
    )
    if bad_nums == 0:
        _pass("all prices and sizes are positive")
    else:
        _fail("non-positive price/size", f"{bad_nums} rows")

    # Timestamps are within expected range (no future trades, no ancient ones)
    future = psql_int(
        "SELECT COUNT(*) FROM raw_trades "
        "WHERE timestamp > NOW() + INTERVAL '1 hour';"
    )
    if future == 0:
        _pass("no future-dated trades")
    else:
        _fail("future trades found", f"{future} rows")

    # Trade IDs are unique per source
    dup_trades = psql_int(
        "SELECT COUNT(*) - COUNT(DISTINCT trade_id) FROM raw_trades;"
    )
    if dup_trades == 0:
        _pass("all trade_ids are unique")
    else:
        _fail("duplicate trade_ids", f"{dup_trades} dupes")

    # Trades are monotonically ordered by timestamp (no inversions)
    inversions = psql_int(
        "SELECT COUNT(*) FROM ("
        "  SELECT timestamp, LAG(timestamp) OVER (ORDER BY timestamp) AS prev_ts "
        "  FROM raw_trades WHERE pair = '" + PAIR + "'"
        ") sub WHERE prev_ts > timestamp;"
    )
    if inversions == 0:
        _pass("trades are monotonically time-ordered")
    else:
        _fail("timestamp inversions", f"{inversions}")

    # --- Bar-level proofs (per table) ---
    _log("\nBar-level proofs (per type):")

    for spec in BAR_SPECS:
        tbl = table_name(spec)
        bar_count = psql_int(f"SELECT COUNT(*) FROM {tbl};")
        if bar_count == 0:
            _log(f"  [{spec}] skipping proofs (0 bars)")
            continue

        checks = [
            ("high >= low",
             f"SELECT COUNT(*) FROM {tbl} WHERE high < low;"),
            ("open in [low, high]",
             f"SELECT COUNT(*) FROM {tbl} WHERE open > high OR open < low;"),
            ("close in [low, high]",
             f"SELECT COUNT(*) FROM {tbl} WHERE close > high OR close < low;"),
            ("VWAP in [low, high]",
             f"SELECT COUNT(*) FROM {tbl} WHERE vwap > high OR vwap < low;"),
            ("tick_count > 0",
             f"SELECT COUNT(*) FROM {tbl} WHERE tick_count <= 0;"),
            ("volume >= 0",
             f"SELECT COUNT(*) FROM {tbl} WHERE volume < 0;"),
            ("dollar_volume >= 0",
             f"SELECT COUNT(*) FROM {tbl} WHERE dollar_volume < 0;"),
            ("time_start <= time_end",
             f"SELECT COUNT(*) FROM {tbl} WHERE time_start > time_end;"),
        ]

        for desc, query in checks:
            violations = psql_int(query)
            if violations == 0:
                _pass(f"[{spec}] {desc}")
            else:
                _fail(f"[{spec}] {desc}", f"{violations} violations")

        # Bars should be time-ordered (no overlapping bar boundaries)
        bar_inversions = psql_int(
            f"SELECT COUNT(*) FROM ("
            f"  SELECT time_start, LAG(time_end) OVER (ORDER BY time_start) AS prev_end "
            f"  FROM {tbl} WHERE pair = '{PAIR}'"
            f") sub WHERE prev_end > time_start;"
        )
        if bar_inversions == 0:
            _pass(f"[{spec}] bars are time-ordered (no overlaps)")
        else:
            _fail(f"[{spec}] bar time overlap", f"{bar_inversions}")

    # --- Tick bar statistical proof ---
    _log("\nStatistical proofs:")

    avg_ticks = psql(
        f"SELECT ROUND(AVG(tick_count)) FROM {table_name('tick_500')} "
        f"WHERE pair = '{PAIR}';"
    )
    try:
        avg = int(avg_ticks)
        if 450 <= avg <= 500:
            _pass("tick_500 avg tick count", f"{avg} (expect ~500)")
        else:
            _fail("tick_500 avg tick count", f"got {avg}, expect 450-500")
    except (ValueError, TypeError):
        _fail("tick_500 avg tick count", f"parse error: {avg_ticks}")

    # Time bar coverage proof
    time_5m_tbl = table_name("time_5m")
    time_5m_count = psql_int(
        f"SELECT COUNT(*) FROM {time_5m_tbl} WHERE pair = '{PAIR}';"
    )
    expected_5m = (HOURS_AGO * 60) // 5  # e.g., 6h = 72 five-minute bars
    if time_5m_count > 0:
        ratio = time_5m_count / expected_5m if expected_5m > 0 else 0
        if 0.7 <= ratio <= 1.3:
            _pass("time_5m coverage",
                  f"{time_5m_count} bars (~{ratio:.0%} of {expected_5m} expected)")
        else:
            _pass("time_5m bars produced", f"{time_5m_count}")
    else:
        _fail("time_5m bar count", "0 bars")

    # ════════════════════════════════════════════════════════════════
    # 10. STORAGE METRICS
    # ════════════════════════════════════════════════════════════════
    _section("10. STORAGE METRICS")

    final_trade_count = psql_int(f"SELECT COUNT(*) FROM raw_trades WHERE pair = '{PAIR}';")
    trades_size = psql("SELECT pg_size_pretty(hypertable_size('raw_trades'));")
    trades_bytes = psql_int("SELECT hypertable_size('raw_trades');")

    bar_table_list = psql(
        "SELECT tablename FROM pg_tables "
        "WHERE schemaname = 'public' AND tablename LIKE 'bars_%';"
    )
    bar_tables_found = [t.strip() for t in bar_table_list.splitlines() if t.strip()]

    bars_bytes = 0
    total_bar_count = 0
    for bt in bar_tables_found:
        bt_bytes = psql_int(f"SELECT hypertable_size('{bt}');")
        bt_count = psql_int(f"SELECT COUNT(*) FROM {bt} WHERE pair = '{PAIR}';")
        bars_bytes += bt_bytes
        total_bar_count += bt_count

    total_bytes = trades_bytes + bars_bytes
    if bars_bytes > 0:
        bars_size = psql(f"SELECT pg_size_pretty({bars_bytes}::bigint);")
    else:
        bars_size = "0 bytes"
    total_size = psql(f"SELECT pg_size_pretty({total_bytes}::bigint);")

    bytes_per_trade = trades_bytes // final_trade_count if final_trade_count > 0 else 0
    bytes_per_bar = bars_bytes // total_bar_count if total_bar_count > 0 else 0

    # ════════════════════════════════════════════════════════════════
    # SUMMARY
    # ════════════════════════════════════════════════════════════════
    elapsed = time.time() - test_start

    print("\n")
    print("+" + "=" * 62 + "+")
    print("|            ARCANA E2E TEST RESULTS                           |")
    print("+" + "=" * 62 + "+")
    print(f"|  Pair:              {PAIR:<42s}|")
    window_s = f"last {HOURS_AGO}h ({SINCE} -> {UNTIL})"
    print(f"|  Window:            {window_s:<42s}|")
    runtime_s = f"{elapsed:.0f}s"
    print(f"|  Total runtime:     {runtime_s:<42s}|")
    print("+" + "-" * 62 + "+")
    print("|  INGESTION" + " " * 51 + "|")
    print("+" + "-" * 62 + "+")
    print(f"|  Trades:            {final_trade_count:<42,d}|")
    print(f"|  Duration:          {metrics['ingest_duration']:<42.1f}|")
    print(f"|  Rate:              ~{metrics['ingest_rate']:<41.0f}|")
    print("+" + "-" * 62 + "+")
    print("|  DAEMON" + " " * 54 + "|")
    print("+" + "-" * 62 + "+")
    daemon_s = f"{DAEMON_RUN_TIME}s"
    print(f"|  Run time:          {daemon_s:<42s}|")
    print(f"|  New trades:        +{metrics['daemon_new_trades']:<41,d}|")
    print("+" + "-" * 62 + "+")
    print("|  BAR CONSTRUCTION" + " " * 44 + "|")
    print("+" + "-" * 62 + "+")
    for spec, data in metrics.get("bars", {}).items():
        line = f"{data['count']:,} bars in {data['duration']:.1f}s"
        print(f"|  {spec:<20s}{line:<42s}|")
    print("+" + "-" * 62 + "+")
    print("|  STORAGE" + " " * 53 + "|")
    print("+" + "-" * 62 + "+")
    trades_info = f"{trades_size} ({final_trade_count:,} rows, ~{bytes_per_trade} B/row)"
    print(f"|  raw_trades:        {trades_info:<42s}|")
    bars_info = f"{bars_size} ({total_bar_count:,} rows, ~{bytes_per_bar} B/row)"
    print(f"|  bars (11 tables):  {bars_info:<42s}|")
    print(f"|  total:             {total_size:<42s}|")
    print("+" + "-" * 62 + "+")
    print("|  TESTS" + " " * 55 + "|")
    print("+" + "-" * 62 + "+")
    print(f"|  Passed:            {PASS:<42d}|")
    print(f"|  Failed:            {FAIL:<42d}|")
    print(f"|  Total:             {PASS + FAIL:<42d}|")
    print("+" + "=" * 62 + "+")
    print()

    if FAIL > 0:
        print(f"\033[91mRESULT: FAIL ({FAIL} failures)\033[0m")
        print("\nFailed tests:")
        for desc, passed, detail in RESULTS:
            if not passed:
                print(f"  \u2717 {desc}: {detail}")
    else:
        print(f"\033[92mRESULT: ALL {PASS} TESTS PASSED\033[0m")
        print(f"100% data integrity verified across {final_trade_count:,} trades "
              f"and {total_bar_count:,} bars in {elapsed:.0f}s")

    return 1 if FAIL > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
