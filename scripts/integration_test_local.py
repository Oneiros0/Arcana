#!/usr/bin/env python3
"""Arcana Local Integration Test & Performance Diagnostics.

Runs the full user journey against a live TimescaleDB instance with real
Coinbase API data. Collects performance metrics, storage footprint, and
validates data integrity.

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
from datetime import datetime, timedelta, timezone
from decimal import Decimal

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
    print(f"  \033[92m✓\033[0m {description}{tag}", flush=True)


def _fail(description: str, detail: str = "") -> None:
    global FAIL
    FAIL += 1
    RESULTS.append((description, False, detail))
    print(f"  \033[91m✗\033[0m {description}", flush=True)
    if detail:
        print(f"    → {detail}", flush=True)


def _section(title: str) -> None:
    print(f"\n{'━' * 55}", flush=True)
    print(f"  {title}", flush=True)
    print(f"{'━' * 55}", flush=True)


def run_arcana(*args: str, expect_fail: bool = False, timeout: int = 600) -> tuple[int, str]:
    """Run an arcana CLI command, return (exit_code, combined_output)."""
    cmd = ["arcana"] + list(args)
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        output = result.stdout + result.stderr
        return result.returncode, output
    except subprocess.TimeoutExpired:
        return -1, f"TIMEOUT after {timeout}s"


def psql(query: str) -> str:
    """Run a psql query and return the result as a stripped string."""
    cmd = [
        "docker", "exec", "arcana-tsdb",
        "psql", "-U", "arcana", "-d", "arcana",
        "-t", "-A", "-c", query,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    return result.stdout.strip()


# ── Dates ───────────────────────────────────────────────────────────────────
YESTERDAY = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")
PAIR = "ETH-USD"
PAIR_NORM = PAIR.lower().replace("-", "_")  # "eth_usd" for table names

# ── Metrics ─────────────────────────────────────────────────────────────────
metrics: dict = {
    "ingest_trades": 0,
    "ingest_duration": 0.0,
    "ingest_rate": 0.0,
    "trade_min_ts": "",
    "trade_max_ts": "",
    "bars": {},
    "storage": {},
}


def main() -> int:
    print("=" * 55)
    print("  ARCANA INTEGRATION TEST & DIAGNOSTICS")
    print(f"  Pair: {PAIR}  |  Period: {YESTERDAY} -> {TODAY}")
    print("=" * 55)

    # ── Clean slate: drop existing tables so edge cases work ────────
    _log("Resetting database for clean test run...")
    # Drop all per-type bar tables
    bar_tables = psql(
        "SELECT tablename FROM pg_tables "
        "WHERE schemaname = 'public' AND tablename LIKE 'bars_%';"
    )
    for table in bar_tables.splitlines():
        if table.strip():
            psql(f"DROP TABLE IF EXISTS {table.strip()} CASCADE;")
    psql("DROP TABLE IF EXISTS bars CASCADE;")  # legacy single table
    psql("DROP TABLE IF EXISTS raw_trades CASCADE;")
    _log("Tables dropped.\n")

    # ════════════════════════════════════════════════════════════════
    #  1. SCHEMA INITIALIZATION
    # ════════════════════════════════════════════════════════════════
    _section("1. SCHEMA INITIALIZATION")

    # First run
    rc, out = run_arcana("db", "init")
    if rc == 0:
        _pass("db init (first run)", "exit 0")
    else:
        _fail("db init (first run)", f"exit {rc}: {out[:200]}")

    # Idempotent second run
    rc, out = run_arcana("db", "init")
    if rc == 0:
        _pass("db init (second run — idempotent)", "exit 0")
    else:
        _fail("db init (second run)", f"exit {rc}")

    # Verify raw_trades table (bar tables are created lazily on first build)
    table_count = psql(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = 'raw_trades' AND table_schema = 'public';"
    )
    if table_count == "1":
        _pass("raw_trades table exists")
    else:
        _fail("tables check", f"expected 1, got {table_count}")

    # Verify hypertable
    ht_count = psql(
        "SELECT COUNT(*) FROM timescaledb_information.hypertables "
        "WHERE hypertable_name = 'raw_trades';"
    )
    if ht_count == "1":
        _pass("TimescaleDB hypertable for raw_trades created")
    else:
        _fail("hypertable check", f"expected 1, got {ht_count}")

    # ════════════════════════════════════════════════════════════════
    #  2. PRE-DATA EDGE CASES
    # ════════════════════════════════════════════════════════════════
    _section("2. PRE-DATA EDGE CASES")

    # bars build on empty DB
    rc, out = run_arcana("bars", "build", "tick_500", PAIR)
    if rc == 0 and "0 bars built" in out:
        _pass("bars build on empty DB → 0 bars")
    else:
        _fail("bars build on empty DB", f"exit {rc}, output: {out[:200]}")

    # daemon with no data
    rc, out = run_arcana("run", PAIR, "--interval", "10", timeout=30)
    if rc != 0:
        _pass("daemon refuses to start with no data", f"exit {rc}")
    else:
        _fail("daemon should refuse with no data", "exit 0")

    # invalid bar spec
    rc, out = run_arcana("bars", "build", "invalid_spec", PAIR)
    if rc != 0:
        _pass("invalid bar spec rejected", f"exit {rc}")
    else:
        _fail("invalid bar spec should be rejected", "exit 0")

    # various invalid specs
    for bad_spec in ["tick_", "volume_abc", "time_5x", "dollar", "foo_bar"]:
        rc, _ = run_arcana("bars", "build", bad_spec, PAIR)
        if rc != 0:
            _pass(f"bad spec '{bad_spec}' rejected")
        else:
            _fail(f"bad spec '{bad_spec}' should be rejected")

    # non-existent pair
    rc, out = run_arcana("bars", "build", "tick_500", "FAKE-PAIR")
    if rc == 0 and "0 bars built" in out:
        _pass("bars build on non-existent pair → 0 bars")
    else:
        _fail("bars build on fake pair", f"exit {rc}, output: {out[:200]}")

    # ════════════════════════════════════════════════════════════════
    #  3. TRADE INGESTION
    # ════════════════════════════════════════════════════════════════
    _section(f"3. TRADE INGESTION ({YESTERDAY} → {TODAY})")
    _log(f"Ingesting {PAIR} trades from {YESTERDAY}...")
    _log("(This may take 5-15 minutes depending on volume)\n")

    t0 = time.time()
    rc, out = run_arcana(
        "--log-level", "INFO",
        "ingest", PAIR,
        "--since", YESTERDAY,
        "--until", TODAY,
        timeout=1200,  # 20 min max
    )
    t1 = time.time()
    metrics["ingest_duration"] = t1 - t0

    if rc == 0:
        _pass("ingest completed", f"exit 0 in {metrics['ingest_duration']:.1f}s")
    else:
        _fail("ingest failed", f"exit {rc}: {out[:300]}")
        # Fatal — can't continue without data
        _print_summary()
        return 1

    # Query trade count
    trade_count = int(psql(f"SELECT COUNT(*) FROM raw_trades WHERE pair = '{PAIR}';"))
    metrics["ingest_trades"] = trade_count

    if trade_count > 0:
        _pass(f"trades stored in database", f"{trade_count:,}")
    else:
        _fail("no trades stored", "count = 0")
        _print_summary()
        return 1

    # Timestamp range
    metrics["trade_min_ts"] = psql(f"SELECT MIN(timestamp)::text FROM raw_trades WHERE pair = '{PAIR}';")
    metrics["trade_max_ts"] = psql(f"SELECT MAX(timestamp)::text FROM raw_trades WHERE pair = '{PAIR}';")

    # Ingestion rate
    if metrics["ingest_duration"] > 0:
        metrics["ingest_rate"] = trade_count / metrics["ingest_duration"]

    _log(f"→ {trade_count:,} trades in {metrics['ingest_duration']:.1f}s "
         f"(~{metrics['ingest_rate']:.0f} trades/sec)")

    # ════════════════════════════════════════════════════════════════
    #  4. INGESTION IDEMPOTENCY
    # ════════════════════════════════════════════════════════════════
    _section("4. INGESTION IDEMPOTENCY")
    _log("Re-running identical ingest to verify no duplicates...")

    count_before = int(psql(f"SELECT COUNT(*) FROM raw_trades WHERE pair = '{PAIR}';"))

    rc, out = run_arcana(
        "ingest", PAIR,
        "--since", YESTERDAY,
        "--until", TODAY,
        timeout=1200,
    )

    count_after = int(psql(f"SELECT COUNT(*) FROM raw_trades WHERE pair = '{PAIR}';"))

    if rc == 0:
        _pass("idempotent re-ingest completed", f"exit 0")
    else:
        _fail("re-ingest failed", f"exit {rc}")

    if count_after == count_before:
        _pass("trade count unchanged after re-ingest", f"{count_before:,} == {count_after:,}")
    else:
        _fail("trade count changed!", f"before={count_before}, after={count_after}")

    # ════════════════════════════════════════════════════════════════
    #  5. BAR CONSTRUCTION — ALL TYPES
    # ════════════════════════════════════════════════════════════════
    _section("5. BAR CONSTRUCTION")
    _log(f"Building bars from {trade_count:,} trades...\n")

    # Standard bars (fixed threshold) + information-driven bars (EWMA adaptive)
    bar_specs = [
        "tick_500", "volume_100", "dollar_50000", "time_5m", "time_1h",
        "tib_10", "vib_10", "dib_10", "trb_10", "vrb_10", "drb_10",
    ]

    # Info-driven types that require metadata validation
    info_driven_types = {"tib_10", "vib_10", "dib_10", "trb_10", "vrb_10", "drb_10"}

    for spec in bar_specs:
        _log(f"Building {spec}...")
        t0 = time.time()
        rc, out = run_arcana("--log-level", "INFO", "bars", "build", spec, PAIR, timeout=300)
        t1 = time.time()
        duration = t1 - t0

        if rc == 0:
            _pass(f"bars build {spec}", f"exit 0 in {duration:.1f}s")
        else:
            _fail(f"bars build {spec}", f"exit {rc}: {out[:200]}")
            continue

        # Per-pair table name: bars_{spec}_{pair_norm}
        # For specs with dots (volume_10.5) the dot becomes underscore
        table_name = f"bars_{spec.replace('.', '_')}_{PAIR_NORM}"
        bar_count = int(psql(
            f"SELECT COUNT(*) FROM {table_name} WHERE pair = '{PAIR}';"
        ))

        if bar_count > 0:
            _pass(f"{spec} bar count", f"{bar_count:,}")
        else:
            _fail(f"{spec} bar count", "0 bars produced")

        # Info-driven bars must have non-null metadata with EWMA state
        if spec in info_driven_types and bar_count > 0:
            null_meta_count = int(psql(
                f"SELECT COUNT(*) FROM {table_name} WHERE metadata IS NULL;"
            ))
            if null_meta_count == 0:
                _pass(f"{spec} metadata present on all bars")
            else:
                _fail(f"{spec} metadata", f"{null_meta_count} bars missing metadata")

            # Verify EWMA keys exist in a sample metadata
            sample_meta = psql(
                f"SELECT metadata::text FROM {table_name} LIMIT 1;"
            )
            if "ewma_window" in sample_meta and "ewma_expected" in sample_meta:
                _pass(f"{spec} metadata has EWMA keys")
            else:
                _fail(f"{spec} metadata EWMA keys", f"got: {sample_meta[:100]}")

        metrics["bars"][spec] = {"count": bar_count, "duration": duration}
        _log(f"→ {bar_count:,} bars in {duration:.1f}s\n")

    # ════════════════════════════════════════════════════════════════
    #  6. BAR BUILD IDEMPOTENCY
    # ════════════════════════════════════════════════════════════════
    _section("6. BAR BUILD IDEMPOTENCY")
    _log("Re-running tick_500 bar build...")

    tick_table = f"bars_tick_500_{PAIR_NORM}"
    count_before = int(psql(
        f"SELECT COUNT(*) FROM {tick_table} WHERE pair = '{PAIR}';"
    ))

    rc, out = run_arcana("bars", "build", "tick_500", PAIR)

    count_after = int(psql(
        f"SELECT COUNT(*) FROM {tick_table} WHERE pair = '{PAIR}';"
    ))

    if count_after == count_before:
        _pass("tick_500 count unchanged after rebuild", f"{count_before} == {count_after}")
    else:
        _fail("tick_500 count changed!", f"before={count_before}, after={count_after}")

    # ════════════════════════════════════════════════════════════════
    #  7. STATUS COMMAND
    # ════════════════════════════════════════════════════════════════
    _section("7. STATUS COMMAND")

    rc, out = run_arcana("status", PAIR)
    if rc == 0:
        _pass("status command", "exit 0")
    else:
        _fail("status command", f"exit {rc}")

    if "Total trades" in out:
        _pass("status shows trade count")
    else:
        _fail("status missing trade count")

    if PAIR in out:
        _pass("status shows pair name")
    else:
        _fail("status missing pair name")

    _log(f"→ Status output:")
    for line in out.strip().splitlines():
        _log(f"  {line}")

    # ════════════════════════════════════════════════════════════════
    #  8. DAEMON LIFECYCLE
    # ════════════════════════════════════════════════════════════════
    _section("8. DAEMON LIFECYCLE")
    _log("Starting daemon in background...")

    # On Windows, CREATE_NEW_PROCESS_GROUP is needed for CTRL_BREAK_EVENT
    creation_flags = 0
    if sys.platform == "win32":
        creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP

    daemon_proc = subprocess.Popen(
        ["arcana", "run", PAIR, "--interval", "30"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=creation_flags,
    )

    time.sleep(8)

    if daemon_proc.poll() is None:
        _pass("daemon is running", f"PID {daemon_proc.pid}")
    else:
        rc = daemon_proc.returncode
        _fail("daemon should be running", f"exited with {rc}")

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
        # On Windows, CTRL_BREAK may cause non-zero exit — that's OK
        _pass("daemon exited after interrupt", f"exit {daemon_proc.returncode}")
    except subprocess.TimeoutExpired:
        _log("  Daemon didn't respond to interrupt, terminating...")
        daemon_proc.terminate()
        try:
            daemon_proc.wait(timeout=5)
            _pass("daemon terminated", f"exit {daemon_proc.returncode}")
        except subprocess.TimeoutExpired:
            daemon_proc.kill()
            daemon_proc.wait()
            _fail("daemon required kill", "didn't respond to terminate")

    # ════════════════════════════════════════════════════════════════
    #  9. DATA VALIDATION
    # ════════════════════════════════════════════════════════════════
    _section("9. DATA VALIDATION")
    _log("Running data integrity checks...")

    # Trade-level checks
    trade_checks = [
        (
            "no NULL prices/sizes/sides",
            "SELECT COUNT(*) FROM raw_trades WHERE price IS NULL OR size IS NULL OR side IS NULL;",
            "0",
        ),
        (
            f"all trades are for {PAIR}",
            f"SELECT COUNT(*) FROM raw_trades WHERE pair != '{PAIR}';",
            "0",
        ),
    ]

    for desc, query, expected in trade_checks:
        result = psql(query)
        if result == expected:
            _pass(desc)
        else:
            _fail(desc, f"expected {expected}, got {result}")

    # Per-table bar integrity checks (per-pair-per-type table naming)
    for spec in bar_specs:
        table = f"bars_{spec.replace('.', '_')}_{PAIR_NORM}"
        bar_checks = [
            (f"[{spec}] high >= low", f"SELECT COUNT(*) FROM {table} WHERE high < low;", "0"),
            (f"[{spec}] open in range", f"SELECT COUNT(*) FROM {table} WHERE open > high OR open < low;", "0"),
            (f"[{spec}] close in range", f"SELECT COUNT(*) FROM {table} WHERE close > high OR close < low;", "0"),
            (f"[{spec}] positive tick count", f"SELECT COUNT(*) FROM {table} WHERE tick_count <= 0;", "0"),
            (f"[{spec}] non-negative volume", f"SELECT COUNT(*) FROM {table} WHERE volume < 0;", "0"),
            (f"[{spec}] VWAP in range", f"SELECT COUNT(*) FROM {table} WHERE vwap > high OR vwap < low;", "0"),
            (f"[{spec}] time_start <= time_end", f"SELECT COUNT(*) FROM {table} WHERE time_start > time_end;", "0"),
        ]
        for desc, query, expected in bar_checks:
            result = psql(query)
            if result == expected:
                _pass(desc)
            else:
                _fail(desc, f"expected {expected}, got {result}")

    # Tick bar average tick count (~500 +/- 50)
    avg_ticks = psql(
        f"SELECT ROUND(AVG(tick_count)) FROM bars_tick_500_{PAIR_NORM} WHERE pair = '{PAIR}';"
    )
    try:
        avg = int(avg_ticks)
        if 450 <= avg <= 500:
            _pass(f"tick_500 average tick count", f"{avg} (expected ~500)")
        else:
            _fail(f"tick_500 average tick count", f"got {avg}, expected 450-500")
    except (ValueError, TypeError):
        _fail("tick_500 avg tick count", f"could not parse: {avg_ticks}")

    # Time bar coverage: 5m bars should have ~288 for a full day (up to ~336 for ~28h span)
    time_5m_count = int(psql(
        f"SELECT COUNT(*) FROM bars_time_5m_{PAIR_NORM} WHERE pair = '{PAIR}';"
    ))
    if 200 <= time_5m_count <= 350:
        _pass(f"time_5m bar count reasonable for ~1 day", f"{time_5m_count} (expect ~288)")
    elif time_5m_count > 0:
        _pass(f"time_5m bars produced", f"{time_5m_count}")
    else:
        _fail("time_5m bar count", "0 bars")

    # 1h bars should have ~24 (up to ~28 for slightly >1 day span)
    time_1h_count = int(psql(
        f"SELECT COUNT(*) FROM bars_time_1h_{PAIR_NORM} WHERE pair = '{PAIR}';"
    ))
    if 20 <= time_1h_count <= 30:
        _pass(f"time_1h bar count reasonable for ~1 day", f"{time_1h_count} (expect ~24)")
    elif time_1h_count > 0:
        _pass(f"time_1h bars produced", f"{time_1h_count}")
    else:
        _fail("time_1h bar count", "0 bars")

    # ════════════════════════════════════════════════════════════════
    # 10. STORAGE METRICS
    # ════════════════════════════════════════════════════════════════
    _section("10. STORAGE METRICS")

    # Use TimescaleDB hypertable_size for accurate chunk-inclusive sizes
    trades_size = psql("SELECT pg_size_pretty(hypertable_size('raw_trades'));")
    trades_bytes = int(psql("SELECT hypertable_size('raw_trades');"))

    # Sum sizes across all per-type bar tables
    bar_table_list = psql(
        "SELECT tablename FROM pg_tables "
        "WHERE schemaname = 'public' AND tablename LIKE 'bars_%';"
    )
    bar_tables_found = [t.strip() for t in bar_table_list.splitlines() if t.strip()]

    bars_bytes = 0
    total_bar_count = 0
    bar_storage_detail: dict[str, dict] = {}
    for bt in bar_tables_found:
        bt_bytes = int(psql(f"SELECT hypertable_size('{bt}');"))
        bt_count = int(psql(f"SELECT COUNT(*) FROM {bt} WHERE pair = '{PAIR}';"))
        bars_bytes += bt_bytes
        total_bar_count += bt_count
        bar_storage_detail[bt] = {
            "size": psql(f"SELECT pg_size_pretty(hypertable_size('{bt}'));"),
            "bytes": bt_bytes,
            "count": bt_count,
        }

    bars_size = psql(f"SELECT pg_size_pretty({bars_bytes}::bigint);") if bars_bytes > 0 else "0 bytes"
    total_size = psql(f"SELECT pg_size_pretty({trades_bytes + bars_bytes}::bigint);")

    bytes_per_trade = trades_bytes // trade_count if trade_count > 0 else 0
    bytes_per_bar = bars_bytes // total_bar_count if total_bar_count > 0 else 0

    metrics["storage"] = {
        "trades_size": trades_size,
        "bars_size": bars_size,
        "total_size": total_size,
        "bytes_per_trade": bytes_per_trade,
        "bytes_per_bar": bytes_per_bar,
        "total_bar_count": total_bar_count,
        "bar_tables": bar_storage_detail,
    }

    # ════════════════════════════════════════════════════════════════
    # SUMMARY
    # ════════════════════════════════════════════════════════════════
    _print_summary()

    return 1 if FAIL > 0 else 0


def _print_summary() -> None:
    trade_count = metrics["ingest_trades"]
    total_bar_count = metrics.get("storage", {}).get("total_bar_count", 0)

    print("\n")
    print("╔═══════════════════════════════════════════════════════════════╗")
    print("║            ARCANA INTEGRATION TEST RESULTS                  ║")
    print("╠═══════════════════════════════════════════════════════════════╣")
    print("║                                                             ║")
    print(f"║  Pair:              {PAIR:<40s}║")
    print(f"║  Period:            {YESTERDAY} → {TODAY:<28s}║")
    print("║                                                             ║")
    print("╠═══════════════════════════════════════════════════════════════╣")
    print("║  INGESTION                                                  ║")
    print("╠═══════════════════════════════════════════════════════════════╣")
    print(f"║  Total trades:      {trade_count:<40,d}║")
    print(f"║  Duration:          {metrics['ingest_duration']:<40.1f}║")
    print(f"║  Rate:              ~{metrics['ingest_rate']:<39.0f}║")
    print(f"║  First trade:       {metrics['trade_min_ts']:<40s}║")
    print(f"║  Last trade:        {metrics['trade_max_ts']:<40s}║")
    print("║                                                             ║")
    print("╠═══════════════════════════════════════════════════════════════╣")
    print("║  BAR CONSTRUCTION                                           ║")
    print("╠═══════════════════════════════════════════════════════════════╣")
    for spec, data in metrics.get("bars", {}).items():
        count = data["count"]
        dur = data["duration"]
        line = f"{count:,} bars in {dur:.1f}s"
        print(f"║  {spec:<20s} {line:<38s}║")
    print("║                                                             ║")
    print("╠═══════════════════════════════════════════════════════════════╣")
    print("║  STORAGE                                                    ║")
    print("╠═══════════════════════════════════════════════════════════════╣")
    st = metrics.get("storage", {})
    trades_line = f"{st.get('trades_size', '?')} ({trade_count:,} rows, ~{st.get('bytes_per_trade', 0)} B/row)"
    bars_line = f"{st.get('bars_size', '?')} ({total_bar_count:,} rows, ~{st.get('bytes_per_bar', 0)} B/row)"
    print(f"║  raw_trades:        {trades_line:<40s}║")
    print(f"║  bars (all tables): {bars_line:<40s}║")
    bar_tables_st = st.get("bar_tables", {})
    for bt_name, bt_info in bar_tables_st.items():
        bt_line = f"{bt_info['size']} ({bt_info['count']:,} rows)"
        print(f"║    {bt_name:<18s}{bt_line:<37s}║")
    print(f"║  total:             {st.get('total_size', '?'):<40s}║")
    print("║                                                             ║")
    print("╠═══════════════════════════════════════════════════════════════╣")
    print("║  TESTS                                                      ║")
    print("╠═══════════════════════════════════════════════════════════════╣")
    print(f"║  Passed:            {PASS:<40d}║")
    print(f"║  Failed:            {FAIL:<40d}║")
    print(f"║  Total:             {PASS + FAIL:<40d}║")
    print("║                                                             ║")
    print("╚═══════════════════════════════════════════════════════════════╝")
    print()
    if FAIL > 0:
        print(f"\033[91mRESULT: FAIL ({FAIL} failures)\033[0m")
        print("\nFailed tests:")
        for desc, passed, detail in RESULTS:
            if not passed:
                print(f"  ✗ {desc}: {detail}")
    else:
        print(f"\033[92mRESULT: ALL {PASS} TESTS PASSED\033[0m")


if __name__ == "__main__":
    sys.exit(main())
