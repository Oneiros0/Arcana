#!/usr/bin/env bash
# ============================================================================
#  Arcana Integration Test Suite
#
#  End-to-end validation against a real TimescaleDB instance with live
#  Coinbase API data. Exercises the full user journey:
#
#    arcana db init → arcana ingest → arcana bars build → arcana run
#
#  Collects performance metrics, storage footprint, and tests edge cases.
#
#  Usage: docker compose up --build --abort-on-container-exit
# ============================================================================
set -uo pipefail

# ── Constants ───────────────────────────────────────────────────────────────
PAIR="ETH-USD"
SOURCE="coinbase"
YESTERDAY=$(date -d "yesterday" +%Y-%m-%d)
TODAY=$(date +%Y-%m-%d)

# PSQL shorthand — tuples-only, no align, quiet
PSQL="psql -h ${ARCANA_DB_HOST} -p ${ARCANA_DB_PORT} -U ${ARCANA_DB_USER} -d ${ARCANA_DB_NAME} -t -A -q"
export PGPASSWORD="${ARCANA_DB_PASSWORD}"

# Bar specs to test
BAR_SPECS=("tick_500" "volume_100" "dollar_50000" "time_5m" "time_1h" "tib_10" "vib_10" "dib_10" "trb_10" "vrb_10" "drb_10")

# Per-pair table naming: ETH-USD → eth_usd
PAIR_NORM=$(echo "$PAIR" | tr '[:upper:]' '[:lower:]' | tr '-' '_')

# ── Metrics storage ─────────────────────────────────────────────────────────
INGEST_TRADE_COUNT=0
INGEST_DURATION=0
INGEST_RATE=0
TRADE_MIN_TS=""
TRADE_MAX_TS=""
declare -A BAR_COUNTS
declare -A BAR_DURATIONS

# ── Test harness ────────────────────────────────────────────────────────────
PASS=0
FAIL=0
TOTAL=0

pass() {
    PASS=$((PASS + 1))
    TOTAL=$((TOTAL + 1))
    echo "  ✓ $1"
}

fail() {
    FAIL=$((FAIL + 1))
    TOTAL=$((TOTAL + 1))
    echo "  ✗ $1"
    if [ -n "${2:-}" ]; then
        echo "    → $2"
    fi
}

assert_exit_code() {
    local description="$1"
    local expected="$2"
    local actual="$3"
    if [ "$actual" -eq "$expected" ]; then
        pass "$description (exit $expected)"
    else
        fail "$description" "expected exit $expected, got $actual"
    fi
}

assert_gt() {
    local description="$1"
    local value="$2"
    local threshold="$3"
    if [ "$value" -gt "$threshold" ]; then
        pass "$description ($value > $threshold)"
    else
        fail "$description" "expected > $threshold, got $value"
    fi
}

assert_eq() {
    local description="$1"
    local actual="$2"
    local expected="$3"
    if [ "$actual" -eq "$expected" ]; then
        pass "$description ($actual == $expected)"
    else
        fail "$description" "expected $expected, got $actual"
    fi
}

assert_contains() {
    local description="$1"
    local haystack="$2"
    local needle="$3"
    if echo "$haystack" | grep -q "$needle"; then
        pass "$description"
    else
        fail "$description" "output missing '$needle'"
    fi
}

now_epoch() {
    date +%s
}

section() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  $1"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

# ============================================================================
#  SECTION 1: Schema Initialization
# ============================================================================
section "1. SCHEMA INITIALIZATION"

echo "  Testing idempotent schema creation..."

output=$(arcana db init 2>&1)
rc=$?
assert_exit_code "db init (first run)" 0 $rc
assert_contains "db init outputs success message" "$output" "initialized successfully"

output=$(arcana db init 2>&1)
rc=$?
assert_exit_code "db init (second run — idempotent)" 0 $rc

# Verify raw_trades table exists (bar tables are created lazily on first build)
table_count=$($PSQL -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'raw_trades' AND table_schema = 'public';")
assert_eq "raw_trades table exists" "$table_count" 1

# Verify hypertable created
hypertable_count=$($PSQL -c "SELECT COUNT(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'raw_trades';")
assert_eq "TimescaleDB hypertable for raw_trades created" "$hypertable_count" 1

# ============================================================================
#  SECTION 2: Pre-Data Edge Cases
# ============================================================================
section "2. PRE-DATA EDGE CASES"

echo "  Testing behavior with empty database..."

# bars build on empty DB should exit 0 with 0 bars
output=$(arcana bars build tick_500 $PAIR 2>&1)
rc=$?
assert_exit_code "bars build on empty DB" 0 $rc
assert_contains "bars build reports 0 bars" "$output" "0 bars built"

# daemon should fail with exit 1 when no data
output=$(arcana run $PAIR --interval 10 2>&1)
rc=$?
assert_exit_code "daemon refuses to start with no data" 1 $rc

# invalid bar spec
output=$(arcana bars build invalid_spec $PAIR 2>&1)
rc=$?
if [ $rc -ne 0 ]; then
    pass "invalid bar spec rejected (exit $rc)"
else
    fail "invalid bar spec should be rejected" "got exit 0"
fi

# bars build on non-existent pair (still empty DB)
output=$(arcana bars build tick_500 FAKE-PAIR 2>&1)
rc=$?
assert_exit_code "bars build on non-existent pair" 0 $rc
assert_contains "bars build with no data reports 0 bars" "$output" "0 bars built"

# ============================================================================
#  SECTION 3: Ingest Yesterday's Trades
# ============================================================================
section "3. TRADE INGESTION ($YESTERDAY → $TODAY)"

echo "  Ingesting $PAIR trades from $YESTERDAY..."
echo "  (This may take 5-15 minutes depending on volume)"
echo ""

INGEST_START=$(now_epoch)

output=$(arcana --log-level INFO ingest $PAIR --since "$YESTERDAY" --until "$TODAY" 2>&1)
rc=$?

INGEST_END=$(now_epoch)
INGEST_DURATION=$((INGEST_END - INGEST_START))

assert_exit_code "ingest completed" 0 $rc
assert_contains "ingest reports completion" "$output" "new trades ingested"

# Query actual trade count from DB
INGEST_TRADE_COUNT=$($PSQL -c "SELECT COUNT(*) FROM raw_trades WHERE pair = '$PAIR' AND source = '$SOURCE';")
assert_gt "trades stored in database" "$INGEST_TRADE_COUNT" 0

# Query timestamp range
TRADE_MIN_TS=$($PSQL -c "SELECT MIN(timestamp)::text FROM raw_trades WHERE pair = '$PAIR';")
TRADE_MAX_TS=$($PSQL -c "SELECT MAX(timestamp)::text FROM raw_trades WHERE pair = '$PAIR';")

# Calculate ingestion rate
if [ "$INGEST_DURATION" -gt 0 ]; then
    INGEST_RATE=$((INGEST_TRADE_COUNT / INGEST_DURATION))
fi

echo ""
echo "  → $INGEST_TRADE_COUNT trades in ${INGEST_DURATION}s (~${INGEST_RATE} trades/sec)"

# ============================================================================
#  SECTION 4: Ingestion Idempotency
# ============================================================================
section "4. INGESTION IDEMPOTENCY"

echo "  Re-running identical ingest to verify no duplicates..."

count_before=$($PSQL -c "SELECT COUNT(*) FROM raw_trades WHERE pair = '$PAIR';")

output=$(arcana ingest $PAIR --since "$YESTERDAY" --until "$TODAY" 2>&1)
rc=$?
assert_exit_code "idempotent re-ingest" 0 $rc

count_after=$($PSQL -c "SELECT COUNT(*) FROM raw_trades WHERE pair = '$PAIR';")
assert_eq "trade count unchanged after re-ingest" "$count_after" "$count_before"

echo "  → Before: $count_before | After: $count_after (no duplicates)"

# ============================================================================
#  SECTION 5: Bar Construction — All Types
# ============================================================================
section "5. BAR CONSTRUCTION"

echo "  Building bars from $INGEST_TRADE_COUNT trades..."
echo ""

for spec in "${BAR_SPECS[@]}"; do
    echo "  Building $spec..."

    bar_start=$(now_epoch)
    output=$(arcana --log-level INFO bars build "$spec" $PAIR 2>&1)
    rc=$?
    bar_end=$(now_epoch)

    bar_duration=$((bar_end - bar_start))
    assert_exit_code "bars build $spec" 0 $rc

    # Per-pair table name: bars_{spec}_{pair_norm} (dots become underscores)
    table_name="bars_${spec//./_}_${PAIR_NORM}"
    bar_count=$($PSQL -c "SELECT COUNT(*) FROM $table_name WHERE pair = '$PAIR';")
    assert_gt "$spec bar count" "$bar_count" 0

    # Information-driven bars must have non-null metadata with EWMA state
    case "$spec" in
        tib_*|vib_*|dib_*|trb_*|vrb_*|drb_*)
            null_meta=$($PSQL -c "SELECT COUNT(*) FROM $table_name WHERE metadata IS NULL;")
            assert_eq "$spec metadata present on all bars" "$null_meta" 0
            ;;
    esac

    BAR_COUNTS[$spec]=$bar_count
    BAR_DURATIONS[$spec]=$bar_duration

    echo "  → $bar_count bars in ${bar_duration}s"
    echo ""
done

# ============================================================================
#  SECTION 6: Bar Build Idempotency
# ============================================================================
section "6. BAR BUILD IDEMPOTENCY"

echo "  Re-running tick_500 bar build to verify idempotency..."

tick_table="bars_tick_500_${PAIR_NORM}"
count_before=$($PSQL -c "SELECT COUNT(*) FROM $tick_table WHERE pair = '$PAIR';")

output=$(arcana bars build tick_500 $PAIR 2>&1)
rc=$?
assert_exit_code "idempotent bar rebuild" 0 $rc

count_after=$($PSQL -c "SELECT COUNT(*) FROM $tick_table WHERE pair = '$PAIR';")
assert_eq "tick_500 bar count unchanged after rebuild" "$count_after" "$count_before"

echo "  → Before: $count_before | After: $count_after"

# ============================================================================
#  SECTION 7: Status Command
# ============================================================================
section "7. STATUS COMMAND"

output=$(arcana status $PAIR 2>&1)
rc=$?
assert_exit_code "status command" 0 $rc
assert_contains "status shows trade count" "$output" "Total trades"
assert_contains "status shows pair name" "$output" "$PAIR"

# Status without pair arg
output_all=$(arcana status 2>&1)
rc=$?
assert_exit_code "status (all pairs)" 0 $rc

echo "  → Status output:"
echo "$output" | sed 's/^/    /'

# ============================================================================
#  SECTION 8: Daemon Lifecycle
# ============================================================================
section "8. DAEMON LIFECYCLE"

echo "  Starting daemon in background..."

arcana run $PAIR --interval 30 > /tmp/daemon_out.log 2>&1 &
DAEMON_PID=$!

sleep 5

# Check daemon is running
if kill -0 $DAEMON_PID 2>/dev/null; then
    pass "daemon is running (PID $DAEMON_PID)"
else
    fail "daemon should be running" "process $DAEMON_PID not found"
fi

# Send SIGINT for graceful shutdown
echo "  Sending SIGINT for graceful shutdown..."
kill -INT $DAEMON_PID 2>/dev/null

# Wait for daemon to exit (up to 15 seconds)
WAIT_COUNT=0
while kill -0 $DAEMON_PID 2>/dev/null && [ $WAIT_COUNT -lt 15 ]; do
    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

if ! kill -0 $DAEMON_PID 2>/dev/null; then
    # Get exit code
    wait $DAEMON_PID 2>/dev/null
    daemon_rc=$?
    assert_exit_code "daemon graceful shutdown" 0 $daemon_rc
else
    fail "daemon should have exited after SIGINT" "still running after 15s"
    kill -9 $DAEMON_PID 2>/dev/null
fi

# ============================================================================
#  SECTION 9: Data Validation
# ============================================================================
section "9. DATA VALIDATION"

echo "  Running data integrity checks..."

# Verify all trades have required fields (no NULLs in critical columns)
null_count=$($PSQL -c "SELECT COUNT(*) FROM raw_trades WHERE price IS NULL OR size IS NULL OR side IS NULL;")
assert_eq "no NULL prices/sizes/sides in trades" "$null_count" 0

# Verify trades are from correct pair
wrong_pair=$($PSQL -c "SELECT COUNT(*) FROM raw_trades WHERE pair != '$PAIR';")
assert_eq "all trades are for $PAIR" "$wrong_pair" 0

# Per-table bar integrity checks
for spec in "${BAR_SPECS[@]}"; do
    table="bars_${spec//./_}_${PAIR_NORM}"

    bad_hl=$($PSQL -c "SELECT COUNT(*) FROM $table WHERE high < low;")
    assert_eq "[$spec] high >= low" "$bad_hl" 0

    bad_oc=$($PSQL -c "SELECT COUNT(*) FROM $table WHERE open > high OR open < low OR close > high OR close < low;")
    assert_eq "[$spec] open/close in range" "$bad_oc" 0

    zero_ticks=$($PSQL -c "SELECT COUNT(*) FROM $table WHERE tick_count <= 0;")
    assert_eq "[$spec] positive tick count" "$zero_ticks" 0

    neg_vol=$($PSQL -c "SELECT COUNT(*) FROM $table WHERE volume < 0;")
    assert_eq "[$spec] non-negative volume" "$neg_vol" 0

    bad_vwap=$($PSQL -c "SELECT COUNT(*) FROM $table WHERE vwap > high OR vwap < low;")
    assert_eq "[$spec] VWAP in range" "$bad_vwap" 0

    bad_time=$($PSQL -c "SELECT COUNT(*) FROM $table WHERE time_start > time_end;")
    assert_eq "[$spec] time_start <= time_end" "$bad_time" 0
done

# Verify tick bar tick counts are close to threshold
avg_ticks=$($PSQL -c "SELECT ROUND(AVG(tick_count)) FROM bars_tick_500_${PAIR_NORM} WHERE pair = '$PAIR';")
# Tick bars should average ~500 (last bar can be partial)
if [ "$avg_ticks" -ge 450 ] && [ "$avg_ticks" -le 500 ]; then
    pass "tick_500 bars average ~500 trades ($avg_ticks)"
else
    fail "tick_500 bars average should be ~500" "got $avg_ticks"
fi

# ============================================================================
#  SECTION 10: Storage Metrics
# ============================================================================
section "10. STORAGE METRICS"

TRADES_SIZE=$($PSQL -c "SELECT pg_size_pretty(pg_total_relation_size('raw_trades'));")
TRADES_BYTES=$($PSQL -c "SELECT pg_total_relation_size('raw_trades');")

# Sum sizes across all per-pair-per-type bar tables
BARS_BYTES=0
TOTAL_BAR_COUNT=0
for spec in "${BAR_SPECS[@]}"; do
    table="bars_${spec//./_}_${PAIR_NORM}"
    bt_bytes=$($PSQL -c "SELECT pg_total_relation_size('$table');")
    bt_count=$($PSQL -c "SELECT COUNT(*) FROM $table WHERE pair = '$PAIR';")
    BARS_BYTES=$((BARS_BYTES + bt_bytes))
    TOTAL_BAR_COUNT=$((TOTAL_BAR_COUNT + bt_count))
done

BARS_SIZE=$($PSQL -c "SELECT pg_size_pretty(${BARS_BYTES}::bigint);")
TOTAL_SIZE=$($PSQL -c "SELECT pg_size_pretty(${TRADES_BYTES}::bigint + ${BARS_BYTES}::bigint);")

if [ "$INGEST_TRADE_COUNT" -gt 0 ]; then
    BYTES_PER_TRADE=$((TRADES_BYTES / INGEST_TRADE_COUNT))
else
    BYTES_PER_TRADE=0
fi

if [ "$TOTAL_BAR_COUNT" -gt 0 ]; then
    BYTES_PER_BAR=$((BARS_BYTES / TOTAL_BAR_COUNT))
else
    BYTES_PER_BAR=0
fi

# ============================================================================
#  SUMMARY REPORT
# ============================================================================
echo ""
echo ""
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║            ARCANA INTEGRATION TEST RESULTS                  ║"
echo "╠═══════════════════════════════════════════════════════════════╣"
echo "║                                                             ║"
printf "║  Pair:              %-40s║\n" "$PAIR"
printf "║  Ingestion Period:  %-40s║\n" "$YESTERDAY → $TODAY"
echo "║                                                             ║"
echo "╠═══════════════════════════════════════════════════════════════╣"
echo "║  INGESTION                                                  ║"
echo "╠═══════════════════════════════════════════════════════════════╣"
printf "║  Total trades:      %-40s║\n" "$(printf "%'d" $INGEST_TRADE_COUNT)"
printf "║  Duration:          %-40s║\n" "${INGEST_DURATION}s"
printf "║  Rate:              %-40s║\n" "~${INGEST_RATE} trades/sec"
printf "║  First trade:       %-40s║\n" "$TRADE_MIN_TS"
printf "║  Last trade:        %-40s║\n" "$TRADE_MAX_TS"
echo "║                                                             ║"
echo "╠═══════════════════════════════════════════════════════════════╣"
echo "║  BAR CONSTRUCTION                                           ║"
echo "╠═══════════════════════════════════════════════════════════════╣"
for spec in "${BAR_SPECS[@]}"; do
    count="${BAR_COUNTS[$spec]:-0}"
    duration="${BAR_DURATIONS[$spec]:-0}"
    printf "║  %-20s %-10s bars in %-17s ║\n" "$spec:" "$count" "${duration}s"
done
echo "║                                                             ║"
echo "╠═══════════════════════════════════════════════════════════════╣"
echo "║  STORAGE                                                    ║"
echo "╠═══════════════════════════════════════════════════════════════╣"
printf "║  raw_trades:        %-40s║\n" "$TRADES_SIZE (${INGEST_TRADE_COUNT} rows, ~${BYTES_PER_TRADE} B/row)"
printf "║  bars (all tables): %-40s║\n" "$BARS_SIZE (${TOTAL_BAR_COUNT} rows, ~${BYTES_PER_BAR} B/row)"
printf "║  total:             %-40s║\n" "$TOTAL_SIZE"
echo "║                                                             ║"
echo "╠═══════════════════════════════════════════════════════════════╣"
echo "║  TESTS                                                      ║"
echo "╠═══════════════════════════════════════════════════════════════╣"
printf "║  Passed:            %-40s║\n" "$PASS"
printf "║  Failed:            %-40s║\n" "$FAIL"
printf "║  Total:             %-40s║\n" "$TOTAL"
echo "║                                                             ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

if [ "$FAIL" -gt 0 ]; then
    echo "RESULT: FAIL ($FAIL failures)"
    exit 1
else
    echo "RESULT: ALL TESTS PASSED"
    exit 0
fi
