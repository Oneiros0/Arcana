# Information-Driven Bar Stability: Precision, Accuracy & Accountability

This document covers the work done to mathematically verify, fix, and tune the
information-driven bar implementations (Prado AFML Ch. 2) in the Arcana pipeline.

---

## 1. The Problem

Production `build-all` output for ETH-USD over 181.2 days showed healthy standard
bars (~50 bars/day) but severely underperforming information-driven bars:

| Bar Type | Bars/Day | Status | Expected |
|----------|----------|--------|----------|
| dollar_auto | 48.6 | Pass | 30-100 |
| tick_auto | 50.0 | Pass | 30-100 |
| volume_auto | 43.4 | Pass | 30-100 |
| tib_20 | 1.5 | Fail | 30-100 |
| vib_200 | 6.8 | Fail | 30-100 |
| dib_200 | 6.7 | Fail | 30-100 |
| trb_20 | 13.2 | Marginal | 30-100 |
| vrb_40 | 5.6 | Fail | 30-100 |
| drb_40 | 5.6 | Fail | 30-100 |

The question: is the implementation wrong, or is the math working as expected
under conditions we hadn't accounted for?

---

## 2. Mathematical Proof Suite

### Approach

Rather than guessing, we built a **constructive proof suite**: each test derives
the expected result from first principles using Prado's formulas, then verifies
the implementation matches exactly. No mocks, no approximations on the
derivation side.

### Coverage: 68 Tests Across 12 Test Classes

**`tests/test_bars/test_mathematical_proofs.py`**

| Test Class | Tests | What It Proves |
|------------|-------|----------------|
| `TestEWMAMathematicalProofs` | 6 | Exact alpha values, multi-step arithmetic traces, convergence bounds, step-change response time |
| `TestTickImbalanceBarMathProofs` | 11 | Exact bar boundaries for all-buy/all-sell/mixed sequences, threshold decomposition algebra, EWMA update ordering at emission, cold-start behavior, cumulative imbalance reset |
| `TestVolumeImbalanceBarMathProofs` | 5 | Signed volume cancellation, varying trade sizes, volume-weighted contributions |
| `TestDollarImbalanceBarMathProofs` | 3 | price x size contribution, price sensitivity across identical volumes |
| `TestTickRunBarMathProofs` | 9 | No-reset accumulation (Def. 2.4), P_dom clamping at [0.55, 0.95], sell-dominated runs, both-sides-reset after emission |
| `TestVolumeRunBarMathProofs` | 3 | Volume accumulation without reset per Prado |
| `TestDollarRunBarMathProofs` | 2 | Dollar-volume contribution correctness |
| `TestTickRuleIntegration` | 4 | Uptick/downtick classification, equal-price carry-forward, explicit side override |
| `TestCrossVariantConsistency` | 4 | TIB=VIB when size=1, VIB=DIB when price=1, TRB=VRB when size=1, VRB=DRB when price=1 |
| `TestEquilibriumConvergence` | 4 | Biased market stable equilibrium, balanced market **unstable** equilibrium, trending > balanced bar production |
| `TestEdgeCases` | 8 | Flush, zero volume, large/small EWMA windows, metadata roundtrips, OHLCV correctness, imbalance vs run sensitivity |
| `TestExpectedTicksClamping` | 9 | Collapse prevention, explosion prevention, no interference in biased markets, metadata roundtrips, backward compatibility, repeated clamping, all subclass acceptance |

### Verdict: Implementation Is Correct

Every EWMA calculation, CUSUM trigger, threshold decomposition, tick rule, and
run-bar accumulation matches Prado's formulas exactly. The proofs confirmed that
**the code is mathematically faithful to AFML Ch. 2**.

---

## 3. Critical Discovery: Unstable Equilibrium

### The Finding

The `TestEquilibriumConvergence` tests revealed a fundamental instability that
explains the production failures. For TIB in a balanced market (P_buy = 0.5):

**The cumulative imbalance is a symmetric random walk.** The first-passage time
to a threshold h follows:

```
E[first-passage] = h^2    (quadratic, not linear)
```

The adaptive threshold is:

```
threshold = E[T] x |E[2P-1]| x E[|v|]
         = E[T] x 0.1 x 1.0        (with directional bias floor at 0.1)
         = E[T] / 10
```

So the actual bar size (T_actual) relates to the expected bar size (E[T]) as:

```
T_actual = (E[T] / 10)^2 = E[T]^2 / 100
```

Setting T_actual = E[T] for equilibrium:

```
E[T] = E[T]^2 / 100
100 = E[T]
```

**E[T] = 100 is an unstable fixed point:**

- If E[T] < 100: T_actual < E[T], so EWMA pulls E[T] *down* further. Positive
  feedback drives E[T] toward 1 (degenerate 1-tick bars).
- If E[T] > 100: T_actual > E[T], so EWMA pushes E[T] *up* further. Positive
  feedback drives E[T] toward infinity (ever-fewer bars).

### Production Impact

Auto-calibration seeded E[T] = total_trades / (days x bars_per_day) which, for
ETH-USD with ~462K trades/day and bars_per_day=50, gives E[T] = 9,240. This is
far above the unstable equilibrium at 100, so E[T] exploded upward, producing
the observed 1.5 bars/day for TIB.

### This Is a Known Problem

Prado acknowledges the cold-start problem but does not address the equilibrium
instability. The practitioner community (Hudson & Thames / mlfinlab) solved it
with `exp_num_ticks_constraints` -- a [min, max] clamp on E[T].

---

## 4. The Fix: E[T] Clamping

### Design

Following mlfinlab's pattern and the existing P_dom clamping already in our
run bar implementation:

1. After each EWMA update of E[T], clamp to `[min, max]`
2. Auto-derive range from calibration: `min = max(1, E[T]/10)`, `max = E[T]*10`
3. Allow explicit override via `expected_ticks_constraints` in config
4. Persist range in bar metadata for daemon restart recovery

### Files Modified

| File | Change |
|------|--------|
| `src/arcana/bars/imbalance.py` | `_ImbalanceBarBuilder` base + TIB/VIB/DIB subclasses: accept `expected_ticks_range`, clamp after EWMA update, persist in metadata, restore on restart |
| `src/arcana/bars/runs.py` | `_RunBarBuilder` base + TRB/VRB/DRB subclasses: mirror of imbalance changes |
| `src/arcana/pipeline.py` | `calibrate_info_bar_initial_expected()`: compute and include `expected_ticks_range` in calibration output |
| `src/arcana/config.py` | `BarSpecConfig`: added `expected_ticks_constraints` field |
| `src/arcana/cli.py` | `_parse_bar_spec()`: extract range from calibration or config override, pass to builder. Both `build` and `build-all` wired. |
| `arcana.toml` | Documented `expected_ticks_constraints` config option with examples |

### Clamping Logic (3 lines)

```python
# In process_trade(), after EWMA update:
if self._expected_ticks_range is not None:
    lo, hi = self._expected_ticks_range
    self._ewma_t._expected = max(lo, min(hi, self._ewma_t._expected))
```

### Backward Compatibility

- `expected_ticks_range=None` (default) preserves original unclamped behavior
- All 84 existing tests continue to pass unchanged
- Legacy metadata without range is handled gracefully on restore

---

## 5. Production Tuning

### Root Causes of Low bars/day (Beyond the Equilibrium Bug)

1. **EWMA windows too large**: `vib_200` at 6.8 bars/day means the EWMA has a
   200-bar lookback = 29 days of real time. Adaptation is glacially slow.
   Similarly `vrb_40`/`drb_40` at ~5.6 bars/day = 7-day lookback.

2. **`bars_per_day=50` seeds E[T] too high**: Standard bars scale linearly with
   the target, but info bars in balanced markets have a quadratic penalty. The
   same target that works for tick/dollar/volume bars overshoots dramatically
   for imbalance and run bars.

3. **Auto-derived clamp range too wide**: `[E[T]/10, E[T]*10]` with E[T]=9,240
   gives `[924, 92400]` -- effectively no constraint.

### Tuned Settings

```toml
# Imbalance bars -- higher bars_per_day to compensate for quadratic penalty
[[bars]]
spec = "tib_20"
bars_per_day = 300
expected_ticks_constraints = [100.0, 5000.0]

[[bars]]
spec = "vib_20"               # was vib_200 (window too large)
bars_per_day = 300
expected_ticks_constraints = [100.0, 5000.0]

[[bars]]
spec = "dib_20"               # was dib_200 (window too large)
bars_per_day = 300
expected_ticks_constraints = [100.0, 5000.0]

# Run bars -- less overcompensation needed (no cancellation)
[[bars]]
spec = "trb_20"
bars_per_day = 150
expected_ticks_constraints = [100.0, 10000.0]

[[bars]]
spec = "vrb_20"               # was vrb_40 (window too large)
bars_per_day = 150
expected_ticks_constraints = [100.0, 10000.0]

[[bars]]
spec = "drb_20"               # was drb_40 (window too large)
bars_per_day = 150
expected_ticks_constraints = [100.0, 10000.0]
```

### Rationale

| Parameter | Before | After | Why |
|-----------|--------|-------|-----|
| EWMA window (VIB/DIB) | 200 | 20 | 29-day lookback was frozen; 20 gives ~4-day lookback |
| EWMA window (VRB/DRB) | 40 | 20 | Consistent fast adaptation across all info bars |
| bars_per_day (imbalance) | 50 | 300 | Seeds E[T]=1,540 instead of 9,240; compensates for quadratic random-walk penalty |
| bars_per_day (run) | 50 | 150 | Run bars accumulate without cancellation; less overcompensation needed |
| constraints (imbalance) | auto [924, 92400] | explicit [100, 5000] | Tight enough to prevent collapse/explosion; wide enough for natural adaptation |
| constraints (run) | auto [924, 92400] | explicit [100, 10000] | Wider upper bound since run bars naturally have larger E[T] |

---

## 6. Test Results Summary

| Metric | Count |
|--------|-------|
| Total tests | 152 |
| Mathematical proof tests | 68 |
| Pre-existing bar tests | 84 |
| Lint errors | 0 |
| Failures | 0 |

### Key Properties Proved

- Every EWMA calculation matches the closed-form formula
- Every CUSUM trigger fires at the exact mathematically-predicted trade
- Every threshold decomposition multiplies to the expected scalar
- Cross-variant consistency holds (TIB=VIB when size=1, etc.)
- E[T] clamping prevents collapse in balanced markets (E[T] >= min)
- E[T] clamping prevents explosion (E[T] <= max)
- Clamping does not interfere when equilibrium is naturally within range
- Metadata roundtrips preserve all state including clamp range

---

## 7. Accountability Chain

Every claim in the pipeline is now backed by a specific, runnable proof:

| Claim | Proof |
|-------|-------|
| "EWMA uses alpha = 2/(window+1)" | `test_ewma_exact_alpha_*` |
| "TIB emits when \|cum_imbalance\| >= threshold" | `test_tib_all_buys_exact_boundary`, `test_tib_all_sells_exact_boundary` |
| "Run bars accumulate without reset" | `test_no_reset_accumulation_across_direction_changes` |
| "P_dom is clamped to [0.55, 0.95]" | `test_p_dominant_clamping_lower`, `test_p_dominant_clamping_upper` |
| "Tick rule carries forward on equal prices" | `test_equal_price_carries_forward` |
| "Explicit side overrides tick rule" | `test_known_side_overrides_tick_rule` |
| "Balanced markets cause E[T] instability" | `test_tib_balanced_market_unstable_equilibrium` |
| "E[T] clamping prevents collapse" | `test_clamping_prevents_collapse_in_balanced_market` |
| "E[T] clamping prevents explosion" | `test_clamping_prevents_threshold_explosion` |
| "Clamping is transparent in biased markets" | `test_clamping_preserves_biased_market_behavior` |
| "OHLCV fields are correct" | `test_ohlcv_correctness_across_bar` |

To re-verify at any time:

```bash
python -m pytest tests/test_bars/test_mathematical_proofs.py -v
```
