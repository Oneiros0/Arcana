"""Tests for EWMA estimator and tick rule utilities."""

from decimal import Decimal

import pytest

from arcana.bars.utils import EWMAEstimator, tick_rule

# ── EWMA Estimator ────────────────────────────────────────────────────


class TestEWMAEstimator:
    def test_initial_value(self):
        ewma = EWMAEstimator(window=10)
        assert ewma.expected == 0.0

    def test_custom_initial_value(self):
        ewma = EWMAEstimator(window=10, initial_value=42.0)
        assert ewma.expected == 42.0

    def test_single_update_math(self):
        """Window=9 → alpha=0.2.  E = 0.2*100 + 0.8*0 = 20.0"""
        ewma = EWMAEstimator(window=9)
        result = ewma.update(100.0)
        assert result == pytest.approx(20.0)
        assert ewma.expected == pytest.approx(20.0)

    def test_convergence(self):
        """After many identical values, EWMA should converge to that value."""
        ewma = EWMAEstimator(window=10)
        for _ in range(200):
            ewma.update(50.0)
        assert ewma.expected == pytest.approx(50.0, abs=0.01)

    def test_window_1_tracks_exactly(self):
        """Window=1 → alpha=1.0.  EWMA tracks the last value exactly."""
        ewma = EWMAEstimator(window=1)
        ewma.update(10.0)
        assert ewma.expected == pytest.approx(10.0)
        ewma.update(20.0)
        assert ewma.expected == pytest.approx(20.0)
        ewma.update(5.0)
        assert ewma.expected == pytest.approx(5.0)

    def test_serialization_roundtrip(self):
        ewma = EWMAEstimator(window=20, initial_value=5.0)
        ewma.update(100.0)
        ewma.update(50.0)

        data = ewma.to_dict()
        restored = EWMAEstimator.from_dict(data)

        assert restored.window == ewma.window
        assert restored.expected == pytest.approx(ewma.expected)

    def test_to_dict_keys(self):
        ewma = EWMAEstimator(window=10, initial_value=3.5)
        d = ewma.to_dict()
        assert set(d.keys()) == {"ewma_window", "ewma_expected"}
        assert d["ewma_window"] == 10
        assert d["ewma_expected"] == 3.5

    def test_rejects_invalid_window(self):
        with pytest.raises(ValueError, match="window must be >= 1"):
            EWMAEstimator(window=0)
        with pytest.raises(ValueError, match="window must be >= 1"):
            EWMAEstimator(window=-5)

    def test_repr(self):
        ewma = EWMAEstimator(window=9)
        r = repr(ewma)
        assert "window=9" in r
        assert "alpha=0.2000" in r

    def test_two_step_math(self):
        """Window=4 → alpha=0.4.  Step-by-step verification."""
        ewma = EWMAEstimator(window=4)
        # Step 1: E = 0.4*10 + 0.6*0 = 4.0
        assert ewma.update(10.0) == pytest.approx(4.0)
        # Step 2: E = 0.4*20 + 0.6*4 = 10.4
        assert ewma.update(20.0) == pytest.approx(10.4)


# ── Tick Rule ─────────────────────────────────────────────────────────


class TestTickRule:
    def test_uptick(self):
        assert tick_rule(Decimal("101"), Decimal("100"), 0) == 1

    def test_downtick(self):
        assert tick_rule(Decimal("99"), Decimal("100"), 0) == -1

    def test_equal_carries_forward_positive(self):
        assert tick_rule(Decimal("100"), Decimal("100"), 1) == 1

    def test_equal_carries_forward_negative(self):
        assert tick_rule(Decimal("100"), Decimal("100"), -1) == -1

    def test_equal_carries_forward_zero(self):
        """When prev_sign is 0 and prices equal, stays at 0."""
        assert tick_rule(Decimal("100"), Decimal("100"), 0) == 0

    def test_works_with_floats(self):
        assert tick_rule(101.0, 100.0, 0) == 1
        assert tick_rule(99.0, 100.0, 0) == -1
        assert tick_rule(100.0, 100.0, 1) == 1

    def test_works_with_mixed_types(self):
        """Decimal price vs float prev_price — duck-typed comparisons."""
        assert tick_rule(Decimal("101"), 100.0, 0) == 1
        assert tick_rule(100.0, Decimal("101"), 0) == -1
