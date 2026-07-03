"""Unit tests for the DQ rule evaluators (pure Python, no Databricks needed)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import dq_rules as dq  # noqa: E402


class TestNullRate:
    def test_clean_column_passes(self):
        m = dq.evaluate_null_rate("order_id", null_count=0, total=100)
        assert m.status == dq.PASS
        assert m.failure_rate == 0.0

    def test_below_threshold_warns(self):
        # 3/100 = 3% < 5% threshold -> WARN (nulls exist but tolerable)
        m = dq.evaluate_null_rate("customer_id", null_count=3, total=100)
        assert m.status == dq.WARN

    def test_above_threshold_fails(self):
        # 10/100 = 10% > 5% -> FAIL
        m = dq.evaluate_null_rate("unit_price", null_count=10, total=100)
        assert m.status == dq.FAIL
        assert m.failed

    def test_zero_total_is_safe(self):
        m = dq.evaluate_null_rate("order_id", null_count=0, total=0)
        assert m.status == dq.PASS
        assert m.failure_rate == 0.0


class TestSchemaDrift:
    def test_no_drift_passes(self):
        m = dq.evaluate_schema_drift(total=100, rescued_count=0,
                                     unexpected_cols=[], missing_cols=[])
        assert m.status == dq.PASS

    def test_unexpected_column_fails(self):
        m = dq.evaluate_schema_drift(total=100, rescued_count=0,
                                     unexpected_cols=["price"], missing_cols=[])
        assert m.status == dq.FAIL

    def test_missing_column_fails(self):
        m = dq.evaluate_schema_drift(total=100, rescued_count=0,
                                     unexpected_cols=[], missing_cols=["store_region"])
        assert m.status == dq.FAIL

    def test_rescued_rows_fail(self):
        m = dq.evaluate_schema_drift(total=100, rescued_count=5,
                                     unexpected_cols=[], missing_cols=[])
        assert m.status == dq.FAIL
        assert m.failure_count == 5


class TestFreshness:
    def test_within_sla_passes(self):
        assert dq.evaluate_freshness(2.0, sla_hours=24.0).status == dq.PASS

    def test_beyond_sla_fails(self):
        assert dq.evaluate_freshness(48.0, sla_hours=24.0).status == dq.FAIL

    def test_none_lag_fails(self):
        assert dq.evaluate_freshness(None).status == dq.FAIL


class TestDuplicates:
    def test_no_dupes_passes(self):
        assert dq.evaluate_duplicates(total=100, distinct_keys=100).status == dq.PASS

    def test_dupes_fail_with_count(self):
        m = dq.evaluate_duplicates(total=110, distinct_keys=100)
        assert m.status == dq.FAIL
        assert m.failure_count == 10


class TestLateArrivals:
    def test_none_passes(self):
        assert dq.evaluate_late_arrivals(total=100, late_count=0).status == dq.PASS

    def test_some_warns_not_fails(self):
        # late arrivals are expected in streaming -> WARN, never blocks the pipeline
        m = dq.evaluate_late_arrivals(total=100, late_count=7)
        assert m.status == dq.WARN
        assert not m.failed


class TestRowCount:
    def test_first_run_is_baseline(self):
        m = dq.evaluate_row_count(500, trailing_avg=None)
        assert m.status == dq.PASS
        assert "baseline" in m.detail

    def test_within_band_passes(self):
        assert dq.evaluate_row_count(500, trailing_avg=480).status == dq.PASS

    def test_outside_band_warns(self):
        assert dq.evaluate_row_count(1500, trailing_avg=500).status == dq.WARN

    def test_zero_rows_fails(self):
        assert dq.evaluate_row_count(0, trailing_avg=500).status == dq.FAIL
