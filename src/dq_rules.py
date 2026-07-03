"""Data-quality rule definitions and evaluation logic.

This module is deliberately **pure Python with no Databricks / Spark dependency** so
the rule logic can be unit-tested locally (see tests/). The orchestrator
(`run_pipeline.py`) runs cheap aggregate SQL on the warehouse to gather the raw
counts, then feeds those counts into these functions to decide PASS / WARN / FAIL and
to produce rows for the `data_quality_metrics` table.

Design decision — WARN vs FAIL and *quarantine over hard-fail*:
    A streaming quality framework that hard-fails on the first bad record is useless in
    production: one malformed row would halt the whole feed. Instead we *quantify* the
    damage (failure_count / failure_rate), route offending records to a `*_quarantine`
    table, and keep the good data flowing. FAIL is a signal on the dashboard, not a
    pipeline abort.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

# --- Thresholds (the SLA/SLO knobs) -----------------------------------------
NULL_RATE_THRESHOLD = 0.05        # >5% nulls in a required column => FAIL
FRESHNESS_SLA_HOURS = 24.0        # latest record must be <24h old
LATE_ARRIVAL_HOURS = 24.0         # event backdated >24h before ingest => "late"
DUPLICATE_TOLERANCE = 0           # any duplicate order_id beyond this => FAIL
ROWCOUNT_DEVIATION = 0.5          # run size within +/-50% of trailing avg

REQUIRED_COLUMNS = ["order_id", "customer_id", "unit_price", "total_amount", "order_ts"]

PASS, WARN, FAIL = "PASS", "WARN", "FAIL"


@dataclass
class Metric:
    """One evaluated data-quality check; maps 1:1 to a data_quality_metrics row."""
    layer: str
    table_name: str
    rule_name: str
    status: str
    column_name: Optional[str] = None
    failure_count: int = 0
    total_count: int = 0
    failure_rate: float = 0.0
    metric_value: float = 0.0          # arbitrary numeric for trend charts (e.g. lag hrs)
    threshold: float = 0.0
    detail: str = ""

    @property
    def failed(self) -> bool:
        return self.status == FAIL


def _rate(n: int, total: int) -> float:
    return (n / total) if total else 0.0


def evaluate_null_rate(column: str, null_count: int, total: int,
                       threshold: float = NULL_RATE_THRESHOLD) -> Metric:
    rate = _rate(null_count, total)
    status = FAIL if rate > threshold else (WARN if null_count > 0 else PASS)
    return Metric(
        layer="bronze", table_name="bronze_sales", rule_name="null_rate",
        column_name=column, status=status, failure_count=null_count,
        total_count=total, failure_rate=round(rate, 4), metric_value=round(rate, 4),
        threshold=threshold,
        detail=f"{null_count}/{total} nulls in '{column}' (rate={rate:.3f}, thr={threshold})",
    )


def evaluate_schema_drift(total: int, rescued_count: int,
                          unexpected_cols: list[str], missing_cols: list[str]) -> Metric:
    drift = bool(rescued_count) or bool(unexpected_cols) or bool(missing_cols)
    status = FAIL if drift else PASS
    detail = (
        f"rescued_rows={rescued_count}; "
        f"unexpected={sorted(unexpected_cols) or '-'}; "
        f"missing={sorted(missing_cols) or '-'}"
    )
    return Metric(
        layer="bronze", table_name="bronze_sales", rule_name="schema_drift",
        column_name=None, status=status, failure_count=rescued_count,
        total_count=total, failure_rate=round(_rate(rescued_count, total), 4),
        metric_value=float(len(unexpected_cols) + len(missing_cols)),
        threshold=0.0, detail=detail,
    )


def evaluate_freshness(max_lag_hours: Optional[float],
                       sla_hours: float = FRESHNESS_SLA_HOURS) -> Metric:
    lag = max_lag_hours if max_lag_hours is not None else 1e9
    status = FAIL if lag > sla_hours else PASS
    return Metric(
        layer="bronze", table_name="bronze_sales", rule_name="freshness_sla",
        column_name="order_ts", status=status, failure_count=0, total_count=0,
        failure_rate=0.0, metric_value=round(lag, 3), threshold=sla_hours,
        detail=f"latest record is {lag:.2f}h old (SLA={sla_hours}h)",
    )


def evaluate_late_arrivals(total: int, late_count: int,
                           threshold_hours: float = LATE_ARRIVAL_HOURS) -> Metric:
    status = WARN if late_count > 0 else PASS
    return Metric(
        layer="bronze", table_name="bronze_sales", rule_name="late_arriving",
        column_name="order_ts", status=status, failure_count=late_count,
        total_count=total, failure_rate=round(_rate(late_count, total), 4),
        metric_value=float(late_count), threshold=threshold_hours,
        detail=f"{late_count}/{total} records backdated >{threshold_hours}h before ingest",
    )


def evaluate_duplicates(total: int, distinct_keys: int,
                        tolerance: int = DUPLICATE_TOLERANCE) -> Metric:
    dup = max(total - distinct_keys, 0)
    status = FAIL if dup > tolerance else PASS
    return Metric(
        layer="bronze", table_name="bronze_sales", rule_name="duplicate",
        column_name="order_id", status=status, failure_count=dup, total_count=total,
        failure_rate=round(_rate(dup, total), 4), metric_value=float(dup),
        threshold=float(tolerance),
        detail=f"{dup} duplicate order_id rows (total={total}, distinct={distinct_keys})",
    )


def evaluate_row_count(row_count: int, trailing_avg: Optional[float],
                       deviation: float = ROWCOUNT_DEVIATION) -> Metric:
    if not trailing_avg:  # first run / no history => establish baseline
        status, detail = PASS, f"baseline run: {row_count} rows (no history)"
    else:
        low, high = trailing_avg * (1 - deviation), trailing_avg * (1 + deviation)
        if row_count == 0:
            status, detail = FAIL, "0 rows ingested this run"
        elif not (low <= row_count <= high):
            status = WARN
            detail = (f"row count {row_count} outside +/-{deviation:.0%} of "
                      f"trailing avg {trailing_avg:.0f} [{low:.0f}, {high:.0f}]")
        else:
            status, detail = PASS, f"{row_count} rows within expected band"
    return Metric(
        layer="bronze", table_name="bronze_sales", rule_name="row_count_anomaly",
        column_name=None, status=status, failure_count=0, total_count=row_count,
        failure_rate=0.0, metric_value=float(row_count),
        threshold=float(trailing_avg or 0), detail=detail,
    )
