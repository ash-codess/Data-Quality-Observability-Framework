"""End-to-end orchestrator: runs Bronze -> Silver -> Gold on the serverless SQL
warehouse from your laptop, logging DQ metrics and quarantining bad rows at each gate.

    python -m src.run_pipeline                 # full run (setup + upload + pipeline)
    python -m src.run_pipeline --setup-only     # just create catalog/schema/volume/tables
    python -m src.run_pipeline --skip-upload     # reuse files already in the volume
    python -m src.run_pipeline --verify-only     # just print row counts

The `run_id` stamped on every DQ metric + silver row ties one execution together, so
the Lakeview dashboard can show pass/fail trends per run over time.
"""
from __future__ import annotations

import argparse
import io
import uuid
from pathlib import Path

from . import sql_pipeline as sp
from .config import LOCAL_LANDING, get_client, load_settings, run_sql
from .dq_rules import (
    REQUIRED_COLUMNS,
    evaluate_duplicates,
    evaluate_freshness,
    evaluate_late_arrivals,
    evaluate_null_rate,
    evaluate_row_count,
    evaluate_schema_drift,
)
from .generate_data import EXPECTED_COLUMNS

# bronze metadata columns that are not part of the data contract
_META_COLS = {"_rescued_data", "_source_file", "_ingest_ts", "_batch_id"}
_STATUS_ICON = {"PASS": "PASS ", "WARN": "WARN ", "FAIL": "FAIL "}


def _hr(title: str) -> None:
    print(f"\n{'=' * 68}\n  {title}\n{'=' * 68}")


def _print_metric(m) -> None:
    print(f"  [{_STATUS_ICON.get(m.status, m.status)}] "
          f"{m.rule_name:<18} {(m.column_name or ''):<14} {m.detail}")


def setup(s, w) -> None:
    _hr("STEP 1 - Setup: catalog / schema / volume / tables")
    for stmt in sp.ddl_statements(s.catalog, s.schema, s.volume):
        run_sql(stmt, settings=s, client=w)
    print(f"  schema  : {s.full_schema}")
    print(f"  volume  : {s.volume_path}")
    print("  tables  : bronze_sales, bronze_sales_quarantine, silver_sales,")
    print("            gold_daily_sales, gold_region_category, data_quality_metrics")


def upload_landing(s, w) -> int:
    _hr("STEP 2 - Upload synthetic batches to the landing volume")
    files = sorted(Path(LOCAL_LANDING).glob("*.json"))
    if not files:
        print("  (no local files found; run `python -m src.generate_data` first)")
        return 0
    for f in files:
        dest = f"{s.volume_path}/{f.name}"
        with open(f, "rb") as fh:
            w.files.upload(dest, io.BytesIO(fh.read()), overwrite=True)
        print(f"  uploaded {f.name}  ->  {dest}")
    return len(files)


def run_bronze(s, w, run_id: str) -> None:
    _hr("STEP 3 - Bronze ingestion (COPY INTO, schema-evolving)")
    run_sql(sp.bronze_copy_into(s.catalog, s.schema, s.volume_path, run_id),
            settings=s, client=w)
    n = run_sql(
        f"SELECT count(*) FROM {sp._fq(s.catalog, s.schema, sp.BRONZE)} "
        f"WHERE _batch_id = {sp._sql_str(run_id)}", settings=s, client=w).scalar()
    print(f"  ingested {n} new bronze rows for run {run_id}")


def check_bronze(s, w, run_id: str) -> list:
    _hr("STEP 4 - Bronze data-quality gates")
    probe = run_sql(sp.bronze_probe(s.catalog, s.schema, run_id),
                    settings=s, client=w).dicts()[0]
    total = int(probe["total"] or 0)

    # columns present now vs. the contract -> schema drift
    cols = {r[0] for r in run_sql(
        sp.bronze_columns_query(s.catalog, s.schema), settings=s, client=w).rows}
    expected = {c.lower() for c in EXPECTED_COLUMNS}
    unexpected = sorted((cols - expected) - _META_COLS)
    missing = sorted(expected - cols)

    trailing = run_sql(sp.trailing_rowcount_query(s.catalog, s.schema, run_id),
                       settings=s, client=w).scalar()
    trailing = float(trailing) if trailing is not None else None

    metrics = []
    metrics.append(evaluate_schema_drift(total, int(probe["rescued"] or 0),
                                         unexpected, missing))
    for col in REQUIRED_COLUMNS:
        metrics.append(evaluate_null_rate(col, int(probe[f"null_{col}"] or 0), total))
    metrics.append(evaluate_duplicates(total, int(probe["distinct_orders"] or 0)))
    lag = probe["freshness_lag_hours"]
    metrics.append(evaluate_freshness(float(lag) if lag is not None else None))
    metrics.append(evaluate_late_arrivals(total, int(probe["late_arrivals"] or 0)))
    metrics.append(evaluate_row_count(total, trailing))

    for m in metrics:
        _print_metric(m)
    stmt = sp.insert_metrics(s.catalog, s.schema, run_id, metrics)
    if stmt:
        run_sql(stmt, settings=s, client=w)
    fails = sum(1 for m in metrics if m.failed)
    print(f"\n  logged {len(metrics)} checks ({fails} FAIL) to data_quality_metrics")
    return metrics


def run_silver(s, w, run_id: str) -> None:
    _hr("STEP 5 - Bronze -> Silver (quarantine bad, dedup + conform good)")
    run_sql(sp.quarantine_bad_rows(s.catalog, s.schema, run_id), settings=s, client=w)
    qn = run_sql(
        f"SELECT count(*) FROM {sp._fq(s.catalog, s.schema, sp.BRONZE_QUARANTINE)} "
        f"WHERE run_id = {sp._sql_str(run_id)}", settings=s, client=w).scalar()
    run_sql(sp.build_silver(s.catalog, s.schema, run_id), settings=s, client=w)
    sn = run_sql(
        f"SELECT count(*) FROM {sp._fq(s.catalog, s.schema, sp.SILVER)} "
        f"WHERE run_id = {sp._sql_str(run_id)}", settings=s, client=w).scalar()
    print(f"  quarantined {qn} bad rows  ->  bronze_sales_quarantine")
    print(f"  wrote/merged {sn} clean rows  ->  silver_sales")


def check_silver(s, w, run_id: str) -> None:
    _hr("STEP 6 - Silver freshness gate")
    probe = run_sql(sp.gold_freshness_probe(s.catalog, s.schema),
                    settings=s, client=w).dicts()[0]
    lag = probe["silver_freshness_lag_hours"]
    m = evaluate_freshness(float(lag) if lag is not None else None)
    m.layer, m.table_name = "silver", "silver_sales"
    _print_metric(m)
    stmt = sp.insert_metrics(s.catalog, s.schema, run_id, [m])
    if stmt:
        run_sql(stmt, settings=s, client=w)


def run_gold(s, w, run_id: str) -> None:
    _hr("STEP 7 - Silver -> Gold (business aggregates)")
    for stmt in sp.build_gold(s.catalog, s.schema):
        run_sql(stmt, settings=s, client=w)
    for t in (sp.GOLD_DAILY, sp.GOLD_REGION_CAT):
        n = run_sql(f"SELECT count(*) FROM {sp._fq(s.catalog, s.schema, t)}",
                    settings=s, client=w).scalar()
        print(f"  {t:<22} {n} rows")


def verify(s, w) -> None:
    _hr("STEP 8 - Verify: row counts landed in Unity Catalog")
    tables = [sp.BRONZE, sp.BRONZE_QUARANTINE, sp.SILVER,
              sp.GOLD_DAILY, sp.GOLD_REGION_CAT, sp.METRICS]
    for t in tables:
        n = run_sql(f"SELECT count(*) FROM {sp._fq(s.catalog, s.schema, t)}",
                    settings=s, client=w).scalar()
        print(f"  {s.catalog}.{s.schema}.{t:<24} {n:>8} rows")


def main() -> None:
    ap = argparse.ArgumentParser(description="Run the DQ observability pipeline end-to-end.")
    ap.add_argument("--setup-only", action="store_true")
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--verify-only", action="store_true")
    args = ap.parse_args()

    s = load_settings()
    w = get_client(s)
    print(f"Connected to {s.host}  (warehouse {s.warehouse_id})")

    if args.verify_only:
        verify(s, w)
        return

    setup(s, w)
    if args.setup_only:
        print("\nSetup complete.")
        return

    run_id = str(uuid.uuid4())[:8]
    print(f"\n>>> RUN ID: {run_id}")
    if not args.skip_upload:
        upload_landing(s, w)
    run_bronze(s, w, run_id)
    check_bronze(s, w, run_id)
    run_silver(s, w, run_id)
    check_silver(s, w, run_id)
    run_gold(s, w, run_id)
    verify(s, w)
    print(f"\nDone. Run {run_id} complete.")


if __name__ == "__main__":
    main()
