"""Tests for the synthetic sales generator: schema contract + anomaly injection."""
import json
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from datetime import datetime, timezone

from src import generate_data as gen  # noqa: E402


def test_clean_record_has_full_contract():
    rec = gen._clean_record(random.Random(1), datetime.now(timezone.utc))
    for col in gen.EXPECTED_COLUMNS:
        assert col in rec, f"clean record missing contract column {col}"
    # total_amount should equal quantity * unit_price for a clean record
    assert abs(rec["total_amount"] - rec["quantity"] * rec["unit_price"]) < 0.011


def test_batch_injects_anomalies_at_high_rate():
    rng = random.Random(42)
    now = datetime.now(timezone.utc)
    records, stats = gen.generate_batch(rng, n_rows=300, anomaly_rate=0.5, now=now)
    assert len(records) >= 300                 # duplicates add extra rows
    assert stats["nulls"] > 0
    assert stats["duplicates"] > 0
    assert stats["late"] > 0


def test_zero_anomaly_rate_is_clean():
    rng = random.Random(0)
    now = datetime.now(timezone.utc)
    records, stats = gen.generate_batch(rng, n_rows=200, anomaly_rate=0.0, now=now)
    assert stats["nulls"] == 0
    assert stats["duplicates"] == 0
    assert stats["schema_drift"] == 0
    # every record is JSON-serialisable and carries the full contract
    for rec in records:
        json.loads(json.dumps(rec))
        assert set(gen.EXPECTED_COLUMNS).issubset(rec.keys())


def test_schema_drift_modes_change_columns():
    rng = random.Random(3)
    base = gen._clean_record(rng, datetime.now(timezone.utc))
    renamed = gen._apply_schema_drift(rng, base, "renamed")
    assert "price" in renamed and "unit_price" not in renamed
    extra = gen._apply_schema_drift(rng, base, "extra")
    assert "discount_code" in extra
    missing = gen._apply_schema_drift(rng, base, "missing")
    assert "store_region" not in missing


def test_write_batch_produces_jsonl(tmp_path):
    rng = random.Random(7)
    now = datetime.now(timezone.utc)
    records, _ = gen.generate_batch(rng, n_rows=10, anomaly_rate=0.0, now=now)
    path = gen.write_batch(tmp_path, 0, records, "20260101000000")
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == len(records)
    assert json.loads(lines[0])["order_id"]
