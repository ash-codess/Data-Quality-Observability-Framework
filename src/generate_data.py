"""Synthetic e-commerce **sales** stream generator.

Drops batches of newline-delimited JSON files into a landing folder to mimic a
streaming ingestion source. Each run can append new batches, so repeated runs look
like a live stream. Deliberately injects data-quality problems so the downstream
observability pipeline has something to catch:

  * nulls               -- required fields randomly blanked / omitted
  * schema drift        -- extra columns, missing columns, renamed columns per batch
  * duplicate records   -- the same order_id emitted more than once
  * late-arriving data  -- order_ts backdated days/weeks before ingestion time

Usage:
    python -m src.generate_data --batches 5 --rows-per-batch 200 --seed 7
    python -m src.generate_data --batches 1 --rows-per-batch 500 --anomaly-rate 0.25

Clean record schema (the "contract"):
    order_id, customer_id, product_id, product_category, quantity, unit_price,
    total_amount, currency, payment_method, store_region, order_status, order_ts
"""
from __future__ import annotations

import argparse
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

# The agreed-upon schema contract. The pipeline's schema-drift check compares
# incoming columns against this set.
EXPECTED_COLUMNS = [
    "order_id",
    "customer_id",
    "product_id",
    "product_category",
    "quantity",
    "unit_price",
    "total_amount",
    "currency",
    "payment_method",
    "store_region",
    "order_status",
    "order_ts",
]

CATEGORIES = ["Electronics", "Apparel", "Home", "Grocery", "Beauty", "Sports", "Toys"]
REGIONS = ["NA-EAST", "NA-WEST", "EU-CENTRAL", "APAC-SOUTH", "LATAM"]
PAYMENTS = ["CARD", "PAYPAL", "WALLET", "COD", "GIFT_CARD"]
STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
CURRENCIES = ["USD", "EUR", "GBP", "INR"]

REQUIRED_FIELDS = ["order_id", "customer_id", "unit_price", "total_amount", "order_ts"]


def _clean_record(rng: random.Random, now: datetime) -> dict:
    qty = rng.randint(1, 6)
    unit_price = round(rng.uniform(4.99, 899.99), 2)
    # Most orders are recent (within the last hour) to simulate near-real-time.
    order_ts = now - timedelta(seconds=rng.randint(0, 3600))
    return {
        "order_id": str(uuid.UUID(int=rng.getrandbits(128))),
        "customer_id": f"CUST-{rng.randint(1000, 9999)}",
        "product_id": f"SKU-{rng.randint(10000, 99999)}",
        "product_category": rng.choice(CATEGORIES),
        "quantity": qty,
        "unit_price": unit_price,
        "total_amount": round(qty * unit_price, 2),
        "currency": rng.choice(CURRENCIES),
        "payment_method": rng.choice(PAYMENTS),
        "store_region": rng.choice(REGIONS),
        "order_status": rng.choice(STATUSES),
        "order_ts": order_ts.replace(microsecond=0).isoformat(),
    }


def _inject_nulls(rng: random.Random, rec: dict) -> dict:
    """Blank or omit a required field."""
    field = rng.choice(REQUIRED_FIELDS)
    if rng.random() < 0.5:
        rec[field] = None          # explicit null
    else:
        rec.pop(field, None)       # missing key
    return rec


def _inject_late(rng: random.Random, rec: dict, now: datetime) -> dict:
    """Backdate the event timestamp by 2-30 days."""
    late = now - timedelta(days=rng.randint(2, 30), minutes=rng.randint(0, 1440))
    rec["order_ts"] = late.replace(microsecond=0).isoformat()
    return rec


def _apply_schema_drift(rng: random.Random, rec: dict, mode: str) -> dict:
    """Return a schema-drifted copy of the record."""
    rec = dict(rec)
    if mode == "extra":
        rec["discount_code"] = rng.choice(["SAVE10", "FREESHIP", "VIP20", "NONE"])
        rec["loyalty_tier"] = rng.choice(["BRONZE", "SILVER", "GOLD"])
    elif mode == "missing":
        rec.pop("store_region", None)
        rec.pop("payment_method", None)
    elif mode == "renamed":
        if "unit_price" in rec:
            rec["price"] = rec.pop("unit_price")       # renamed unit_price -> price
        if "product_category" in rec:
            rec["category"] = rec.pop("product_category")
    return rec


def generate_batch(
    rng: random.Random,
    n_rows: int,
    anomaly_rate: float,
    now: datetime,
) -> tuple[list[dict], dict]:
    """Build one batch of records plus a stats dict describing injected issues."""
    stats = {"nulls": 0, "duplicates": 0, "late": 0, "schema_drift": 0, "clean": 0}

    # Decide if this whole batch carries a schema-drift flavor.
    drift_mode = None
    if rng.random() < anomaly_rate:
        drift_mode = rng.choice(["extra", "missing", "renamed"])

    records: list[dict] = []
    for _ in range(n_rows):
        rec = _clean_record(rng, now)
        roll = rng.random()

        if roll < anomaly_rate * 0.4:
            rec = _inject_nulls(rng, rec)
            stats["nulls"] += 1
        elif roll < anomaly_rate * 0.6:
            rec = _inject_late(rng, rec, now)
            stats["late"] += 1
        else:
            stats["clean"] += 1

        if drift_mode:
            rec = _apply_schema_drift(rng, rec, drift_mode)
            stats["schema_drift"] += 1

        records.append(rec)

        # Duplicate emission: repeat the same record (same order_id) occasionally.
        if rng.random() < anomaly_rate * 0.15:
            records.append(dict(rec))
            stats["duplicates"] += 1

    if drift_mode:
        stats["drift_mode"] = drift_mode
    return records, stats


def write_batch(out_dir: Path, batch_idx: int, records: list[dict], stamp: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    fname = out_dir / f"sales_{stamp}_{batch_idx:03d}.json"
    with fname.open("w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")
    return fname


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic sales stream batches.")
    parser.add_argument("--batches", type=int, default=5, help="number of JSON files to emit")
    parser.add_argument("--rows-per-batch", type=int, default=200)
    parser.add_argument("--anomaly-rate", type=float, default=0.20,
                        help="0..1 target fraction of records with injected issues")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument(
        "--out",
        type=str,
        default=str(Path(__file__).resolve().parents[1] / "data" / "synthetic"),
    )
    args = parser.parse_args()

    rng = random.Random(args.seed)
    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y%m%d%H%M%S")
    out_dir = Path(args.out)

    totals = {"rows": 0, "nulls": 0, "duplicates": 0, "late": 0, "schema_drift": 0}
    print(f"Generating {args.batches} batch(es) -> {out_dir}")
    for i in range(args.batches):
        records, stats = generate_batch(rng, args.rows_per_batch, args.anomaly_rate, now)
        path = write_batch(out_dir, i, records, stamp)
        totals["rows"] += len(records)
        for k in ("nulls", "duplicates", "late", "schema_drift"):
            totals[k] += stats.get(k, 0)
        drift = f" drift={stats['drift_mode']}" if "drift_mode" in stats else ""
        print(f"  {path.name}: {len(records):4d} rows "
              f"(nulls={stats['nulls']} dups={stats['duplicates']} "
              f"late={stats['late']}{drift})")

    print("\nSummary:")
    print(f"  total rows        : {totals['rows']}")
    print(f"  injected nulls    : {totals['nulls']}")
    print(f"  injected dups     : {totals['duplicates']}")
    print(f"  injected late     : {totals['late']}")
    print(f"  schema-drift rows : {totals['schema_drift']}")


if __name__ == "__main__":
    main()
