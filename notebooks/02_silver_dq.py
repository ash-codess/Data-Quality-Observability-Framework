# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: DQ gates + quarantine + conform
# MAGIC Runs the data-quality checks over the latest bronze batch, logs every result to
# MAGIC `data_quality_metrics`, quarantines bad rows (null required fields / bad timestamp
# MAGIC / schema drift) to `bronze_sales_quarantine`, then deduplicates + conforms the
# MAGIC good rows into `silver_sales`. Mirrors `src/dq_rules.py` + `src/sql_pipeline.py`.

# COMMAND ----------

from pyspark.sql import functions as F, Window

dbutils.widgets.text("catalog", "people_org")
dbutils.widgets.text("schema", "dq_observability")
dbutils.widgets.text("run_id", "")   # optional: pin to a bronze batch; else use latest

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FQ = f"`{CATALOG}`.`{SCHEMA}`"

NULL_RATE_THRESHOLD = 0.05
FRESHNESS_SLA_HOURS = 24.0
REQUIRED = ["order_id", "customer_id", "unit_price", "total_amount", "order_ts"]
EXPECTED = {"order_id", "customer_id", "product_id", "product_category", "quantity",
            "unit_price", "total_amount", "currency", "payment_method",
            "store_region", "order_status", "order_ts"}
META = {"_rescued_data", "_source_file", "_ingest_ts", "_batch_id"}

bronze = spark.table(f"{FQ}.bronze_sales")
run_id = dbutils.widgets.get("run_id").strip()
if not run_id:
    run_id = bronze.orderBy(F.col("_ingest_ts").desc()).select("_batch_id").first()[0]
batch = bronze.filter(F.col("_batch_id") == run_id)
total = batch.count()
print(f"Evaluating run_id={run_id}  ({total} bronze rows)")

# COMMAND ----------

# ---- Collect metrics (list of Row dicts) ----
metrics = []

def add(rule, status, col=None, fc=0, tot=total, mv=0.0, thr=0.0, detail="", layer="bronze",
        table="bronze_sales"):
    rate = (fc / tot) if tot else 0.0
    metrics.append((run_id, layer, table, rule, col, status, int(fc), int(tot),
                    float(round(rate, 4)), float(mv), float(thr), detail))

# schema drift
present = set(batch.columns)
unexpected = sorted((present - EXPECTED) - META)
missing = sorted(EXPECTED - present)
rescued = batch.filter(F.col("_rescued_data").isNotNull()).count() if "_rescued_data" in present else 0
drift = bool(rescued or unexpected or missing)
add("schema_drift", "FAIL" if drift else "PASS", fc=rescued,
    mv=len(unexpected) + len(missing),
    detail=f"rescued={rescued}; unexpected={unexpected or '-'}; missing={missing or '-'}")

# null rates
nulls = batch.select([F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in REQUIRED]).first()
for c in REQUIRED:
    fc = nulls[c]
    rate = (fc / total) if total else 0.0
    status = "FAIL" if rate > NULL_RATE_THRESHOLD else ("WARN" if fc else "PASS")
    add("null_rate", status, col=c, fc=fc, mv=round(rate, 4), thr=NULL_RATE_THRESHOLD,
        detail=f"{fc}/{total} nulls in {c}")

# duplicates
distinct = batch.select("order_id").distinct().count()
dup = max(total - distinct, 0)
add("duplicate", "FAIL" if dup else "PASS", col="order_id", fc=dup, mv=dup,
    detail=f"{dup} duplicate order_id rows")

# freshness + late arrivals
ts = F.to_timestamp("order_ts")
agg = batch.select(
    F.max(ts).alias("max_ts"),
    F.count(F.when(F.col("_ingest_ts") > ts + F.expr("INTERVAL 24 HOURS"), 1)).alias("late"),
).first()
lag = None
if agg["max_ts"] is not None:
    lag = (batch.select(
        (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.max(ts))) / 3600.0
    ).first()[0])
add("freshness_sla", "FAIL" if (lag is None or lag > FRESHNESS_SLA_HOURS) else "PASS",
    col="order_ts", mv=round(lag or 1e9, 3), thr=FRESHNESS_SLA_HOURS,
    detail=f"latest record {lag:.2f}h old" if lag is not None else "no valid timestamps")
add("late_arriving", "WARN" if agg["late"] else "PASS", col="order_ts",
    fc=agg["late"], mv=agg["late"], thr=24.0,
    detail=f"{agg['late']}/{total} backdated >24h")

# ---- Persist metrics ----
cols = ["run_id", "layer", "table_name", "rule_name", "column_name", "status",
        "failure_count", "total_count", "failure_rate", "metric_value", "threshold", "detail"]
(spark.createDataFrame(metrics, cols)
    .withColumn("check_ts", F.current_timestamp())
    .select("run_id", "check_ts", "layer", "table_name", "rule_name", "column_name",
            "status", "failure_count", "total_count", "failure_rate", "metric_value",
            "threshold", "detail")
    .write.mode("append").saveAsTable(f"{FQ}.data_quality_metrics"))
for m in metrics:
    print(f"  [{m[5]:<4}] {m[3]:<18} {m[4] or '':<14} {m[11]}")

# COMMAND ----------

# ---- Quarantine bad rows ----
bad_cond = (
    F.col("order_id").isNull() | F.col("customer_id").isNull()
    | F.col("unit_price").isNull() | F.col("total_amount").isNull()
    | F.col("order_ts").isNull() | ts.isNull()
    | F.col("_rescued_data").isNotNull()
)
reason = F.concat_ws("; ",
    F.when(F.col("order_id").isNull(), "null_order_id"),
    F.when(F.col("customer_id").isNull(), "null_customer_id"),
    F.when(F.col("unit_price").isNull(), "null_unit_price"),
    F.when(F.col("total_amount").isNull(), "null_total_amount"),
    F.when(F.col("order_ts").isNull() | ts.isNull(), "bad_order_ts"),
    F.when(F.col("_rescued_data").isNotNull(), "schema_drift"),
)
(batch.filter(bad_cond)
    .select("order_id", "customer_id", "unit_price", "total_amount", "order_ts",
            "_rescued_data", "_source_file", "_batch_id",
            reason.alias("quarantine_reason"),
            F.lit(run_id).alias("run_id"), F.current_timestamp().alias("quarantine_ts"))
    .write.mode("append").saveAsTable(f"{FQ}.bronze_sales_quarantine"))

# ---- Conform + dedup good rows into silver ----
w = Window.partitionBy("order_id").orderBy(F.col("_ingest_ts").desc())
silver = (batch.filter(~bad_cond)
    .withColumn("order_ts", ts)
    .withColumn("order_date", ts.cast("date"))
    .withColumn("total_amount",
                F.coalesce(F.col("total_amount").cast("double"),
                           (F.col("quantity") * F.col("unit_price")).cast("double")))
    .withColumn("currency", F.upper(F.coalesce("currency", F.lit("USD"))))
    .withColumn("payment_method", F.upper(F.coalesce("payment_method", F.lit("UNKNOWN"))))
    .withColumn("store_region", F.upper(F.coalesce("store_region", F.lit("UNKNOWN"))))
    .withColumn("order_status", F.upper(F.coalesce("order_status", F.lit("UNKNOWN"))))
    .withColumn("is_late", F.col("_ingest_ts") > F.col("order_ts") + F.expr("INTERVAL 24 HOURS"))
    .withColumn("run_id", F.lit(run_id))
    .withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1)
    .select("order_id", "customer_id", "product_id", "product_category",
            F.col("quantity").cast("bigint"), F.col("unit_price").cast("double"),
            "total_amount", "currency", "payment_method", "store_region",
            "order_status", "order_ts", "order_date", "is_late", "_ingest_ts", "run_id"))

(silver.write.mode("append").option("mergeSchema", "true")
    .saveAsTable(f"{FQ}.silver_sales"))
print(f"quarantined={batch.filter(bad_cond).count()}  silver_written={silver.count()}")
