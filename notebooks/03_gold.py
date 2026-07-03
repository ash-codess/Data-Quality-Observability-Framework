# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: business aggregates
# MAGIC Full-refresh aggregate tables built from `silver_sales` (idempotent). Mirrors
# MAGIC `build_gold()` in `src/sql_pipeline.py`.

# COMMAND ----------

dbutils.widgets.text("catalog", "people_org")
dbutils.widgets.text("schema", "dq_observability")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FQ = f"`{CATALOG}`.`{SCHEMA}`"

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ}.gold_daily_sales
COMMENT 'Daily sales KPIs by region & category' AS
SELECT order_date, store_region, product_category,
       count(*) AS order_count, sum(quantity) AS total_units,
       round(sum(total_amount), 2) AS total_revenue,
       round(avg(total_amount), 2) AS avg_order_value,
       current_timestamp() AS refreshed_ts
FROM {FQ}.silver_sales
WHERE order_status <> 'CANCELLED'
GROUP BY order_date, store_region, product_category
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ}.gold_region_category
COMMENT 'All-time revenue leaderboard by region & category' AS
SELECT store_region, product_category,
       count(*) AS order_count,
       round(sum(total_amount), 2) AS total_revenue,
       round(sum(total_amount) / nullif(count(DISTINCT customer_id), 0), 2) AS revenue_per_customer,
       current_timestamp() AS refreshed_ts
FROM {FQ}.silver_sales
WHERE order_status <> 'CANCELLED'
GROUP BY store_region, product_category
""")

for t in ["gold_daily_sales", "gold_region_category"]:
    print(f"{t}: {spark.table(f'{FQ}.{t}').count()} rows")

# COMMAND ----------

# ---- Log a gold-layer freshness metric ----
from pyspark.sql import functions as F
import uuid
lag = spark.table(f"{FQ}.silver_sales").select(
    (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.max("order_ts"))) / 3600.0
).first()[0]
row = [(str(uuid.uuid4())[:8], "gold", "gold_daily_sales", "freshness_sla", "order_ts",
        "PASS" if (lag is not None and lag <= 24.0) else "FAIL", 0, 0, 0.0,
        float(round(lag or 1e9, 3)), 24.0, f"gold refreshed; silver lag {lag:.2f}h")]
cols = ["run_id", "layer", "table_name", "rule_name", "column_name", "status",
        "failure_count", "total_count", "failure_rate", "metric_value", "threshold", "detail"]
(spark.createDataFrame(row, cols).withColumn("check_ts", F.current_timestamp())
    .select("run_id", "check_ts", "layer", "table_name", "rule_name", "column_name",
            "status", "failure_count", "total_count", "failure_rate", "metric_value",
            "threshold", "detail")
    .write.mode("append").saveAsTable(f"{FQ}.data_quality_metrics"))
print("gold freshness metric logged")
