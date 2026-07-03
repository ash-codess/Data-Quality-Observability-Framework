# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup: catalog, schema, volume, tables
# MAGIC Run this once. Creates the Unity Catalog objects the pipeline writes to.
# MAGIC Works on **serverless** SQL/compute — no dedicated cluster required.

# COMMAND ----------

dbutils.widgets.text("catalog", "people_org")
dbutils.widgets.text("schema", "dq_observability")
dbutils.widgets.text("volume", "landing")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
FQ = f"`{CATALOG}`.`{SCHEMA}`"
print(f"Target: {FQ}  volume=/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ} COMMENT 'DQ observability (sales)'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQ}.`{VOLUME}` COMMENT 'Landing zone for sales JSON'")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ}.data_quality_metrics (
  run_id STRING, check_ts TIMESTAMP, layer STRING, table_name STRING,
  rule_name STRING, column_name STRING, status STRING, failure_count BIGINT,
  total_count BIGINT, failure_rate DOUBLE, metric_value DOUBLE,
  threshold DOUBLE, detail STRING
) USING DELTA COMMENT 'Append-only DQ check log'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ}.bronze_sales_quarantine (
  order_id STRING, customer_id STRING, unit_price DOUBLE, total_amount DOUBLE,
  order_ts STRING, _rescued_data STRING, _source_file STRING, _batch_id STRING,
  quarantine_reason STRING, run_id STRING, quarantine_ts TIMESTAMP
) USING DELTA COMMENT 'Rejected bronze rows (kept, not dropped)'
""")

# bronze_sales / silver_sales are created by their streaming writers (schema-on-read).
print("Setup complete.")
