# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: Auto Loader ingestion
# MAGIC Structured Streaming + Auto Loader (`cloudFiles`) tails the landing volume and
# MAGIC appends raw sales events to `bronze_sales`. `schemaEvolutionMode=rescue` captures
# MAGIC schema drift into `_rescued_data` instead of failing. `Trigger.AvailableNow`
# MAGIC processes whatever has landed and stops — ideal for scheduled serverless runs.
# MAGIC (The local `src/run_pipeline.py` does the equivalent with `COPY INTO`.)

# COMMAND ----------

import uuid
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "people_org")
dbutils.widgets.text("schema", "dq_observability")
dbutils.widgets.text("volume", "landing")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
FQ = f"`{CATALOG}`.`{SCHEMA}`"
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
CHECKPOINT = f"{LANDING}/_checkpoints/bronze"
RUN_ID = str(uuid.uuid4())[:8]
print(f"run_id={RUN_ID}  landing={LANDING}")

# COMMAND ----------

bronze_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", CHECKPOINT)
    .option("cloudFiles.schemaEvolutionMode", "rescue")   # drift -> _rescued_data
    .option("rescuedDataColumn", "_rescued_data")
    .load(LANDING)
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_batch_id", F.lit(RUN_ID))
)

(
    bronze_stream.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{FQ}.bronze_sales")
    .awaitTermination()
)

# COMMAND ----------

n = spark.table(f"{FQ}.bronze_sales").filter(F.col("_batch_id") == RUN_ID).count()
print(f"Bronze ingested {n} rows for run {RUN_ID}")
dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID) if hasattr(dbutils, "jobs") else None
