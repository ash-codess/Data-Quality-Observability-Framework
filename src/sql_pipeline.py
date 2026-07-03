"""SQL statement builders for the Bronze -> Silver -> Gold pipeline.

All SQL here targets the **serverless SQL warehouse** (no cluster). Ingestion uses
`COPY INTO` (idempotent, incremental file tracking + schema evolution), which gives
Auto-Loader-like behaviour without a running cluster. The equivalent PySpark /
Auto Loader implementation lives in notebooks/ for the "run as a Databricks notebook"
story; both write to the same Unity Catalog tables.
"""
from __future__ import annotations

from typing import Iterable

from .dq_rules import Metric

# Logical table names (unqualified). Fully qualified at build time.
BRONZE = "bronze_sales"
BRONZE_QUARANTINE = "bronze_sales_quarantine"
SILVER = "silver_sales"
GOLD_DAILY = "gold_daily_sales"
GOLD_REGION_CAT = "gold_region_category"
METRICS = "data_quality_metrics"


def _fq(catalog: str, schema: str, table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


def _sql_str(val) -> str:
    """Render a Python value as a SQL literal (single-quote escaped)."""
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    if isinstance(val, (int, float)):
        return repr(val)
    return "'" + str(val).replace("'", "''") + "'"


# --------------------------------------------------------------------------- #
# DDL
# --------------------------------------------------------------------------- #
def ddl_statements(catalog: str, schema: str, volume: str) -> list[str]:
    fq = lambda t: _fq(catalog, schema, t)
    return [
        f"CREATE CATALOG IF NOT EXISTS `{catalog}` "
        f"COMMENT 'Owner org catalog for DQ observability demo'",

        f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}` "
        f"COMMENT 'Real-time Data Quality Observability Framework (sales domain)'",

        f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}` "
        f"COMMENT 'Landing zone for synthetic sales JSON batches'",

        f"""CREATE TABLE IF NOT EXISTS {fq(METRICS)} (
              run_id        STRING,
              check_ts      TIMESTAMP,
              layer         STRING,
              table_name    STRING,
              rule_name     STRING,
              column_name   STRING,
              status        STRING,
              failure_count BIGINT,
              total_count   BIGINT,
              failure_rate  DOUBLE,
              metric_value  DOUBLE,
              threshold     DOUBLE,
              detail        STRING
            ) USING DELTA
            COMMENT 'Append-only log of every DQ check result across all layers'""",

        f"""CREATE TABLE IF NOT EXISTS {fq(BRONZE)} (
              order_id         STRING,
              customer_id      STRING,
              product_id       STRING,
              product_category STRING,
              quantity         BIGINT,
              unit_price       DOUBLE,
              total_amount     DOUBLE,
              currency         STRING,
              payment_method   STRING,
              store_region     STRING,
              order_status     STRING,
              order_ts         STRING,
              _rescued_data    STRING,
              _source_file     STRING,
              _ingest_ts       TIMESTAMP,
              _batch_id        STRING
            ) USING DELTA
            COMMENT 'Raw ingested sales events (schema-on-read; evolves via mergeSchema)'""",

        f"""CREATE TABLE IF NOT EXISTS {fq(BRONZE_QUARANTINE)} (
              order_id          STRING,
              customer_id       STRING,
              unit_price        DOUBLE,
              total_amount      DOUBLE,
              order_ts          STRING,
              _rescued_data     STRING,
              _source_file      STRING,
              _batch_id         STRING,
              quarantine_reason STRING,
              run_id            STRING,
              quarantine_ts     TIMESTAMP
            ) USING DELTA
            COMMENT 'Records rejected by bronze->silver DQ gates (kept, not dropped)'""",

        f"""CREATE TABLE IF NOT EXISTS {fq(SILVER)} (
              order_id         STRING,
              customer_id      STRING,
              product_id       STRING,
              product_category STRING,
              quantity         BIGINT,
              unit_price       DOUBLE,
              total_amount     DOUBLE,
              currency         STRING,
              payment_method   STRING,
              store_region     STRING,
              order_status     STRING,
              order_ts         TIMESTAMP,
              order_date       DATE,
              is_late          BOOLEAN,
              _ingest_ts       TIMESTAMP,
              run_id           STRING
            ) USING DELTA
            COMMENT 'Cleaned, deduplicated, conformed sales'""",
    ]


# --------------------------------------------------------------------------- #
# Bronze ingestion
# --------------------------------------------------------------------------- #
def bronze_copy_into(catalog: str, schema: str, volume_path: str, run_id: str) -> str:
    """Idempotent incremental load of new JSON files from the landing volume."""
    return f"""
        COPY INTO {_fq(catalog, schema, BRONZE)}
        FROM (
          SELECT
            *,
            _metadata.file_path AS _source_file,
            current_timestamp() AS _ingest_ts,
            {_sql_str(run_id)}  AS _batch_id
          FROM '{volume_path}'
        )
        FILEFORMAT = JSON
        FORMAT_OPTIONS ('mergeSchema' = 'true', 'rescuedDataColumn' = '_rescued_data')
        COPY_OPTIONS  ('mergeSchema' = 'true')
    """


# --------------------------------------------------------------------------- #
# Aggregate probes (feed dq_rules)
# --------------------------------------------------------------------------- #
def bronze_probe(catalog: str, schema: str, run_id: str) -> str:
    """One-shot aggregate over this run's bronze rows for the DQ evaluators."""
    b = _fq(catalog, schema, BRONZE)
    r = _sql_str(run_id)
    return f"""
        SELECT
          count(*)                                                    AS total,
          count(*) FILTER (WHERE order_id     IS NULL)                AS null_order_id,
          count(*) FILTER (WHERE customer_id  IS NULL)                AS null_customer_id,
          count(*) FILTER (WHERE unit_price   IS NULL)                AS null_unit_price,
          count(*) FILTER (WHERE total_amount IS NULL)                AS null_total_amount,
          count(*) FILTER (WHERE order_ts     IS NULL)                AS null_order_ts,
          count(*) FILTER (WHERE _rescued_data IS NOT NULL)           AS rescued,
          count(DISTINCT order_id)                                    AS distinct_orders,
          count(*) FILTER (
            WHERE order_ts IS NOT NULL
              AND _ingest_ts > try_cast(order_ts AS TIMESTAMP) + INTERVAL 24 HOURS
          )                                                           AS late_arrivals,
          try_divide(
            date_diff(SECOND, max(try_cast(order_ts AS TIMESTAMP)), current_timestamp()),
            3600.0
          )                                                           AS freshness_lag_hours
        FROM {b}
        WHERE _batch_id = {r}
    """


def bronze_columns_query(catalog: str, schema: str) -> str:
    return f"""
        SELECT lower(column_name) AS column_name
        FROM `{catalog}`.information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = '{BRONZE}'
    """


def trailing_rowcount_query(catalog: str, schema: str, run_id: str) -> str:
    """Average bronze row count of *previous* runs (for anomaly detection)."""
    m = _fq(catalog, schema, METRICS)
    return f"""
        SELECT avg(metric_value) AS trailing_avg
        FROM {m}
        WHERE rule_name = 'row_count_anomaly' AND run_id <> {_sql_str(run_id)}
    """


# --------------------------------------------------------------------------- #
# Metrics logging
# --------------------------------------------------------------------------- #
def insert_metrics(catalog: str, schema: str, run_id: str,
                   metrics: Iterable[Metric]) -> str | None:
    rows = []
    for m in metrics:
        rows.append(
            "(" + ", ".join([
                _sql_str(run_id), "current_timestamp()",
                _sql_str(m.layer), _sql_str(m.table_name), _sql_str(m.rule_name),
                _sql_str(m.column_name), _sql_str(m.status),
                _sql_str(int(m.failure_count)), _sql_str(int(m.total_count)),
                _sql_str(float(m.failure_rate)), _sql_str(float(m.metric_value)),
                _sql_str(float(m.threshold)), _sql_str(m.detail),
            ]) + ")"
        )
    if not rows:
        return None
    cols = ("run_id, check_ts, layer, table_name, rule_name, column_name, status, "
            "failure_count, total_count, failure_rate, metric_value, threshold, detail")
    return (f"INSERT INTO {_fq(catalog, schema, METRICS)} ({cols}) VALUES\n"
            + ",\n".join(rows))


# --------------------------------------------------------------------------- #
# Bronze -> Silver (quarantine bad, dedup + conform good)
# --------------------------------------------------------------------------- #
def quarantine_bad_rows(catalog: str, schema: str, run_id: str) -> str:
    """Route this run's rejected rows into the quarantine table (kept, not dropped)."""
    b = _fq(catalog, schema, BRONZE)
    q = _fq(catalog, schema, BRONZE_QUARANTINE)
    r = _sql_str(run_id)
    return f"""
        INSERT INTO {q}
        SELECT
          order_id, customer_id, unit_price, total_amount, order_ts,
          _rescued_data, _source_file, _batch_id,
          concat_ws('; ',
            CASE WHEN order_id     IS NULL THEN 'null_order_id'     END,
            CASE WHEN customer_id  IS NULL THEN 'null_customer_id'  END,
            CASE WHEN unit_price   IS NULL THEN 'null_unit_price'   END,
            CASE WHEN total_amount IS NULL THEN 'null_total_amount' END,
            CASE WHEN order_ts     IS NULL
                  OR try_cast(order_ts AS TIMESTAMP) IS NULL THEN 'bad_order_ts' END,
            CASE WHEN _rescued_data IS NOT NULL THEN 'schema_drift' END
          )                          AS quarantine_reason,
          {r}                        AS run_id,
          current_timestamp()        AS quarantine_ts
        FROM {b}
        WHERE _batch_id = {r}
          AND (
                order_id     IS NULL OR customer_id IS NULL
             OR unit_price   IS NULL OR total_amount IS NULL
             OR order_ts     IS NULL OR try_cast(order_ts AS TIMESTAMP) IS NULL
             OR _rescued_data IS NOT NULL
          )
    """


def build_silver(catalog: str, schema: str, run_id: str) -> str:
    """MERGE clean, deduplicated, conformed rows into silver.

    Dedup: keep the latest-ingested row per order_id. MERGE makes reruns idempotent.
    """
    b = _fq(catalog, schema, BRONZE)
    s = _fq(catalog, schema, SILVER)
    r = _sql_str(run_id)
    return f"""
        MERGE INTO {s} AS tgt
        USING (
          SELECT
            order_id,
            customer_id,
            product_id,
            product_category,
            cast(quantity AS BIGINT)                                  AS quantity,
            cast(unit_price AS DOUBLE)                                AS unit_price,
            -- repair total_amount when obviously wrong / missing
            coalesce(cast(total_amount AS DOUBLE),
                     cast(quantity AS DOUBLE) * cast(unit_price AS DOUBLE)) AS total_amount,
            upper(coalesce(currency, 'USD'))                          AS currency,
            upper(coalesce(payment_method, 'UNKNOWN'))                AS payment_method,
            upper(coalesce(store_region, 'UNKNOWN'))                  AS store_region,
            upper(coalesce(order_status, 'UNKNOWN'))                  AS order_status,
            try_cast(order_ts AS TIMESTAMP)                           AS order_ts,
            cast(try_cast(order_ts AS TIMESTAMP) AS DATE)             AS order_date,
            (_ingest_ts > try_cast(order_ts AS TIMESTAMP) + INTERVAL 24 HOURS) AS is_late,
            _ingest_ts,
            {r}                                                       AS run_id
          FROM {b}
          WHERE _batch_id = {r}
            AND order_id IS NOT NULL AND customer_id IS NOT NULL
            AND unit_price IS NOT NULL AND total_amount IS NOT NULL
            AND try_cast(order_ts AS TIMESTAMP) IS NOT NULL
            AND _rescued_data IS NULL
          QUALIFY row_number() OVER (
                    PARTITION BY order_id ORDER BY _ingest_ts DESC) = 1
        ) AS src
        ON tgt.order_id = src.order_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """


# --------------------------------------------------------------------------- #
# Silver -> Gold (full-refresh aggregates; idempotent)
# --------------------------------------------------------------------------- #
def build_gold(catalog: str, schema: str) -> list[str]:
    s = _fq(catalog, schema, SILVER)
    daily = _fq(catalog, schema, GOLD_DAILY)
    regcat = _fq(catalog, schema, GOLD_REGION_CAT)
    return [
        f"""CREATE OR REPLACE TABLE {daily}
            COMMENT 'Daily sales KPIs by region & category' AS
            SELECT
              order_date,
              store_region,
              product_category,
              count(*)                        AS order_count,
              sum(quantity)                   AS total_units,
              round(sum(total_amount), 2)     AS total_revenue,
              round(avg(total_amount), 2)     AS avg_order_value,
              current_timestamp()             AS refreshed_ts
            FROM {s}
            WHERE order_status <> 'CANCELLED'
            GROUP BY order_date, store_region, product_category""",

        f"""CREATE OR REPLACE TABLE {regcat}
            COMMENT 'All-time revenue leaderboard by region & category' AS
            SELECT
              store_region,
              product_category,
              count(*)                        AS order_count,
              round(sum(total_amount), 2)     AS total_revenue,
              round(sum(total_amount) / nullif(count(DISTINCT customer_id), 0), 2)
                                              AS revenue_per_customer,
              current_timestamp()             AS refreshed_ts
            FROM {s}
            WHERE order_status <> 'CANCELLED'
            GROUP BY store_region, product_category""",
    ]


def gold_freshness_probe(catalog: str, schema: str) -> str:
    s = _fq(catalog, schema, SILVER)
    return f"""
        SELECT
          count(*) AS silver_rows,
          try_divide(
            date_diff(SECOND, max(order_ts), current_timestamp()), 3600.0
          ) AS silver_freshness_lag_hours
        FROM {s}
    """
