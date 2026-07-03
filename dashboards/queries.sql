-- =====================================================================
-- Lakeview dashboard queries — Real-Time DQ Observability (sales)
-- Target: people_org.dq_observability
-- Each query below backs one dashboard tile. Copy each into its own
-- dataset in the Lakeview dashboard editor (see INSTRUCTIONS.md).
-- If you used a different catalog/schema, find-and-replace the prefix.
-- =====================================================================


-- ---------------------------------------------------------------------
-- TILE 1 — Pass / Warn / Fail rate over time  (stacked bar or line)
--   X: check_hour   Y: check_count   Series/Color: status
-- ---------------------------------------------------------------------
SELECT
  date_trunc('HOUR', check_ts)              AS check_hour,
  status,
  count(*)                                  AS check_count,
  round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY date_trunc('HOUR', check_ts)), 1)
                                            AS pct_of_hour
FROM people_org.dq_observability.data_quality_metrics
GROUP BY date_trunc('HOUR', check_ts), status
ORDER BY check_hour, status;


-- ---------------------------------------------------------------------
-- TILE 2 — Failures by rule type  (bar chart)
--   X: rule_name   Y: fail_count   (optionally color by layer)
--   NOTE: aggregates are computed in a CTE and derived columns (fail_pct)
--   in the outer query. This avoids UNSUPPORTED_FEATURE.LATERAL_COLUMN_ALIAS,
--   which Lakeview raises when a widget references a SELECT alias
--   (e.g. fail_count) inside an aggregate/measure.
-- ---------------------------------------------------------------------
WITH by_rule AS (
  SELECT
    rule_name,
    layer,
    count(*)                    AS total_checks,
    count_if(status = 'FAIL')   AS fail_count,
    count_if(status = 'WARN')   AS warn_count,
    sum(failure_count)          AS total_failing_rows
  FROM people_org.dq_observability.data_quality_metrics
  GROUP BY rule_name, layer
)
SELECT
  rule_name,
  layer,
  total_checks,
  fail_count,
  warn_count,
  total_failing_rows,
  round(100.0 * fail_count / total_checks, 1) AS fail_pct
FROM by_rule
ORDER BY fail_count DESC, warn_count DESC;


-- ---------------------------------------------------------------------
-- TILE 3 — Freshness lag trend  (line chart)
--   X: check_ts   Y: lag_hours   Series: table_name
--   Reference line at 24 (the SLA threshold).
-- ---------------------------------------------------------------------
SELECT
  check_ts,
  table_name,
  metric_value                              AS lag_hours,
  threshold                                 AS sla_hours,
  status
FROM people_org.dq_observability.data_quality_metrics
WHERE rule_name = 'freshness_sla'
ORDER BY check_ts;


-- ---------------------------------------------------------------------
-- TILE 4 — Quarantined rows by table & reason  (bar chart)
--   X: quarantine_reason   Y: rows
-- ---------------------------------------------------------------------
SELECT
  'bronze_sales' AS source_table,
  quarantine_reason,
  count(*)                                  AS rows
FROM people_org.dq_observability.bronze_sales_quarantine
GROUP BY quarantine_reason
ORDER BY rows DESC;


-- ---------------------------------------------------------------------
-- TILE 5 — KPI counters  (big-number tiles / single-value)
--   One row; add several counter tiles pointing at each column.
-- ---------------------------------------------------------------------
SELECT
  (SELECT count(*) FROM people_org.dq_observability.bronze_sales)             AS bronze_rows,
  (SELECT count(*) FROM people_org.dq_observability.silver_sales)             AS silver_rows,
  (SELECT count(*) FROM people_org.dq_observability.bronze_sales_quarantine)  AS quarantined_rows,
  (SELECT round(100.0 * count_if(status='PASS') / count(*), 1)
     FROM people_org.dq_observability.data_quality_metrics)                   AS overall_pass_pct;


-- ---------------------------------------------------------------------
-- TILE 6 — Latest run scorecard  (table)
--   Most recent run's checks, worst status first.
-- ---------------------------------------------------------------------
WITH latest AS (
  SELECT run_id
  FROM people_org.dq_observability.data_quality_metrics
  WHERE layer = 'bronze'
  ORDER BY check_ts DESC
  LIMIT 1
)
SELECT m.layer, m.rule_name, m.column_name, m.status,
       m.failure_count, m.total_count, m.failure_rate, m.detail
FROM people_org.dq_observability.data_quality_metrics m
JOIN latest USING (run_id)
ORDER BY CASE m.status WHEN 'FAIL' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END, m.rule_name;


-- ---------------------------------------------------------------------
-- TILE 7 — Gold: revenue by region & category  (bar, business context)
-- ---------------------------------------------------------------------
SELECT store_region, product_category, total_revenue, order_count
FROM people_org.dq_observability.gold_region_category
ORDER BY total_revenue DESC;
