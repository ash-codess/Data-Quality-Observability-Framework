# Building the Lakeview Dashboard

Databricks **Lakeview** dashboards aren't fully creatable via API/CLI yet (datasets +
widget layout are UI-driven), so the queries live in [`queries.sql`](./queries.sql) and
you wire them up in the UI. It takes ~10 minutes.

> Prereq: you've run the pipeline at least once (`python -m src.run_pipeline`) so the
> tables under `people_org.dq_observability` contain data.

## 1. Create the dashboard
1. In the Databricks workspace sidebar, click **Dashboards → Create dashboard**.
2. Name it `Sales DQ Observability`.
3. At the top, set the **Warehouse** to *Serverless Starter Warehouse* (the SQL warehouse
   this project uses). All datasets run on it.

## 2. Add datasets (one per tile)
Open the **Data** tab (bottom of the canvas) → **+ Create from SQL**. For each block in
`queries.sql`, paste the SQL and give the dataset the matching name:

| Dataset name        | Source block in `queries.sql`             |
|---------------------|-------------------------------------------|
| `passfail_over_time`| TILE 1 — Pass/Warn/Fail rate over time    |
| `failures_by_rule`  | TILE 2 — Failures by rule type            |
| `freshness_trend`   | TILE 3 — Freshness lag trend              |
| `quarantine_reasons`| TILE 4 — Quarantined rows by reason       |
| `kpis`              | TILE 5 — KPI counters                     |
| `latest_run`        | TILE 6 — Latest run scorecard             |
| `revenue_by_region` | TILE 7 — Gold revenue by region/category  |

## 3. Add widgets (Canvas tab)
Click **Add visualization**, pick the dataset, then configure:

| Widget                     | Dataset             | Type          | X / Y / Series |
|----------------------------|---------------------|---------------|----------------|
| Pass/Warn/Fail over time   | `passfail_over_time`| Stacked bar   | X=`check_hour`, Y=`check_count`, Color=`status` |
| Failures by rule           | `failures_by_rule`  | Bar           | X=`rule_name`, Y=`fail_count`, Color=`layer` |
| Freshness lag trend        | `freshness_trend`   | Line          | X=`check_ts`, Y=`lag_hours`, Series=`table_name` |
| Quarantined by reason      | `quarantine_reasons`| Bar (horizontal) | X=`quarantine_reason`, Y=`rows` |
| Bronze / Silver / Quarantine / Pass% | `kpis`   | Counter (×4)  | one counter per column |
| Latest run scorecard       | `latest_run`        | Table         | all columns |
| Revenue by region          | `revenue_by_region` | Bar           | X=`store_region`, Y=`total_revenue`, Color=`product_category` |

For **Freshness lag trend**, add a reference line at `y = 24` (the SLA) so breaches are
obvious. For **status** colors, map `PASS→green`, `WARN→amber`, `FAIL→red`.

### Troubleshooting: `UNSUPPORTED_FEATURE.LATERAL_COLUMN_ALIAS`
Lakeview throws this when a widget's measure references a `SELECT` alias (e.g. building
a ratio from `fail_count`) and inlines it *inside* an aggregate. Two ways to avoid it:
- Use the queries as written — TILE 2 pre-aggregates in a CTE and exposes `fail_pct`
  directly, so you never need a calculated measure.
- In the visualization, drag the already-aggregated column (e.g. `fail_count`) onto the
  axis and set its aggregation to **Sum**; do **not** create a custom measure that
  references another column's alias.

## 4. (Optional) Schedule a refresh
Top-right **Schedule → Add schedule** → e.g. every hour on the serverless warehouse.
Re-running `src/run_pipeline.py` (or the notebook job) appends new `run_id`s, so the
time-series tiles fill in over successive runs.

## 5. Publish + screenshot
Click **Publish** (top-right). Then capture a screenshot and drop it into
`../README.md` where the placeholders are (`docs/dashboard.png`).
