# Forecast Views & Production Data Flow

**Document Version**: 1.0  
**Last Updated**: 2026-01-31  
**Owner**: Data Science & Analytics Team  

---

## Overview

This document describes the production revenue forecasting data flow, table schemas, and operational procedures for Finance stakeholders consuming monthly revenue forecasts.

**Key Principle**: All forecast predictions (backtest AND published) are stored in a single canonical table: `FORECAST_MODEL_BACKTEST_PREDICTIONS`. There is no separate "published forecasts" table. Published forecasts are identified by filtering for the champion model_run_id and most recent anchor month.

---

## Canonical Data Model

### Primary Tables

```
FORECAST_MODEL_BACKTEST_PREDICTIONS (canonical source)
  ├── Contains ALL predictions: backtest (historical) + published (future)
  ├── Schema: anchor + horizon → target pattern
  ├── 94,068 rows (as of 2026-01-31)
  └── Used by: Modeling notebooks, Scoring procedure
  
FORECAST_MODEL_RUNS (model metadata)
  ├── Tracks all model training runs with parameters, timestamps, code_ref
  └── Append-only (never delete historical runs)
  
FORECAST_MODEL_CHAMPIONS (champion selection)
  ├── Stores current champion model_run_id per scope (GLOBAL or PC_REASON)
  └── Updated after each backtest/evaluation cycle
  
FORECAST_OUTPUT_PC_REASON_MTH (consumption table)
  ├── Published forecasts at PC × Reason × Month level
  ├── Written by: SP_SCORE_AND_PUBLISH_FORECASTS
  └── Append-only (each forecast_run_id is unique UUID)
  
FORECAST_OUTPUT_PC_REASON_CUST_MTH (consumption table)
  ├── Disaggregated forecasts by customer group
  └── Uses allocation shares from FORECAST_CUST_MIX_PC_REASON
```

### Supporting Tables

```
FORECAST_ACTUALS_PC_REASON_MTH
  ├── Actual revenue history (source of truth)
  └── Used for: lag features, seasonal naive baseline, forecast vs actuals
  
FORECAST_BUDGET_PC_REASON_MTH
  ├── Budget targets from Finance (for comparison only, NOT a model feature)
  └── Source: RNA_RCT_TMT_CCCI_PL_RO_REVENUE_5YEARS view
  
FORECAST_CUST_MIX_PC_REASON
  ├── Customer group allocation shares
  └── Used to disaggregate PC×Reason forecasts to customer level
  
FORECAST_FISCAL_MONTH_DIM
  ├── Fiscal calendar dimension (YYYYMM, year, month, seq, dates)
  └── Used to convert month_seq ↔ fiscal_yyyymm
```

---

## Anchor + Horizon Pattern

### Core Concept

The forecasting model uses an **anchor + horizon** pattern instead of direct "forecast_month":

- **Anchor Month** (`anchor_fiscal_yyyymm`): The latest closed month when forecast was created (as-of date)
- **Horizon** (`horizon`): How many months ahead to forecast (1-12)
- **Target Month** (`target_fiscal_yyyymm`): The forecast month = anchor + horizon months

**Example**:
- Anchor: 202601 (January 2026)
- Horizon: 3
- Target: 202604 (April 2026)

### Why This Pattern?

1. **Enables backtesting**: Can generate predictions for historical anchors and compare to actuals
2. **Horizon-specific models**: Model can learn different patterns for H=1 (next month) vs H=12 (1 year out)
3. **Consistent training/scoring**: Same logic works for both backtest evaluation and production scoring

### Converting to forecast_month

For Finance users expecting a simple "forecast_month" column:

```sql
-- In SQL
target_fiscal_yyyymm as forecast_month

-- In Python (from YYYYMM integer)
pd.to_datetime(df['TARGET_FISCAL_YYYYMM'].astype(str), format='%Y%m')

-- Computing target from anchor + horizon
SELECT 
  anchor_fiscal_yyyymm,
  horizon,
  DATE_PART('year', DATEADD('month', horizon, TO_DATE(anchor_fiscal_yyyymm::TEXT, 'YYYYMM'))) * 100
    + DATE_PART('month', DATEADD('month', horizon, TO_DATE(anchor_fiscal_yyyymm::TEXT, 'YYYYMM')))
  AS target_fiscal_yyyymm
FROM ...
```

---

## Monthly Forecast Production Workflow

### Timing

**Monthly Cadence**: 5th business day of each month  
**Anchor Month**: Previous month (latest closed month with complete actuals)  
**Horizons**: 1-12 (next 12 months)  

**Example**:
- Execution Date: 2026-02-05 (February 5th)
- Anchor Month: 202601 (January 2026, latest closed month)
- Target Months: 202602-202701 (Feb 2026 - Jan 2027)

### Workflow Steps

#### 1. Pre-Flight Checks

```sql
-- Verify actuals loaded through latest closed month
SELECT max(fiscal_yyyymm) as latest_actuals
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_ACTUALS_PC_REASON_MTH;
-- Expected: 202601 (when running on 2026-02-05)

-- Verify ASOF month is set correctly
SELECT * FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_ASOF_FISCAL_MONTH;
-- Expected: fiscal_yyyymm = 202601

-- Check for data quality issues
SELECT 
  roll_up_shop,
  count(*) as month_count,
  sum(case when total_revenue is null then 1 else 0 end) as null_count
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_ACTUALS_PC_REASON_MTH
WHERE fiscal_yyyymm >= 202501
GROUP BY roll_up_shop
HAVING null_count > 0;
-- Expected: 0 rows (no nulls)
```

#### 2. Execute Data Pipeline

**Option A: Automated (if Snowflake TASK enabled)**
```sql
-- Resume task (one-time)
ALTER TASK DB_BI_P_SANDBOX.SANDBOX.TASK_RUN_RCT_BACKTEST RESUME;

-- Monitor task execution
SELECT *
FROM TABLE(DB_BI_P_SANDBOX.INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'TASK_RUN_RCT_BACKTEST'
ORDER BY SCHEDULED_TIME DESC
LIMIT 1;
```

**Option B: Manual Execution**
```sql
-- Step 1: Run orchestrator (builds feature datasets)
CALL DB_BI_P_SANDBOX.SANDBOX.SP_RUN_ORCHESTRATOR(
  'MONTHLY_FORECAST',  -- run_type
  NULL,                -- use current ASOF month
  12                   -- max_horizon
);

-- Verify run succeeded
SELECT run_id, status, status_message
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS
ORDER BY triggered_at DESC
LIMIT 1;
-- Expected: status = 'SUCCEEDED'

-- Step 2: Run modeling notebook (Python)
-- Execute: 10__modeling__backtest_global_plus_overrides.ipynb
-- This generates new model_run_id and inserts into FORECAST_MODEL_BACKTEST_PREDICTIONS

-- Step 3: Upsert champion (if new model is better)
MERGE INTO DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_CHAMPIONS t
USING (
  SELECT
    'GLOBAL' as champion_scope,
    NULL as roll_up_shop,
    NULL as reason_group,
    '<NEW_MODEL_RUN_ID>' as model_run_id,
    '<ASOF_YYYYMM>' as asof_fiscal_yyyymm,
    'WAPE' as selection_metric,
    <WAPE_VALUE> as selection_value,
    'New GBR model outperformed baseline by X%' as selection_reason,
    CURRENT_TIMESTAMP() as selected_at
) s
ON t.champion_scope = s.champion_scope
   AND t.asof_fiscal_yyyymm = s.asof_fiscal_yyyymm
WHEN MATCHED THEN
  UPDATE SET
    model_run_id = s.model_run_id,
    selection_metric = s.selection_metric,
    selection_value = s.selection_value,
    selection_reason = s.selection_reason,
    selected_at = s.selected_at
WHEN NOT MATCHED THEN
  INSERT (champion_scope, roll_up_shop, reason_group, model_run_id, 
          asof_fiscal_yyyymm, selection_metric, selection_value, 
          selection_reason, selected_at)
  VALUES (s.champion_scope, s.roll_up_shop, s.reason_group, s.model_run_id,
          s.asof_fiscal_yyyymm, s.selection_metric, s.selection_value,
          s.selection_reason, s.selected_at);

-- Step 4: Run scoring (generate published forecasts)
CALL DB_BI_P_SANDBOX.SANDBOX.SP_SCORE_AND_PUBLISH_FORECASTS(
  (SELECT run_id FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS 
   WHERE status = 'SUCCEEDED' ORDER BY triggered_at DESC LIMIT 1),
  NULL,  -- use current ASOF month
  12,    -- max_horizon
  NULL   -- auto-generate forecast_run_id
);
-- Returns: {status: 'OK', forecast_run_id: '<UUID>', rows_pc_reason_forecast: 2412, ...}
```

#### 3. Post-Flight Validation

```sql
-- Verify row counts for latest forecast_run_id
SELECT 
  forecast_run_id,
  asof_fiscal_yyyymm,
  count(*) as row_count,
  count(distinct roll_up_shop) as pc_count,
  count(distinct concat(roll_up_shop, '|', reason_group)) as pc_reason_count,
  min(target_fiscal_yyyymm) as min_forecast_month,
  max(target_fiscal_yyyymm) as max_forecast_month
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
WHERE forecast_run_id = (
  SELECT forecast_run_id 
  FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
  ORDER BY forecast_created_at DESC 
  LIMIT 1
)
GROUP BY forecast_run_id, asof_fiscal_yyyymm;
-- Expected: row_count ~2400 (67 PCs × ~3 reasons × 12 horizons)

-- Spot-check top 10 PCs by forecasted revenue
SELECT 
  roll_up_shop,
  reason_group,
  target_fiscal_yyyymm,
  revenue_forecast,
  revenue_forecast_lo,
  revenue_forecast_hi
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
WHERE forecast_run_id = '<LATEST_FORECAST_RUN_ID>'
  AND horizon = 1  -- Next month
ORDER BY revenue_forecast DESC
LIMIT 10;

-- Compare vs previous month (month-over-month variance check)
WITH current_fcst AS (
  SELECT roll_up_shop, reason_group, target_fiscal_yyyymm,
         sum(revenue_forecast) as fcst
  FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
  WHERE asof_fiscal_yyyymm = 202601  -- Current anchor
  GROUP BY 1,2,3
),
prev_fcst AS (
  SELECT roll_up_shop, reason_group, target_fiscal_yyyymm,
         sum(revenue_forecast) as fcst
  FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT
  WHERE snapshot_month = 202512  -- Previous anchor
  GROUP BY 1,2,3
)
SELECT 
  c.roll_up_shop,
  c.target_fiscal_yyyymm,
  p.fcst as prev_month_forecast,
  c.fcst as curr_month_forecast,
  (c.fcst - p.fcst) / nullif(p.fcst, 0) * 100 as pct_change
FROM current_fcst c
JOIN prev_fcst p
  ON p.roll_up_shop = c.roll_up_shop
 AND p.reason_group = c.reason_group
 AND p.target_fiscal_yyyymm = c.target_fiscal_yyyymm
WHERE abs((c.fcst - p.fcst) / nullif(p.fcst, 0)) > 0.20  -- >20% change
ORDER BY abs((c.fcst - p.fcst) / nullif(p.fcst, 0)) DESC;
-- Expected: Few or no rows (most changes <20%)
-- If many large changes: investigate data issues or model drift
```

#### 4. Snapshot and Archive

```sql
-- Run monthly snapshot (preserves audit trail)
CALL DB_BI_P_SANDBOX.SANDBOX.SP_SNAPSHOT_MONTHLY_FORECASTS(202601);
-- Returns: {status: 'OK', snapshot_month: 202601, rows_snapshotted: 2412, ...}

-- Verify snapshot matches current forecasts
SELECT 
  'CURRENT' as source,
  count(*) as rows
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
WHERE asof_fiscal_yyyymm = 202601

UNION ALL

SELECT 
  'SNAPSHOT' as source,
  count(*) as rows
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT
WHERE snapshot_month = 202601;
-- Expected: Both rows should match
```

#### 5. Delivery to Finance

**Export Options**:

**Option A: Direct SQL Query**
```sql
-- Export to CSV for Excel
SELECT * 
FROM DB_BI_P_SANDBOX.SANDBOX.VW_FORECAST_NEXT12_BY_SERIES
WHERE horizon <= 12
ORDER BY pc, reason_group, forecast_month;
```

**Option B: Tableau Extract**
- Connect Tableau to `VW_FORECAST_NEXT12_BY_SERIES` view
- Build dashboard with filters: PC, Reason, Forecast Month, Horizon
- Publish to Tableau Server

**Option C: Python Export**
```python
# Export to Excel with formatting
import pandas as pd
from snowflake.snowpark import Session

fcst_df = session.sql("""
    SELECT * FROM DB_BI_P_SANDBOX.SANDBOX.VW_FORECAST_NEXT12_BY_SERIES
    WHERE horizon <= 12
""").to_pandas()

with pd.ExcelWriter('revenue_forecast_202601.xlsx') as writer:
    fcst_df.to_excel(writer, sheet_name='Forecasts', index=False)
    
    # Add summary sheet
    summary = fcst_df.groupby('forecast_month').agg({
        'revenue_forecast': 'sum',
        'pc': 'nunique'
    }).reset_index()
    summary.to_excel(writer, sheet_name='Summary', index=False)
```

---

## Forecast vs Actuals Comparison

### Monthly Reconciliation

After month-end close, compare forecasted vs actual revenue:

```sql
-- Forecast vs Actuals for December 2025
-- (Forecasted in November 2025, comparing to actual December results)

WITH forecast AS (
  SELECT 
    roll_up_shop,
    reason_group,
    target_fiscal_yyyymm,
    revenue_forecast,
    revenue_forecast_lo,
    revenue_forecast_hi
  FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT
  WHERE snapshot_month = 202511  -- Forecasted in November
    AND target_fiscal_yyyymm = 202512  -- For December
    AND horizon = 1  -- H=1 (next month forecast)
),
actuals AS (
  SELECT 
    roll_up_shop,
    reason_group,
    fiscal_yyyymm,
    total_revenue
  FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_ACTUALS_PC_REASON_MTH
  WHERE fiscal_yyyymm = 202512  -- December actuals
)
SELECT 
  f.roll_up_shop,
  f.reason_group,
  f.revenue_forecast,
  a.total_revenue as actual_revenue,
  f.revenue_forecast_lo,
  f.revenue_forecast_hi,
  (a.total_revenue - f.revenue_forecast) as variance,
  (a.total_revenue - f.revenue_forecast) / nullif(f.revenue_forecast, 0) * 100 as variance_pct,
  CASE 
    WHEN a.total_revenue between f.revenue_forecast_lo and f.revenue_forecast_hi THEN 'Within CI'
    WHEN a.total_revenue > f.revenue_forecast_hi THEN 'Above CI'
    WHEN a.total_revenue < f.revenue_forecast_lo THEN 'Below CI'
  END as ci_result
FROM forecast f
JOIN actuals a
  ON a.roll_up_shop = f.roll_up_shop
 AND a.reason_group = f.reason_group
ORDER BY abs(variance) DESC;
```

### Accuracy Metrics

```sql
-- Company-level accuracy metrics for H=1 forecasts (last 6 months)

WITH fcst_vs_actuals AS (
  SELECT 
    f.snapshot_month,
    f.target_fiscal_yyyymm,
    f.revenue_forecast as y_pred,
    a.total_revenue as y_true,
    abs(a.total_revenue - f.revenue_forecast) as abs_error,
    (a.total_revenue - f.revenue_forecast) as error
  FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT f
  JOIN DB_BI_P_SANDBOX.SANDBOX.FORECAST_ACTUALS_PC_REASON_MTH a
    ON a.roll_up_shop = f.roll_up_shop
   AND a.reason_group = f.reason_group
   AND a.fiscal_yyyymm = f.target_fiscal_yyyymm
  WHERE f.horizon = 1
    AND f.snapshot_month >= 202507  -- Last 6 months
)
SELECT 
  snapshot_month,
  sum(y_true) as total_actual,
  sum(y_pred) as total_forecast,
  sum(abs_error) / nullif(sum(y_true), 0) * 100 as wape_pct,
  avg(abs_error) as mae,
  sum(error) / nullif(sum(y_true), 0) * 100 as bias_pct,
  count(*) as series_count
FROM fcst_vs_actuals
GROUP BY snapshot_month
ORDER BY snapshot_month DESC;
```

---

## Troubleshooting Guide

### Common Issues

#### Issue 1: Forecast row count is lower than expected

**Symptom**: FORECAST_OUTPUT_PC_REASON_MTH has <2000 rows instead of ~2400

**Causes**:
1. PC eligibility filtering removed some PCs
2. Missing actuals data for some PCs
3. Customer mix allocation failed for some series

**Diagnosis**:
```sql
-- Check PC eligibility
SELECT count(distinct roll_up_shop) as eligible_pcs
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_PC_ELIGIBILITY
WHERE is_eligible = true;
-- Expected: ~67 PCs

-- Check which PCs are missing from forecasts
SELECT DISTINCT e.roll_up_shop
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_PC_ELIGIBILITY e
WHERE e.is_eligible = true
  AND NOT EXISTS (
    SELECT 1 FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH f
    WHERE f.roll_up_shop = e.roll_up_shop
      AND f.forecast_run_id = '<LATEST_FORECAST_RUN_ID>'
  );
```

#### Issue 2: Forecasts are all zero or very low

**Symptom**: revenue_forecast values are near zero

**Causes**:
1. Backtest table is empty (deleted accidentally)
2. Champion model_run_id has no backtest data
3. Lag-12 fallback has no data (new PCs)

**Diagnosis**:
```sql
-- Check backtest data for champion model
SELECT 
  model_run_id,
  count(*) as row_count,
  avg(y_pred) as avg_pred,
  min(y_pred) as min_pred,
  max(y_pred) as max_pred
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS
WHERE model_run_id = (
  SELECT model_run_id FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_CHAMPIONS
  WHERE champion_scope = 'GLOBAL'
  ORDER BY selected_at DESC LIMIT 1
)
GROUP BY model_run_id;
-- Expected: row_count > 10000, avg_pred > $10000
```

#### Issue 3: Month-over-month forecast variance >50%

**Symptom**: February forecast for April is 50%+ different from January forecast for April

**Causes**:
1. Champion model changed (new model_run_id)
2. Actuals data revised (February anchor has revised January actuals)
3. Model drift or data quality issue

**Diagnosis**:
```sql
-- Check if champion changed
SELECT asof_fiscal_yyyymm, model_run_id, selected_at
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_CHAMPIONS
WHERE champion_scope = 'GLOBAL'
ORDER BY asof_fiscal_yyyymm DESC
LIMIT 3;

-- Compare actuals used in each forecast run
SELECT fiscal_yyyymm, total_revenue
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_ACTUALS_PC_REASON_MTH
WHERE roll_up_shop = '555'
  AND reason_group = 'Commercial'
  AND fiscal_yyyymm >= 202501
ORDER BY fiscal_yyyymm DESC;
```

---

## Data Retention & Archival

### Retention Policies

| Table | Retention | Archival | Rationale |
|-------|-----------|----------|-----------|
| FORECAST_MODEL_BACKTEST_PREDICTIONS | Indefinite | None | Need historical backtest for model comparison |
| FORECAST_MODEL_RUNS | Indefinite | None | Compliance: audit trail of all models |
| FORECAST_MODEL_CHAMPIONS | Indefinite | None | Compliance: champion selection history |
| FORECAST_OUTPUT_PC_REASON_MTH | 24 months | Archive older to _ARCHIVE table | Active consumption for last 2 years |
| FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT | 7 years | Cold storage after 7 years | Compliance: financial audit requirement |
| FORECAST_ACTUALS_PC_REASON_MTH | Indefinite | None | Source of truth for actuals |

### Cleanup Scripts

```sql
-- Archive forecasts older than 24 months (run annually)
INSERT INTO DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH_ARCHIVE
SELECT * 
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
WHERE asof_fiscal_yyyymm < DATE_PART('year', DATEADD('month', -24, CURRENT_DATE())) * 100
                         + DATE_PART('month', DATEADD('month', -24, CURRENT_DATE()));

DELETE FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
WHERE asof_fiscal_yyyymm < DATE_PART('year', DATEADD('month', -24, CURRENT_DATE())) * 100
                         + DATE_PART('month', DATEADD('month', -24, CURRENT_DATE()));
```

---

## FAQs

**Q: Why is there no FORECAST_MODEL_PREDICTIONS table?**  
A: All predictions (backtest AND published) are stored in `FORECAST_MODEL_BACKTEST_PREDICTIONS`. Published forecasts are the rows with champion model_run_id and most recent anchor.

**Q: How do I get "next month" forecast only?**  
A: Filter for `horizon = 1` in the view or output table.

**Q: Can I compare forecasts from two different months for the same target month?**  
A: Yes, use `FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT` and compare rows with different `snapshot_month` but same `target_fiscal_yyyymm`.

**Q: What happens if actuals are revised after forecast is published?**  
A: Forecast is NOT automatically updated. Next month's forecast will use the revised actuals as anchor baseline.

**Q: How do I know which model_run_id is the champion?**  
A: Query `FORECAST_MODEL_CHAMPIONS` for `champion_scope = 'GLOBAL'` and latest `asof_fiscal_yyyymm`.

**Q: Can I override the champion model for a specific PC?**  
A: Currently: No override table exists. Must update `FORECAST_MODEL_CHAMPIONS` with `champion_scope = 'PC_REASON'` row.  
Future: Implement `FORECAST_MODEL_OVERRIDES` table (see Risk Matrix R5).

---

## Contact & Support

**Data Science Team**: Arthur, Merlin  
**Slack Channel**: #revenue-forecasting  
**Documentation**: [GitHub repo](https://github.com/WaynezWorld/revenue_forecast)  
**Snowflake Database**: DB_BI_P_SANDBOX.SANDBOX  

**Escalation**:
- Data issues: Contact Data Engineering
- Model questions: Contact Data Science
- Business questions: Contact Finance (Wayne)
