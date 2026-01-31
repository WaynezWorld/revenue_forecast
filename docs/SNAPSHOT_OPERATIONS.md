# Monthly Forecast Snapshot Operations Guide

**Document Version**: 1.0  
**Last Updated**: 2026-01-31  
**Owner**: Data Science & Analytics Team  
**Related PR**: infra/snapshot-forecasts-monthly  
**Fixes**: Risk R2 (HIGH) from production alignment audit  

---

## Purpose

This document provides step-by-step instructions for creating monthly snapshots of published revenue forecasts.

**Why Snapshots?**
- **Audit Trail**: Compliance requirement for financial forecasts (7-year retention)
- **Forecast vs Actuals**: Compare published forecasts to actual results after month close
- **Rollback**: Restore accidentally deleted/overwritten forecasts
- **Historical Analysis**: Track forecast accuracy over time

---

## Quick Start

### Compute UTC Timestamp

**In PowerShell**:
```powershell
python -c "from datetime import datetime; print(datetime.utcnow().strftime('%Y%m%d%H'))"
```

**In Bash**:
```bash
date -u +%Y%m%d%H
```

**Example Output**: `2026013112` (January 31, 2026 at 12:00 UTC)

### Create Snapshot

1. Open [sql/archival/create_snapshot_forecast_monthly.sql](../sql/archival/create_snapshot_forecast_monthly.sql)
2. Replace placeholders:
   - `:TS` → UTC timestamp (e.g., `2026013112`)
   - `:ANCHOR_FISCAL_YYYYMM` → Anchor month (e.g., `202601`)
3. Execute in Snowflake (role: `SNFL_PRD_BI_POWERUSER_FR`, warehouse: `BI_P_QRY_FIN_OPT_WH`)

**Result**: New table `FORECAST_MODEL_PREDICTIONS_SNAPSHOT_2026013112` created with ~2,400 rows

---

## Monthly Workflow

### Timing

**Recommended Schedule**: Run on **1st business day of each month** (after forecast scoring)

**Example**:
- Forecast Run Date: February 5, 2026
- Anchor Month: `202601` (January 2026)
- Snapshot Created: `FORECAST_MODEL_PREDICTIONS_SNAPSHOT_2026020510` (Feb 5, 10am UTC)
- Forecast Months: `202602` through `202701` (Feb 2026 - Jan 2027)

### Step-by-Step Procedure

#### 1. Pre-Snapshot Checks

```sql
-- Verify scoring completed successfully
SELECT 
  forecast_run_id,
  asof_fiscal_yyyymm,
  forecast_created_at,
  COUNT(*) as row_count
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
WHERE asof_fiscal_yyyymm = 202601
GROUP BY 1, 2, 3
ORDER BY forecast_created_at DESC
LIMIT 1;
-- Expected: 1 row with row_count ~2,400

-- Verify champion model exists
SELECT *
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_CHAMPIONS
WHERE champion_scope = 'GLOBAL'
  AND asof_fiscal_yyyymm = 202601;
-- Expected: 1 row with model_run_id
```

#### 2. Create Snapshot

See [Quick Start](#quick-start) above.

#### 3. Post-Snapshot Verification

```sql
-- Verify row counts
SELECT 
  COUNT(*) AS total_rows,
  COUNT(DISTINCT pc) AS unique_pcs,
  COUNT(DISTINCT reason_group) AS unique_reasons,
  COUNT(DISTINCT horizon) AS unique_horizons,
  MIN(forecast_month) AS min_forecast_month,
  MAX(forecast_month) AS max_forecast_month,
  SUM(revenue_forecast) AS total_forecasted_revenue
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_PREDICTIONS_SNAPSHOT_2026013112;

-- Expected:
--   total_rows: ~2,400
--   unique_pcs: ~67
--   unique_reasons: ~3
--   unique_horizons: 12
--   min_forecast_month: 202602 (anchor+1)
--   max_forecast_month: 202701 (anchor+12)

-- Sample top 10 PCs by revenue (H=1 forecast)
SELECT 
  pc,
  reason_group,
  forecast_month,
  revenue_forecast,
  model_name
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_PREDICTIONS_SNAPSHOT_2026013112
WHERE horizon = 1
ORDER BY revenue_forecast DESC
LIMIT 10;
```

#### 4. Log Snapshot Metadata

**Manual Logging** (until FORECAST_SNAPSHOT_LOG table created):

```sql
-- Record snapshot in tracking spreadsheet or Confluence page:
--   Snapshot Table: FORECAST_MODEL_PREDICTIONS_SNAPSHOT_2026013112
--   Anchor Month: 202601
--   Created By: <your_name>
--   Created At: 2026-01-31 12:00 UTC
--   Row Count: 2,412
--   Notes: Monthly forecast for FY2026
```

---

## Snapshot Table Naming Convention

### Recommended: Hourly Timestamps (`_YYYYMMDDHH`)

**Format**: `FORECAST_MODEL_PREDICTIONS_SNAPSHOT_{YYYYMMDDHH}`  
**Example**: `FORECAST_MODEL_PREDICTIONS_SNAPSHOT_2026013112`

**Advantages**:
- Guarantees uniqueness (no accidental overwrites)
- Supports multiple snapshots per day (e.g., re-runs)
- Easier to trace exact execution time

### Alternative: Monthly Timestamps (`_YYYYMM`)

**Format**: `FORECAST_MODEL_PREDICTIONS_SNAPSHOT_{YYYYMM}`  
**Example**: `FORECAST_MODEL_PREDICTIONS_SNAPSHOT_202601`

**Advantages**:
- Simpler naming (one snapshot per month)
- Easier for humans to remember

**Disadvantages**:
- Requires `DROP TABLE` or `CREATE OR REPLACE` on re-runs
- Cannot track multiple versions per month

---

## Forecast vs Actuals Comparison

### Monthly Close Reconciliation

After actuals are loaded for a month, compare forecasted vs actual revenue:

```sql
WITH forecast_snapshot AS (
  SELECT 
    pc,
    reason_group,
    forecast_month,
    revenue_forecast
  FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_PREDICTIONS_SNAPSHOT_2025123112
  -- Use snapshot from PRIOR month (forecasted in Dec for Jan)
  WHERE horizon = 1  -- H=1 (next month forecast)
    AND forecast_month = 202601  -- January 2026
),
actuals AS (
  SELECT 
    roll_up_shop AS pc,
    reason_group,
    fiscal_yyyymm AS forecast_month,
    total_revenue AS actual_revenue
  FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_ACTUALS_PC_REASON_MTH
  WHERE fiscal_yyyymm = 202601  -- January 2026 actuals
)
SELECT 
  f.pc,
  f.reason_group,
  f.forecast_month,
  f.revenue_forecast,
  a.actual_revenue,
  (a.actual_revenue - f.revenue_forecast) AS variance,
  (a.actual_revenue - f.revenue_forecast) / NULLIF(f.revenue_forecast, 0) * 100 AS variance_pct,
  CASE 
    WHEN ABS((a.actual_revenue - f.revenue_forecast) / NULLIF(f.revenue_forecast, 0)) < 0.05 THEN '✅ <5%'
    WHEN ABS((a.actual_revenue - f.revenue_forecast) / NULLIF(f.revenue_forecast, 0)) < 0.10 THEN '⚠️ 5-10%'
    ELSE '❌ >10%'
  END AS accuracy
FROM forecast_snapshot f
INNER JOIN actuals a
  ON a.pc = f.pc
 AND a.reason_group = f.reason_group
 AND a.forecast_month = f.forecast_month
ORDER BY ABS(variance) DESC
LIMIT 50;
```

### Accuracy Metrics (Company-Wide)

```sql
WITH fcst_vs_actuals AS (
  SELECT 
    f.forecast_month,
    SUM(f.revenue_forecast) AS total_forecast,
    SUM(a.total_revenue) AS total_actual,
    SUM(ABS(a.total_revenue - f.revenue_forecast)) AS total_abs_error
  FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_PREDICTIONS_SNAPSHOT_2025123112 f
  INNER JOIN DB_BI_P_SANDBOX.SANDBOX.FORECAST_ACTUALS_PC_REASON_MTH a
    ON a.roll_up_shop = f.pc
   AND a.reason_group = f.reason_group
   AND a.fiscal_yyyymm = f.forecast_month
  WHERE f.horizon = 1
    AND f.forecast_month = 202601
  GROUP BY 1
)
SELECT 
  forecast_month,
  total_actual,
  total_forecast,
  (total_forecast - total_actual) / NULLIF(total_actual, 0) * 100 AS bias_pct,
  total_abs_error / NULLIF(total_actual, 0) * 100 AS wape_pct
FROM fcst_vs_actuals;

-- Expected:
--   bias_pct: -5% to +5% (forecast close to actual)
--   wape_pct: <10% (good accuracy)
```

---

## Snapshot Management

### List All Snapshots

```sql
SHOW TABLES LIKE 'FORECAST_MODEL_PREDICTIONS_SNAPSHOT_%' 
IN DB_BI_P_SANDBOX.SANDBOX;
```

### View Snapshot Table Sizes

```sql
SELECT 
  table_name,
  row_count,
  ROUND(bytes / 1024 / 1024, 2) AS size_mb,
  created,
  DATEDIFF('day', created, CURRENT_TIMESTAMP()) AS age_days
FROM DB_BI_P_SANDBOX.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'SANDBOX'
  AND table_name LIKE 'FORECAST_MODEL_PREDICTIONS_SNAPSHOT_%'
ORDER BY created DESC;
```

### Cleanup Old Snapshots

**Retention Policy**:
- **0-90 days**: Keep in hot storage (Snowflake table)
- **90 days - 2 years**: Move to cold storage (external S3/Azure)
- **2-7 years**: Archive to compliance storage
- **>7 years**: Drop (financial audit retention met)

**Archive to External Stage** (requires DBA setup):

```sql
-- Example: Archive snapshots older than 2 years
COPY INTO @FORECAST_ARCHIVE_STAGE/snapshots/FORECAST_MODEL_PREDICTIONS_SNAPSHOT_2024013112.parquet
FROM (
  SELECT * 
  FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_PREDICTIONS_SNAPSHOT_2024013112
)
FILE_FORMAT = (TYPE = PARQUET COMPRESSION = SNAPPY);

-- Verify archive succeeded, then drop table
DROP TABLE DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_PREDICTIONS_SNAPSHOT_2024013112;
```

---

## Rollback & Recovery

### Scenario: Accidentally Deleted Published Forecasts

**Problem**: FORECAST_OUTPUT_PC_REASON_MTH table was truncated or deleted

**Solution**: Restore from most recent snapshot

```sql
-- 1. Verify snapshot exists and has correct data
SELECT COUNT(*) 
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_PREDICTIONS_SNAPSHOT_2026013112;
-- Expected: ~2,400 rows

-- 2. Restore forecasts to output table
INSERT INTO DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
(
  model_run_id,
  forecast_run_id,
  asof_fiscal_yyyymm,
  roll_up_shop,
  reason_group,
  horizon,
  target_fiscal_yyyymm,
  revenue_forecast,
  revenue_forecast_lo,
  revenue_forecast_hi,
  forecast_created_at
)
SELECT 
  model_run_id,
  '<SNAPSHOT_RESTORE_' || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MI') || '>' AS forecast_run_id,
  anchor_fiscal_yyyymm AS asof_fiscal_yyyymm,
  pc AS roll_up_shop,
  reason_group,
  horizon,
  forecast_month AS target_fiscal_yyyymm,
  revenue_forecast,
  revenue_forecast_lo,
  revenue_forecast_hi,
  CURRENT_TIMESTAMP() AS forecast_created_at
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_PREDICTIONS_SNAPSHOT_2026013112;

-- 3. Verify restoration
SELECT 
  forecast_run_id,
  COUNT(*) as row_count
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
WHERE forecast_run_id LIKE 'SNAPSHOT_RESTORE%'
GROUP BY 1;
```

---

## Automation (Future Enhancement)

### Snowflake TASK Template

A Snowflake TASK template is provided in [sql/archival/create_snapshot_forecast_monthly.sql](../sql/archival/create_snapshot_forecast_monthly.sql) (commented).

**Requirements for Automation**:
1. DBA review and approval
2. Service account ownership (not personal user)
3. Error handling and alerting configured
4. Idempotency (handle re-runs gracefully)
5. Log table created: `FORECAST_SNAPSHOT_LOG`, `FORECAST_SNAPSHOT_ERRORS`

**DO NOT deploy TASK without DBA review.**

---

## Troubleshooting

### Issue 1: Snapshot Has 0 Rows

**Symptom**: `CREATE TABLE ... AS SELECT` completes but snapshot table is empty

**Causes**:
1. Champion model not set for anchor month
2. Anchor month not in backtest predictions table
3. Wrong `asof_fiscal_yyyymm` parameter

**Diagnosis**:
```sql
-- Check if champion exists for anchor month
SELECT *
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_CHAMPIONS
WHERE champion_scope = 'GLOBAL'
  AND asof_fiscal_yyyymm = 202601;

-- Check if backtest predictions exist
SELECT 
  anchor_fiscal_yyyymm,
  model_run_id,
  COUNT(*) as row_count
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS
WHERE anchor_fiscal_yyyymm = 202601
GROUP BY 1, 2;
```

### Issue 2: Row Count Mismatch

**Symptom**: Snapshot has 1,800 rows instead of expected 2,400

**Causes**:
1. Some PCs are ineligible (filtered by `FORECAST_PC_ELIGIBILITY`)
2. Customer mix allocation missing for some series
3. Horizons filtered incorrectly

**Diagnosis**:
```sql
-- Compare expected vs actual
SELECT 
  'EXPECTED' as source,
  COUNT(DISTINCT roll_up_shop) * 3 * 12 AS expected_rows
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_PC_ELIGIBILITY
WHERE is_eligible = TRUE

UNION ALL

SELECT 
  'ACTUAL' as source,
  COUNT(*) as actual_rows
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_PREDICTIONS_SNAPSHOT_2026013112;
```

---

## Contact & Support

**Data Science Team**: Arthur, Merlin  
**Slack Channel**: #revenue-forecasting  
**Documentation**: [GitHub repo](https://github.com/WaynezWorld/revenue_forecast)  

**Escalation**:
- Snapshot issues: Contact Data Science
- Snowflake permissions: Contact DBA team
- Compliance questions: Contact Finance (Wayne)
