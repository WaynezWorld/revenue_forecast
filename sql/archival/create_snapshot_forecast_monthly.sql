-- ═══════════════════════════════════════════════════════════════════════════════
-- MONTHLY FORECAST SNAPSHOT WITH TIMESTAMPED TABLES
-- ═══════════════════════════════════════════════════════════════════════════════
-- 
-- PURPOSE:
--   Create timestamped snapshot of published forecasts for audit trail and compliance
--
-- USAGE:
--   1. Compute UTC timestamp:
--      # In PowerShell:
--      python -c "from datetime import datetime; print(datetime.utcnow().strftime('%Y%m%d%H'))"
--      
--      # In Bash:
--      date -u +%Y%m%d%H
--
--   2. Replace placeholders:
--      :TS                          → UTC timestamp (e.g., 2026013112 for Jan 31, 2026 12:00 UTC)
--      :ANCHOR_FISCAL_YYYYMM        → Anchor month to snapshot (e.g., 202601 for January 2026)
--
--   3. Execute in Snowflake with role SNFL_PRD_BI_POWERUSER_FR
--
-- NAMING CONVENTIONS:
--   - Hourly snapshots:  FORECAST_MODEL_PREDICTIONS_SNAPSHOT_{YYYYMMDDHH}
--   - Monthly snapshots: FORECAST_MODEL_PREDICTIONS_SNAPSHOT_{YYYYMM} (alternative)
--
--   Recommendation: Use hourly (_YYYYMMDDHH) for most workflows to guarantee uniqueness
--
-- EXAMPLE:
--   -- Set session parameters
--   USE ROLE SNFL_PRD_BI_POWERUSER_FR;
--   USE WAREHOUSE BI_P_QRY_FIN_OPT_WH;
--   USE DATABASE DB_BI_P_SANDBOX;
--   USE SCHEMA SANDBOX;
--
--   -- Replace :TS with 2026013112 and :ANCHOR_FISCAL_YYYYMM with 202601
--
-- RETENTION:
--   - Keep for 7 years (financial audit compliance)
--   - Move to cold storage after 2 years
--
-- ROLLBACK:
--   If published forecasts were accidentally overwritten, restore from snapshot:
--   
--   INSERT INTO DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
--   SELECT * FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_PREDICTIONS_SNAPSHOT_{TS}
--   WHERE forecast_month >= 202602;  -- Only restore specific months
--
-- AUTHOR: Data Science Team
-- CREATED: 2026-01-31
-- RELATED: infra/snapshot-forecasts-monthly PR
-- FIXES: Risk R2 (HIGH) from production alignment audit
-- ═══════════════════════════════════════════════════════════════════════════════

-- ───────────────────────────────────────────────────────────────────────────────
-- OPTION A: Timestamped Snapshot (RECOMMENDED - Guarantees Uniqueness)
-- ───────────────────────────────────────────────────────────────────────────────

CREATE TABLE DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_PREDICTIONS_SNAPSHOT_:TS AS
SELECT 
  -- Model metadata
  c.model_run_id,
  mr.model_name,
  mr.model_family,
  
  -- Time dimensions
  p.anchor_fiscal_yyyymm,
  p.horizon,
  p.target_fiscal_yyyymm AS forecast_month,
  fd.fiscal_year AS forecast_fiscal_year,
  fd.fiscal_month AS forecast_fiscal_month_num,
  
  -- Business dimensions
  p.roll_up_shop AS pc,
  p.reason_group,
  
  -- Predictions
  ROUND(p.y_pred, 2) AS revenue_forecast,
  ROUND(p.y_pred_lo, 2) AS revenue_forecast_lo,
  ROUND(p.y_pred_hi, 2) AS revenue_forecast_hi,
  
  -- Metadata
  p.created_at AS prediction_created_at,
  CURRENT_TIMESTAMP() AS snapshot_created_at,
  :ANCHOR_FISCAL_YYYYMM AS snapshot_month
  
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS p

-- Join to champion model (filter for published forecasts only)
INNER JOIN DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_CHAMPIONS c
  ON c.champion_scope = 'GLOBAL'
 AND c.asof_fiscal_yyyymm = :ANCHOR_FISCAL_YYYYMM
 AND p.model_run_id = c.model_run_id

-- Join to model runs metadata
LEFT JOIN DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS mr
  ON mr.model_run_id = p.model_run_id

-- Join to fiscal month dimension for date details
LEFT JOIN DB_BI_P_SANDBOX.SANDBOX.FORECAST_FISCAL_MONTH_DIM fd
  ON fd.fiscal_yyyymm = p.target_fiscal_yyyymm

WHERE p.anchor_fiscal_yyyymm = :ANCHOR_FISCAL_YYYYMM
  AND p.horizon BETWEEN 1 AND 12
;

-- Expected: X rows inserted (typically ~2,400 for 67 PCs × 3 reasons × 12 horizons)

-- ───────────────────────────────────────────────────────────────────────────────
-- OPTION B: Monthly Snapshot (Simpler Naming, May Conflict on Re-runs)
-- ───────────────────────────────────────────────────────────────────────────────

-- Uncomment below if you prefer YYYYMM naming (less granular, may need DROP TABLE first)

/*
CREATE OR REPLACE TABLE DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_PREDICTIONS_SNAPSHOT_:ANCHOR_FISCAL_YYYYMM AS
SELECT 
  -- [Same columns as Option A]
FROM ...
WHERE ...
;
*/

-- ───────────────────────────────────────────────────────────────────────────────
-- POST-SNAPSHOT VERIFICATION
-- ───────────────────────────────────────────────────────────────────────────────

-- Verify row count
SELECT 
  COUNT(*) AS total_rows,
  COUNT(DISTINCT pc) AS unique_pcs,
  COUNT(DISTINCT reason_group) AS unique_reasons,
  COUNT(DISTINCT horizon) AS unique_horizons,
  MIN(forecast_month) AS min_forecast_month,
  MAX(forecast_month) AS max_forecast_month,
  SUM(revenue_forecast) AS total_forecasted_revenue
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_PREDICTIONS_SNAPSHOT_:TS
;

-- Expected:
--   total_rows: ~2,400
--   unique_pcs: ~67
--   unique_reasons: ~3
--   unique_horizons: 12
--   min_forecast_month: (anchor+1)
--   max_forecast_month: (anchor+12)

-- Sample rows for top 10 PCs by revenue
SELECT 
  pc,
  reason_group,
  forecast_month,
  revenue_forecast,
  model_name
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_PREDICTIONS_SNAPSHOT_:TS
WHERE horizon = 1  -- Next month
ORDER BY revenue_forecast DESC
LIMIT 10
;

-- ═══════════════════════════════════════════════════════════════════════════════
-- OPTIONAL: Snowflake TASK for Automated Monthly Snapshots
-- ═══════════════════════════════════════════════════════════════════════════════
-- 
-- **IMPORTANT**: This TASK is provided as a TEMPLATE only.
-- DO NOT execute automatically. DBA must review and customize before deployment.
-- 
-- Requirements:
--   1. TASK must be owned by a service account (not personal user)
--   2. Warehouse must be appropriately sized for snapshot volume
--   3. Error handling and alerting must be configured
--   4. Snapshot naming strategy must handle re-runs (idempotency)
-- 
-- Template TASK (COMMENTED - Requires DBA Review):

/*
CREATE OR REPLACE TASK DB_BI_P_SANDBOX.SANDBOX.TASK_SNAPSHOT_MONTHLY_FORECASTS
  WAREHOUSE = BI_P_QRY_FIN_OPT_WH
  SCHEDULE = 'USING CRON 0 2 1 * * America/New_York'  -- 2am on 1st of month
  COMMENT = 'Automated monthly snapshot of published forecasts. Owner: Data Science Team. Contact: #revenue-forecasting'
AS
DECLARE
  ts VARCHAR;
  anchor_month VARCHAR;
  snapshot_table VARCHAR;
BEGIN
  -- Compute UTC timestamp and anchor month
  ts := TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24');
  anchor_month := TO_CHAR(DATEADD('month', -1, CURRENT_DATE()), 'YYYYMM');  -- Previous month
  snapshot_table := 'FORECAST_MODEL_PREDICTIONS_SNAPSHOT_' || ts;
  
  -- Create snapshot via dynamic SQL
  EXECUTE IMMEDIATE '
    CREATE TABLE DB_BI_P_SANDBOX.SANDBOX.' || snapshot_table || ' AS
    SELECT 
      c.model_run_id,
      mr.model_name,
      p.anchor_fiscal_yyyymm,
      p.horizon,
      p.target_fiscal_yyyymm AS forecast_month,
      p.roll_up_shop AS pc,
      p.reason_group,
      ROUND(p.y_pred, 2) AS revenue_forecast,
      CURRENT_TIMESTAMP() AS snapshot_created_at,
      ' || anchor_month || ' AS snapshot_month
    FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS p
    INNER JOIN DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_CHAMPIONS c
      ON c.champion_scope = ''GLOBAL''
     AND c.asof_fiscal_yyyymm = ' || anchor_month || '
     AND p.model_run_id = c.model_run_id
    LEFT JOIN DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS mr
      ON mr.model_run_id = p.model_run_id
    WHERE p.anchor_fiscal_yyyymm = ' || anchor_month || '
      AND p.horizon BETWEEN 1 AND 12
  ';
  
  -- Log completion
  INSERT INTO DB_BI_P_SANDBOX.SANDBOX.FORECAST_SNAPSHOT_LOG (snapshot_table, anchor_month, created_at, row_count)
  SELECT 
    :snapshot_table,
    :anchor_month,
    CURRENT_TIMESTAMP(),
    (SELECT COUNT(*) FROM IDENTIFIER(:snapshot_table))
  ;
  
  RETURN 'SUCCESS: Snapshot created - ' || snapshot_table;
EXCEPTION
  WHEN OTHER THEN
    -- Error handling: Log to error table and re-raise
    INSERT INTO DB_BI_P_SANDBOX.SANDBOX.FORECAST_SNAPSHOT_ERRORS (task_name, error_message, error_at)
    VALUES ('TASK_SNAPSHOT_MONTHLY_FORECASTS', SQLERRM, CURRENT_TIMESTAMP());
    
    RAISE;
END;
*/

-- Activate TASK (after DBA review):
-- ALTER TASK DB_BI_P_SANDBOX.SANDBOX.TASK_SNAPSHOT_MONTHLY_FORECASTS RESUME;

-- Monitor TASK execution:
-- SELECT *
-- FROM TABLE(DB_BI_P_SANDBOX.INFORMATION_SCHEMA.TASK_HISTORY())
-- WHERE NAME = 'TASK_SNAPSHOT_MONTHLY_FORECASTS'
-- ORDER BY SCHEDULED_TIME DESC
-- LIMIT 10;

-- ═══════════════════════════════════════════════════════════════════════════════
-- SNAPSHOT TABLE MANAGEMENT
-- ═══════════════════════════════════════════════════════════════════════════════

-- List all snapshot tables
SHOW TABLES LIKE 'FORECAST_MODEL_PREDICTIONS_SNAPSHOT_%' 
IN DB_BI_P_SANDBOX.SANDBOX;

-- View snapshot table sizes
SELECT 
  table_name,
  row_count,
  bytes / 1024 / 1024 AS size_mb,
  created,
  DATEDIFF('day', created, CURRENT_TIMESTAMP()) AS age_days
FROM DB_BI_P_SANDBOX.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'SANDBOX'
  AND table_name LIKE 'FORECAST_MODEL_PREDICTIONS_SNAPSHOT_%'
ORDER BY created DESC
;

-- Archive old snapshots to cold storage (example: >2 years old)
-- (Requires EXTERNAL STAGE configured by DBA)

/*
COPY INTO @FORECAST_ARCHIVE_STAGE/snapshots/
FROM (
  SELECT * 
  FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_PREDICTIONS_SNAPSHOT_{OLD_TS}
)
FILE_FORMAT = (TYPE = PARQUET COMPRESSION = SNAPPY)
;

-- Drop archived table after verification
DROP TABLE DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_PREDICTIONS_SNAPSHOT_{OLD_TS};
*/

-- ═══════════════════════════════════════════════════════════════════════════════
-- COMPLIANCE & AUDIT TRAIL
-- ═══════════════════════════════════════════════════════════════════════════════

-- Query to demonstrate forecast vs actuals comparison (for monthly close)

/*
WITH forecast_snapshot AS (
  SELECT 
    pc,
    reason_group,
    forecast_month,
    revenue_forecast
  FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_PREDICTIONS_SNAPSHOT_{PRIOR_MONTH_TS}
  WHERE horizon = 1  -- H=1 forecast (next month)
),
actuals AS (
  SELECT 
    roll_up_shop AS pc,
    reason_group,
    fiscal_yyyymm AS forecast_month,
    total_revenue AS actual_revenue
  FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_ACTUALS_PC_REASON_MTH
  WHERE fiscal_yyyymm = {ACTUAL_MONTH}
)
SELECT 
  f.pc,
  f.reason_group,
  f.forecast_month,
  f.revenue_forecast,
  a.actual_revenue,
  (a.actual_revenue - f.revenue_forecast) AS variance,
  (a.actual_revenue - f.revenue_forecast) / NULLIF(f.revenue_forecast, 0) * 100 AS variance_pct
FROM forecast_snapshot f
INNER JOIN actuals a
  ON a.pc = f.pc
 AND a.reason_group = f.reason_group
 AND a.forecast_month = f.forecast_month
ORDER BY ABS(variance) DESC
LIMIT 50
;
*/

-- ═══════════════════════════════════════════════════════════════════════════════
