-- ═══════════════════════════════════════════════════════════════════════════════
-- SAFE BACKTEST DELETE WITH TIMESTAMPED SNAPSHOT
-- ═══════════════════════════════════════════════════════════════════════════════
-- 
-- PURPOSE:
--   Safely delete backtest predictions by model_run_id with automatic timestamped backup
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
--      :TS                              → UTC timestamp (e.g., 2026013112 for Jan 31, 2026 12:00 UTC)
--      :TARGET_MODEL_RUN_IDS_COMMA_SEPARATED → Comma-separated model_run_ids (e.g., 'abc-123', 'def-456')
--
--   3. Review backup row count BEFORE running DELETE
--   4. Execute in Snowflake with role SNFL_PRD_BI_POWERUSER_FR
--
-- EXAMPLE:
--   -- Set session parameters
--   USE ROLE SNFL_PRD_BI_POWERUSER_FR;
--   USE WAREHOUSE BI_P_QRY_FIN_OPT_WH;
--   USE DATABASE DB_BI_P_SANDBOX;
--   USE SCHEMA SANDBOX;
--
--   -- Replace :TS with 2026013112 and :TARGET_MODEL_RUN_IDS_COMMA_SEPARATED with:
--   -- 'abc-123-def', 'xyz-789-ghi'
--
-- SAFETY:
--   - Backup table is timestamped and never overwritten (append-only)
--   - Row count verification step prevents silent failures
--   - DELETE is guarded by WHERE model_run_id IN (...)
--   - Rollback plan provided
--
-- ROLLBACK:
--   If deletion was accidental, restore from backup:
--   
--   INSERT INTO DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS
--   SELECT * FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS_BACKUP_{TS};
--
-- AUTHOR: Data Science Team
-- CREATED: 2026-01-31
-- RELATED: hotfix/backtest-delete-safeguard PR
-- ═══════════════════════════════════════════════════════════════════════════════

-- ───────────────────────────────────────────────────────────────────────────────
-- STEP 1: CREATE TIMESTAMPED BACKUP TABLE (CTAS)
-- ───────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS_BACKUP_:TS AS
SELECT * 
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS
WHERE model_run_id IN (:TARGET_MODEL_RUN_IDS_COMMA_SEPARATED)
;

-- Expected: Statement executed successfully. X rows inserted.

-- ───────────────────────────────────────────────────────────────────────────────
-- STEP 2: VERIFY ROW COUNTS (MANUAL REVIEW REQUIRED)
-- ───────────────────────────────────────────────────────────────────────────────

SELECT 
  (SELECT COUNT(*) 
   FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS_BACKUP_:TS) 
   AS backup_count,
   
  (SELECT COUNT(*) 
   FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS 
   WHERE model_run_id IN (:TARGET_MODEL_RUN_IDS_COMMA_SEPARATED)) 
   AS to_delete_count,
   
  CASE 
    WHEN backup_count = to_delete_count THEN '✅ SAFE TO PROCEED'
    ELSE '❌ COUNTS MISMATCH - DO NOT DELETE'
  END AS safety_check
;

-- Expected: backup_count = to_delete_count (both should match)
-- If counts don't match, STOP and investigate discrepancy

-- ───────────────────────────────────────────────────────────────────────────────
-- STEP 3: SAMPLE BACKUP DATA (OPTIONAL VALIDATION)
-- ───────────────────────────────────────────────────────────────────────────────

SELECT 
  model_run_id,
  roll_up_shop,
  reason_group,
  anchor_fiscal_yyyymm,
  horizon,
  target_fiscal_yyyymm,
  COUNT(*) as row_count
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS_BACKUP_:TS
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY model_run_id, anchor_fiscal_yyyymm, roll_up_shop, horizon
LIMIT 50
;

-- Expected: Rows grouped by model_run_id and key dimensions

-- ───────────────────────────────────────────────────────────────────────────────
-- STEP 4: DELETE (ONLY AFTER MANUAL CONFIRMATION)
-- ───────────────────────────────────────────────────────────────────────────────
-- 
-- ⚠️  STOP: Review STEP 2 results before proceeding
-- ⚠️  Confirm backup_count = to_delete_count
-- ⚠️  Verify backup table exists and has expected data
-- 
-- If all checks pass, execute DELETE below:

DELETE FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS
WHERE model_run_id IN (:TARGET_MODEL_RUN_IDS_COMMA_SEPARATED)
;

-- Expected: X number of rows deleted (should match backup_count from STEP 2)

-- ───────────────────────────────────────────────────────────────────────────────
-- STEP 5: POST-DELETE VERIFICATION
-- ───────────────────────────────────────────────────────────────────────────────

-- Verify no rows remain for deleted model_run_ids
SELECT 
  model_run_id,
  COUNT(*) as remaining_rows
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS
WHERE model_run_id IN (:TARGET_MODEL_RUN_IDS_COMMA_SEPARATED)
GROUP BY model_run_id
;

-- Expected: 0 rows returned (all deleted)
-- If any rows returned, DELETE failed partially - investigate

-- Verify backup integrity
SELECT 
  'BACKUP' as source,
  COUNT(*) as row_count,
  COUNT(DISTINCT model_run_id) as model_run_count
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS_BACKUP_:TS

UNION ALL

SELECT 
  'CURRENT' as source,
  COUNT(*) as row_count,
  COUNT(DISTINCT model_run_id) as model_run_count
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS
;

-- Expected: BACKUP shows deleted rows, CURRENT shows remaining rows

-- ═══════════════════════════════════════════════════════════════════════════════
-- CLEANUP (OPTIONAL - Run after confirming new backtest results are valid)
-- ═══════════════════════════════════════════════════════════════════════════════

-- Drop backup table after 90 days or when no longer needed for rollback
-- DROP TABLE IF EXISTS DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS_BACKUP_:TS;

-- ═══════════════════════════════════════════════════════════════════════════════
-- RETENTION POLICY FOR BACKUP TABLES
-- ═══════════════════════════════════════════════════════════════════════════════
-- 
-- Backup tables follow naming convention: FORECAST_MODEL_BACKTEST_PREDICTIONS_BACKUP_{YYYYMMDDHH}
-- 
-- Recommended retention:
--   - 7 days:  Keep for immediate rollback (hot storage)
--   - 30 days: Move to cold storage or external archive
--   - 90 days: Drop permanently (only if new backtest validated)
-- 
-- To list all backup tables:
-- 
-- SHOW TABLES LIKE 'FORECAST_MODEL_BACKTEST_PREDICTIONS_BACKUP_%' 
-- IN DB_BI_P_SANDBOX.SANDBOX;
-- 
-- To archive old backups (example: backup tables older than 30 days):
-- 
-- SELECT 
--   table_name,
--   created,
--   row_count,
--   bytes / 1024 / 1024 as size_mb
-- FROM DB_BI_P_SANDBOX.INFORMATION_SCHEMA.TABLES
-- WHERE table_schema = 'SANDBOX'
--   AND table_name LIKE 'FORECAST_MODEL_BACKTEST_PREDICTIONS_BACKUP_%'
--   AND created < DATEADD(day, -30, CURRENT_TIMESTAMP())
-- ORDER BY created DESC
-- ;
-- 
-- ═══════════════════════════════════════════════════════════════════════════════
