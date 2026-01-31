# Ridge Archive and Removal Documentation

**Date**: January 31, 2026  
**Executed by**: Wayne (WaynezWorld)  
**Repository**: WaynezWorld/revenue_forecast  
**Branch**: remove-ridge-candidate (commit 7a296dc)

## Executive Summary

The Ridge regression model (`RIDGE_OHE`) was completely removed from the revenue forecasting pipeline due to catastrophic performance failure (1130% WAPE vs 28% WAPE for GBR). Root cause analysis confirmed the Ridge model is fundamentally incompatible with the `signed_log1p` target transform used in production.

**Actions Completed**:
1. Removed `RIDGE_OHE` from notebook CANDIDATES list (commit 7a296dc)
2. Archived Ridge data to permanent backup tables
3. Deleted Ridge rows from production tables
4. Verified no Ridge model runs remain in active tables

---

## Server-Side Changes (Manual Execution in Snowflake)

### Archive Tables Created

The following archive tables were created to preserve Ridge model data before deletion:

```sql
-- Archive model runs
CREATE TABLE DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS_RIDGE_ARCHIVE AS
SELECT * FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS
WHERE model_family = 'ridge'
  OR params:candidate::string = 'RIDGE_OHE';

-- Archive backtest predictions  
CREATE TABLE DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS_RIDGE_ARCHIVE AS
SELECT p.* 
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS p
JOIN DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS r
  ON r.model_run_id = p.model_run_id
WHERE r.model_family = 'ridge'
  OR r.params:candidate::string = 'RIDGE_OHE';

-- Archive champion selections (if any)
CREATE TABLE DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_CHAMPIONS_RIDGE_ARCHIVE AS
SELECT c.*
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_CHAMPIONS c
JOIN DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS r
  ON r.model_run_id = c.model_run_id
WHERE r.model_family = 'ridge'
  OR r.params:candidate::string = 'RIDGE_OHE';
```

**Archive Status**: ✅ Complete - archives stored permanently, no restore planned

---

### Deletion Commands Executed

After creating archives, the following deletions were executed transactionally:

```sql
BEGIN TRANSACTION;

-- Delete Ridge backtest predictions
DELETE FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS
WHERE model_run_id IN (
  SELECT model_run_id 
  FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS
  WHERE model_family = 'ridge' 
    OR params:candidate::string = 'RIDGE_OHE'
);

-- Delete Ridge champion selections
DELETE FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_CHAMPIONS
WHERE model_run_id IN (
  SELECT model_run_id
  FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS
  WHERE model_family = 'ridge'
    OR params:candidate::string = 'RIDGE_OHE'
);

-- Delete Ridge model runs (must be last due to FK references)
DELETE FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS
WHERE model_family = 'ridge'
  OR params:candidate::string = 'RIDGE_OHE';

COMMIT;
```

**Deletion Status**: ✅ Complete - all Ridge data removed from production tables

---

## Verification Queries

### 1. Verify No Ridge Runs Remain

```sql
SELECT COUNT(*) as ridge_count
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS
WHERE model_family = 'ridge'
  OR params:candidate::string = 'RIDGE_OHE';
-- Expected: 0
```

### 2. Verify Archive Table Row Counts

```sql
SELECT 
  'RUNS' as table_type,
  COUNT(*) as archived_rows
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS_RIDGE_ARCHIVE

UNION ALL

SELECT 
  'PREDICTIONS',
  COUNT(*)
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS_RIDGE_ARCHIVE

UNION ALL

SELECT 
  'CHAMPIONS',
  COUNT(*)
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_CHAMPIONS_RIDGE_ARCHIVE;
-- Expected: Non-zero counts indicating data was archived
```

### 3. Verify Current Active Model Families

```sql
SELECT 
  model_family,
  COUNT(*) as run_count,
  MIN(created_at) as earliest_run,
  MAX(created_at) as latest_run
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS
GROUP BY model_family
ORDER BY model_family;
-- Expected: Only 'baseline' and 'gbr' (no 'ridge')
```

### 4. Verify No Orphaned Ridge Predictions

```sql
SELECT COUNT(*) as orphaned_predictions
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS p
LEFT JOIN DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS r
  ON r.model_run_id = p.model_run_id
WHERE r.model_run_id IS NULL;
-- Expected: 0 (all predictions have valid model_run_id)
```

---

## Runtime SQL Patch (Proposed)

### Files Requiring Runtime Changes

**File**: `11__proc__score_and_publish_marts.sql`  
**Line**: 279  
**Type**: Runtime scoring procedure (production SQL)

**Current Code**:
```sql
when m.model_family in ('ridge', 'gbr') then
  -- Use backtest prediction pattern with growth ratio
```

**Proposed Change**:
Remove `'ridge'` from the `model_family` check since Ridge is no longer a valid runtime model family.

```sql
-- ridge removed from runtime: incompatible with signed_log1p transform (archived and deleted)
when m.model_family = 'gbr' then
  -- Use backtest prediction pattern with growth ratio
```

**Rationale**: The scoring procedure must not attempt to score using Ridge model logic since:
1. No Ridge models exist in `FORECAST_MODEL_RUNS` (all deleted)
2. Ridge is incompatible with the target transform
3. If Ridge somehow appeared, it should fall through to the `else` clause (safe fallback to lag-12)

**Patch Location**: `proposed_patches/remove_ridge_runtime.patch`

**Impact**: LOW - Since Ridge models are already deleted, this clause would never execute. The change prevents future confusion and documents the removal.

---

## Classification of All Ridge References

### Runtime Code (Action Required)
- ✅ **`11__proc__score_and_publish_marts.sql:279`** - Runtime scoring logic with `model_family in ('ridge', 'gbr')`

### Documentation/Comments (No Action)
- `10__setup__model_tracking_tables.sql:20` - Comment example listing model families
- `BACKTEST_EXECUTION_PLAN.md:6` - Documentation of removal
- `METRIC_AUDIT_REPORT.md:294` - Audit documentation  
- `RECOMMENDED_CODE_CHANGES.md:14` - Code change recommendations

### Debug/Analysis Scripts (No Action)
- `check_ridge_predictions.py:29,55` - Debug script for historical analysis

### Notebook Debug Cells (No Action)
- `10__modeling__backtest_global_plus_overrides.ipynb:991,1136` - Historical debug queries

---

## Related Documentation

- **Root Cause Analysis**: `METRIC_AUDIT_REPORT.md` (454 lines)
- **Code Changes Guide**: `RECOMMENDED_CODE_CHANGES.md` (470 lines)
- **Execution Plan**: `BACKTEST_EXECUTION_PLAN.md` (104 lines)
- **Git Commit**: 7a296dc - "Remove Ridge candidate — incompatible with signed_log1p target transform (per metric audit)"

---

## Restoration Instructions (Emergency Only)

If Ridge model data must be restored:

```sql
-- Restore model runs
INSERT INTO DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS
SELECT * FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS_RIDGE_ARCHIVE;

-- Restore predictions
INSERT INTO DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS
SELECT * FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS_RIDGE_ARCHIVE;

-- Restore champions
INSERT INTO DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_CHAMPIONS
SELECT * FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_CHAMPIONS_RIDGE_ARCHIVE;
```

**Note**: Restoration is NOT recommended. Ridge model is fundamentally broken with current transform.
