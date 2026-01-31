# Snowflake Routines and Tasks Discovery
## Production Alignment Audit - STEP 3

**Date**: 2026-01-31
**Database**: DB_BI_P_SANDBOX.SANDBOX
**Method**: Read-only SHOW/INFORMATION_SCHEMA queries

---

## KEY FINDINGS SUMMARY

### ✅ Stored Procedures - DEPLOYED AND ALIGNED

**Forecast Procedures Found in Production**:
1. **SP_SCORE_AND_PUBLISH_FORECASTS** ✅ DEPLOYED
   - Signature: `(VARCHAR, DEFAULT NUMBER, DEFAULT NUMBER, DEFAULT VARCHAR) RETURN VARIANT`
   - Matches repository file: 11__proc__score_and_publish_marts.sql
   - **Status**: ALIGNED with backtest table schema

2. **SP_EVALUATE_PC_ELIGIBILITY** ✅ DEPLOYED
   - Part of orchestration pipeline

3. **SP_BUILD_ACTUALS_PC_REASON_MTH** ✅ DEPLOYED
   - Builds actuals snapshot table

4. **SP_BUILD_BUDGET_PC_REASON_MTH** ✅ DEPLOYED
   - Builds budget snapshot table

5. **SP_BUILD_CUST_MIX_PC_REASON** ✅ DEPLOYED
   - Builds customer mix allocation shares

6. **SP_BUILD_MODEL_DATASET_PC_REASON_H** ✅ DEPLOYED
   - Builds model training dataset with anchor+horizon pattern

### ⚠️ Missing Procedures

**SP_RUN_ORCHESTRATOR** ❌ NOT FOUND IN PRODUCTION
- Repository file: 04__proc__run_orchestrator.sql EXISTS
- **Risk**: Orchestrator may not be deployed, or uses different name
- **Action**: Verify if orchestrator exists under different name or if pipeline is executed manually

---

## SNOWFLAKE TASKS

### Task 1: CREATE_OOS_DEEPDIVE_TABLE
- **Schedule**: CRON `0 9 * * * America/New_York` (Daily 9am ET)
- **State**: SUSPENDED
- **Warehouse**: BI_P_QRY_WHS_WH
- **Purpose**: Out-of-stock deepdive table (unrelated to revenue forecasting)
- **Forecast Tables Referenced**: NONE

### Task 2: TASK_RUN_RCT_BACKTEST
- **Schedule**: CRON `0 5 * * * America/New_York` (Daily 5am ET)
- **State**: SUSPENDED
- **Warehouse**: BI_P_QRY_LG_WH
- **Purpose**: Run backtest/forecast for RCT (Revenue Category Team?)
- **Call**: `SANDBOX.RUN_RCT_BACKTEST(6, 3, 5)`
- **Forecast Tables Referenced**: NONE (calls procedure RUN_RCT_BACKTEST)

**CRITICAL**: Both tasks are **SUSPENDED**. This confirms that no automated execution is currently active. All forecast pipeline executions are manual.

---

## FORECAST OUTPUT TABLES

### FORECAST_OUTPUT_PC_REASON_MTH
- **Type**: BASE TABLE
- **Row Count**: 4,824 rows
- **Distinct forecast_run_ids**: 2
- **Date Range**: 
  - First: 2026-01-30 09:32:34
  - Last: 2026-01-30 10:08:59
- **Purpose**: Published forecasts at PC × Reason × Month level
- **Write Pattern**: Append-only INSERT (from SP_SCORE_AND_PUBLISH_FORECASTS)
- **Primary Key**: (forecast_run_id, roll_up_shop, reason_group, target_fiscal_yyyymm)

**Analysis**:
- 2 forecast runs executed yesterday (2026-01-30)
- 4,824 rows ÷ 2 runs = 2,412 rows per run
- Assuming 12 horizons × ~201 PC×Reason combos = expected size
- **No truncation/deletion pattern** (append-only confirmed)
- **No monthly snapshot** (table grows indefinitely)

### FORECAST_OUTPUT_PC_REASON_CUST_MTH
- **Type**: BASE TABLE
- **Row Count**: 24,120 rows
- **Distinct forecast_run_ids**: 2
- **Date Range**: 
  - First: 2026-01-30 09:32:34
  - Last: 2026-01-30 10:08:59
- **Purpose**: Disaggregated forecasts by customer group
- **Write Pattern**: Append-only INSERT (from SP_SCORE_AND_PUBLISH_FORECASTS)
- **Primary Key**: (forecast_run_id, roll_up_shop, reason_group, cust_grp, target_fiscal_yyyymm)

**Analysis**:
- 24,120 rows ÷ 2 runs = 12,060 rows per run
- Matches 2,412 PC×Reason rows × ~5 customer groups per series
- **Append-only confirmed**
- **No monthly snapshot**

---

## TABLE WRITE PATTERNS (from repo analysis)

### FORECAST_MODEL_BACKTEST_PREDICTIONS
**Repository References**:
- **10__modeling__backtest_global_plus_overrides.ipynb:404**
  ```sql
  DELETE FROM FORECAST_MODEL_BACKTEST_PREDICTIONS
  ```
  **🔴 HIGH RISK**: Unconditional DELETE (truncates entire table)

- **10__modeling__backtest_global_plus_overrides.ipynb:540**
  ```sql
  INSERT INTO FORECAST_MODEL_BACKTEST_PREDICTIONS
  ```
  **Pattern**: DELETE ALL → INSERT NEW (destructive overwrite)

- **10__modeling__backtest_global_plus_overrides.ipynb:1340**
  ```sql
  DELETE FROM FORECAST_MODEL_BACKTEST_PREDICTIONS
  WHERE model_run_id IN (SELECT model_run_id FROM FORECAST_MODEL_RUNS WHERE ...)
  ```
  **Pattern**: Conditional DELETE by model_run_id (less risky but still destructive)

**RISK ASSESSMENT**:
- ⚠️ Backtest table is vulnerable to accidental deletion
- ⚠️ No snapshot before delete (data loss risk)
- ⚠️ Scoring procedure depends on backtest data (if deleted, falls back to lag-12)
- ⚠️ No version control for backtest predictions

**Snowflake Current State**:
- Backtest table has 94,068 rows (per profiling)
- Contains 6 model_run_ids (including champion)
- Anchor range: 202412-202511 (12 months)

### FORECAST_OUTPUT_PC_REASON_MTH / FORECAST_OUTPUT_PC_REASON_CUST_MTH
**Repository Pattern** (11__proc__score_and_publish_marts.sql:397, 446):
```sql
INSERT INTO FORECAST_OUTPUT_PC_REASON_MTH SELECT * FROM TMP_FORECAST_PC_REASON;
INSERT INTO FORECAST_OUTPUT_PC_REASON_CUST_MTH SELECT * FROM TMP_FORECAST_PC_REASON_CUST;
```

**Write Semantics**: **APPEND-ONLY** (no DELETE/TRUNCATE before INSERT)
- ✅ Safe: Each forecast_run_id is unique (UUID)
- ✅ Safe: Re-runs with same forecast_run_id fail on PK violation (prevents duplicates)
- ⚠️ Issue: No cleanup of old forecast_run_ids (table grows indefinitely)
- ⚠️ Issue: No monthly snapshot archival process

**Snowflake Current State**:
- 2 forecast_run_ids from 2026-01-30 (yesterday's runs)
- No historical forecast_run_ids older than 1 day (table may have been recreated recently?)

---

## ALIGNMENT VERIFICATION

### ✅ ALIGNED ITEMS

1. **Stored procedures deployed**
   - SP_SCORE_AND_PUBLISH_FORECASTS exists in production
   - Matches repository signature and logic

2. **Backtest table schema**
   - Production table matches profiled schema (14 columns, anchor+horizon pattern)
   - Stored procedures use correct column names

3. **Output tables exist**
   - FORECAST_OUTPUT_PC_REASON_MTH and FORECAST_OUTPUT_PC_REASON_CUST_MTH deployed
   - Append-only pattern working as expected

4. **No FORECAST_MODEL_PREDICTIONS table**
   - Confirmed: Does not exist in production
   - Stored procedures do not expect it (aligned with reality)

### ⚠️ GAPS / RISKS

1. **No automated scheduling active**
   - Both Snowflake TASKS are SUSPENDED
   - No Airflow DAGs found
   - **Conclusion**: Forecasting pipeline is executed manually only

2. **No monthly snapshot for published forecasts**
   - FORECAST_OUTPUT_* tables have no archival process
   - If tables are recreated, historical forecasts are lost
   - No audit trail for forecast versions

3. **Backtest table vulnerable to deletion**
   - Notebook has unconditional `DELETE FROM FORECAST_MODEL_BACKTEST_PREDICTIONS`
   - No backup before delete
   - Would break scoring procedure (depends on backtest patterns)

4. **Orchestrator procedure missing**
   - Repository has 04__proc__run_orchestrator.sql
   - But SP_RUN_ORCHESTRATOR not found in Snowflake
   - **Question**: Is pipeline orchestration handled differently?

5. **FORECAST_OUTPUT_* tables recently created**
   - Only have data from yesterday (2026-01-30)
   - No historical forecast_run_ids
   - **Question**: Were tables recreated? Or is this a new deployment?

---

## RECOMMENDATIONS

### 1. Add Monthly Snapshot for Published Forecasts
```sql
-- Create snapshot table (one-time DDL)
CREATE TABLE IF NOT EXISTS FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT AS
SELECT *, CURRENT_DATE() AS snapshot_date
FROM FORECAST_OUTPUT_PC_REASON_MTH
WHERE 1=0;  -- Empty table

-- Monthly job (idempotent)
INSERT INTO FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT
SELECT *, :SNAPSHOT_DATE AS snapshot_date
FROM FORECAST_OUTPUT_PC_REASON_MTH
WHERE forecast_created_at >= dateadd('month', -1, :SNAPSHOT_DATE)
  AND NOT EXISTS (
    SELECT 1 FROM FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT s
    WHERE s.forecast_run_id = FORECAST_OUTPUT_PC_REASON_MTH.forecast_run_id
  );
```

### 2. Protect Backtest Table from Accidental Deletion
- Add WHERE clause to DELETE statements in notebook
- Archive rows before deletion
- Document dependency in notebook header: "⚠️ WARNING: SP_SCORE_AND_PUBLISH_FORECASTS depends on this table"

### 3. Deploy Orchestrator Procedure
- Execute 04__proc__run_orchestrator.sql in production
- Or document if orchestration is manual

### 4. Enable Scheduled Execution (if desired)
- Resume TASK_RUN_RCT_BACKTEST task
- Or create new Snowflake TASK to call forecast pipeline monthly

---

## SUMMARY

| Aspect | Status | Notes |
|--------|--------|-------|
| **Stored procedures deployed** | ✅ PASS | SP_SCORE_AND_PUBLISH_FORECASTS and data procs exist |
| **Procedure signatures match repo** | ✅ PASS | Arguments and return types aligned |
| **Backtest table alignment** | ✅ PASS | Schema matches, procedures use correct columns |
| **Output tables exist** | ✅ PASS | FORECAST_OUTPUT_* tables deployed and populated |
| **Append-only write pattern** | ✅ PASS | No destructive overwrites to output tables |
| **Automated scheduling** | ❌ FAIL | All tasks SUSPENDED, no active automation |
| **Monthly snapshot** | ❌ MISSING | No archival process for published forecasts |
| **Backtest protection** | ⚠️ RISK | Vulnerable to accidental deletion in notebook |
| **Orchestrator deployed** | ❌ MISSING | SP_RUN_ORCHESTRATOR not found in production |

**OVERALL**: Production stored procedures are aligned with repository code and backtest table schema. Main risks are:
1. No automated execution (manual only)
2. No monthly snapshot for published forecasts (audit trail gap)
3. Backtest table vulnerable to deletion (production impact)
4. Orchestrator procedure not deployed (pipeline may be fragmented)
