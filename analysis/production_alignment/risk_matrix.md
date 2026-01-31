# Production Alignment Audit - Risk Matrix & Remediation Plan

**Date**: 2026-01-31  
**Auditor**: Arthur (supervised by Merlin)  
**Scope**: DB_BI_P_SANDBOX.SANDBOX revenue forecasting pipeline  

---

## EXECUTIVE SUMMARY

**Audit Status**: ⚠️ **MOSTLY ALIGNED with HIGH-PRIORITY RISKS IDENTIFIED**

The repository code and Snowflake production environment are generally well-aligned. The forecasting pipeline correctly uses `FORECAST_MODEL_BACKTEST_PREDICTIONS` as the canonical table (the missing `FORECAST_MODEL_PREDICTIONS` table is not expected anywhere). However, critical gaps exist in data protection, archival processes, and operational automation.

**Critical Findings**:
1. 🔴 **HIGH RISK**: Backtest table vulnerable to accidental deletion (no snapshot before truncate)
2. 🔴 **HIGH RISK**: No monthly archival for published forecasts (audit trail gap)
3. 🟡 **MEDIUM RISK**: No automated scheduling (all manual execution)
4. 🟡 **MEDIUM RISK**: Orchestrator procedure not deployed

---

## RISK MATRIX

| Risk ID | Severity | Component | Issue | Impact | Remediation | Effort |
|---------|----------|-----------|-------|--------|-------------|--------|
| **R1** | 🔴 **HIGH** | Backtest Notebook | `DELETE FROM FORECAST_MODEL_BACKTEST_PREDICTIONS` (line 404) with no WHERE clause truncates entire table | Production scoring breaks: falls back to lag-12 or zero forecasts. Historical backtest data lost permanently. | 1. Add conditional DELETE with WHERE clause<br>2. Archive to `_BACKUP` table before delete<br>3. Add warning comment in notebook | **2-4 hours** |
| **R2** | 🔴 **HIGH** | Published Forecasts | No monthly snapshot for `FORECAST_OUTPUT_PC_REASON_MTH` | If table recreated or corrupted, historical forecasts lost. No audit trail for forecast versions. Finance cannot compare forecasts month-over-month. | 1. Create `FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT` table<br>2. Add monthly TASK or manual job to snapshot<br>3. Document snapshot procedure | **2-3 hours** |
| **R3** | 🟡 **MEDIUM** | Scheduling | No automated execution: all Snowflake TASKS suspended, no Airflow DAGs | Pipeline must be run manually. Risk of missing monthly forecast deadlines. No consistency in execution timing. | 1. Resume `TASK_RUN_RCT_BACKTEST` or create new monthly TASK<br>2. Or document manual execution SOP<br>3. Add monitoring/alerts for missed runs | **1-2 hours** (resume task)<br>**4-8 hours** (new Airflow DAG) |
| **R4** | 🟡 **MEDIUM** | Orchestrator | `SP_RUN_ORCHESTRATOR` not found in production | Orchestration may be fragmented or manual. Unclear how full pipeline is executed. | 1. Execute `04__proc__run_orchestrator.sql` in production<br>2. Or document that orchestration is manual<br>3. Update runbooks with orchestration steps | **1 hour** (deploy proc)<br>**2-3 hours** (document manual flow) |
| **R5** | 🟡 **MEDIUM** | Champion Selection | No override table for per-series champion exceptions | Cannot override champion model for specific PC×Reason without editing `FORECAST_MODEL_CHAMPIONS` directly | 1. Design `FORECAST_MODEL_OVERRIDES` table schema<br>2. Update scoring logic to check overrides first<br>3. Document override process for Finance | **4-6 hours** |
| **R6** | 🟢 **LOW** | Output Tables | `FORECAST_OUTPUT_*` tables grow indefinitely (no cleanup of old forecast_run_ids) | Storage bloat over time. Queries may slow down. | 1. Add retention policy (keep last 24 months)<br>2. Archive old forecast_run_ids to historical table<br>3. Add cleanup job to monthly schedule | **2-3 hours** |
| **R7** | 🟢 **LOW** | Column Naming | No `forecast_month` column (uses `anchor+horizon→target` pattern) | Finance stakeholders may expect a direct "forecast_month" column | 1. Create view `VW_FORECAST_NEXT12_BY_SERIES` that computes forecast_month<br>2. Expose view to Finance users<br>3. Document column mapping | **1-2 hours** |

---

## DETAILED RISK ANALYSIS

### 🔴 R1: Backtest Table Deletion Risk (HIGH)

**Location**: `10__modeling__backtest_global_plus_overrides.ipynb:404`

**Current Code**:
```python
session.sql("DELETE FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS").collect()
```

**Problem**: 
- Unconditional DELETE removes ALL backtest data (94,068 rows across 6 model_run_ids)
- No backup before deletion
- `SP_SCORE_AND_PUBLISH_FORECASTS` depends on backtest averages (last 6 anchors) for scaling predictions
- If backtest data deleted, scoring falls back to:
  - lag-12 actuals (seasonal naive)
  - OR zero (if no lag-12 available)

**Impact Scenario**:
1. Data scientist runs backtest notebook
2. Line 404 executes: `DELETE FROM FORECAST_MODEL_BACKTEST_PREDICTIONS` 
3. Table is empty
4. Notebook re-inserts only NEW model_run_id predictions (~15,678 rows for 1 model)
5. Old model_run_ids lost (including champion `3c4074b1-ba9d-47aa-ab45-1d16adb1bfef`)
6. Next scoring run: Champion model has no backtest data
7. Scoring falls back to lag-12 for ALL series
8. Forecast quality degrades significantly

**Remediation Steps**:

**Option A: Conditional Delete (Preferred)**
```sql
-- Before INSERT, delete only the NEW model_run_id
DELETE FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS
WHERE model_run_id = '<NEW_MODEL_RUN_ID>';

-- Then INSERT new predictions for this model_run_id only
INSERT INTO DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS
SELECT ...;
```

**Option B: Archive Before Delete**
```sql
-- Create backup table (one-time)
CREATE TABLE FORECAST_MODEL_BACKTEST_PREDICTIONS_BACKUP LIKE FORECAST_MODEL_BACKTEST_PREDICTIONS;

-- Archive before delete
INSERT INTO FORECAST_MODEL_BACKTEST_PREDICTIONS_BACKUP
SELECT *, CURRENT_TIMESTAMP() AS archived_at
FROM FORECAST_MODEL_BACKTEST_PREDICTIONS;

-- Then safe to delete
DELETE FROM FORECAST_MODEL_BACKTEST_PREDICTIONS;
```

**Option C: Upsert Pattern**
```sql
-- MERGE instead of DELETE+INSERT
MERGE INTO FORECAST_MODEL_BACKTEST_PREDICTIONS t
USING (SELECT ... FROM new_predictions) s
ON t.model_run_id = s.model_run_id
   AND t.roll_up_shop = s.roll_up_shop
   AND t.reason_group = s.reason_group
   AND t.anchor_fiscal_yyyymm = s.anchor_fiscal_yyyymm
   AND t.horizon = s.horizon
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

**Estimated Effort**: 2-4 hours
- Update notebook (30 min)
- Test on dev copy (1 hour)
- Code review (30 min)
- Deploy to production (1 hour)
- Document change (1 hour)

---

### 🔴 R2: No Monthly Snapshot for Published Forecasts (HIGH)

**Tables Affected**: 
- `FORECAST_OUTPUT_PC_REASON_MTH`
- `FORECAST_OUTPUT_PC_REASON_CUST_MTH`

**Problem**:
- Current pattern: Append-only INSERT (safe from overwrites)
- BUT: No archival process
- If table recreated (e.g., schema change, data corruption), all historical forecasts lost
- Cannot compare "what we forecasted in Dec 2025" vs "what actually happened"
- No audit trail for Finance compliance

**Current State**:
- Only 2 forecast_run_ids exist (from 2026-01-30)
- No historical forecasts older than 1 day
- **Question**: Were tables recently recreated?

**Use Cases for Historical Forecasts**:
1. **Forecast vs Actuals Comparison**: "In November, we forecasted $5M for December. Actual was $4.8M. Why?"
2. **Model Performance Tracking**: "Has our forecast accuracy improved over the past 6 months?"
3. **Budget Variance Analysis**: "Compare forecast from Jan vs Feb vs Mar for same target month (Apr)"
4. **Compliance / Audit**: "Show me all forecasts published in Q4 2025"

**Remediation SQL**:

```sql
-- Step 1: Create snapshot table (one-time DDL)
CREATE TABLE IF NOT EXISTS DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT (
  snapshot_month NUMBER,           -- YYYYMM of when snapshot was taken
  snapshot_at TIMESTAMP_NTZ,       -- Timestamp of snapshot
  forecast_run_id STRING,
  asof_fiscal_yyyymm NUMBER,
  forecast_created_at TIMESTAMP_NTZ,
  roll_up_shop STRING,
  reason_group STRING,
  target_fiscal_yyyymm NUMBER,
  target_fiscal_year NUMBER,
  target_fiscal_month NUMBER,
  target_month_seq NUMBER,
  target_month_start DATE,
  target_month_end DATE,
  horizon NUMBER,
  revenue_forecast NUMBER(18,2),
  revenue_forecast_lo NUMBER(18,2),
  revenue_forecast_hi NUMBER(18,2),
  model_run_id STRING,
  model_family STRING,
  model_scope STRING,
  published_at TIMESTAMP_NTZ,
  PRIMARY KEY (snapshot_month, forecast_run_id, roll_up_shop, reason_group, target_fiscal_yyyymm)
);

-- Step 2: Monthly snapshot job (idempotent)
-- Run at end of each month or beginning of next month
-- Example: Snapshot January forecasts on 2026-02-01

SET SNAPSHOT_MONTH = 202601;  -- January 2026

INSERT INTO DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT
SELECT 
  :SNAPSHOT_MONTH AS snapshot_month,
  CURRENT_TIMESTAMP() AS snapshot_at,
  *
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
WHERE asof_fiscal_yyyymm = :SNAPSHOT_MONTH
  AND NOT EXISTS (
    SELECT 1 FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT s
    WHERE s.snapshot_month = :SNAPSHOT_MONTH
      AND s.forecast_run_id = FORECAST_OUTPUT_PC_REASON_MTH.forecast_run_id
      AND s.roll_up_shop = FORECAST_OUTPUT_PC_REASON_MTH.roll_up_shop
      AND s.reason_group = FORECAST_OUTPUT_PC_REASON_MTH.reason_group
      AND s.target_fiscal_yyyymm = FORECAST_OUTPUT_PC_REASON_MTH.target_fiscal_yyyymm
  );

-- Step 3: Optional - Create Snowflake TASK for automation
CREATE OR REPLACE TASK TASK_SNAPSHOT_PUBLISHED_FORECASTS
  WAREHOUSE = BI_P_QRY_FIN_OPT_WH
  SCHEDULE = 'USING CRON 0 2 1 * * America/New_York'  -- 2am on 1st of each month
  AS
  CALL DB_BI_P_SANDBOX.SANDBOX.SP_SNAPSHOT_MONTHLY_FORECASTS();

-- Resume task
ALTER TASK TASK_SNAPSHOT_PUBLISHED_FORECASTS RESUME;
```

**Estimated Effort**: 2-3 hours
- Create snapshot table (15 min)
- Test snapshot SQL (30 min)
- Create stored procedure wrapper (30 min)
- Create Snowflake TASK (15 min)
- Document snapshot process (1 hour)

---

### 🟡 R3: No Automated Scheduling (MEDIUM)

**Current State**:
- `TASK_RUN_RCT_BACKTEST`: SUSPENDED (schedule: daily 5am)
- `CREATE_OOS_DEEPDIVE_TABLE`: SUSPENDED (schedule: daily 9am)
- No Airflow DAGs found in repository
- **Conclusion**: All forecast pipeline execution is manual

**Problem**:
- Risk of missing monthly forecast deadlines
- No consistency in execution timing
- Manual process is error-prone
- Unclear who is responsible for running pipeline

**Questions for Stakeholders**:
1. Is forecast pipeline intended to be manual or automated?
2. What is the monthly forecast delivery schedule (e.g., 5th business day of month)?
3. Who is responsible for triggering manual runs?
4. What is the escalation path if forecast is not delivered on time?

**Remediation Options**:

**Option A: Resume Existing Task**
```sql
-- If TASK_RUN_RCT_BACKTEST is the right job
ALTER TASK DB_BI_P_SANDBOX.SANDBOX.TASK_RUN_RCT_BACKTEST RESUME;

-- Monitor task runs
SELECT *
FROM TABLE(DB_BI_P_SANDBOX.INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'TASK_RUN_RCT_BACKTEST'
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;
```

**Option B: Create New Monthly Forecast Task**
```sql
CREATE OR REPLACE TASK TASK_MONTHLY_FORECAST_PIPELINE
  WAREHOUSE = BI_P_QRY_FIN_OPT_WH
  SCHEDULE = 'USING CRON 0 6 5 * * America/New_York'  -- 6am on 5th of month
  COMMENT = 'Monthly revenue forecast pipeline execution'
  AS
  CALL DB_BI_P_SANDBOX.SANDBOX.SP_RUN_ORCHESTRATOR(
    'MONTHLY_FORECAST',
    NULL,  -- Use current month as ASOF
    12     -- 12 horizons
  );

ALTER TASK TASK_MONTHLY_FORECAST_PIPELINE RESUME;
```

**Option C: Document Manual Execution SOP**
If automation is not desired, create runbook:
1. Navigate to Snowflake web UI
2. Execute: `CALL SP_RUN_ORCHESTRATOR('MONTHLY_FORECAST', <ASOF_YYYYMM>, 12)`
3. Verify completion: Check `FORECAST_RUNS` table for status
4. Execute scoring: `CALL SP_SCORE_AND_PUBLISH_FORECASTS(<run_id>, NULL, 12)`
5. Verify output: Check `FORECAST_OUTPUT_PC_REASON_MTH` row count
6. Notify Finance stakeholders

**Estimated Effort**:
- Resume existing task: 1 hour (testing + monitoring)
- Create new task: 2-3 hours (design + testing + docs)
- Document manual SOP: 1-2 hours

---

### 🟡 R4: Orchestrator Procedure Not Deployed (MEDIUM)

**Current State**:
- Repository file: `04__proc__run_orchestrator.sql` EXISTS
- Snowflake: `SP_RUN_ORCHESTRATOR` NOT FOUND

**Problem**:
- Unclear how full pipeline is executed end-to-end
- Individual procs must be called manually in sequence:
  1. SP_EVALUATE_PC_ELIGIBILITY
  2. SP_BUILD_ACTUALS_PC_REASON_MTH
  3. SP_BUILD_BUDGET_PC_REASON_MTH
  4. SP_BUILD_CUST_MIX_PC_REASON
  5. SP_BUILD_MODEL_DATASET_PC_REASON_H
  6. (Python backtest notebook)
  7. SP_SCORE_AND_PUBLISH_FORECASTS

**Remediation**:

**Option A: Deploy Orchestrator**
```bash
# Execute SQL file in Snowflake
snowsql -c <connection> -f 04__proc__run_orchestrator.sql
```

**Option B: Verify Alternative Orchestration**
- Check if orchestrator exists under different name
- Check if orchestration is in Airflow (external to Snowflake)
- Document actual orchestration flow

**Estimated Effort**: 1 hour (deploy proc) or 2-3 hours (document manual flow)

---

### 🟡 R5: No Override Table for Champion Exceptions (MEDIUM)

**Current Capability**:
- `FORECAST_MODEL_CHAMPIONS` stores champion selection
- Supports GLOBAL and PC_REASON scopes
- Scoring logic: PC_REASON champion overrides GLOBAL champion

**Gap**:
- Cannot temporarily override champion for specific PC without updating `FORECAST_MODEL_CHAMPIONS`
- Cannot A/B test: "Use model X for PC 555, model Y for everything else"
- Cannot freeze champion for specific series while testing new models globally

**Proposed Schema**:
```sql
CREATE TABLE FORECAST_MODEL_OVERRIDES (
  override_id STRING DEFAULT UUID_STRING(),
  override_scope STRING,  -- 'PC', 'PC_REASON', 'GLOBAL'
  roll_up_shop STRING,
  reason_group STRING,
  model_run_id STRING,
  override_reason STRING,
  start_date DATE,
  end_date DATE,
  created_by STRING,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (override_id)
);
```

**Scoring Logic Update**:
```sql
-- In SP_SCORE_AND_PUBLISH_FORECASTS, add override check
champions_with_overrides AS (
  SELECT
    sg.roll_up_shop,
    sg.reason_group,
    COALESCE(
      ov_pc_reason.model_run_id,  -- Override PC_REASON (highest priority)
      ov_pc.model_run_id,          -- Override PC
      champ_pc_reason.model_run_id,  -- Champion PC_REASON
      champ_global.model_run_id       -- Champion GLOBAL (default)
    ) AS model_run_id
  FROM score_grid sg
  LEFT JOIN FORECAST_MODEL_OVERRIDES ov_pc_reason
    ON ov_pc_reason.override_scope = 'PC_REASON'
   AND ov_pc_reason.roll_up_shop = sg.roll_up_shop
   AND ov_pc_reason.reason_group = sg.reason_group
   AND :V_ASOF_YYYYMM BETWEEN ov_pc_reason.start_date AND ov_pc_reason.end_date
  LEFT JOIN FORECAST_MODEL_OVERRIDES ov_pc
    ON ov_pc.override_scope = 'PC'
   AND ov_pc.roll_up_shop = sg.roll_up_shop
   AND :V_ASOF_YYYYMM BETWEEN ov_pc.start_date AND ov_pc.end_date
  ...
)
```

**Estimated Effort**: 4-6 hours (design schema + update scoring logic + test)

---

### 🟢 R6: Output Tables Grow Indefinitely (LOW)

**Current State**:
- `FORECAST_OUTPUT_PC_REASON_MTH` has 2 forecast_run_ids (from yesterday)
- No retention policy
- Table will grow: ~2,400 rows per forecast_run × 12 months/year = 28,800 rows/year

**Problem**:
- Storage bloat over time
- Queries may slow down (scanning millions of rows)
- Unclear which forecast_run_id is "current" vs "historical"

**Remediation**:
```sql
-- Retention policy: Keep last 24 months, archive older
INSERT INTO FORECAST_OUTPUT_PC_REASON_MTH_ARCHIVE
SELECT * 
FROM FORECAST_OUTPUT_PC_REASON_MTH
WHERE asof_fiscal_yyyymm < DATE_PART('year', DATEADD('month', -24, CURRENT_DATE())) * 100
                         + DATE_PART('month', DATEADD('month', -24, CURRENT_DATE()));

DELETE FROM FORECAST_OUTPUT_PC_REASON_MTH
WHERE asof_fiscal_yyyymm < DATE_PART('year', DATEADD('month', -24, CURRENT_DATE())) * 100
                         + DATE_PART('month', DATEADD('month', -24, CURRENT_DATE()));
```

**Estimated Effort**: 2-3 hours

---

### 🟢 R7: No Direct forecast_month Column (LOW)

**Current State**:
- Tables use `anchor_fiscal_yyyymm + horizon → target_fiscal_yyyymm`
- No column named `forecast_month`

**Finance User Expectation**:
- May expect a simple "forecast_month" column instead of "target_fiscal_yyyymm"

**Remediation**:
```sql
-- Create user-friendly view
CREATE OR REPLACE VIEW VW_FORECAST_NEXT12_BY_SERIES AS
SELECT
  forecast_run_id,
  asof_fiscal_yyyymm AS anchor_month,
  roll_up_shop AS pc,
  reason_group,
  target_fiscal_yyyymm AS forecast_month,  -- Rename for clarity
  horizon,
  revenue_forecast,
  revenue_forecast_lo,
  revenue_forecast_hi,
  model_run_id,
  model_family,
  published_at
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
WHERE forecast_run_id = (
  SELECT forecast_run_id 
  FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
  ORDER BY forecast_created_at DESC 
  LIMIT 1
);
```

**Estimated Effort**: 1-2 hours

---

## SUMMARY METRICS

| Risk Category | Count | Total Effort Estimate |
|---------------|-------|-----------------------|
| 🔴 HIGH       | 2     | 4-7 hours             |
| 🟡 MEDIUM     | 3     | 8-17 hours            |
| 🟢 LOW        | 2     | 3-5 hours             |
| **TOTAL**     | **7** | **15-29 hours**       |

---

## PRIORITIZED REMEDIATION ROADMAP

### Phase 1: Immediate (Week 1) — Address HIGH Risks
1. **R1: Protect Backtest Table** (2-4 hours)
   - Update notebook to use conditional DELETE
   - Test on dev environment
   - Deploy to production

2. **R2: Create Monthly Snapshot** (2-3 hours)
   - Create snapshot table
   - Run first manual snapshot
   - Document process

### Phase 2: Short-Term (Week 2-3) — Address MEDIUM Risks
3. **R4: Deploy Orchestrator** (1 hour)
   - Execute 04__proc__run_orchestrator.sql

4. **R3: Decide on Scheduling** (1-3 hours)
   - Stakeholder decision: manual vs automated
   - If automated: resume task or create new
   - If manual: document SOP

5. **R5: Design Override Table** (4-6 hours)
   - Design schema (stakeholder review)
   - Implement override logic
   - Test with sample overrides

### Phase 3: Long-Term (Month 2+) — Address LOW Risks
6. **R6: Add Retention Policy** (2-3 hours)
   - Archive old forecast_run_ids
   - Add monthly cleanup job

7. **R7: Create User-Friendly Views** (1-2 hours)
   - Create VW_FORECAST_NEXT12_BY_SERIES
   - Grant access to Finance users
   - Document view usage

---

## OPERATIONAL RECOMMENDATIONS

### Monthly Forecast Production Checklist

**Timing**: 5th business day of each month

**Stakeholders**:
- **Data Science**: Execute modeling pipeline
- **Data Engineering**: Verify data quality and pipeline success
- **Finance**: Consume forecasts and report actuals vs forecast

**Steps**:

1. **Pre-Flight Checks** (before execution)
   - [ ] Verify actuals data loaded through latest closed month
   - [ ] Verify budget data refreshed
   - [ ] Check FORECAST_ASOF_FISCAL_MONTH is set to latest closed month

2. **Execution** (manual or automated)
   - [ ] Run orchestrator: `CALL SP_RUN_ORCHESTRATOR('MONTHLY_FORECAST', NULL, 12)`
   - [ ] Verify run success: Check `FORECAST_RUNS.status = 'SUCCEEDED'`
   - [ ] Run modeling notebook (if not automated)
   - [ ] Upsert champion: Update `FORECAST_MODEL_CHAMPIONS`
   - [ ] Run scoring: `CALL SP_SCORE_AND_PUBLISH_FORECASTS(<run_id>, NULL, 12)`

3. **Post-Flight Validation**
   - [ ] Verify row counts: `SELECT COUNT(*) FROM FORECAST_OUTPUT_PC_REASON_MTH WHERE forecast_run_id = '<latest>'`
   - [ ] Expected: ~2,400 rows (67 PCs × 12 horizons × varying reasons)
   - [ ] Spot-check forecasts: Top 10 PCs by revenue
   - [ ] Compare vs previous month: Month-over-month variance < 20%

4. **Snapshot and Archive**
   - [ ] Run monthly snapshot: `INSERT INTO FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT ...`
   - [ ] Verify snapshot row count matches current forecast rows

5. **Delivery to Finance**
   - [ ] Export forecasts to Excel or Tableau
   - [ ] Send forecast summary email with key metrics
   - [ ] Schedule review meeting with Finance stakeholders

---

## COMPLIANCE & AUDIT TRAIL

### Required Artifacts for Audit

1. **Model Versioning**
   - `FORECAST_MODEL_RUNS`: All model training runs with parameters, timestamps, code_ref
   - Retention: Indefinite (audit requirement)

2. **Champion Selection History**
   - `FORECAST_MODEL_CHAMPIONS`: Historical champion selections with reason and timestamp
   - Retention: Indefinite

3. **Published Forecasts**
   - `FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT`: Monthly snapshots of published forecasts
   - Retention: Minimum 7 years (compliance)

4. **Backtest Results**
   - `FORECAST_MODEL_BACKTEST_PREDICTIONS`: Historical model performance
   - Retention: Minimum 3 years

5. **Actuals Data**
   - `FORECAST_ACTUALS_PC_REASON_MTH`: Actual revenue for forecast vs actuals comparison
   - Retention: Indefinite (source of truth)

### Recommended Audit Queries

```sql
-- Q1: Show forecast vs actuals for December 2025
SELECT 
  f.roll_up_shop,
  f.reason_group,
  f.revenue_forecast,
  a.total_revenue AS actual_revenue,
  (a.total_revenue - f.revenue_forecast) AS variance,
  (a.total_revenue - f.revenue_forecast) / NULLIF(f.revenue_forecast, 0) * 100 AS variance_pct
FROM FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT f
JOIN FORECAST_ACTUALS_PC_REASON_MTH a
  ON a.roll_up_shop = f.roll_up_shop
 AND a.reason_group = f.reason_group
 AND a.fiscal_yyyymm = f.target_fiscal_yyyymm
WHERE f.snapshot_month = 202511  -- Forecasted in November
  AND f.target_fiscal_yyyymm = 202512  -- For December
  AND f.horizon = 1;

-- Q2: Champion model change history
SELECT 
  asof_fiscal_yyyymm,
  champion_scope,
  roll_up_shop,
  reason_group,
  model_run_id,
  selection_metric,
  selection_reason,
  selected_at
FROM FORECAST_MODEL_CHAMPIONS
ORDER BY asof_fiscal_yyyymm DESC, selected_at DESC;

-- Q3: Model performance comparison (all model runs)
SELECT 
  r.model_run_id,
  r.model_family,
  r.training_ended_at,
  m.metric_name,
  m.value AS metric_value
FROM FORECAST_MODEL_RUNS r
JOIN FORECAST_MODEL_BACKTEST_METRICS m
  ON m.model_run_id = r.model_run_id
WHERE m.metric_scope = 'GLOBAL'
  AND m.metric_name IN ('WAPE', 'MAE', 'BIAS')
ORDER BY r.training_ended_at DESC, m.metric_name;
```

---

## SIGN-OFF

**Audit Completed**: 2026-01-31  
**Auditor**: Arthur  
**Supervisor**: Merlin  

**Next Steps**:
1. Review risk matrix with Wayne (product owner)
2. Prioritize remediation items
3. Assign owners for Phase 1 tasks
4. Schedule follow-up audit in 1 month

**Outstanding Questions for Stakeholders**:
1. Is forecast pipeline intended to be manual or automated?
2. What is the acceptable monthly forecast delivery SLA?
3. Should we resume TASK_RUN_RCT_BACKTEST or create new monthly task?
4. Is SP_RUN_ORCHESTRATOR needed, or is orchestration handled elsewhere?
5. What is the retention policy for published forecasts? (suggested: 7 years)
