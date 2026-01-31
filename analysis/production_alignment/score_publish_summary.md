# Scoring & Publish Procedure Analysis
## File: 11__proc__score_and_publish_marts.sql

**Procedure**: `SP_SCORE_AND_PUBLISH_FORECASTS`
**Database**: DB_BI_P_SANDBOX.SANDBOX
**Purpose**: Generate forecasts for future periods using champion models and publish to consumption tables

---

## OUTPUT TABLES (WRITE TARGETS)

### 1. FORECAST_OUTPUT_PC_REASON_MTH
- **Schema**: Forecast run ID, ASOF month, PC, Reason, Target month, Horizon, Revenue forecast with CIs
- **Write Pattern**: INSERT INTO (line 397)
- **Primary Key**: (forecast_run_id, roll_up_shop, reason_group, target_fiscal_yyyymm)
- **Purpose**: Published forecasts at PC × Reason × Month level
- **Column Semantics**:
  - `asof_fiscal_yyyymm`: The anchor month (latest closed month when forecast was created)
  - `target_fiscal_yyyymm`: The forecast month
  - `horizon`: Months ahead from anchor (1-12)
  - `revenue_forecast`, `revenue_forecast_lo`, `revenue_forecast_hi`: Point estimate and 80% prediction interval

### 2. FORECAST_OUTPUT_PC_REASON_CUST_MTH
- **Schema**: Forecast run ID, ASOF month, PC, Reason, Customer Group, Target month, Revenue forecast with allocation share
- **Write Pattern**: INSERT INTO (line 446)
- **Primary Key**: (forecast_run_id, roll_up_shop, reason_group, cust_grp, target_fiscal_yyyymm)
- **Purpose**: Disaggregated forecasts by customer group using mix shares

---

## INPUT TABLES (READ SOURCES)

### 1. FORECAST_MODEL_CHAMPIONS (line 136)
- **Purpose**: Get champion model_run_id for GLOBAL or PC_REASON scope
- **Filter**: `asof_fiscal_yyyymm = :V_ASOF_YYYYMM`
- **Columns**: champion_scope, roll_up_shop, reason_group, model_run_id, selection_metric

### 2. FORECAST_MODEL_RUNS (line 213)
- **Purpose**: Get model metadata (model_family, model_scope, params)
- **No Filter**: Reads all model runs
- **Used For**: Determine scoring logic (baseline vs ridge vs gbr)

### 3. FORECAST_MODEL_BACKTEST_PREDICTIONS (lines 246, 259, 333) ✅ CRITICAL
- **Usage 1 (line 246)**: Compute recent backtest averages (last 6 anchors)
  ```sql
  select model_run_id, roll_up_shop, reason_group, horizon,
         avg(y_pred) as avg_pred_recent,
         avg(y_true) as avg_true_recent
  from FORECAST_MODEL_BACKTEST_PREDICTIONS
  where anchor_month_seq >= (:V_ANCHOR_SEQ - 6)
  group by 1, 2, 3, 4
  ```

- **Usage 2 (line 259)**: Compute all-time backtest averages (fallback)
  ```sql
  select model_run_id, roll_up_shop, reason_group, horizon,
         avg(y_pred) as avg_pred_all
  from FORECAST_MODEL_BACKTEST_PREDICTIONS
  group by 1, 2, 3, 4
  ```

- **Usage 3 (line 333)**: Compute prediction intervals from backtest residuals
  ```sql
  select model_run_id, roll_up_shop, reason_group, horizon,
         percentile_cont(0.1) within group (order by abs(y_true - y_pred)) as error_10pct,
         percentile_cont(0.9) within group (order by abs(y_true - y_pred)) as error_90pct
  from FORECAST_MODEL_BACKTEST_PREDICTIONS
  group by 1, 2, 3, 4
  ```

- **Purpose**: Use historical backtest patterns to scale future predictions
- **Pattern**: anchor + horizon model (NO forecast_month column expected)
- **Requires**: `y_true IS NOT NULL` for residuals (backtest rows only)

### 4. FORECAST_ACTUALS_PC_REASON_MTH
- **Purpose**: Get lag-12 actuals (seasonal naive baseline) and anchor actuals for scaling
- **Columns**: roll_up_shop, reason_group, month_seq, total_revenue

### 5. FORECAST_PC_ELIGIBILITY
- **Purpose**: Determine which PC×Reason combinations are eligible for forecasting
- **Filter**: `run_id = :P_RUN_ID AND is_eligible = true`

### 6. FORECAST_CUST_MIX_PC_REASON
- **Purpose**: Disaggregate PC×Reason forecasts to customer level using allocation shares
- **Filter**: `asof_fiscal_yyyymm = :V_ASOF_YYYYMM`

### 7. FORECAST_FISCAL_MONTH_DIM
- **Purpose**: Convert month_seq to fiscal YYYYMM and get date boundaries
- **Join**: `month_seq = anchor_month_seq + horizon`

---

## SCORING LOGIC ANALYSIS

### Anchor = Latest Closed Month (ASOF)
- Line 164: `anchor_fiscal_yyyymm = :V_ASOF_YYYYMM`
- Uses FORECAST_ASOF_FISCAL_MONTH or parameter override
- **Computation**: `target_month_seq = anchor_month_seq + horizon`

### Prediction Methods by Model Family

#### Baseline (Seasonal Naive)
```sql
when m.model_family = 'baseline' then
  coalesce(lag12.total_revenue, 0)
```
- Uses lag-12 actual as prediction
- No backtest patterns

#### Ridge / GBR
```sql
when m.model_family in ('ridge', 'gbr') then
  coalesce(
    -- Try recent backtests with anchor scaling
    (br.avg_pred_recent * aa.anchor_revenue / nullif(br.avg_true_recent, 0)),
    -- Fallback: all backtests average
    ba.avg_pred_all,
    -- Final fallback: lag-12
    lag12.total_revenue,
    0
  )
```
- **Primary method**: Recent backtest average × (anchor_actual / recent_backtest_actual)
  - Scales historical backtest patterns to current anchor level
  - Uses last 6 anchors for recency weighting
- **Fallback 1**: All-time backtest average (if recent not available)
- **Fallback 2**: Lag-12 actual (if no backtests exist)
- **Fallback 3**: Zero (final safety)

### Confidence Intervals
- **Lower bound (80% PI)**: `greatest(0, y_pred - error_90pct)` (floored at 0)
- **Upper bound (80% PI)**: `y_pred + error_90pct`
- Computed from backtest residuals (10th and 90th percentiles)

---

## COLUMN ALIGNMENT WITH BACKTEST TABLE

### ✅ ALIGNED COLUMNS

| Proc Expects | Backtest Has | Match? |
|--------------|--------------|--------|
| model_run_id | MODEL_RUN_ID | ✅ YES |
| roll_up_shop | ROLL_UP_SHOP | ✅ YES |
| reason_group | REASON_GROUP | ✅ YES |
| anchor_fiscal_yyyymm | ANCHOR_FISCAL_YYYYMM | ✅ YES |
| anchor_month_seq | ANCHOR_MONTH_SEQ | ✅ YES |
| horizon | HORIZON | ✅ YES |
| target_month_seq | TARGET_MONTH_SEQ | ✅ YES |
| y_true | Y_TRUE | ✅ YES |
| y_pred | Y_PRED | ✅ YES |

### ❌ COLUMN MISMATCHES

**NONE**: All columns expected by the procedure exist in FORECAST_MODEL_BACKTEST_PREDICTIONS.

---

## CRITICAL FINDINGS

### ✅ ALIGNED BEHAVIOR

1. **Procedure does NOT expect FORECAST_MODEL_PREDICTIONS**
   - Uses FORECAST_MODEL_BACKTEST_PREDICTIONS exclusively
   - Matches reality (FORECAST_MODEL_PREDICTIONS does not exist)

2. **Procedure uses anchor+horizon pattern**
   - Computes target_month_seq = anchor_month_seq + horizon (line 178)
   - Joins on month_seq to get target_fiscal_yyyymm (line 177)
   - Does NOT expect a "forecast_month" column

3. **Procedure outputs to dedicated consumption tables**
   - FORECAST_OUTPUT_PC_REASON_MTH (PC × Reason × Month level)
   - FORECAST_OUTPUT_PC_REASON_CUST_MTH (Customer level disaggregation)
   - These are SEPARATE from backtest table (no overwrite risk to backtest data)

4. **Procedure requires backtest data with y_true**
   - Needs historical backtests to compute residuals for prediction intervals
   - Needs recent backtests (last 6 anchors) to scale predictions
   - Works correctly with current FORECAST_MODEL_BACKTEST_PREDICTIONS schema

### ⚠️ POTENTIAL ISSUES

1. **No snapshot of published forecasts**
   - Procedure INSERTs into FORECAST_OUTPUT_PC_REASON_MTH
   - No DELETE or TRUNCATE before INSERT (line 397: `insert into ...`)
   - **QUESTION**: Is FORECAST_OUTPUT_PC_REASON_MTH append-only?
   - **QUESTION**: Is there a monthly archival process?
   - **RISK**: If table is re-created or truncated elsewhere, historical forecasts are lost

2. **Assumes backtest table is stable**
   - Scoring logic depends on backtest averages (last 6 anchors)
   - If backtest table is truncated (as in notebook line 404 `DELETE FROM FORECAST_MODEL_BACKTEST_PREDICTIONS`), scoring will fail or fall back to lag-12
   - **RISK**: Backtest deletions in notebook directly impact production scoring quality

3. **No handling of missing backtest data for new PCs**
   - If a PC×Reason×Horizon combo has no backtest rows, falls back to lag-12 or zero
   - May produce poor forecasts for new PCs or newly champion models

4. **Confidence intervals require backtest residuals**
   - If backtest table is missing y_true (future predictions), residuals can't be computed
   - Falls back to ±20% of point estimate (line 321: `abs(mp.y_pred * 0.2)`)

---

## TABLE WRITE PATTERNS

### FORECAST_OUTPUT_PC_REASON_MTH

**Current Pattern** (line 397):
```sql
insert into DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
select * from TMP_FORECAST_PC_REASON;
```

**Semantics**: APPEND-ONLY INSERT
- **No DELETE or TRUNCATE before insert**
- **Primary key**: (forecast_run_id, roll_up_shop, reason_group, target_fiscal_yyyymm)
- **Risk Assessment**: 
  - ✅ Safe: Each forecast_run_id is unique (UUID generated at runtime)
  - ✅ Safe: Re-running with same forecast_run_id would violate PK and fail (preventing duplicates)
  - ⚠️ Issue: No cleanup of old forecast_run_ids (table grows indefinitely)
  - ⚠️ Issue: No monthly snapshot or archival process evident

**Recommended Pattern**: Add monthly archival to historical snapshot table

### FORECAST_OUTPUT_PC_REASON_CUST_MTH

**Current Pattern** (line 446):
```sql
insert into DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_CUST_MTH
select * from TMP_FORECAST_PC_REASON_CUST;
```

**Semantics**: APPEND-ONLY INSERT (same as above)
- **Primary key**: (forecast_run_id, roll_up_shop, reason_group, cust_grp, target_fiscal_yyyymm)
- **Risk Assessment**: Same as PC×Reason table

---

## MISMATCHES VS PROFILING RESULTS

### ✅ NO MISMATCHES FOUND

The scoring procedure:
- Expects FORECAST_MODEL_BACKTEST_PREDICTIONS (matches reality)
- Uses anchor+horizon→target pattern (matches backtest schema)
- Uses all 14 columns from backtest table correctly
- Does NOT expect FORECAST_MODEL_PREDICTIONS (matches reality - table does not exist)
- Does NOT expect a "forecast_month" column (matches reality)

### Champion Selection

**Procedure Logic** (lines 125-149):
```sql
champions as (
  select champion_scope, roll_up_shop, reason_group, model_run_id, selection_metric
  from FORECAST_MODEL_CHAMPIONS
  where asof_fiscal_yyyymm = :V_ASOF_YYYYMM
)

-- Join logic prioritizes PC_REASON champion over GLOBAL
coalesce(
  pc_reason_champ.model_run_id,
  global_champ.model_run_id
) as model_run_id
```

**Alignment**: ✅ Uses FORECAST_MODEL_CHAMPIONS table correctly
- Supports both GLOBAL and PC_REASON scopes
- Per-series champions override global champion

---

## RECOMMENDATIONS

### 1. Add Monthly Snapshot for Published Forecasts

**Issue**: FORECAST_OUTPUT_PC_REASON_MTH grows indefinitely with no archival
**Risk**: No historical audit trail if table is recreated or data is lost
**Remediation**: Create monthly snapshot table

```sql
-- Example monthly snapshot (non-destructive)
create table if not exists FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT (
  snapshot_month number,
  snapshot_at timestamp_ntz,
  forecast_run_id string,
  -- ... all columns from FORECAST_OUTPUT_PC_REASON_MTH
);

-- Monthly job to snapshot (idempotent)
insert into FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT
select 
  :SNAPSHOT_MONTH as snapshot_month,
  current_timestamp() as snapshot_at,
  *
from FORECAST_OUTPUT_PC_REASON_MTH
where asof_fiscal_yyyymm = :SNAPSHOT_MONTH
  and not exists (
    select 1 from FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT s
    where s.snapshot_month = :SNAPSHOT_MONTH
      and s.forecast_run_id = FORECAST_OUTPUT_PC_REASON_MTH.forecast_run_id
  );
```

### 2. Protect Backtest Table from Accidental Deletion

**Issue**: Backtest notebook has `DELETE FROM FORECAST_MODEL_BACKTEST_PREDICTIONS` (line 404)
**Risk**: Deleting backtest data breaks scoring procedure (falls back to lag-12 or zero)
**Remediation**: 
- Add WHERE clause to DELETE (only delete specific model_run_id)
- Archive backtest rows before deletion
- Document dependency in notebook header

### 3. Add View for "Published Forecasts" from Backtest Table

**Current**: Scoring procedure reads backtest table directly
**Issue**: No clear separation of "backtest" vs "published forecast" rows
**Remediation**: Create view `VW_FORECAST_NEXT12_BY_SERIES` that filters backtest table for:
- Champion model_run_id only
- Most recent anchor month
- Horizons 1-12
- Computes forecast_month = anchor + horizon

This view would serve Finance stakeholders while maintaining single source of truth.

---

## SUMMARY

| Aspect | Status | Notes |
|--------|--------|-------|
| **Table alignment** | ✅ PASS | Uses FORECAST_MODEL_BACKTEST_PREDICTIONS correctly |
| **Column alignment** | ✅ PASS | All expected columns exist in backtest table |
| **Anchor+horizon pattern** | ✅ PASS | Computes target from anchor+horizon, no forecast_month expected |
| **Champion selection** | ✅ PASS | Uses FORECAST_MODEL_CHAMPIONS table |
| **Output tables** | ✅ PASS | Writes to dedicated consumption tables (no overwrite risk to backtest) |
| **Snapshot/archival** | ⚠️ MISSING | No monthly snapshot for published forecasts |
| **Backtest protection** | ⚠️ RISK | Backtest table vulnerable to DELETE in notebook |
| **Confidence intervals** | ✅ PASS | Computed from backtest residuals, falls back to ±20% |

**OVERALL**: Procedure is well-aligned with Snowflake reality. Main risks are:
1. No snapshot for published forecasts (audit trail gap)
2. Backtest table vulnerable to accidental deletion (production impact)
