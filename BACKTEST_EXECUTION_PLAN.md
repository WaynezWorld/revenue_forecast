# Post-Change Backtest Execution Plan

## Changes Applied

✅ **Notebook Updated**: `10__modeling__backtest_global_plus_overrides.ipynb`
- Removed RIDGE_OHE from CANDIDATES list (cell 4)
- Added audit report reference comments
- CANDIDATES now contains only: SEASONAL_NAIVE_LAG12, GBR_OHE

## Manual Execution Required

Due to Snowflake authentication requirements (external browser auth with specific user), the notebook must be run manually.

### Steps to Execute:

1. **Open Notebook**: `10__modeling__backtest_global_plus_overrides.ipynb` in VS Code or Jupyter

2. **Run All Cells**: Execute cells 1-17 (setup through metric calculation)
   - Cell 1: Session creation
   - Cell 2: Parameters
   - Cell 3: Transform functions
   - Cell 4: **Model candidates (UPDATED - now excludes Ridge)**
   - Cell 5: Model training loop
   - Cell 6: Baseline predictions (SQL)
   - Cell 7-16: Backtest and metric calculations
   - Cell 17: Metrics insertion

3. **Capture Results**: After cell 17 completes, run this query to compare metrics:

```sql
-- Compare metrics: Before (3 models) vs After (2 models)
with current_run as (
  select model_run_id
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS
  where status = 'SUCCEEDED'
    and created_at >= current_date - 1  -- Today's run
),
metrics_summary as (
  select
    r.params:candidate::string as model,
    m.metric_name,
    round(m.value, 2) as value
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_METRICS m
  join DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS r
    on r.model_run_id = m.model_run_id
  join current_run cr on cr.model_run_id = m.model_run_id
  where m.metric_scope = 'OVERALL'
    and m.horizon is null
    and m.metric_name in ('WAPE', 'MAE', 'BIAS')
)
select
  model,
  max(case when metric_name = 'WAPE' then value end) as wape_pct,
  max(case when metric_name = 'MAE' then value end) as mae,
  max(case when metric_name = 'BIAS' then value end) as bias_pct
from metrics_summary
group by 1
order by wape_pct;
```

Expected output:
```
MODEL                 | WAPE_PCT | MAE      | BIAS_PCT
----------------------+----------+----------+----------
GBR_OHE               | 0.28     | 11911.00 | -0.14
SEASONAL_NAIVE_LAG12  | 0.32     | 13423.00 | -0.18
```

(Ridge row ELIMINATED - previously showed 11.31 WAPE = 1131%)

4. **Verify Champion Selection**: Run cell 18+ to select champion

```sql
-- Verify GBR is selected as champion
select
  champion_scope,
  selection_metric,
  r.params:candidate::string as champion_model,
  selection_value,
  selected_at
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_CHAMPIONS c
join DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS r
  on r.model_run_id = c.model_run_id
where c.asof_fiscal_yyyymm = (
  select fiscal_yyyymm
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_ASOF_FISCAL_MONTH
)
order by champion_scope;
```

Expected: GBR_OHE selected as GLOBAL champion with ~28% WAPE

## Metrics Comparison (Expected)

### BEFORE (Ridge included):
| Model | WAPE | MAE | Bias | Status |
|-------|------|-----|------|--------|
| Ridge | 1130.55% | $476,580 | N/A | ❌ FAIL |
| GBR | 28.26% | $11,911 | -14% | ✅ GOOD |
| Baseline | 31.91% | $13,423 | -18% | ✅ OK |

**Issue**: Ridge catastrophic failure due to linear extrapolation in log-space

### AFTER (Ridge removed):
| Model | WAPE | MAE | Bias | Status |
|-------|------|-----|------|--------|
| GBR | 28.26% | $11,911 | -14% | ✅ CHAMPION |
| Baseline | 31.91% | $13,423 | -18% | ✅ FALLBACK |

**Result**: Clean model leaderboard, GBR remains champion (no change in performance)

## Validation Checklist

After running the notebook:

- [ ] Only 2 model_run_ids created (GBR_OHE, SEASONAL_NAIVE_LAG12)
- [ ] No Ridge model_run_id in FORECAST_MODEL_RUNS table
- [ ] GBR WAPE remains ~28% (unchanged from previous run)
- [ ] GBR selected as GLOBAL champion
- [ ] Backtest predictions table contains ~94,068 rows (47,034 per model × 2 models)
- [ ] No metrics with WAPE > 100% (catastrophic failure threshold)

## Success Criteria

✅ **Code committed** (see git log)
✅ **Ridge candidate removed** from notebook
✅ **Documentation added** (audit report reference)
⏳ **Manual execution required** (Snowflake auth)
⏳ **Metrics validation** (after notebook run)

## Next Steps

1. Run the notebook manually (requires Snowflake browser auth)
2. Capture metric comparison screenshot
3. Verify GBR remains champion
4. Update this document with actual results
5. Push commit if all validations pass

---

**Commit**: `Remove Ridge candidate — incompatible with signed_log1p target transform (per metric audit)`
**Author**: Wayne Jones  
**Date**: 2026-01-30
