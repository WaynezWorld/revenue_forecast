-- Backtest Performance Analysis
-- Evaluate how well our models performed on historical data

-- ============================================================================
-- 1. OVERALL MODEL PERFORMANCE vs ACTUALS
-- ============================================================================
select
  model_family,
  count(*) as predictions,
  count(distinct roll_up_shop) as pcs,
  round(avg(abs(y_pred - y_true)), 2) as mae,
  round(avg(abs(y_pred - y_true) / nullif(abs(y_true), 0)) * 100, 2) as mape_pct,
  round(sum(abs(y_pred - y_true)) / nullif(sum(abs(y_true)), 0) * 100, 2) as wape_pct,
  round(sqrt(avg(power(y_pred - y_true, 2))), 2) as rmse,
  round(corr(y_pred, y_true), 4) as correlation
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS bp
join DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS mr
  on mr.model_run_id = bp.model_run_id
where mr.asof_fiscal_yyyymm = 202512
group by model_family
order by wape_pct;


-- ============================================================================
-- 2. PERFORMANCE BY HORIZON (how accuracy degrades over time)
-- ============================================================================
select
  horizon,
  count(*) as predictions,
  round(avg(abs(y_pred - y_true)), 2) as mae,
  round(sum(abs(y_pred - y_true)) / nullif(sum(abs(y_true)), 0) * 100, 2) as wape_pct,
  round(avg(y_pred), 2) as avg_prediction,
  round(avg(y_true), 2) as avg_actual
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS bp
join DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS mr
  on mr.model_run_id = bp.model_run_id
join DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_CHAMPIONS champ
  on champ.model_run_id = bp.model_run_id
where mr.asof_fiscal_yyyymm = 202512
  and champ.asof_fiscal_yyyymm = 202512
group by horizon
order by horizon;


-- ============================================================================
-- 3. CHAMPION MODEL PERFORMANCE (the one we're using for forecasts)
-- ============================================================================
with champion_model as (
  select model_run_id
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_CHAMPIONS
  where asof_fiscal_yyyymm = 202512
    and champion_scope = 'GLOBAL'
)
select
  mr.model_family,
  mr.model_run_id,
  count(*) as predictions,
  round(avg(abs(y_pred - y_true)), 2) as mae,
  round(sum(abs(y_pred - y_true)) / nullif(sum(abs(y_true)), 0) * 100, 2) as wape_pct,
  round(sum(y_pred), 2) as total_predicted,
  round(sum(y_true), 2) as total_actual,
  round((sum(y_pred) - sum(y_true)) / nullif(sum(y_true), 0) * 100, 2) as bias_pct
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS bp
join DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS mr
  on mr.model_run_id = bp.model_run_id
where bp.model_run_id in (select model_run_id from champion_model)
group by mr.model_family, mr.model_run_id;


-- ============================================================================
-- 4. WORST PERFORMING SERIES (where to improve)
-- ============================================================================
with champion_model as (
  select model_run_id
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_CHAMPIONS
  where asof_fiscal_yyyymm = 202512
    and champion_scope = 'GLOBAL'
)
select
  roll_up_shop,
  reason_group,
  count(*) as predictions,
  round(sum(abs(y_pred - y_true)) / nullif(sum(abs(y_true)), 0) * 100, 2) as wape_pct,
  round(avg(abs(y_true)), 2) as avg_actual_revenue
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS
where model_run_id in (select model_run_id from champion_model)
group by roll_up_shop, reason_group
having sum(abs(y_true)) > 50000  -- Only material series
order by wape_pct desc
limit 20;


-- ============================================================================
-- 5. FORECAST vs BUDGET COMPARISON (for current forecast period)
-- ============================================================================
-- Compare our new forecasts to budget for the forecast period
select
  f.target_fiscal_yyyymm,
  round(sum(f.revenue_forecast), 2) as total_forecast,
  round(sum(b.budget_revenue), 2) as total_budget,
  round(sum(f.revenue_forecast) - sum(b.budget_revenue), 2) as variance,
  round((sum(f.revenue_forecast) - sum(b.budget_revenue)) / nullif(sum(b.budget_revenue), 0) * 100, 2) as variance_pct,
  count(distinct f.roll_up_shop) as pcs_forecasted
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH f
left join DB_BI_P_SANDBOX.SANDBOX.FORECAST_BUDGET_PC_REASON_MTH b
  on b.roll_up_shop = f.roll_up_shop
 and b.reason_group = f.reason_group
 and b.fiscal_yyyymm = f.target_fiscal_yyyymm
where f.forecast_run_id = '843b7ccf-540b-4707-ad57-1819acc453e2'
group by f.target_fiscal_yyyymm
order by f.target_fiscal_yyyymm;


-- ============================================================================
-- 6. ACTUALS vs FORECAST TREND (last 12 months actual + 12 months forecast)
-- ============================================================================
-- Historical actuals
select
  fiscal_yyyymm as period,
  'ACTUAL' as type,
  round(sum(total_revenue), 2) as revenue,
  null as revenue_lo,
  null as revenue_hi
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_ACTUALS_PC_REASON_MTH
where month_seq between 49 and 60  -- Last 12 months
group by fiscal_yyyymm

union all

-- Future forecasts
select
  target_fiscal_yyyymm as period,
  'FORECAST' as type,
  round(sum(revenue_forecast), 2) as revenue,
  round(sum(revenue_forecast_lo), 2) as revenue_lo,
  round(sum(revenue_forecast_hi), 2) as revenue_hi
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
where forecast_run_id = '843b7ccf-540b-4707-ad57-1819acc453e2'
group by target_fiscal_yyyymm

order by period;
