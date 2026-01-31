-- sql/views/vw_forecast_next12_by_series.sql
-- User-friendly view of next 12 months published forecasts
--
-- Purpose: Expose latest published forecasts to Finance stakeholders
-- Data Source: FORECAST_MODEL_BACKTEST_PREDICTIONS (canonical table)
-- Filtering: Champion model_run_id, most recent anchor month, horizons 1-12
--
-- DO NOT EXECUTE AUTOMATICALLY - This is a proposal for manual execution

create or replace view DB_BI_P_SANDBOX.SANDBOX.VW_FORECAST_NEXT12_BY_SERIES
as
with
-- Get most recent anchor month from backtest table
latest_anchor as (
  select max(anchor_fiscal_yyyymm) as anchor_yyyymm
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS
),

-- Get current champion model_run_id (GLOBAL scope)
current_champion as (
  select model_run_id
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_CHAMPIONS
  where champion_scope = 'GLOBAL'
    and asof_fiscal_yyyymm = (select anchor_yyyymm from latest_anchor)
  limit 1
)

select
  -- Anchor (when forecast was created)
  p.anchor_fiscal_yyyymm as forecast_anchor_month,
  d_anchor.fiscal_year as forecast_anchor_year,
  d_anchor.fiscal_month as forecast_anchor_month_num,
  d_anchor.month_end_date as forecast_anchor_date,
  
  -- Series identifiers
  p.roll_up_shop as pc,
  p.reason_group,
  
  -- Target (forecast month) - renamed for Finance users
  p.target_fiscal_yyyymm as forecast_month,
  d_target.fiscal_year as forecast_year,
  d_target.fiscal_month as forecast_month_num,
  d_target.month_start_date as forecast_month_start,
  d_target.month_end_date as forecast_month_end,
  
  -- Horizon (months ahead)
  p.horizon,
  
  -- Forecast values
  p.y_pred as revenue_forecast,
  p.y_pred_lo as revenue_forecast_lower_bound,
  p.y_pred_hi as revenue_forecast_upper_bound,
  
  -- Model metadata
  p.model_run_id,
  r.model_family,
  r.model_scope,
  r.model_tag,
  
  -- Timestamps
  p.created_at as forecast_created_at
  
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS p

-- Join to get champion model_run_id
cross join current_champion c

-- Join to fiscal month dimension for anchor month details
join DB_BI_P_SANDBOX.SANDBOX.FORECAST_FISCAL_MONTH_DIM d_anchor
  on d_anchor.fiscal_yyyymm = p.anchor_fiscal_yyyymm

-- Join to fiscal month dimension for target month details
join DB_BI_P_SANDBOX.SANDBOX.FORECAST_FISCAL_MONTH_DIM d_target
  on d_target.fiscal_yyyymm = p.target_fiscal_yyyymm

-- Join to model runs for metadata
join DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS r
  on r.model_run_id = p.model_run_id

where
  -- Filter for most recent anchor month
  p.anchor_fiscal_yyyymm = (select anchor_yyyymm from latest_anchor)
  
  -- Filter for champion model only
  and p.model_run_id = c.model_run_id
  
  -- Filter for next 12 months (horizons 1-12)
  and p.horizon between 1 and 12

order by
  p.roll_up_shop,
  p.reason_group,
  p.target_fiscal_yyyymm;

-- Grant access to Finance users
-- grant select on DB_BI_P_SANDBOX.SANDBOX.VW_FORECAST_NEXT12_BY_SERIES to role FINANCE_ANALYST_ROLE;

-- Example usage:
-- select * from DB_BI_P_SANDBOX.SANDBOX.VW_FORECAST_NEXT12_BY_SERIES
-- where pc = '555' and horizon = 1
-- order by forecast_month;
