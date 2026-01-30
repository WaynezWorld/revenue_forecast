-- BUSINESS FORECAST REPORT
-- Share this with business partners for planning and decision-making

-- ============================================================================
-- EXECUTIVE SUMMARY - Monthly Forecast with Confidence Intervals
-- ============================================================================
-- This shows total revenue forecast by month with ranges
select
  target_fiscal_yyyymm as fiscal_month,
  target_fiscal_year as fiscal_year,
  target_fiscal_month as fiscal_period,
  count(distinct roll_up_shop) as profit_centers,
  round(sum(revenue_forecast), 0) as forecast_revenue,
  round(sum(revenue_forecast_lo), 0) as forecast_low,
  round(sum(revenue_forecast_hi), 0) as forecast_high,
  round(sum(revenue_forecast_hi) - sum(revenue_forecast_lo), 0) as forecast_range
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
where forecast_run_id = '843b7ccf-540b-4707-ad57-1819acc453e2'
group by target_fiscal_yyyymm, target_fiscal_year, target_fiscal_month
order by target_fiscal_yyyymm;


-- ============================================================================
-- PROFIT CENTER SUMMARY - Forecast by PC and Reason
-- ============================================================================
-- Use this to understand which PCs and reasons drive the forecast
select
  roll_up_shop as profit_center,
  reason_group,
  count(*) as months_forecasted,
  round(sum(revenue_forecast), 0) as total_forecast_revenue,
  round(avg(revenue_forecast), 0) as avg_monthly_revenue,
  round(min(revenue_forecast), 0) as min_monthly_revenue,
  round(max(revenue_forecast), 0) as max_monthly_revenue
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
where forecast_run_id = '843b7ccf-540b-4707-ad57-1819acc453e2'
group by roll_up_shop, reason_group
order by total_forecast_revenue desc;


-- ============================================================================
-- YEAR-OVER-YEAR COMPARISON - Forecast vs Prior Year Actuals
-- ============================================================================
-- Shows growth trends vs last year
with forecast_summary as (
  select
    target_fiscal_month as fiscal_month,
    sum(revenue_forecast) as forecast_revenue
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
  where forecast_run_id = '843b7ccf-540b-4707-ad57-1819acc453e2'
  group by target_fiscal_month
),
prior_year_actuals as (
  select
    fiscal_month,
    sum(total_revenue) as prior_year_revenue
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_ACTUALS_PC_REASON_MTH
  where fiscal_year = 2025  -- Prior year
  group by fiscal_month
)
select
  f.fiscal_month,
  round(f.forecast_revenue, 0) as fy2026_forecast,
  round(p.prior_year_revenue, 0) as fy2025_actual,
  round(f.forecast_revenue - p.prior_year_revenue, 0) as variance,
  round((f.forecast_revenue - p.prior_year_revenue) / nullif(p.prior_year_revenue, 0) * 100, 2) as growth_pct
from forecast_summary f
left join prior_year_actuals p
  on p.fiscal_month = f.fiscal_month
order by f.fiscal_month;


-- ============================================================================
-- FORECAST vs BUDGET - Full Year Comparison
-- ============================================================================
with forecast_annual as (
  select
    sum(revenue_forecast) as total_forecast,
    sum(revenue_forecast_lo) as total_forecast_lo,
    sum(revenue_forecast_hi) as total_forecast_hi
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
  where forecast_run_id = '843b7ccf-540b-4707-ad57-1819acc453e2'
),
budget_annual as (
  select
    sum(budget_revenue) as total_budget
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_BUDGET_PC_REASON_MTH
  where fiscal_year = 2026
)
select
  round(f.total_forecast, 0) as forecast_fy2026,
  round(f.total_forecast_lo, 0) as forecast_low,
  round(f.total_forecast_hi, 0) as forecast_high,
  round(b.total_budget, 0) as budget_fy2026,
  round(f.total_forecast - b.total_budget, 0) as variance_to_budget,
  round((f.total_forecast - b.total_budget) / nullif(b.total_budget, 0) * 100, 2) as variance_pct
from forecast_annual f
cross join budget_annual b;


-- ============================================================================
-- TOP CONTRIBUTORS - Largest Revenue Drivers
-- ============================================================================
select
  roll_up_shop as profit_center,
  reason_group,
  round(sum(revenue_forecast), 0) as total_forecast_revenue,
  round(sum(revenue_forecast) / (select sum(revenue_forecast) from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH where forecast_run_id = '843b7ccf-540b-4707-ad57-1819acc453e2') * 100, 2) as pct_of_total
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
where forecast_run_id = '843b7ccf-540b-4707-ad57-1819acc453e2'
group by roll_up_shop, reason_group
order by total_forecast_revenue desc
limit 20;


-- ============================================================================
-- CUSTOMER-LEVEL FORECAST - Top Customers by Revenue
-- ============================================================================
select
  roll_up_shop as profit_center,
  cust_grp as customer_group,
  count(distinct target_fiscal_yyyymm) as months,
  round(sum(revenue_forecast), 0) as total_forecast_revenue,
  round(avg(allocation_share) * 100, 2) as avg_share_pct
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_CUST_MTH
where forecast_run_id = '843b7ccf-540b-4707-ad57-1819acc453e2'
group by roll_up_shop, cust_grp
order by total_forecast_revenue desc
limit 50;


-- ============================================================================
-- EXPORT FORMAT - Clean Monthly Forecast for Excel/PowerBI
-- ============================================================================
-- Copy this result and paste into Excel for business partners
select
  forecast_run_id,
  forecast_created_at,
  roll_up_shop as profit_center,
  reason_group,
  target_fiscal_yyyymm as fiscal_month,
  target_fiscal_year as fiscal_year,
  target_fiscal_month as period,
  horizon as months_ahead,
  round(revenue_forecast, 2) as forecast_revenue,
  round(revenue_forecast_lo, 2) as forecast_low_80pct,
  round(revenue_forecast_hi, 2) as forecast_high_80pct,
  model_family as model_used
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
where forecast_run_id = '843b7ccf-540b-4707-ad57-1819acc453e2'
order by roll_up_shop, reason_group, target_fiscal_yyyymm;
