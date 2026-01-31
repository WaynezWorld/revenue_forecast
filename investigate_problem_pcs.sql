-- Investigate PCs with worst negative confidence bounds
-- Compare to the excluded PC 752 which had high revenue but was excluded

-- Query 1: Get eligibility stats for the problem PCs (555, 710, 695) vs excluded 752
select
  roll_up_shop,
  is_eligible,
  exclusion_reasons,
  months_present_lookback,
  nonzero_months_lookback,
  avg_rev_lookback,
  avg_abs_rev_lookback,
  total_rev_lookback,
  total_abs_rev_lookback
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_PC_ELIGIBILITY
where run_id = '5d0f5e35-b4e1-4af4-a592-e5f7f05f9686'
  and roll_up_shop in ('555', '710', '695', '752')
order by roll_up_shop;

-- Query 2: Check revenue volatility for these PCs over the last 12 months
-- Get month-by-month revenue to see the pattern
with rev_data as (
  select
    v.roll_up_shop::string as roll_up_shop,
    (try_to_number(v."Year")*100 + try_to_number(v."Period")) as fiscal_yyyymm,
    d.month_seq,
    sum(coalesce(v.revenue, 0)) as revenue
  from DB_BI_P_SANDBOX.SANDBOX.RNA_RCT_TMT_CCCI_PL_RO_REVENUE_5YEARS v
  join DB_BI_P_SANDBOX.SANDBOX.FORECAST_FISCAL_MONTH_DIM d
    on d.fiscal_yyyymm = (try_to_number(v."Year")*100 + try_to_number(v."Period"))
  where v.roll_up_shop::string in ('555', '710', '695', '752')
    and d.month_seq between 49 and 60  -- Last 12 months (asof is 60)
  group by 1, 2, 3
)
select
  roll_up_shop,
  fiscal_yyyymm,
  month_seq,
  revenue,
  round(revenue, 2) as revenue_rounded
from rev_data
order by roll_up_shop, month_seq;

-- Query 3: Calculate volatility metrics (CV, std dev, range) for each PC
with rev_data as (
  select
    v.roll_up_shop::string as roll_up_shop,
    d.month_seq,
    sum(coalesce(v.revenue, 0)) as revenue
  from DB_BI_P_SANDBOX.SANDBOX.RNA_RCT_TMT_CCCI_PL_RO_REVENUE_5YEARS v
  join DB_BI_P_SANDBOX.SANDBOX.FORECAST_FISCAL_MONTH_DIM d
    on d.fiscal_yyyymm = (try_to_number(v."Year")*100 + try_to_number(v."Period"))
  where v.roll_up_shop::string in ('555', '710', '695', '752')
    and d.month_seq between 49 and 60
  group by 1, 2
)
select
  roll_up_shop,
  count(*) as months_with_data,
  round(avg(revenue), 2) as avg_revenue,
  round(stddev(revenue), 2) as stddev_revenue,
  round(min(revenue), 2) as min_revenue,
  round(max(revenue), 2) as max_revenue,
  round(max(revenue) - min(revenue), 2) as range_revenue,
  round(stddev(revenue) / nullif(abs(avg(revenue)), 0), 4) as coefficient_of_variation
from rev_data
group by roll_up_shop
order by roll_up_shop;

-- Query 4: Check backtest error distributions for these PCs to see if wide intervals are justified
select
  roll_up_shop,
  reason_group,
  count(*) as backtest_count,
  round(avg(abs(y_pred - y_true)), 2) as avg_abs_error,
  round(stddev(y_pred - y_true), 2) as stddev_error,
  round(percentile_cont(0.1) within group (order by (y_pred - y_true)), 2) as error_p10,
  round(percentile_cont(0.9) within group (order by (y_pred - y_true)), 2) as error_p90,
  round(percentile_cont(0.9) within group (order by (y_pred - y_true)) - 
        percentile_cont(0.1) within group (order by (y_pred - y_true)), 2) as error_range
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS
where roll_up_shop in ('555', '710', '695', '752')
  and model_run_id in (
    select model_run_id 
    from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_CHAMPIONS 
    where asof_fiscal_yyyymm = 202512
  )
group by roll_up_shop, reason_group
order by roll_up_shop, reason_group;

-- Query 5: For PC 752 specifically - understand why it was excluded
select
  rule_code,
  rule_passed,
  observed_value,
  threshold_value
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_PC_ELIGIBILITY_RULES
where run_id = '5d0f5e35-b4e1-4af4-a592-e5f7f05f9686'
  and roll_up_shop = '752'
order by rule_passed, rule_code;
