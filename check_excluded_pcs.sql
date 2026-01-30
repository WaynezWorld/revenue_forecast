-- Check which profit centers were EXCLUDED and why
-- Run this in Snowflake

-- Query 1: Show excluded PCs with exclusion reasons
select
  roll_up_shop,
  is_eligible,
  exclusion_reasons,
  months_present_lookback,
  avg_rev_lookback,
  total_rev_lookback
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_PC_ELIGIBILITY
where run_id = '5d0f5e35-b4e1-4af4-a592-e5f7f05f9686'
  and is_eligible = false
order by roll_up_shop;

-- Query 2: Count eligible vs excluded PCs
select
  is_eligible,
  count(*) as pc_count
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_PC_ELIGIBILITY
where run_id = '5d0f5e35-b4e1-4af4-a592-e5f7f05f9686'
group by is_eligible
order by is_eligible desc;

-- Query 3: Verify forecasted PCs match eligible list
select
  count(distinct f.profit_center) as forecasted_pc_count,
  count(distinct e.roll_up_shop) as eligible_pc_count,
  count(distinct case when e.roll_up_shop is null then f.profit_center end) as pcs_forecasted_but_not_eligible
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH f
left join DB_BI_P_SANDBOX.SANDBOX.FORECAST_PC_ELIGIBILITY e
  on f.profit_center = e.roll_up_shop
 and e.run_id = '5d0f5e35-b4e1-4af4-a592-e5f7f05f9686'
 and e.is_eligible = true
where f.forecast_run_id = '418a2568-5027-4611-a64e-a2e741b9a90b';

-- Query 4: Identify PC×Reason with negative confidence bounds
select
  profit_center,
  pc_reason,
  count(*) as months_with_negative_lo,
  min(revenue_forecast_lo) as min_lo,
  avg(revenue_forecast) as avg_forecast,
  avg(revenue_forecast_lo) as avg_lo
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
where forecast_run_id = '418a2568-5027-4611-a64e-a2e741b9a90b'
  and revenue_forecast_lo < 0
group by profit_center, pc_reason
order by min_lo asc
limit 20;
