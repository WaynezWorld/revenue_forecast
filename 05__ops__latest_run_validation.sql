-- 04
select run_id, status, status_message, config_snapshot
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS
order by triggered_at desc
limit 1;


-- 05__ops__latest_run_validation.sql

-- A) Fiscal anchor sanity
select * from DB_BI_P_SANDBOX.SANDBOX.FORECAST_ASOF_FISCAL_MONTH;

-- B) Config snapshot (eligibility-related)
select *
from DB_BI_P_SANDBOX.SANDBOX.RNA_RCT_CONFIG_SHOP_ELIGIBILITY
order by config_key;

-- C) Latest run header
with last_run as (
  select run_id
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS
  order by triggered_at desc
  limit 1
)
select r.*
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS r
join last_run lr on r.run_id = lr.run_id;

-- D) Eligibility summary counts (latest run)
with last_run as (
  select run_id
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS
  order by triggered_at desc
  limit 1
)
select
  e.is_eligible,
  count(*) as pc_count
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_PC_ELIGIBILITY e
join last_run lr on e.run_id = lr.run_id
group by 1
order by 1 desc;

-- E) Rule breakdown (latest run)
with last_run as (
  select run_id
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS
  order by triggered_at desc
  limit 1
)
select
  rr.rule_code,
  rr.rule_passed,
  count(*) as cnt
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_PC_ELIGIBILITY_RULES rr
join last_run lr on rr.run_id = lr.run_id
group by 1,2
order by 1,2;

-- F) Show excluded PCs with reasons (latest run)
with last_run as (
  select run_id
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS
  order by triggered_at desc
  limit 1
)
select
  e.roll_up_shop,
  e.total_rev_lookback,
  e.avg_rev_lookback,
  e.exclusion_reasons,
  e.thresholds
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_PC_ELIGIBILITY e
join last_run lr on e.run_id = lr.run_id
where e.is_eligible = false
order by e.total_rev_lookback desc;

call DB_BI_P_SANDBOX.SANDBOX.SP_BUILD_ACTUALS_PC_REASON_MTH(
  (select run_id from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS order by triggered_at desc limit 1),
  (select asof_fiscal_yyyymm from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS order by triggered_at desc limit 1),
  72
);

-- See returned VARIANT from the CALL
select $1 as proc_result
from table(result_scan(last_query_id()));

select count(*) as rows_current
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_ACTUALS_PC_REASON_MTH;

select count(*) as rows_snap
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_ACTUALS_PC_REASON_MTH_SNAP
where run_id = (select run_id from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS order by triggered_at desc limit 1);


-- 07
-- Row counts (ASOF + SNAP should match)
with last_run as (
  select run_id, asof_fiscal_yyyymm
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS
  order by triggered_at desc
  limit 1
)
select
  (select count(*)
   from DB_BI_P_SANDBOX.SANDBOX.FORECAST_CUST_MIX_PC_REASON
   where asof_fiscal_yyyymm = (select asof_fiscal_yyyymm from last_run)
  ) as rows_asof,
  (select count(*)
   from DB_BI_P_SANDBOX.SANDBOX.FORECAST_CUST_MIX_PC_REASON_SNAP
   where run_id = (select run_id from last_run)
  ) as rows_snap;

-- Shares sum to ~1 for any denom > 0
with last_run as (
  select run_id
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS
  order by triggered_at desc
  limit 1
)
select
  roll_up_shop,
  reason_group,
  allocation_level,
  round(sum(share_abs_rev), 6) as share_sum,
  max(denominator_abs_rev) as denom_abs_rev
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_CUST_MIX_PC_REASON_SNAP
where run_id = (select run_id from last_run)
group by 1,2,3
having max(denominator_abs_rev) > 0
   and abs(sum(share_abs_rev) - 1) > 0.0001
order by abs(sum(share_abs_rev) - 1) desc
limit 25;

-- Allocation level distribution (quick health signal)
with last_run as (
  select run_id
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS
  order by triggered_at desc
  limit 1
)
select allocation_level, count(distinct roll_up_shop || '|' || reason_group) as pc_reason_pairs
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_CUST_MIX_PC_REASON_SNAP
where run_id = (select run_id from last_run)
group by 1
order by 2 desc;


-- 08
select count(*) as rows_current
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_BUDGET_PC_REASON_MTH;

select count(*) as rows_snap
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_BUDGET_PC_REASON_MTH_SNAP
where run_id = (select run_id from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS order by triggered_at desc limit 1);
