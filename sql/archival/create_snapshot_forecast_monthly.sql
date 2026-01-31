-- sql/archival/create_snapshot_forecast_monthly.sql
-- Monthly snapshot procedure for published forecasts
--
-- Purpose: Archive monthly published forecasts to preserve audit trail
-- Frequency: Run at end of each month or beginning of next month
-- Safety: Idempotent (won't create duplicates)
--
-- DO NOT EXECUTE AUTOMATICALLY - This is a proposal for manual execution

-- ========================================================================
-- STEP 1: Create snapshot table (one-time DDL)
-- ========================================================================

create table if not exists DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT (
  snapshot_month number,           -- YYYYMM of when snapshot was taken
  snapshot_at timestamp_ntz,       -- Timestamp of snapshot
  
  -- All columns from FORECAST_OUTPUT_PC_REASON_MTH
  forecast_run_id string,
  asof_fiscal_yyyymm number,
  forecast_created_at timestamp_ntz,
  roll_up_shop string,
  reason_group string,
  target_fiscal_yyyymm number,
  target_fiscal_year number,
  target_fiscal_month number,
  target_month_seq number,
  target_month_start date,
  target_month_end date,
  horizon number,
  revenue_forecast number(18,2),
  revenue_forecast_lo number(18,2),
  revenue_forecast_hi number(18,2),
  model_run_id string,
  model_family string,
  model_scope string,
  published_at timestamp_ntz,
  
  primary key (snapshot_month, forecast_run_id, roll_up_shop, reason_group, target_fiscal_yyyymm)
);

-- ========================================================================
-- STEP 2: Create stored procedure for monthly snapshot
-- ========================================================================

create or replace procedure DB_BI_P_SANDBOX.SANDBOX.SP_SNAPSHOT_MONTHLY_FORECASTS(
  P_SNAPSHOT_MONTH number default null
)
returns variant
language sql
execute as caller
as
$$
declare
  V_SNAPSHOT_MONTH number;
  V_ROWS_INSERTED number;
  V_LATEST_ASOF number;
begin

  -- Determine snapshot month (default: latest ASOF month in output table)
  if (P_SNAPSHOT_MONTH is null) then
    select max(asof_fiscal_yyyymm)
      into :V_SNAPSHOT_MONTH
    from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH;
  else
    V_SNAPSHOT_MONTH := P_SNAPSHOT_MONTH;
  end if;

  if (V_SNAPSHOT_MONTH is null) then
    return object_construct(
      'status', 'ERROR',
      'message', 'No forecasts found to snapshot'
    );
  end if;

  -- Snapshot all forecast_run_ids for this ASOF month
  -- Idempotent: skips rows that already exist in snapshot table
  insert into DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT
  select 
    :V_SNAPSHOT_MONTH as snapshot_month,
    current_timestamp() as snapshot_at,
    *
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
  where asof_fiscal_yyyymm = :V_SNAPSHOT_MONTH
    and not exists (
      select 1 
      from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT s
      where s.snapshot_month = :V_SNAPSHOT_MONTH
        and s.forecast_run_id = FORECAST_OUTPUT_PC_REASON_MTH.forecast_run_id
        and s.roll_up_shop = FORECAST_OUTPUT_PC_REASON_MTH.roll_up_shop
        and s.reason_group = FORECAST_OUTPUT_PC_REASON_MTH.reason_group
        and s.target_fiscal_yyyymm = FORECAST_OUTPUT_PC_REASON_MTH.target_fiscal_yyyymm
    );

  select count(*) into :V_ROWS_INSERTED 
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT
  where snapshot_month = :V_SNAPSHOT_MONTH;

  return object_construct(
    'status', 'OK',
    'snapshot_month', :V_SNAPSHOT_MONTH,
    'rows_snapshotted', :V_ROWS_INSERTED,
    'snapshot_at', current_timestamp()
  );

exception
  when other then
    return object_construct(
      'status', 'ERROR',
      'message', SQLERRM
    );
end;
$$;

-- ========================================================================
-- STEP 3: Create optional Snowflake TASK for automation
-- ========================================================================

create or replace task DB_BI_P_SANDBOX.SANDBOX.TASK_SNAPSHOT_PUBLISHED_FORECASTS
  warehouse = BI_P_QRY_FIN_OPT_WH
  schedule = 'USING CRON 0 2 1 * * America/New_York'  -- 2am on 1st of each month
  comment = 'Monthly snapshot of published forecasts for audit trail'
  as
  call DB_BI_P_SANDBOX.SANDBOX.SP_SNAPSHOT_MONTHLY_FORECASTS(null);

-- NOTE: Task is created in SUSPENDED state. To enable automation:
-- alter task DB_BI_P_SANDBOX.SANDBOX.TASK_SNAPSHOT_PUBLISHED_FORECASTS resume;

-- ========================================================================
-- STEP 4: Manual execution (if not using automated task)
-- ========================================================================

/*
-- Run at end of January 2026 to snapshot January forecasts:
call DB_BI_P_SANDBOX.SANDBOX.SP_SNAPSHOT_MONTHLY_FORECASTS(202601);

-- Verify snapshot:
select 
  snapshot_month,
  count(distinct forecast_run_id) as distinct_runs,
  count(*) as total_rows,
  min(snapshot_at) as first_snapshot,
  max(snapshot_at) as last_snapshot
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT
group by snapshot_month
order by snapshot_month desc;

-- Compare snapshot to current forecasts:
select 
  'CURRENT' as source,
  count(distinct forecast_run_id) as runs,
  count(*) as rows
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
where asof_fiscal_yyyymm = 202601

union all

select 
  'SNAPSHOT' as source,
  count(distinct forecast_run_id) as runs,
  count(*) as rows
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH_SNAPSHOT
where snapshot_month = 202601;
*/
