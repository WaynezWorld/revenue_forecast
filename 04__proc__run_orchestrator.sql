-- 04__proc__run_orchestrator.sql

create or replace table DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS (
  run_id            string,
  run_type          string,
  triggered_by      string,
  triggered_at      timestamp_ntz,
  asof_fiscal_yyyymm number,
  asof_month_end    date,
  status            string,
  status_message    string,
  config_snapshot   variant,
  created_at        timestamp_ntz default current_timestamp(),
  updated_at        timestamp_ntz default current_timestamp()
);

---------------------------------------------------------------
create or replace procedure DB_BI_P_SANDBOX.SANDBOX.SP_START_FORECAST_RUN(
    P_RUN_TYPE string default 'MONTHLY'
)
returns variant
language sql
execute as caller
as
$$
declare
  V_RUN_ID string;
  V_ASOF_YYYYMM number;
  V_ASOF_END date;

  V_ELIG variant;
  V_ACT  variant;
  V_MIX  variant;
  V_BUD  variant;
  V_DS   variant;

  V_ERR string;
begin
  select fiscal_yyyymm, month_end_date
    into :V_ASOF_YYYYMM, :V_ASOF_END
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_ASOF_FISCAL_MONTH;

  if (V_ASOF_YYYYMM is null) then
    return object_construct('status','ERROR','message','Could not resolve latest closed fiscal month.');
  end if;

  select uuid_string() into :V_RUN_ID;

  insert into DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS
  (run_id, run_type, triggered_by, triggered_at, asof_fiscal_yyyymm, asof_month_end, status, status_message, config_snapshot, updated_at)
  select
    :V_RUN_ID,
    :P_RUN_TYPE,
    current_user(),
    current_timestamp(),
    :V_ASOF_YYYYMM,
    :V_ASOF_END,
    'STARTED',
    null,
    to_variant(object_construct('note','run started')),
    current_timestamp();

  call DB_BI_P_SANDBOX.SANDBOX.SP_EVALUATE_PC_ELIGIBILITY(:V_RUN_ID, :V_ASOF_YYYYMM);
  select $1 into :V_ELIG from table(result_scan(last_query_id()));

  call DB_BI_P_SANDBOX.SANDBOX.SP_BUILD_ACTUALS_PC_REASON_MTH(:V_RUN_ID, :V_ASOF_YYYYMM, 72);
  select $1 into :V_ACT from table(result_scan(last_query_id()));

  call DB_BI_P_SANDBOX.SANDBOX.SP_BUILD_CUST_MIX_PC_REASON(:V_RUN_ID, :V_ASOF_YYYYMM);
  select $1 into :V_MIX from table(result_scan(last_query_id()));

  call DB_BI_P_SANDBOX.SANDBOX.SP_BUILD_BUDGET_PC_REASON_MTH(:V_RUN_ID, :V_ASOF_YYYYMM, 72);
  select $1 into :V_BUD from table(result_scan(last_query_id()));

  call DB_BI_P_SANDBOX.SANDBOX.SP_BUILD_MODEL_DATASET_PC_REASON_H(:V_RUN_ID, :V_ASOF_YYYYMM, 12);
  select $1 into :V_DS from table(result_scan(last_query_id()));

  update DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS
  set status = 'SUCCEEDED',
      status_message = 'Eligibility + Actuals + CustMix + Budget + ModelDataset built',
      config_snapshot = to_variant(object_construct(
        'eligibility', :V_ELIG,
        'actuals_pc_reason_mth', :V_ACT,
        'cust_mix_pc_reason', :V_MIX,
        'budget_pc_reason_mth', :V_BUD,
        'model_dataset_pc_reason_h', :V_DS
      )),
      updated_at = current_timestamp()
  where run_id = :V_RUN_ID;

  return object_construct(
    'status','OK',
    'run_id', :V_RUN_ID,
    'asof_fiscal_yyyymm', :V_ASOF_YYYYMM,
    'asof_month_end', :V_ASOF_END,
    'eligibility', :V_ELIG,
    'actuals_pc_reason_mth', :V_ACT,
    'cust_mix_pc_reason', :V_MIX,
    'budget_pc_reason_mth', :V_BUD,
    'model_dataset_pc_reason_h', :V_DS
  );

exception
  when other then
    V_ERR := SQLERRM;

    if (V_RUN_ID is not null) then
      update DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS
      set status = 'FAILED',
          status_message = :V_ERR,
          updated_at = current_timestamp()
      where run_id = :V_RUN_ID;
    end if;

    return object_construct('status','FAILED','run_id',:V_RUN_ID,'error',:V_ERR);
end;
$$;




call DB_BI_P_SANDBOX.SANDBOX.SP_START_FORECAST_RUN('MONTHLY');

select run_id, triggered_at, status
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS
order by triggered_at desc
limit 1;

select
  count(distinct ("Year"*100 + "Period")) as months_total_in_view,
  count(distinct iff(is_current_month=false, ("Year"*100 + "Period"), null)) as months_closed_in_view,
  max(iff(is_current_month=true, ("Year"*100 + "Period"), null)) as current_month_yyyymm
from DB_BI_P_SANDBOX.SANDBOX.RNA_RCT_TMT_CCCI_PL_RO_REVENUE_5YEARS;


select
  count(*) as rows_actuals_snap,
  count(distinct month_seq) as month_seq_count,
  min(month_seq) as min_seq,
  max(month_seq) as max_seq
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_ACTUALS_PC_REASON_MTH_SNAP
where run_id = '5d0f5e35-b4e1-4af4-a592-e5f7f05f9686';

select
  count(*) as rows_budget_snap,
  count(distinct month_seq) as month_seq_count,
  min(month_seq) as min_seq,
  max(month_seq) as max_seq
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_BUDGET_PC_REASON_MTH_SNAP
where run_id = '5d0f5e35-b4e1-4af4-a592-e5f7f05f9686';

select count(*) as rows_dataset_snap
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_DATASET_PC_REASON_H_SNAP
where run_id = '5d0f5e35-b4e1-4af4-a592-e5f7f05f9686';







select
  run_id,
  status,
  status_message,
  asof_fiscal_yyyymm,
  triggered_at
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS
order by triggered_at desc
limit 1;

select count(*) as rows_dataset_snap
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_DATASET_PC_REASON_H_SNAP
where run_id = '9cbe0ecd-4521-4067-806d-6dad79bdf666';


