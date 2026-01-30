-- 08__proc__build_budget_pc_reason_mth.sql

create or replace table DB_BI_P_SANDBOX.SANDBOX.FORECAST_BUDGET_PC_REASON_MTH (
  fiscal_yyyymm        number,
  fiscal_year          number,
  fiscal_month         number,
  month_seq            number,
  month_start_date     date,
  month_end_date       date,

  roll_up_shop         string,
  reason_group         string,

  total_budget         number(18,2),

  source_object        string,
  loaded_at            timestamp_ntz,
  row_hash             string,

  primary key (fiscal_yyyymm, roll_up_shop, reason_group)
);

create or replace table DB_BI_P_SANDBOX.SANDBOX.FORECAST_BUDGET_PC_REASON_MTH_SNAP (
  run_id               string,
  asof_fiscal_yyyymm    number,

  fiscal_yyyymm        number,
  fiscal_year          number,
  fiscal_month         number,
  month_seq            number,
  month_start_date     date,
  month_end_date       date,

  roll_up_shop         string,
  reason_group         string,

  total_budget         number(18,2),

  source_object        string,
  snapshotted_at       timestamp_ntz,
  row_hash             string
);

create or replace procedure DB_BI_P_SANDBOX.SANDBOX.SP_BUILD_BUDGET_PC_REASON_MTH(
    P_RUN_ID string,
    P_ASOF_FISCAL_YYYYMM number default null,
    P_HISTORY_MONTHS number default 72
)
returns variant
language sql
execute as caller
as
$$
declare
  V_ASOF_YYYYMM number;
  V_ASOF_SEQ number;
  V_ASOF_END date;

  V_START_SEQ number;
  V_SRC string;
begin
  V_SRC := 'DB_BI_P_SANDBOX.SANDBOX.RNA_RCT_TMT_CCCI_PL_RO_REVENUE_5YEARS';

  -- Resolve as-of fiscal month
  if (P_ASOF_FISCAL_YYYYMM is null) then
    select fiscal_yyyymm, month_seq, month_end_date
      into :V_ASOF_YYYYMM, :V_ASOF_SEQ, :V_ASOF_END
    from DB_BI_P_SANDBOX.SANDBOX.FORECAST_ASOF_FISCAL_MONTH;
  else
    select fiscal_yyyymm, month_seq, month_end_date
      into :V_ASOF_YYYYMM, :V_ASOF_SEQ, :V_ASOF_END
    from DB_BI_P_SANDBOX.SANDBOX.FORECAST_FISCAL_MONTH_DIM
    where fiscal_yyyymm = :P_ASOF_FISCAL_YYYYMM;
  end if;

  if (V_ASOF_YYYYMM is null) then
    return object_construct('status','ERROR','message','Could not resolve as-of fiscal month.');
  end if;

  V_START_SEQ := greatest(1, V_ASOF_SEQ - (P_HISTORY_MONTHS - 1));

  -- Stage budget window at PC x Reason x Month
  create or replace temporary table TMP_BUD_PC_REASON as
  with base as (
    select
      (try_to_number(v."Year")*100 + try_to_number(v."Period")) as fiscal_yyyymm,
      v.roll_up_shop::string as roll_up_shop,
      coalesce(v."Reason Code Group"::string, 'UNKNOWN') as reason_group,
      sum(coalesce(v.budget,0)) as total_budget
    from DB_BI_P_SANDBOX.SANDBOX.RNA_RCT_TMT_CCCI_PL_RO_REVENUE_5YEARS v
    group by 1,2,3
  ),
  joined as (
    select
      d.fiscal_yyyymm,
      d.fiscal_year,
      d.fiscal_month,
      d.month_seq,
      d.month_start_date,
      d.month_end_date,
      b.roll_up_shop,
      b.reason_group,
      b.total_budget
    from base b
    join DB_BI_P_SANDBOX.SANDBOX.FORECAST_FISCAL_MONTH_DIM d
      on d.fiscal_yyyymm = b.fiscal_yyyymm
    where d.month_seq between :V_START_SEQ and :V_ASOF_SEQ
  )
  select
    *,
    :V_SRC as source_object,
    current_timestamp() as loaded_at,
    md5(
      coalesce(fiscal_yyyymm::string,'') || '|' ||
      coalesce(roll_up_shop,'') || '|' ||
      coalesce(reason_group,'') || '|' ||
      coalesce(total_budget::string,'')
    ) as row_hash
  from joined;

  -- Upsert CURRENT table
  merge into DB_BI_P_SANDBOX.SANDBOX.FORECAST_BUDGET_PC_REASON_MTH t
  using TMP_BUD_PC_REASON s
  on  t.fiscal_yyyymm = s.fiscal_yyyymm
  and t.roll_up_shop  = s.roll_up_shop
  and t.reason_group  = s.reason_group
  when matched and (t.row_hash <> s.row_hash) then update set
      fiscal_year      = s.fiscal_year,
      fiscal_month     = s.fiscal_month,
      month_seq        = s.month_seq,
      month_start_date = s.month_start_date,
      month_end_date   = s.month_end_date,
      total_budget     = s.total_budget,
      source_object    = s.source_object,
      loaded_at        = s.loaded_at,
      row_hash         = s.row_hash
  when not matched then insert (
      fiscal_yyyymm, fiscal_year, fiscal_month, month_seq, month_start_date, month_end_date,
      roll_up_shop, reason_group, total_budget,
      source_object, loaded_at, row_hash
  ) values (
      s.fiscal_yyyymm, s.fiscal_year, s.fiscal_month, s.month_seq, s.month_start_date, s.month_end_date,
      s.roll_up_shop, s.reason_group, s.total_budget,
      s.source_object, s.loaded_at, s.row_hash
  );

  -- Snapshot table: overwrite-by-run semantics
  delete from DB_BI_P_SANDBOX.SANDBOX.FORECAST_BUDGET_PC_REASON_MTH_SNAP
  where run_id = :P_RUN_ID
    and asof_fiscal_yyyymm = :V_ASOF_YYYYMM;

  insert into DB_BI_P_SANDBOX.SANDBOX.FORECAST_BUDGET_PC_REASON_MTH_SNAP
  (run_id, asof_fiscal_yyyymm,
   fiscal_yyyymm, fiscal_year, fiscal_month, month_seq, month_start_date, month_end_date,
   roll_up_shop, reason_group, total_budget,
   source_object, snapshotted_at, row_hash)
  select
    :P_RUN_ID, :V_ASOF_YYYYMM,
    fiscal_yyyymm, fiscal_year, fiscal_month, month_seq, month_start_date, month_end_date,
    roll_up_shop, reason_group, total_budget,
    source_object, current_timestamp(), row_hash
  from TMP_BUD_PC_REASON;

  return object_construct(
    'status','OK',
    'run_id', :P_RUN_ID,
    'asof_fiscal_yyyymm', :V_ASOF_YYYYMM,
    'month_seq_start', :V_START_SEQ,
    'month_seq_end', :V_ASOF_SEQ,
    'rows_staged', (select count(*) from TMP_BUD_PC_REASON)
  );
end;
$$;

call DB_BI_P_SANDBOX.SANDBOX.SP_BUILD_BUDGET_PC_REASON_MTH(
  (select run_id from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS order by triggered_at desc limit 1),
  (select asof_fiscal_yyyymm from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS order by triggered_at desc limit 1),
  72
);

select $1 as proc_result
from table(result_scan(last_query_id()));
