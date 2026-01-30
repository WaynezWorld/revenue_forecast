-- 09__proc__build_model_dataset_pc_reason_h.sql

create or replace table DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_DATASET_PC_REASON_H (
  asof_fiscal_yyyymm    number,
  run_id                string,

  roll_up_shop          string,
  reason_group          string,

  anchor_fiscal_yyyymm  number,
  anchor_month_seq      number,
  anchor_fiscal_year    number,
  anchor_fiscal_month   number,

  horizon               number,          -- 1..12
  target_fiscal_yyyymm  number,
  target_month_seq      number,

  y_revenue             number(18,2),    -- target
  budget_target         number(18,2),

  fiscal_month_sin      float,
  fiscal_month_cos      float,

  lag_1                 number(18,2),
  lag_2                 number(18,2),
  lag_3                 number(18,2),
  lag_6                 number(18,2),
  lag_12                number(18,2),

  roll_mean_3           float,
  roll_mean_6           float,
  roll_mean_12          float,
  roll_std_12           float,

  yoy_diff_12           float,
  yoy_pct_12            float,

  budget_anchor         number(18,2),
  budget_lag_12         number(18,2),

  built_at              timestamp_ntz,
  row_hash              string,

  primary key (asof_fiscal_yyyymm, run_id, roll_up_shop, reason_group, anchor_fiscal_yyyymm, horizon)
);

create or replace table DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_DATASET_PC_REASON_H_SNAP (
  run_id                string,
  asof_fiscal_yyyymm     number,

  roll_up_shop          string,
  reason_group          string,

  anchor_fiscal_yyyymm  number,
  anchor_month_seq      number,
  anchor_fiscal_year    number,
  anchor_fiscal_month   number,

  horizon               number,
  target_fiscal_yyyymm  number,
  target_month_seq      number,

  y_revenue             number(18,2),
  budget_target         number(18,2),

  fiscal_month_sin      float,
  fiscal_month_cos      float,

  lag_1                 number(18,2),
  lag_2                 number(18,2),
  lag_3                 number(18,2),
  lag_6                 number(18,2),
  lag_12                number(18,2),

  roll_mean_3           float,
  roll_mean_6           float,
  roll_mean_12          float,
  roll_std_12           float,

  yoy_diff_12           float,
  yoy_pct_12            float,

  budget_anchor         number(18,2),
  budget_lag_12         number(18,2),

  built_at              timestamp_ntz,
  row_hash              string
);

create or replace procedure DB_BI_P_SANDBOX.SANDBOX.SP_BUILD_MODEL_DATASET_PC_REASON_H(
    P_RUN_ID string,
    P_ASOF_FISCAL_YYYYMM number default null,
    P_MAX_HORIZON number default 12
)
returns variant
language sql
execute as caller
as
$$
declare
  V_ASOF_YYYYMM number;
  V_ASOF_SEQ number;

  V_MIN_ANCHOR_SEQ number;
  V_MAX_ANCHOR_SEQ number;

begin
  -- Resolve as-of
  if (P_ASOF_FISCAL_YYYYMM is null) then
    select fiscal_yyyymm, month_seq
      into :V_ASOF_YYYYMM, :V_ASOF_SEQ
    from DB_BI_P_SANDBOX.SANDBOX.FORECAST_ASOF_FISCAL_MONTH;
  else
    select fiscal_yyyymm, month_seq
      into :V_ASOF_YYYYMM, :V_ASOF_SEQ
    from DB_BI_P_SANDBOX.SANDBOX.FORECAST_FISCAL_MONTH_DIM
    where fiscal_yyyymm = :P_ASOF_FISCAL_YYYYMM;
  end if;

  if (V_ASOF_YYYYMM is null) then
    return object_construct('status','ERROR','message','Could not resolve as-of fiscal month.');
  end if;

  -- Anchors must allow targets up to horizon months ahead (targets must be <= ASOF)
  V_MAX_ANCHOR_SEQ := V_ASOF_SEQ - 1;

  -- Need lag_12 and 12-month rolling window available
  V_MIN_ANCHOR_SEQ := 13;

  -- Base series from ACTUALS snapshot
  create or replace temporary table TMP_SERIES as
  select
    a.roll_up_shop,
    a.reason_group,
    a.fiscal_yyyymm,
    a.month_seq,
    a.fiscal_year,
    a.fiscal_month,
    a.total_revenue as revenue,

    lag(a.total_revenue, 1)  over (partition by a.roll_up_shop, a.reason_group order by a.month_seq) as lag_1,
    lag(a.total_revenue, 2)  over (partition by a.roll_up_shop, a.reason_group order by a.month_seq) as lag_2,
    lag(a.total_revenue, 3)  over (partition by a.roll_up_shop, a.reason_group order by a.month_seq) as lag_3,
    lag(a.total_revenue, 6)  over (partition by a.roll_up_shop, a.reason_group order by a.month_seq) as lag_6,
    lag(a.total_revenue, 12) over (partition by a.roll_up_shop, a.reason_group order by a.month_seq) as lag_12,

    avg(a.total_revenue) over (
      partition by a.roll_up_shop, a.reason_group
      order by a.month_seq
      rows between 2 preceding and current row
    ) as roll_mean_3,

    avg(a.total_revenue) over (
      partition by a.roll_up_shop, a.reason_group
      order by a.month_seq
      rows between 5 preceding and current row
    ) as roll_mean_6,

    avg(a.total_revenue) over (
      partition by a.roll_up_shop, a.reason_group
      order by a.month_seq
      rows between 11 preceding and current row
    ) as roll_mean_12,

    stddev_samp(a.total_revenue) over (
      partition by a.roll_up_shop, a.reason_group
      order by a.month_seq
      rows between 11 preceding and current row
    ) as roll_std_12

  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_ACTUALS_PC_REASON_MTH_SNAP a
  join DB_BI_P_SANDBOX.SANDBOX.FORECAST_PC_ELIGIBILITY e
    on e.run_id = :P_RUN_ID
   and e.roll_up_shop = a.roll_up_shop
   and e.is_eligible = true
  where a.run_id = :P_RUN_ID
    and a.asof_fiscal_yyyymm = :V_ASOF_YYYYMM;

  -- Budget series (anchor/target features)
  create or replace temporary table TMP_BUD as
  select
    b.roll_up_shop,
    b.reason_group,
    b.month_seq,
    b.total_budget
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_BUDGET_PC_REASON_MTH_SNAP b
  where b.run_id = :P_RUN_ID
    and b.asof_fiscal_yyyymm = :V_ASOF_YYYYMM;

  -- Horizons grid (1..P_MAX_HORIZON)
  create or replace temporary table TMP_H as
  select seq4()+1 as horizon
  from table(generator(rowcount => :P_MAX_HORIZON));

  -- Supervised rows
  create or replace temporary table TMP_DS as
  with anchors as (
    select *
    from TMP_SERIES
    where month_seq between :V_MIN_ANCHOR_SEQ and :V_MAX_ANCHOR_SEQ
      and lag_12 is not null
  ),
  targets as (
    select roll_up_shop, reason_group, month_seq, fiscal_yyyymm, revenue as y_revenue
    from TMP_SERIES
  )
  select
    :V_ASOF_YYYYMM as asof_fiscal_yyyymm,
    :P_RUN_ID::string as run_id,

    a.roll_up_shop,
    a.reason_group,

    a.fiscal_yyyymm as anchor_fiscal_yyyymm,
    a.month_seq     as anchor_month_seq,
    a.fiscal_year   as anchor_fiscal_year,
    a.fiscal_month  as anchor_fiscal_month,

    h.horizon,
    t.fiscal_yyyymm as target_fiscal_yyyymm,
    t.month_seq     as target_month_seq,

    t.y_revenue,
    bt.total_budget as budget_target,

    sin(2 * pi() * (a.fiscal_month / 12.0)) as fiscal_month_sin,
    cos(2 * pi() * (a.fiscal_month / 12.0)) as fiscal_month_cos,

    a.lag_1, a.lag_2, a.lag_3, a.lag_6, a.lag_12,
    a.roll_mean_3, a.roll_mean_6, a.roll_mean_12, a.roll_std_12,

    (a.revenue - a.lag_12) as yoy_diff_12,
    iff(a.lag_12 = 0, null, (a.revenue - a.lag_12) / nullif(a.lag_12,0)) as yoy_pct_12,

    ba.total_budget as budget_anchor,
    bb12.total_budget as budget_lag_12,

    current_timestamp() as built_at,

    md5(
      coalesce(:V_ASOF_YYYYMM::string,'') || '|' ||
      coalesce(:P_RUN_ID::string,'') || '|' ||
      coalesce(a.roll_up_shop,'') || '|' ||
      coalesce(a.reason_group,'') || '|' ||
      coalesce(a.fiscal_yyyymm::string,'') || '|' ||
      coalesce(h.horizon::string,'')
    ) as row_hash

  from anchors a
  join TMP_H h on 1=1
  join targets t
    on t.roll_up_shop = a.roll_up_shop
   and t.reason_group = a.reason_group
   and t.month_seq    = a.month_seq + h.horizon
  left join TMP_BUD bt
    on bt.roll_up_shop = a.roll_up_shop
   and bt.reason_group = a.reason_group
   and bt.month_seq    = a.month_seq + h.horizon
  left join TMP_BUD ba
    on ba.roll_up_shop = a.roll_up_shop
   and ba.reason_group = a.reason_group
   and ba.month_seq    = a.month_seq
  left join TMP_BUD bb12
    on bb12.roll_up_shop = a.roll_up_shop
   and bb12.reason_group = a.reason_group
   and bb12.month_seq    = a.month_seq - 12
  ;

  -- Upsert CURRENT
  merge into DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_DATASET_PC_REASON_H t
  using TMP_DS s
  on  t.asof_fiscal_yyyymm   = s.asof_fiscal_yyyymm
  and t.run_id               = s.run_id
  and t.roll_up_shop         = s.roll_up_shop
  and t.reason_group         = s.reason_group
  and t.anchor_fiscal_yyyymm = s.anchor_fiscal_yyyymm
  and t.horizon              = s.horizon
  when matched and (t.row_hash <> s.row_hash) then update set
    anchor_month_seq     = s.anchor_month_seq,
    anchor_fiscal_year   = s.anchor_fiscal_year,
    anchor_fiscal_month  = s.anchor_fiscal_month,
    target_fiscal_yyyymm = s.target_fiscal_yyyymm,
    target_month_seq     = s.target_month_seq,
    y_revenue            = s.y_revenue,
    budget_target        = s.budget_target,
    fiscal_month_sin     = s.fiscal_month_sin,
    fiscal_month_cos     = s.fiscal_month_cos,
    lag_1                = s.lag_1,
    lag_2                = s.lag_2,
    lag_3                = s.lag_3,
    lag_6                = s.lag_6,
    lag_12               = s.lag_12,
    roll_mean_3          = s.roll_mean_3,
    roll_mean_6          = s.roll_mean_6,
    roll_mean_12         = s.roll_mean_12,
    roll_std_12          = s.roll_std_12,
    yoy_diff_12          = s.yoy_diff_12,
    yoy_pct_12           = s.yoy_pct_12,
    budget_anchor        = s.budget_anchor,
    budget_lag_12        = s.budget_lag_12,
    built_at             = s.built_at,
    row_hash             = s.row_hash
  when not matched then insert (
    asof_fiscal_yyyymm, run_id,
    roll_up_shop, reason_group,
    anchor_fiscal_yyyymm, anchor_month_seq, anchor_fiscal_year, anchor_fiscal_month,
    horizon, target_fiscal_yyyymm, target_month_seq,
    y_revenue, budget_target,
    fiscal_month_sin, fiscal_month_cos,
    lag_1, lag_2, lag_3, lag_6, lag_12,
    roll_mean_3, roll_mean_6, roll_mean_12, roll_std_12,
    yoy_diff_12, yoy_pct_12,
    budget_anchor, budget_lag_12,
    built_at, row_hash
  ) values (
    s.asof_fiscal_yyyymm, s.run_id,
    s.roll_up_shop, s.reason_group,
    s.anchor_fiscal_yyyymm, s.anchor_month_seq, s.anchor_fiscal_year, s.anchor_fiscal_month,
    s.horizon, s.target_fiscal_yyyymm, s.target_month_seq,
    s.y_revenue, s.budget_target,
    s.fiscal_month_sin, s.fiscal_month_cos,
    s.lag_1, s.lag_2, s.lag_3, s.lag_6, s.lag_12,
    s.roll_mean_3, s.roll_mean_6, s.roll_mean_12, s.roll_std_12,
    s.yoy_diff_12, s.yoy_pct_12,
    s.budget_anchor, s.budget_lag_12,
    s.built_at, s.row_hash
  );

  -- Snapshot overwrite-by-run
  delete from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_DATASET_PC_REASON_H_SNAP
  where run_id = :P_RUN_ID
    and asof_fiscal_yyyymm = :V_ASOF_YYYYMM;

  insert into DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_DATASET_PC_REASON_H_SNAP
  select
    run_id, asof_fiscal_yyyymm,
    roll_up_shop, reason_group,
    anchor_fiscal_yyyymm, anchor_month_seq, anchor_fiscal_year, anchor_fiscal_month,
    horizon, target_fiscal_yyyymm, target_month_seq,
    y_revenue, budget_target,
    fiscal_month_sin, fiscal_month_cos,
    lag_1, lag_2, lag_3, lag_6, lag_12,
    roll_mean_3, roll_mean_6, roll_mean_12, roll_std_12,
    yoy_diff_12, yoy_pct_12,
    budget_anchor, budget_lag_12,
    built_at, row_hash
  from TMP_DS;

  return object_construct(
    'status','OK',
    'run_id', :P_RUN_ID,
    'asof_fiscal_yyyymm', :V_ASOF_YYYYMM,
    'anchors_min_seq', :V_MIN_ANCHOR_SEQ,
    'anchors_max_seq', :V_MAX_ANCHOR_SEQ,
    'max_horizon', :P_MAX_HORIZON,
    'rows_out', (select count(*) from TMP_DS)
  );
end;
$$;

call DB_BI_P_SANDBOX.SANDBOX.SP_START_FORECAST_RUN('MONTHLY');



select count(*) as rows_dataset_snap
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_DATASET_PC_REASON_H_SNAP
where run_id = (select run_id from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS order by triggered_at desc limit 1);


select $1 as proc_result
from table(result_scan(last_query_id()));

with last_run as (
  select run_id
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS
  order by triggered_at desc
  limit 1
)
select
  count(*) as rows_snap,
  count(distinct roll_up_shop || '|' || reason_group) as pc_reason_pairs,
  min(horizon) as min_h,
  max(horizon) as max_h
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_DATASET_PC_REASON_H_SNAP
where run_id = (select run_id from last_run);



desc table DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_DATASET_PC_REASON_H_SNAP;

