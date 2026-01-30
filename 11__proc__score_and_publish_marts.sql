-- 11__proc__score_and_publish_marts.sql
-- 
-- This procedure generates forecasts for future periods using champion models
-- and publishes results to consumption tables.

-- ========================================================================
-- FORECAST OUTPUT TABLES
-- ========================================================================

-- Final forecast output table (PC x Reason x Month level)
create or replace table DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH (
  forecast_run_id      string,
  asof_fiscal_yyyymm    number,
  forecast_created_at  timestamp_ntz,
  
  roll_up_shop         string,
  reason_group         string,
  
  target_fiscal_yyyymm number,
  target_fiscal_year   number,
  target_fiscal_month  number,
  target_month_seq     number,
  target_month_start   date,
  target_month_end     date,
  
  horizon              number,
  
  revenue_forecast     number(18,2),
  revenue_forecast_lo  number(18,2),
  revenue_forecast_hi  number(18,2),
  
  model_run_id         string,
  model_family         string,
  model_scope          string,
  
  published_at         timestamp_ntz,
  
  primary key (forecast_run_id, roll_up_shop, reason_group, target_fiscal_yyyymm)
);

-- Forecast by customer group (disaggregated)
create or replace table DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_CUST_MTH (
  forecast_run_id      string,
  asof_fiscal_yyyymm    number,
  forecast_created_at  timestamp_ntz,
  
  roll_up_shop         string,
  reason_group         string,
  cust_grp             string,
  
  target_fiscal_yyyymm number,
  target_fiscal_year   number,
  target_fiscal_month  number,
  target_month_seq     number,
  target_month_start   date,
  target_month_end     date,
  
  horizon              number,
  
  revenue_forecast     number(18,2),
  allocation_share     number(18,8),
  allocation_level     string,
  
  published_at         timestamp_ntz,
  
  primary key (forecast_run_id, roll_up_shop, reason_group, cust_grp, target_fiscal_yyyymm)
);


-- ========================================================================
-- STORED PROCEDURE: SCORE AND PUBLISH
-- ========================================================================

create or replace procedure DB_BI_P_SANDBOX.SANDBOX.SP_SCORE_AND_PUBLISH_FORECASTS(
    P_RUN_ID string,
    P_ASOF_FISCAL_YYYYMM number default null,
    P_MAX_HORIZON number default 12,
    P_FORECAST_RUN_ID string default null
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
  V_FORECAST_RUN_ID string;
  
  V_ANCHOR_SEQ number;
  V_ROWS_FORECAST number;
  V_ROWS_CUST_FORECAST number;
begin

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

  -- Use provided forecast_run_id or generate new one
  if (P_FORECAST_RUN_ID is null) then
    select uuid_string() into :V_FORECAST_RUN_ID;
  else
    V_FORECAST_RUN_ID := P_FORECAST_RUN_ID;
  end if;

  -- The anchor for scoring is the latest closed month
  V_ANCHOR_SEQ := V_ASOF_SEQ;

  -- ========================================================================
  -- STEP 1: Generate forecasts at PC x Reason level
  -- ========================================================================
  
  create or replace temporary table TMP_FORECAST_PC_REASON as
  with
  -- Get champion models (global or per-series)
  champions as (
    select
      champion_scope,
      roll_up_shop,
      reason_group,
      model_run_id,
      selection_metric
    from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_CHAMPIONS
    where asof_fiscal_yyyymm = :V_ASOF_YYYYMM
  ),
  
  -- Get eligible PC x Reason combinations
  eligible_series as (
    select distinct
      e.roll_up_shop,
      coalesce(a.reason_group, 'TOTAL') as reason_group
    from DB_BI_P_SANDBOX.SANDBOX.FORECAST_PC_ELIGIBILITY e
    left join DB_BI_P_SANDBOX.SANDBOX.FORECAST_ACTUALS_PC_REASON_MTH a
      on a.roll_up_shop = e.roll_up_shop
    where e.run_id = :P_RUN_ID
      and e.is_eligible = true
  ),
  
  -- Generate future horizons (1 to P_MAX_HORIZON)
  horizons as (
    select seq4() as horizon
    from table(generator(rowcount => :P_MAX_HORIZON))
  ),
  
  -- Cross join to create all combinations to score
  score_grid as (
    select
      e.roll_up_shop,
      e.reason_group,
      h.horizon + 1 as horizon,  -- horizon 1..12
      :V_ASOF_YYYYMM as anchor_fiscal_yyyymm,
      :V_ANCHOR_SEQ as anchor_month_seq
    from eligible_series e
    cross join horizons h
  ),
  
  -- Add target month info
  score_grid_with_target as (
    select
      sg.*,
      d.fiscal_yyyymm as target_fiscal_yyyymm,
      d.fiscal_year as target_fiscal_year,
      d.fiscal_month as target_fiscal_month,
      d.month_seq as target_month_seq,
      d.month_start_date as target_month_start,
      d.month_end_date as target_month_end
    from score_grid sg
    join DB_BI_P_SANDBOX.SANDBOX.FORECAST_FISCAL_MONTH_DIM d
      on d.month_seq = sg.anchor_month_seq + sg.horizon
  ),
  
  -- Join with champions to determine which model to use
  score_grid_with_model as (
    select
      sg.*,
      coalesce(
        pc_reason_champ.model_run_id,
        global_champ.model_run_id
      ) as model_run_id,
      coalesce(
        pc_reason_champ.champion_scope,
        global_champ.champion_scope
      ) as champion_scope
    from score_grid_with_target sg
    left join champions global_champ
      on global_champ.champion_scope = 'GLOBAL'
    left join champions pc_reason_champ
      on pc_reason_champ.champion_scope = 'PC_REASON'
     and pc_reason_champ.roll_up_shop = sg.roll_up_shop
     and pc_reason_champ.reason_group = sg.reason_group
  ),
  
  -- Get model metadata
  models as (
    select
      model_run_id,
      model_family,
      model_scope,
      params
    from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS
  ),
  
  -- For now, use simple forecast logic based on model family
  -- In production, you'd load actual model artifacts and score
  forecasts_base as (
    select
      sg.roll_up_shop,
      sg.reason_group,
      sg.target_fiscal_yyyymm,
      sg.target_fiscal_year,
      sg.target_fiscal_month,
      sg.target_month_seq,
      sg.target_month_start,
      sg.target_month_end,
      sg.horizon,
      sg.model_run_id,
      m.model_family,
      m.model_scope,
      
      -- Simple scoring logic (replace with actual model scoring in production)
      -- For baseline: use lag-12 from actuals
      case 
        when m.model_family = 'baseline' then
          coalesce(
            (select total_revenue 
             from DB_BI_P_SANDBOX.SANDBOX.FORECAST_ACTUALS_PC_REASON_MTH a
             where a.roll_up_shop = sg.roll_up_shop
               and a.reason_group = sg.reason_group
               and a.month_seq = sg.target_month_seq - 12
            ), 0
          )
        
        -- For other models: use average of recent actuals as placeholder
        -- TODO: Replace with actual model predictions
        else
          coalesce(
            (select avg(total_revenue)
             from DB_BI_P_SANDBOX.SANDBOX.FORECAST_ACTUALS_PC_REASON_MTH a
             where a.roll_up_shop = sg.roll_up_shop
               and a.reason_group = sg.reason_group
               and a.month_seq between (sg.anchor_month_seq - 11) and sg.anchor_month_seq
            ), 0
          )
      end as revenue_forecast,
      
      -- Placeholder confidence intervals (±20%)
      revenue_forecast * 0.8 as revenue_forecast_lo,
      revenue_forecast * 1.2 as revenue_forecast_hi
      
    from score_grid_with_model sg
    join models m on m.model_run_id = sg.model_run_id
  )
  
  select
    :V_FORECAST_RUN_ID as forecast_run_id,
    :V_ASOF_YYYYMM as asof_fiscal_yyyymm,
    current_timestamp() as forecast_created_at,
    *,
    current_timestamp() as published_at
  from forecasts_base;

  -- Insert into output table
  insert into DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
  select * from TMP_FORECAST_PC_REASON;

  select count(*) into :V_ROWS_FORECAST from TMP_FORECAST_PC_REASON;


  -- ========================================================================
  -- STEP 2: Disaggregate to customer level using mix shares
  -- ========================================================================
  
  create or replace temporary table TMP_FORECAST_PC_REASON_CUST as
  with
  forecasts as (
    select * from TMP_FORECAST_PC_REASON
  ),
  
  cust_mix as (
    select
      roll_up_shop,
      reason_group,
      cust_grp,
      share_abs_rev,
      allocation_level
    from DB_BI_P_SANDBOX.SANDBOX.FORECAST_CUST_MIX_PC_REASON
    where asof_fiscal_yyyymm = :V_ASOF_YYYYMM
  )
  
  select
    f.forecast_run_id,
    f.asof_fiscal_yyyymm,
    f.forecast_created_at,
    f.roll_up_shop,
    f.reason_group,
    cm.cust_grp,
    f.target_fiscal_yyyymm,
    f.target_fiscal_year,
    f.target_fiscal_month,
    f.target_month_seq,
    f.target_month_start,
    f.target_month_end,
    f.horizon,
    (f.revenue_forecast * cm.share_abs_rev) as revenue_forecast,
    cm.share_abs_rev as allocation_share,
    cm.allocation_level,
    current_timestamp() as published_at
  from forecasts f
  join cust_mix cm
    on cm.roll_up_shop = f.roll_up_shop
   and cm.reason_group = f.reason_group;

  -- Insert into customer-level output table
  insert into DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_CUST_MTH
  select * from TMP_FORECAST_PC_REASON_CUST;

  select count(*) into :V_ROWS_CUST_FORECAST from TMP_FORECAST_PC_REASON_CUST;


  -- ========================================================================
  -- RETURN SUCCESS
  -- ========================================================================
  
  return object_construct(
    'status', 'OK',
    'forecast_run_id', :V_FORECAST_RUN_ID,
    'run_id', :P_RUN_ID,
    'asof_fiscal_yyyymm', :V_ASOF_YYYYMM,
    'anchor_month_seq', :V_ANCHOR_SEQ,
    'max_horizon', :P_MAX_HORIZON,
    'rows_pc_reason_forecast', :V_ROWS_FORECAST,
    'rows_customer_forecast', :V_ROWS_CUST_FORECAST
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
-- EXAMPLE USAGE
-- ========================================================================

/*
-- Score and publish forecasts for the latest successful run
call DB_BI_P_SANDBOX.SANDBOX.SP_SCORE_AND_PUBLISH_FORECASTS(
  (select run_id from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS 
   where status = 'SUCCEEDED' order by triggered_at desc limit 1),
  null,  -- use latest closed month
  12     -- forecast 12 months ahead
);

-- View results
select *
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
order by roll_up_shop, reason_group, target_fiscal_yyyymm
limit 100;

-- View customer-level forecasts
select *
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_CUST_MTH
order by roll_up_shop, reason_group, cust_grp, target_fiscal_yyyymm
limit 100;

-- Summary by month
select
  target_fiscal_yyyymm,
  count(distinct roll_up_shop) as pc_count,
  count(distinct roll_up_shop || '|' || reason_group) as pc_reason_count,
  sum(revenue_forecast) as total_forecast,
  avg(horizon) as avg_horizon
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
where forecast_run_id = (
  select forecast_run_id 
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH 
  order by forecast_created_at desc 
  limit 1
)
group by 1
order by 1;
*/
