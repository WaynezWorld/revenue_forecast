-- 07__proc__build_cust_mix_pc_reason.sql

create or replace table DB_BI_P_SANDBOX.SANDBOX.FORECAST_CUST_MIX_PC_REASON (
  asof_fiscal_yyyymm     number,

  roll_up_shop           string,
  reason_group           string,
  cust_grp               string,

  allocation_level       string,          -- PC_REASON | PC | REASON | GLOBAL

  share_abs_rev          number(18,8),
  numerator_abs_rev      number(18,2),
  denominator_abs_rev    number(18,2),

  months_present         number,
  nonzero_months         number,

  thresholds             variant,
  source_object          string,
  computed_at            timestamp_ntz,
  row_hash               string,

  primary key (asof_fiscal_yyyymm, roll_up_shop, reason_group, cust_grp)
);

create or replace table DB_BI_P_SANDBOX.SANDBOX.FORECAST_CUST_MIX_PC_REASON_SNAP (
  run_id                 string,
  asof_fiscal_yyyymm      number,

  roll_up_shop           string,
  reason_group           string,
  cust_grp               string,

  allocation_level       string,

  share_abs_rev          number(18,8),
  numerator_abs_rev      number(18,2),
  denominator_abs_rev    number(18,2),

  months_present         number,
  nonzero_months         number,

  thresholds             variant,
  source_object          string,
  snapshotted_at         timestamp_ntz,
  row_hash               string
);

create or replace procedure DB_BI_P_SANDBOX.SANDBOX.SP_BUILD_CUST_MIX_PC_REASON(
    P_RUN_ID string,
    P_ASOF_FISCAL_YYYYMM number default null
)
returns variant
language sql
execute as caller
as
$$
declare
  V_ASOF_YYYYMM number;
  V_ASOF_SEQ number;

  V_LOOKBACK_MONTHS number;
  V_MIN_NONZERO number;

  V_START_SEQ number;
  V_SRC string;

begin
  V_SRC := 'DB_BI_P_SANDBOX.SANDBOX.RNA_RCT_TMT_CCCI_PL_RO_REVENUE_5YEARS';

  -- Resolve as-of fiscal month
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

  -- Config precedence: MIX_* then reuse existing defaults
  select
    coalesce(
      max(iff(config_key='MIX_LOOKBACK_MONTHS', value_number, null)),
      max(iff(config_key='LOOKBACK_MONTHS', value_number, null)),
      12
    ),
    coalesce(
      max(iff(config_key='MIX_MIN_NONZERO_MONTHS', value_number, null)),
      max(iff(config_key='MIN_NONZERO_MONTHS', value_number, null)),
      3
    )
  into :V_LOOKBACK_MONTHS, :V_MIN_NONZERO
  from DB_BI_P_SANDBOX.SANDBOX.RNA_RCT_CONFIG_SHOP_ELIGIBILITY;

  V_START_SEQ := greatest(1, V_ASOF_SEQ - (V_LOOKBACK_MONTHS - 1));

  -- Eligible PCs for this run
  create or replace temporary table TMP_ELIG_PCS as
  select roll_up_shop
  from DB_BI_P_SANDBOX.SANDBOX.FORECAST_PC_ELIGIBILITY
  where run_id = :P_RUN_ID
    and is_eligible = true;

  -- Customer group dimension (explicit + UNKNOWN)
  create or replace temporary table TMP_CUST_GRPS as
  select column1::string as cust_grp
  from values ('CAPX'),('CCCI'),('EXT'),('TRANS'),('UNKNOWN');

  -- Base monthly abs revenue by PC x Reason x CustGrp
  create or replace temporary table TMP_BASE_MTH as
  with base as (
    select
      (try_to_number(v."Year")*100 + try_to_number(v."Period")) as fiscal_yyyymm,
      v.roll_up_shop::string as roll_up_shop,
      coalesce(v."Reason Code Group"::string, 'UNKNOWN') as reason_group,
     coalesce(v.CUST_GRP::string, 'UNKNOWN') as cust_grp,
      sum(abs(coalesce(v.revenue,0))) as abs_rev_mth
    from DB_BI_P_SANDBOX.SANDBOX.RNA_RCT_TMT_CCCI_PL_RO_REVENUE_5YEARS v
    join TMP_ELIG_PCS e on e.roll_up_shop = v.roll_up_shop::string
    group by 1,2,3,4
  )
  select
    b.*,
    d.month_seq
  from base b
  join DB_BI_P_SANDBOX.SANDBOX.FORECAST_FISCAL_MONTH_DIM d
    on d.fiscal_yyyymm = b.fiscal_yyyymm
  where d.month_seq between :V_START_SEQ and :V_ASOF_SEQ;

  -- Stats tables (PC_REASON, PC, REASON, GLOBAL)
  create or replace temporary table TMP_STATS_PC_REASON as
  with m as (
    select roll_up_shop, reason_group, month_seq, sum(abs_rev_mth) as abs_rev_tot_mth
    from TMP_BASE_MTH
    group by 1,2,3
  )
  select
    roll_up_shop,
    reason_group,
    count(distinct month_seq) as months_present,
    count(distinct iff(abs_rev_tot_mth > 0, month_seq, null)) as nonzero_months,
    sum(abs_rev_tot_mth) as denom_abs_rev
  from m
  group by 1,2;

  create or replace temporary table TMP_STATS_PC as
  with m as (
    select roll_up_shop, month_seq, sum(abs_rev_mth) as abs_rev_tot_mth
    from TMP_BASE_MTH
    group by 1,2
  )
  select
    roll_up_shop,
    count(distinct month_seq) as months_present,
    count(distinct iff(abs_rev_tot_mth > 0, month_seq, null)) as nonzero_months,
    sum(abs_rev_tot_mth) as denom_abs_rev
  from m
  group by 1;

  create or replace temporary table TMP_STATS_REASON as
  with m as (
    select reason_group, month_seq, sum(abs_rev_mth) as abs_rev_tot_mth
    from TMP_BASE_MTH
    group by 1,2
  )
  select
    reason_group,
    count(distinct month_seq) as months_present,
    count(distinct iff(abs_rev_tot_mth > 0, month_seq, null)) as nonzero_months,
    sum(abs_rev_tot_mth) as denom_abs_rev
  from m
  group by 1;

  create or replace temporary table TMP_STATS_GLOBAL as
  with m as (
    select month_seq, sum(abs_rev_mth) as abs_rev_tot_mth
    from TMP_BASE_MTH
    group by 1
  )
  select
    count(distinct month_seq) as months_present,
    count(distinct iff(abs_rev_tot_mth > 0, month_seq, null)) as nonzero_months,
    sum(abs_rev_tot_mth) as denom_abs_rev
  from m;

  -- Choose allocation level per PC x Reason (priority order)
  create or replace temporary table TMP_LEVEL_CHOICE as
  select
    pr.roll_up_shop,
    pr.reason_group,
    case
      when pr.nonzero_months >= :V_MIN_NONZERO and pr.denom_abs_rev > 0 then 'PC_REASON'
      when pc.nonzero_months >= :V_MIN_NONZERO and pc.denom_abs_rev > 0 then 'PC'
      when rs.nonzero_months >= :V_MIN_NONZERO and rs.denom_abs_rev > 0 then 'REASON'
      else 'GLOBAL'
    end as allocation_level,

    case
      when pr.nonzero_months >= :V_MIN_NONZERO and pr.denom_abs_rev > 0 then pr.months_present
      when pc.nonzero_months >= :V_MIN_NONZERO and pc.denom_abs_rev > 0 then pc.months_present
      when rs.nonzero_months >= :V_MIN_NONZERO and rs.denom_abs_rev > 0 then rs.months_present
      else (select months_present from TMP_STATS_GLOBAL)
    end as months_present,

    case
      when pr.nonzero_months >= :V_MIN_NONZERO and pr.denom_abs_rev > 0 then pr.nonzero_months
      when pc.nonzero_months >= :V_MIN_NONZERO and pc.denom_abs_rev > 0 then pc.nonzero_months
      when rs.nonzero_months >= :V_MIN_NONZERO and rs.denom_abs_rev > 0 then rs.nonzero_months
      else (select nonzero_months from TMP_STATS_GLOBAL)
    end as nonzero_months,

    case
      when pr.nonzero_months >= :V_MIN_NONZERO and pr.denom_abs_rev > 0 then pr.denom_abs_rev
      when pc.nonzero_months >= :V_MIN_NONZERO and pc.denom_abs_rev > 0 then pc.denom_abs_rev
      when rs.nonzero_months >= :V_MIN_NONZERO and rs.denom_abs_rev > 0 then rs.denom_abs_rev
      else (select denom_abs_rev from TMP_STATS_GLOBAL)
    end as denom_abs_rev

  from TMP_STATS_PC_REASON pr
  left join TMP_STATS_PC pc
    on pc.roll_up_shop = pr.roll_up_shop
  left join TMP_STATS_REASON rs
    on rs.reason_group = pr.reason_group;

  -- Numerators for each level
  create or replace temporary table TMP_NUM_PC_REASON as
  select roll_up_shop, reason_group, cust_grp, sum(abs_rev_mth) as num_abs_rev
  from TMP_BASE_MTH
  group by 1,2,3;

  create or replace temporary table TMP_NUM_PC as
  select roll_up_shop, cust_grp, sum(abs_rev_mth) as num_abs_rev
  from TMP_BASE_MTH
  group by 1,2;

  create or replace temporary table TMP_NUM_REASON as
  select reason_group, cust_grp, sum(abs_rev_mth) as num_abs_rev
  from TMP_BASE_MTH
  group by 1,2;

  create or replace temporary table TMP_NUM_GLOBAL as
  select cust_grp, sum(abs_rev_mth) as num_abs_rev
  from TMP_BASE_MTH
  group by 1;

  -- Final mix rows (always emit all cust grps)
  create or replace temporary table TMP_MIX_OUT as
  with base_keys as (
    select lc.roll_up_shop, lc.reason_group, lc.allocation_level, lc.months_present, lc.nonzero_months, lc.denom_abs_rev
    from TMP_LEVEL_CHOICE lc
  )
  select
    :V_ASOF_YYYYMM as asof_fiscal_yyyymm,
    k.roll_up_shop,
    k.reason_group,
    cg.cust_grp,
    k.allocation_level,
    case
      when k.denom_abs_rev = 0 then 0
      else coalesce(n.num_abs_rev,0) / k.denom_abs_rev
    end as share_abs_rev,
    coalesce(n.num_abs_rev,0) as numerator_abs_rev,
    k.denom_abs_rev as denominator_abs_rev,
    k.months_present,
    k.nonzero_months,
    object_construct(
      'mix_lookback_months', :V_LOOKBACK_MONTHS,
      'mix_min_nonzero_months', :V_MIN_NONZERO,
      'start_month_seq', :V_START_SEQ,
      'end_month_seq', :V_ASOF_SEQ
    ) as thresholds,
    :V_SRC as source_object,
    current_timestamp() as computed_at,
    md5(
      coalesce(:V_ASOF_YYYYMM::string,'') || '|' ||
      coalesce(k.roll_up_shop,'') || '|' ||
      coalesce(k.reason_group,'') || '|' ||
      coalesce(cg.cust_grp,'') || '|' ||
      coalesce(k.allocation_level,'') || '|' ||
      coalesce(coalesce(n.num_abs_rev,0)::string,'') || '|' ||
      coalesce(k.denom_abs_rev::string,'')
    ) as row_hash
  from base_keys k
  cross join TMP_CUST_GRPS cg
  left join (
    select * from TMP_NUM_PC_REASON
    union all select null, null, null, null where false
  ) dummy on 1=0
  left join (
    -- pick the correct numerator table by allocation_level
    select
      'PC_REASON' as lvl, roll_up_shop, reason_group, cust_grp, num_abs_rev
    from TMP_NUM_PC_REASON
    union all
    select
      'PC' as lvl, roll_up_shop, null as reason_group, cust_grp, num_abs_rev
    from TMP_NUM_PC
    union all
    select
      'REASON' as lvl, null as roll_up_shop, reason_group, cust_grp, num_abs_rev
    from TMP_NUM_REASON
    union all
    select
      'GLOBAL' as lvl, null as roll_up_shop, null as reason_group, cust_grp, num_abs_rev
    from TMP_NUM_GLOBAL
  ) n
    on n.lvl = k.allocation_level
   and n.cust_grp = cg.cust_grp
   and ( (k.allocation_level='PC_REASON' and n.roll_up_shop = k.roll_up_shop and n.reason_group = k.reason_group)
      or (k.allocation_level='PC'        and n.roll_up_shop = k.roll_up_shop)
      or (k.allocation_level='REASON'    and n.reason_group = k.reason_group)
      or (k.allocation_level='GLOBAL') );

  -- Upsert as-of table
  merge into DB_BI_P_SANDBOX.SANDBOX.FORECAST_CUST_MIX_PC_REASON t
  using TMP_MIX_OUT s
  on  t.asof_fiscal_yyyymm = s.asof_fiscal_yyyymm
  and t.roll_up_shop       = s.roll_up_shop
  and t.reason_group       = s.reason_group
  and t.cust_grp           = s.cust_grp
  when matched and (t.row_hash <> s.row_hash) then update set
    allocation_level    = s.allocation_level,
    share_abs_rev       = s.share_abs_rev,
    numerator_abs_rev   = s.numerator_abs_rev,
    denominator_abs_rev = s.denominator_abs_rev,
    months_present      = s.months_present,
    nonzero_months      = s.nonzero_months,
    thresholds          = s.thresholds,
    source_object       = s.source_object,
    computed_at         = s.computed_at,
    row_hash            = s.row_hash
  when not matched then insert (
    asof_fiscal_yyyymm, roll_up_shop, reason_group, cust_grp,
    allocation_level, share_abs_rev, numerator_abs_rev, denominator_abs_rev,
    months_present, nonzero_months, thresholds, source_object, computed_at, row_hash
  ) values (
    s.asof_fiscal_yyyymm, s.roll_up_shop, s.reason_group, s.cust_grp,
    s.allocation_level, s.share_abs_rev, s.numerator_abs_rev, s.denominator_abs_rev,
    s.months_present, s.nonzero_months, s.thresholds, s.source_object, s.computed_at, s.row_hash
  );

  -- Snapshot (overwrite-by-run semantics)
  delete from DB_BI_P_SANDBOX.SANDBOX.FORECAST_CUST_MIX_PC_REASON_SNAP
  where run_id = :P_RUN_ID
    and asof_fiscal_yyyymm = :V_ASOF_YYYYMM;

  insert into DB_BI_P_SANDBOX.SANDBOX.FORECAST_CUST_MIX_PC_REASON_SNAP
  (run_id, asof_fiscal_yyyymm,
   roll_up_shop, reason_group, cust_grp,
   allocation_level, share_abs_rev, numerator_abs_rev, denominator_abs_rev,
   months_present, nonzero_months, thresholds, source_object, snapshotted_at, row_hash)
  select
    :P_RUN_ID, :V_ASOF_YYYYMM,
    roll_up_shop, reason_group, cust_grp,
    allocation_level, share_abs_rev, numerator_abs_rev, denominator_abs_rev,
    months_present, nonzero_months, thresholds, source_object, current_timestamp(), row_hash
  from TMP_MIX_OUT;

  return object_construct(
    'status','OK',
    'run_id', :P_RUN_ID,
    'asof_fiscal_yyyymm', :V_ASOF_YYYYMM,
    'eligible_pc_reason_pairs', (select count(*) from TMP_LEVEL_CHOICE),
    'rows_out', (select count(*) from TMP_MIX_OUT),
    'level_counts', (
      select object_agg(allocation_level, cnt)
      from (select allocation_level, count(*) cnt from TMP_LEVEL_CHOICE group by 1)
    )
  );
end;
$$;

call DB_BI_P_SANDBOX.SANDBOX.SP_BUILD_CUST_MIX_PC_REASON(
  (select run_id from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS order by triggered_at desc limit 1),
  (select asof_fiscal_yyyymm from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS order by triggered_at desc limit 1)
);

select $1 as proc_result
from table(result_scan(last_query_id()));
