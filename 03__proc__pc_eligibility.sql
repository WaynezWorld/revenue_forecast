create or replace procedure DB_BI_P_SANDBOX.SANDBOX.SP_EVALUATE_PC_ELIGIBILITY(
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
  V_ASOF_MONTH_END date;

  V_LOOKBACK_MONTHS number;
  V_MIN_NONZERO_MONTHS number;
  V_MIN_AVG_ABS_REV_PER_MONTH number;
  V_MIN_HISTORY_MONTHS number;
  V_MIN_MONTHS_PRESENT number;
begin

  -- Resolve as-of fiscal month (default = latest closed fiscal month)
  if (P_ASOF_FISCAL_YYYYMM is null) then
    select fiscal_yyyymm, month_seq, month_end_date
      into :V_ASOF_YYYYMM, :V_ASOF_SEQ, :V_ASOF_MONTH_END
    from DB_BI_P_SANDBOX.SANDBOX.FORECAST_ASOF_FISCAL_MONTH;
  else
    select fiscal_yyyymm, month_seq, month_end_date
      into :V_ASOF_YYYYMM, :V_ASOF_SEQ, :V_ASOF_MONTH_END
    from DB_BI_P_SANDBOX.SANDBOX.FORECAST_FISCAL_MONTH_DIM
    where fiscal_yyyymm = :P_ASOF_FISCAL_YYYYMM;   -- FIX
  end if;

  if (V_ASOF_YYYYMM is null) then
    return object_construct('status','ERROR','message','Could not resolve as-of fiscal month.');
  end if;

  -- Config precedence: canonical -> legacy -> default
  select
    coalesce(max(iff(config_key='LOOKBACK_MONTHS', value_number, null)), 12),
    coalesce(max(iff(config_key='MIN_NONZERO_MONTHS', value_number, null)), 3),
    coalesce(
      max(iff(config_key='MIN_AVG_ABS_REV_PER_MONTH', value_number, null)),
      max(iff(config_key='MIN_AVG_ABS_REVENUE_LAST12', value_number, null)),
      max(iff(config_key='MIN_AVG_REV_PER_MONTH', value_number, null)),
      5000
    ),
    coalesce(max(iff(config_key='MIN_HISTORY_MONTHS_SINCE_FIRST_NONZERO', value_number, null)), 12),
    coalesce(
      max(iff(config_key='MIN_MONTHS_PRESENT', value_number, null)),
      max(iff(config_key='MIN_MONTHS_PRESENT_LAST12', value_number, null)),
      12
    )
  into :V_LOOKBACK_MONTHS, :V_MIN_NONZERO_MONTHS, :V_MIN_AVG_ABS_REV_PER_MONTH, :V_MIN_HISTORY_MONTHS, :V_MIN_MONTHS_PRESENT
  from DB_BI_P_SANDBOX.SANDBOX.RNA_RCT_CONFIG_SHOP_ELIGIBILITY;

  -- Materialize evaluation once (avoids WITH + multiple INSERT issues)
  create or replace temporary table DB_BI_P_SANDBOX.SANDBOX.TMP_PC_ELIG as
  with
  rev_pc_mth as (
    select
      v.roll_up_shop::string as roll_up_shop,
      (try_to_number(v."Year")*100 + try_to_number(v."Period")) as fiscal_yyyymm,
      sum(coalesce(v.revenue,0)) as rev_mth
    from DB_BI_P_SANDBOX.SANDBOX.RNA_RCT_TMT_CCCI_PL_RO_REVENUE_5YEARS v
    group by 1,2
  ),
  rev_pc_mth_seq as (
    select
      r.roll_up_shop,
      d.month_seq,
      r.rev_mth
    from rev_pc_mth r
    join DB_BI_P_SANDBOX.SANDBOX.FORECAST_FISCAL_MONTH_DIM d
      on d.fiscal_yyyymm = r.fiscal_yyyymm
  ),
  first_nonzero as (
    select
      roll_up_shop,
      min(iff(rev_mth <> 0, month_seq, null)) as first_nonzero_month_seq
    from rev_pc_mth_seq
    group by 1
  ),
  lookback as (
    select
      r.roll_up_shop,
      count(distinct r.month_seq) as months_present_lookback,
      count(distinct iff(r.rev_mth <> 0, r.month_seq, null)) as nonzero_months_lookback,
      sum(r.rev_mth) as total_rev_lookback,
      sum(abs(r.rev_mth)) as total_abs_rev_lookback
    from rev_pc_mth_seq r
    where r.month_seq between (:V_ASOF_SEQ - (:V_LOOKBACK_MONTHS - 1)) and :V_ASOF_SEQ
    group by 1
  ),
  manual_excl as (
    select
      roll_up_shop::string as roll_up_shop,
      true as is_manual_excluded
    from DB_BI_P_SANDBOX.SANDBOX.FORECAST_PC_MANUAL_EXCLUSIONS
    where effective_start <= :V_ASOF_MONTH_END
      and (effective_end is null or effective_end >= :V_ASOF_MONTH_END)
    qualify row_number() over (partition by roll_up_shop order by updated_at desc) = 1
  )
  select
    :P_RUN_ID::string as run_id,
    :V_ASOF_MONTH_END as as_of_month_end,
    :V_ASOF_YYYYMM as asof_fiscal_yyyymm,
    :V_ASOF_SEQ as asof_month_seq,

    l.roll_up_shop,
    l.months_present_lookback,
    l.nonzero_months_lookback,
    (l.total_rev_lookback / :V_LOOKBACK_MONTHS) as avg_rev_lookback,
    (l.total_abs_rev_lookback / :V_LOOKBACK_MONTHS) as avg_abs_rev_lookback,
    l.total_rev_lookback,
    l.total_abs_rev_lookback,

    f.first_nonzero_month_seq,
    (:V_ASOF_SEQ - (:V_MIN_HISTORY_MONTHS - 1)) as min_required_first_nonzero_seq,

    coalesce(m.is_manual_excluded,false) as is_manual_excluded,

    -- Rules
    (l.months_present_lookback >= :V_MIN_MONTHS_PRESENT) as pass_months_present,
    (f.first_nonzero_month_seq is not null and f.first_nonzero_month_seq <= (:V_ASOF_SEQ - (:V_MIN_HISTORY_MONTHS - 1))) as pass_history_12m,
    (l.nonzero_months_lookback >= :V_MIN_NONZERO_MONTHS) as pass_nonzero,
    ((l.total_abs_rev_lookback / :V_LOOKBACK_MONTHS) >= :V_MIN_AVG_ABS_REV_PER_MONTH) as pass_avg_abs_rev,
    (not coalesce(m.is_manual_excluded,false)) as pass_manual,

    current_timestamp() as evaluated_at
  from lookback l
  left join first_nonzero f on f.roll_up_shop = l.roll_up_shop
  left join manual_excl m on m.roll_up_shop = l.roll_up_shop
  ;

  -- Insert summary
  insert into DB_BI_P_SANDBOX.SANDBOX.FORECAST_PC_ELIGIBILITY
  (run_id, as_of_month_end, roll_up_shop, months_present_lookback, avg_rev_lookback, total_rev_lookback,
   is_eligible, exclusion_reasons, thresholds, evaluated_at)
  select
    run_id,
    as_of_month_end,
    roll_up_shop,
    months_present_lookback,
    avg_rev_lookback,
    total_rev_lookback,
    (pass_months_present and pass_history_12m and pass_nonzero and pass_avg_abs_rev and pass_manual) as is_eligible,
    array_construct_compact(
      iff(not pass_months_present, 'INSUFFICIENT_MONTHS_PRESENT', null),
      iff(not pass_history_12m,    'INSUFFICIENT_HISTORY', null),
      iff(not pass_nonzero,        'INSUFFICIENT_NONZERO_MONTHS', null),
      iff(not pass_avg_abs_rev,    'LOW_AVG_ABS_REVENUE', null),
      iff(not pass_manual,         'MANUAL_EXCLUDE', null)
    ) as exclusion_reasons,
    object_construct(
      'lookback_months', :V_LOOKBACK_MONTHS,
      'min_months_present', :V_MIN_MONTHS_PRESENT,
      'min_nonzero_months', :V_MIN_NONZERO_MONTHS,
      'min_avg_abs_rev_per_month', :V_MIN_AVG_ABS_REV_PER_MONTH,
      'min_history_months_since_first_nonzero', :V_MIN_HISTORY_MONTHS,
      'asof_fiscal_yyyymm', :V_ASOF_YYYYMM
    ) as thresholds,
    evaluated_at
  from DB_BI_P_SANDBOX.SANDBOX.TMP_PC_ELIG;

  -- Insert rule-level provenance
  insert into DB_BI_P_SANDBOX.SANDBOX.FORECAST_PC_ELIGIBILITY_RULES
  (run_id, as_of_month_end, roll_up_shop, rule_code, rule_passed, observed_value, threshold_value, evaluated_at)
  select run_id, as_of_month_end, roll_up_shop,
         'MIN_MONTHS_PRESENT', pass_months_present,
         object_construct('months_present_lookback', months_present_lookback),
         object_construct('min_months_present', :V_MIN_MONTHS_PRESENT),
         evaluated_at
  from DB_BI_P_SANDBOX.SANDBOX.TMP_PC_ELIG
  union all
  select run_id, as_of_month_end, roll_up_shop,
         'HISTORY_12M', pass_history_12m,
         object_construct('first_nonzero_month_seq', first_nonzero_month_seq),
         object_construct('max_allowed_first_nonzero_seq', min_required_first_nonzero_seq),
         evaluated_at
  from DB_BI_P_SANDBOX.SANDBOX.TMP_PC_ELIG
  union all
  select run_id, as_of_month_end, roll_up_shop,
         'MIN_NONZERO_MONTHS', pass_nonzero,
         object_construct('nonzero_months_lookback', nonzero_months_lookback),
         object_construct('min_nonzero_months', :V_MIN_NONZERO_MONTHS),
         evaluated_at
  from DB_BI_P_SANDBOX.SANDBOX.TMP_PC_ELIG
  union all
  select run_id, as_of_month_end, roll_up_shop,
         'MIN_AVG_ABS_REV_PER_MONTH', pass_avg_abs_rev,
         object_construct('avg_abs_rev_lookback', avg_abs_rev_lookback),
         object_construct('min_avg_abs_rev_per_month', :V_MIN_AVG_ABS_REV_PER_MONTH),
         evaluated_at
  from DB_BI_P_SANDBOX.SANDBOX.TMP_PC_ELIG
  union all
  select run_id, as_of_month_end, roll_up_shop,
         'MANUAL_EXCLUDE', pass_manual,
         object_construct('is_manual_excluded', is_manual_excluded),
         object_construct('must_not_be_manual_excluded', true),
         evaluated_at
  from DB_BI_P_SANDBOX.SANDBOX.TMP_PC_ELIG;

  return object_construct(
    'status','OK',
    'run_id', P_RUN_ID,
    'asof_fiscal_yyyymm', V_ASOF_YYYYMM,
    'asof_month_end', V_ASOF_MONTH_END,
    'eligible_count', (select count_if(pass_months_present and pass_history_12m and pass_nonzero and pass_avg_abs_rev and pass_manual) from DB_BI_P_SANDBOX.SANDBOX.TMP_PC_ELIG),
    'total_pc_count', (select count(*) from DB_BI_P_SANDBOX.SANDBOX.TMP_PC_ELIG)
  );
end;
$$;
