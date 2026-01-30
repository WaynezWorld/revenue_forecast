merge into DB_BI_P_SANDBOX.SANDBOX.RNA_RCT_CONFIG_SHOP_ELIGIBILITY t
using (
  select 'LOOKBACK_MONTHS' as config_key, 12::float as value_number, null::string as value_string union all
  select 'MIN_MONTHS_PRESENT', 12::float, null union all
  select 'MIN_NONZERO_MONTHS', 3::float, null union all
  select 'MIN_AVG_ABS_REV_PER_MONTH', 5000::float, null union all
  select 'MIN_HISTORY_MONTHS_SINCE_FIRST_NONZERO', 12::float, null
) s
on t.config_key = s.config_key
when matched then update set
  t.value_number = s.value_number,
  t.value_string = s.value_string,
  t.updated_at   = current_timestamp()
when not matched then insert (config_key, value_number, value_string, updated_at)
values (s.config_key, s.value_number, s.value_string, current_timestamp());
