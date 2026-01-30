-- 1A) Fiscal month dimension (one row per fiscal month)
create or replace view DB_BI_P_SANDBOX.SANDBOX.FORECAST_FISCAL_MONTH_DIM as
with m as (
  select
    fiscal_year  as fiscal_year,
    fiscal_month as fiscal_month,
    (fiscal_year * 100 + fiscal_month) as fiscal_yyyymm,
    min(actual_date) as month_start_date,
    max(actual_date) as month_end_date,
    max(days_in_fiscal_month) as days_in_fiscal_month
  from DB_BI_P_SANDBOX.SANDBOX.DIM_DATE_5
  group by 1,2
)
select
  m.*,
  dense_rank() over (order by month_start_date) as month_seq
from m;

-- 1B) Latest closed fiscal month (month_end_date < today)
create or replace view DB_BI_P_SANDBOX.SANDBOX.FORECAST_ASOF_FISCAL_MONTH as
select *
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_FISCAL_MONTH_DIM
where month_end_date < current_date()
qualify month_seq = max(month_seq) over ();





