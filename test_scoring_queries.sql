-- Execute the scoring procedure
-- Run this in Snowflake's web UI

call DB_BI_P_SANDBOX.SANDBOX.SP_SCORE_AND_PUBLISH_FORECASTS(
    '5d0f5e35-b4e1-4af4-a592-e5f7f05f9686',
    null,
    12
);

-- Get the latest forecast_run_id
select forecast_run_id, forecast_created_at, asof_fiscal_yyyymm,
       rows_pc_reason_forecast, rows_customer_forecast
from (
    select distinct 
        forecast_run_id,
        forecast_created_at,
        asof_fiscal_yyyymm,
        count(*) over () as rows_pc_reason_forecast,
        0 as rows_customer_forecast
    from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
    order by forecast_created_at desc
    limit 1
);

-- Summary by target month
select
    target_fiscal_yyyymm,
    count(distinct roll_up_shop) as pc_count,
    count(distinct roll_up_shop || '|' || reason_group) as pc_reason_count,
    sum(revenue_forecast) as total_forecast,
    min(revenue_forecast) as min_forecast,
    max(revenue_forecast) as max_forecast,
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

-- Sample forecasts
select
    roll_up_shop,
    reason_group,
    target_fiscal_yyyymm,
    horizon,
    revenue_forecast,
    revenue_forecast_lo,
    revenue_forecast_hi,
    model_family,
    model_run_id
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
where forecast_run_id = (
    select forecast_run_id 
    from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH 
    order by forecast_created_at desc 
    limit 1
)
order by roll_up_shop, reason_group, target_fiscal_yyyymm
limit 50;

-- Customer-level summary
select
    count(*) as total_rows,
    count(distinct roll_up_shop) as pc_count,
    count(distinct roll_up_shop || '|' || reason_group) as pc_reason_count,
    count(distinct cust_grp) as customer_count,
    sum(revenue_forecast) as total_forecast
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_CUST_MTH
where forecast_run_id = (
    select forecast_run_id 
    from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_CUST_MTH 
    order by forecast_created_at desc 
    limit 1
);

-- Sample customer-level forecasts
select
    roll_up_shop,
    reason_group,
    cust_grp,
    target_fiscal_yyyymm,
    revenue_forecast,
    allocation_share,
    allocation_level
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_CUST_MTH
where forecast_run_id = (
    select forecast_run_id 
    from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_CUST_MTH 
    order by forecast_created_at desc 
    limit 1
)
order by roll_up_shop, reason_group, cust_grp, target_fiscal_yyyymm
limit 50;
