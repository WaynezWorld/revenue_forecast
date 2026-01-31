from snowflake.snowpark import Session

# Create session with external browser auth
session = Session.builder.configs({
    "account": "cona-ccci",
    "user": "wayne.jones@coca-cola.com",
    "authenticator": "externalbrowser",
    "role": "BI_P_ADMIN",
    "warehouse": "BI_P_QRY_PU_WH",
    "database": "DB_BI_P_SANDBOX",
    "schema": "SANDBOX"
}).create()

# Query Ridge predictions with worst errors
query = """
select 
    r.params:candidate::string as model,
    p.roll_up_shop,
    p.reason_group,
    p.anchor_month_seq,
    p.horizon,
    round(p.y_true, 2) as y_true,
    round(p.y_pred, 2) as y_pred,
    round(abs(p.y_pred - p.y_true), 2) as abs_err,
    round(abs(p.y_pred - p.y_true) / nullif(abs(p.y_true), 0) * 100, 2) as ape_pct
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS p
join DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS r
  on r.model_run_id = p.model_run_id
where r.params:candidate::string = 'RIDGE_OHE'
  and p.anchor_month_seq = 48
order by ape_pct desc 
limit 15
"""

print("Ridge Model Predictions with Highest Errors:")
print("=" * 80)
session.sql(query).show()

# Also check aggregate stats
aggregate_query = """
select 
    r.params:candidate::string as model,
    count(*) as n_predictions,
    round(min(p.y_pred), 2) as min_pred,
    round(avg(p.y_pred), 2) as avg_pred,
    round(max(p.y_pred), 2) as max_pred,
    round(stddev(p.y_pred), 2) as stddev_pred,
    round(min(p.y_true), 2) as min_true,
    round(avg(p.y_true), 2) as avg_true,
    round(max(p.y_true), 2) as max_true,
    round(sum(abs(p.y_pred - p.y_true)) / nullif(sum(abs(p.y_true)), 0) * 100, 2) as wape_pct
from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS p
join DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS r
  on r.model_run_id = p.model_run_id
where r.params:candidate::string in ('RIDGE_OHE', 'GBR_OHE', 'SEASONAL_NAIVE_LAG12')
  and p.anchor_month_seq = 48
group by 1
order by model
"""

print("\nAggregate Statistics by Model:")
print("=" * 80)
session.sql(aggregate_query).show()

session.close()
