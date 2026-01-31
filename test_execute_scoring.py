from snowflake.snowpark import Session
import json

# Create session
session = Session.builder.configs({
    'account': 'CONA-CCCI',
    'user': 'wayne.piekarski@cokecce.com',
    'authenticator': 'externalbrowser',
    'role': 'BI_P_ADMIN',
    'warehouse': 'BI_P_QRY_PU_WH',
    'database': 'DB_BI_P_SANDBOX',
    'schema': 'SANDBOX'
}).create()

print("Executing SP_SCORE_AND_PUBLISH_FORECASTS...")
print("=" * 80)

# Execute the procedure
result = session.sql("""
    call DB_BI_P_SANDBOX.SANDBOX.SP_SCORE_AND_PUBLISH_FORECASTS(
        '5d0f5e35-b4e1-4af4-a592-e5f7f05f9686',
        null,
        12
    )
""").collect()

proc_result = json.loads(result[0][0])
print("\nProcedure Result:")
print(json.dumps(proc_result, indent=2))

if proc_result.get('status') == 'OK':
    forecast_run_id = proc_result['forecast_run_id']
    
    print("\n" + "=" * 80)
    print("FORECAST SUMMARY")
    print("=" * 80)
    
    # Check PC x Reason level forecasts
    summary = session.sql(f"""
        select
            target_fiscal_yyyymm,
            count(distinct roll_up_shop) as pc_count,
            count(distinct roll_up_shop || '|' || reason_group) as pc_reason_count,
            sum(revenue_forecast) as total_forecast,
            min(revenue_forecast) as min_forecast,
            max(revenue_forecast) as max_forecast,
            avg(horizon) as avg_horizon
        from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
        where forecast_run_id = '{forecast_run_id}'
        group by 1
        order by 1
    """).collect()
    
    print("\nForecasts by Target Month:")
    print(f"{'Month':<10} {'PCs':<6} {'PC×Reason':<12} {'Total Forecast':>18} {'Min':>15} {'Max':>15} {'Avg Horizon':>12}")
    print("-" * 100)
    for row in summary:
        print(f"{row[0]:<10} {row[1]:<6} {row[2]:<12} ${row[3]:>17,.2f} ${row[4]:>14,.2f} ${row[5]:>14,.2f} {row[6]:>12.1f}")
    
    # Show sample forecasts
    print("\n" + "=" * 80)
    print("SAMPLE FORECASTS (First 20 rows)")
    print("=" * 80)
    
    samples = session.sql(f"""
        select
            roll_up_shop,
            reason_group,
            target_fiscal_yyyymm,
            horizon,
            revenue_forecast,
            revenue_forecast_lo,
            revenue_forecast_hi,
            model_family
        from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
        where forecast_run_id = '{forecast_run_id}'
        order by roll_up_shop, reason_group, target_fiscal_yyyymm
        limit 20
    """).collect()
    
    print(f"{'PC':<15} {'Reason':<20} {'Month':<10} {'H':<3} {'Forecast':>15} {'Lo':>15} {'Hi':>15} {'Model':<10}")
    print("-" * 110)
    for row in samples:
        print(f"{row[0]:<15} {row[1]:<20} {row[2]:<10} {row[3]:<3} ${row[4]:>14,.2f} ${row[5]:>14,.2f} ${row[6]:>14,.2f} {row[7]:<10}")
    
    # Check customer-level forecasts
    print("\n" + "=" * 80)
    print("CUSTOMER-LEVEL FORECAST SUMMARY")
    print("=" * 80)
    
    cust_summary = session.sql(f"""
        select
            count(*) as total_rows,
            count(distinct roll_up_shop) as pc_count,
            count(distinct cust_grp) as customer_count,
            sum(revenue_forecast) as total_forecast
        from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_CUST_MTH
        where forecast_run_id = '{forecast_run_id}'
    """).collect()
    
    row = cust_summary[0]
    print(f"Total rows: {row[0]:,}")
    print(f"Profit centers: {row[1]}")
    print(f"Customer groups: {row[2]}")
    print(f"Total forecast: ${row[3]:,.2f}")

else:
    print(f"\nERROR: {proc_result.get('message', 'Unknown error')}")

session.close()
print("\n" + "=" * 80)
print("Done!")
