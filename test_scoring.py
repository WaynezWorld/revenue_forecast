"""
Test the scoring and publishing procedure
"""
import sys
from snowflake.snowpark import Session
import json
import os
from pathlib import Path

def get_session():
    """Get Snowflake session"""
    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "authenticator": "externalbrowser",
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "BI_P_QRY_PU_WH"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "DB_BI_P_SANDBOX"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "SANDBOX"),
    }
    
    if not all([connection_parameters.get("account"), connection_parameters.get("user")]):
        print("Error: Set SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER environment variables")
        sys.exit(1)
    
    print(f"Connecting to Snowflake...")
    return Session.builder.configs(connection_parameters).create()

def main():
    session = get_session()
    print(f"Connected: {session.get_current_database()}.{session.get_current_schema()}")
    print(f"Warehouse: {session.get_current_warehouse()}\n")
    
    # Read and execute the SQL file
    sql_file = Path("11__proc__score_and_publish_marts.sql")
    if not sql_file.exists():
        print(f"Error: {sql_file} not found")
        sys.exit(1)
    
    print(f"Reading {sql_file}...")
    sql_content = sql_file.read_text()
    
    # Split into statements (simple split by semicolon, may need refinement)
    statements = [s.strip() for s in sql_content.split(';') if s.strip() and not s.strip().startswith('--') and not s.strip().startswith('/*')]
    
    print(f"Found {len(statements)} SQL statements\n")
    
    # Execute each statement
    for i, stmt in enumerate(statements, 1):
        # Skip comments and example usage
        if '-- EXAMPLE USAGE' in stmt or 'EXAMPLE USAGE' in stmt:
            print(f"Skipping example usage section...")
            break
            
        if len(stmt) < 20:  # Skip very short statements
            continue
            
        print(f"[{i}/{len(statements)}] Executing statement...")
        preview = stmt[:100].replace('\n', ' ')
        print(f"  Preview: {preview}...")
        
        try:
            session.sql(stmt).collect()
            print(f"  ✓ Success\n")
        except Exception as e:
            print(f"  ✗ Error: {str(e)}\n")
            if "does not exist" not in str(e).lower():
                print(f"Full statement:\n{stmt}\n")
                response = input("Continue anyway? (y/n): ")
                if response.lower() != 'y':
                    sys.exit(1)
    
    print("\n" + "="*80)
    print("TESTING THE PROCEDURE")
    print("="*80 + "\n")
    
    # Get the latest successful run
    print("1. Getting latest successful run...")
    df = session.sql("""
        select run_id, asof_fiscal_yyyymm, triggered_at
        from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS
        where status = 'SUCCEEDED'
        order by triggered_at desc
        limit 1
    """).to_pandas()
    
    if len(df) == 0:
        print("No successful runs found!")
        sys.exit(1)
    
    run_id = df.iloc[0]['RUN_ID']
    asof = df.iloc[0]['ASOF_FISCAL_YYYYMM']
    triggered = df.iloc[0]['TRIGGERED_AT']
    
    print(f"   Run ID: {run_id}")
    print(f"   As-of: {asof}")
    print(f"   Triggered: {triggered}\n")
    
    # Execute the scoring procedure
    print("2. Executing SP_SCORE_AND_PUBLISH_FORECASTS...")
    result = session.sql(f"""
        call DB_BI_P_SANDBOX.SANDBOX.SP_SCORE_AND_PUBLISH_FORECASTS(
            '{run_id}',
            null,
            12
        )
    """).collect()
    
    print(f"   Result: {result[0][0]}\n")
    
    # Check the outputs
    print("3. Checking forecast outputs...\n")
    
    print("   PC x Reason forecasts:")
    df = session.sql("""
        select 
            count(*) as total_rows,
            count(distinct roll_up_shop) as pc_count,
            count(distinct roll_up_shop || '|' || reason_group) as pc_reason_pairs,
            count(distinct target_fiscal_yyyymm) as forecast_months,
            min(target_fiscal_yyyymm) as first_month,
            max(target_fiscal_yyyymm) as last_month,
            sum(revenue_forecast) as total_forecast
        from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
        where forecast_run_id = (
            select forecast_run_id 
            from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH 
            order by forecast_created_at desc 
            limit 1
        )
    """).to_pandas()
    print(df.to_string(index=False))
    
    print("\n   Customer forecasts:")
    df = session.sql("""
        select 
            count(*) as total_rows,
            count(distinct cust_grp) as customer_groups,
            sum(revenue_forecast) as total_forecast
        from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_CUST_MTH
        where forecast_run_id = (
            select forecast_run_id 
            from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_CUST_MTH 
            order by forecast_created_at desc 
            limit 1
        )
    """).to_pandas()
    print(df.to_string(index=False))
    
    print("\n   Sample forecasts by month:")
    df = session.sql("""
        select
            target_fiscal_yyyymm,
            count(distinct roll_up_shop || '|' || reason_group) as series_count,
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
        order by 1
        limit 12
    """).to_pandas()
    print(df.to_string(index=False))
    
    print("\n   Sample detail (first 10 rows):")
    df = session.sql("""
        select
            roll_up_shop,
            reason_group,
            target_fiscal_yyyymm,
            horizon,
            revenue_forecast,
            model_family
        from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH
        where forecast_run_id = (
            select forecast_run_id 
            from DB_BI_P_SANDBOX.SANDBOX.FORECAST_OUTPUT_PC_REASON_MTH 
            order by forecast_created_at desc 
            limit 1
        )
        order by roll_up_shop, reason_group, target_fiscal_yyyymm
        limit 10
    """).to_pandas()
    print(df.to_string(index=False))
    
    print("\n" + "="*80)
    print("TEST COMPLETE!")
    print("="*80)

if __name__ == "__main__":
    main()
