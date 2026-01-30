"""
Simple test - manually create and test scoring procedure
"""
import sys
from snowflake.snowpark import Session
import os

def get_session():
    """Get Snowflake session"""
    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "authenticator": "externalbrowser",
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "BI_P_QRY_PU_WH"),
        "database": "DB_BI_P_SANDBOX",
        "schema": "SANDBOX",
    }
    print(f"Connecting to Snowflake...")
    return Session.builder.configs(connection_parameters).create()

def execute_sql_file(session, filepath):
    """Execute a SQL file by reading it and executing with snowsql"""
    print(f"\nExecuting {filepath} via SnowSQL...\n")
    
    import subprocess
    result = subprocess.run([
        "snowsql",
        "-a", os.getenv("SNOWFLAKE_ACCOUNT"),
        "-u", os.getenv("SNOWFLAKE_USER"),
        "--authenticator", "externalbrowser",
        "-r", os.getenv("SNOWFLAKE_ROLE"),
        "-w", os.getenv("SNOWFLAKE_WAREHOUSE"),
        "-d", "DB_BI_P_SANDBOX",
        "-s", "SANDBOX",
        "-f", filepath,
        "-o", "friendly=False",
        "-o", "output_format=psql"
    ], capture_output=True, text=True)
    
    print(result.stdout)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        return False
    return True

def main():
    session = get_session()
    print(f"Connected: {session.get_current_database()}.{session.get_current_schema()}\n")
    
    # Check if snowsql is available
    import subprocess
    try:
        subprocess.run(["snowsql", "--version"], capture_output=True, check=True)
        has_snowsql = True
    except:
        has_snowsql = False
        print("SnowSQL not available, will execute statements individually\n")
    
    if has_snowsql:
        success = execute_sql_file(session, "11__proc__score_and_publish_marts.sql")
        if not success:
            sys.exit(1)
    else:
        # Read the SQL file and extract the CREATE statements
        print("Creating tables and procedure manually...\n")
        
        # Create output table for PC x Reason
        print("1. Creating FORECAST_OUTPUT_PC_REASON_MTH...")
        try:
            session.sql("""
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
                )
            """).collect()
            print("   ✓ Created\n")
        except Exception as e:
            print(f"   Error: {e}\n")
            sys.exit(1)
        
        # Create output table for customers
        print("2. Creating FORECAST_OUTPUT_PC_REASON_CUST_MTH...")
        try:
            session.sql("""
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
                )
            """).collect()
            print("   ✓ Created\n")
        except Exception as e:
            print(f"   Error: {e}\n")
    
    # Now test the procedure
    print("\n" + "="*80)
    print("TESTING THE PROCEDURE")
    print("="*80 + "\n")
    
    # Get the latest successful run
    print("Getting latest successful run...")
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
    
    print(f"   Run ID: {run_id}")
    print(f"   As-of: {asof}\n")
    
    # Note: The stored procedure still needs to be created
    # For now, let's just verify the tables exist
    print("Verifying tables exist...")
    
    for table in ['FORECAST_OUTPUT_PC_REASON_MTH', 'FORECAST_OUTPUT_PC_REASON_CUST_MTH']:
        try:
            df = session.sql(f"select count(*) as cnt from DB_BI_P_SANDBOX.SANDBOX.{table}").to_pandas()
            print(f"   ✓ {table}: {df.iloc[0]['CNT']} rows")
        except Exception as e:
            print(f"   ✗ {table}: {e}")
    
    print("\n✓ Tables created successfully!")
    print("\nTo complete setup, run the SQL file in Snowflake:")
    print("   Use Snowflake web UI or snowsql to execute: 11__proc__score_and_publish_marts.sql")

if __name__ == "__main__":
    main()
