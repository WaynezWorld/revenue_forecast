"""
Run validation queries from 05__ops__latest_run_validation.sql
"""
import sys
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSessionException
import json

def get_session():
    """Try to get an active Snowpark session or create a new one"""
    try:
        # Try to get active session (if running in Snowflake notebook)
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except:
        # Create new session from connection parameters
        import os
        from pathlib import Path
        
        connection_parameters = None
        
        # Try to load from .snowflake_config.json file
        config_file = Path(".snowflake_config.json")
        if config_file.exists():
            with open(config_file, 'r') as f:
                connection_parameters = json.load(f)
                print(f"Loaded connection config from {config_file}")
        else:
            # Use environment variables with external browser auth
            connection_parameters = {
                "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                "user": os.getenv("SNOWFLAKE_USER"),
                "authenticator": "externalbrowser",  # Use browser-based SSO
                "role": os.getenv("SNOWFLAKE_ROLE"),
                "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
                "database": os.getenv("SNOWFLAKE_DATABASE", "DB_BI_P_SANDBOX"),
                "schema": os.getenv("SNOWFLAKE_SCHEMA", "SANDBOX"),
            }
        
        # For external browser auth, we only need account and user
        if not connection_parameters.get("authenticator"):
            connection_parameters["authenticator"] = "externalbrowser"
        
        # Check if minimum credentials are set
        if not all([connection_parameters.get("account"), 
                   connection_parameters.get("user")]):
            print("Error: Snowflake connection parameters not set.")
            print("\nUsing external browser authentication.")
            print("Set environment variables:")
            print("  $env:SNOWFLAKE_ACCOUNT = 'your_account'")
            print("  $env:SNOWFLAKE_USER = 'your_user'")
            print("  $env:SNOWFLAKE_WAREHOUSE = 'your_warehouse' (optional)")
            print("  $env:SNOWFLAKE_ROLE = 'your_role' (optional)")
            print("\nA browser window will open for authentication.")
            sys.exit(1)
        
        print(f"Connecting to Snowflake account: {connection_parameters['account']}")
        print(f"User: {connection_parameters['user']}")
        print("Authentication: External Browser (SSO)")
        print("Opening browser for authentication...")
        
        return Session.builder.configs(connection_parameters).create()

def run_query(session, query_name, query):
    """Execute a query and display results"""
    print(f"\n{'='*80}")
    print(f"Query: {query_name}")
    print(f"{'='*80}")
    try:
        df = session.sql(query).to_pandas()
        if len(df) > 0:
            print(df.to_string())
            print(f"\nRows: {len(df)}")
        else:
            print("No results returned")
    except Exception as e:
        print(f"ERROR: {str(e)}")

def main():
    session = get_session()
    print(f"Connected to Snowflake: {session.get_current_database()}.{session.get_current_schema()}")
    
    # Set warehouse if not already set
    import os
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "BI_P_QRY_PU_WH")
    try:
        session.sql(f"USE WAREHOUSE {warehouse}").collect()
        print(f"Using warehouse: {warehouse}\n")
    except Exception as e:
        print(f"Warning: Could not set warehouse {warehouse}: {e}")
        print("Continuing with queries...\n")
    
    # Query 04: Latest Run Status
    run_query(session, "04 - Latest Run Status", """
        select run_id, status, status_message, config_snapshot
        from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS
        order by triggered_at desc
        limit 1
    """)
    
    # Query A: Fiscal anchor sanity
    run_query(session, "A - Fiscal Anchor", """
        select * from DB_BI_P_SANDBOX.SANDBOX.FORECAST_ASOF_FISCAL_MONTH
    """)
    
    # Query B: Config snapshot
    run_query(session, "B - Eligibility Config", """
        select *
        from DB_BI_P_SANDBOX.SANDBOX.RNA_RCT_CONFIG_SHOP_ELIGIBILITY
        order by config_key
    """)
    
    # Query C: Latest run header
    run_query(session, "C - Latest Run Header", """
        with last_run as (
          select run_id
          from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS
          order by triggered_at desc
          limit 1
        )
        select r.*
        from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS r
        join last_run lr on r.run_id = lr.run_id
    """)
    
    # Query D: Eligibility summary counts
    run_query(session, "D - Eligibility Summary", """
        with last_run as (
          select run_id
          from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS
          order by triggered_at desc
          limit 1
        )
        select
          e.is_eligible,
          count(*) as pc_count
        from DB_BI_P_SANDBOX.SANDBOX.FORECAST_PC_ELIGIBILITY e
        join last_run lr on e.run_id = lr.run_id
        group by 1
        order by 1 desc
    """)
    
    # Query E: Rule breakdown
    run_query(session, "E - Rule Breakdown", """
        with last_run as (
          select run_id
          from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS
          order by triggered_at desc
          limit 1
        )
        select
          rr.rule_code,
          rr.rule_passed,
          count(*) as cnt
        from DB_BI_P_SANDBOX.SANDBOX.FORECAST_PC_ELIGIBILITY_RULES rr
        join last_run lr on rr.run_id = lr.run_id
        group by 1,2
        order by 1,2
    """)
    
    # Query F: Excluded PCs (top 10)
    run_query(session, "F - Top 10 Excluded PCs", """
        with last_run as (
          select run_id
          from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS
          order by triggered_at desc
          limit 1
        )
        select
          e.roll_up_shop,
          e.total_rev_lookback,
          e.avg_rev_lookback,
          e.exclusion_reasons,
          e.thresholds
        from DB_BI_P_SANDBOX.SANDBOX.FORECAST_PC_ELIGIBILITY e
        join last_run lr on e.run_id = lr.run_id
        where e.is_eligible = false
        order by e.total_rev_lookback desc
        limit 10
    """)
    
    # Actuals counts
    run_query(session, "Actuals - Current Table", """
        select count(*) as rows_current
        from DB_BI_P_SANDBOX.SANDBOX.FORECAST_ACTUALS_PC_REASON_MTH
    """)
    
    run_query(session, "Actuals - Latest Snapshot", """
        select count(*) as rows_snap
        from DB_BI_P_SANDBOX.SANDBOX.FORECAST_ACTUALS_PC_REASON_MTH_SNAP
        where run_id = (select run_id from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS order by triggered_at desc limit 1)
    """)
    
    # Query 07: Customer Mix
    run_query(session, "07 - Customer Mix Row Counts", """
        with last_run as (
          select run_id, asof_fiscal_yyyymm
          from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS
          order by triggered_at desc
          limit 1
        )
        select
          (select count(*)
           from DB_BI_P_SANDBOX.SANDBOX.FORECAST_CUST_MIX_PC_REASON
           where asof_fiscal_yyyymm = (select asof_fiscal_yyyymm from last_run)
          ) as rows_asof,
          (select count(*)
           from DB_BI_P_SANDBOX.SANDBOX.FORECAST_CUST_MIX_PC_REASON_SNAP
           where run_id = (select run_id from last_run)
          ) as rows_snap
    """)
    
    run_query(session, "07 - Customer Mix Allocation Levels", """
        with last_run as (
          select run_id
          from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS
          order by triggered_at desc
          limit 1
        )
        select allocation_level, count(distinct roll_up_shop || '|' || reason_group) as pc_reason_pairs
        from DB_BI_P_SANDBOX.SANDBOX.FORECAST_CUST_MIX_PC_REASON_SNAP
        where run_id = (select run_id from last_run)
        group by 1
        order by 2 desc
    """)
    
    # Query 08: Budget
    run_query(session, "08 - Budget Row Counts", """
        select count(*) as rows_current
        from DB_BI_P_SANDBOX.SANDBOX.FORECAST_BUDGET_PC_REASON_MTH
    """)
    
    run_query(session, "08 - Budget Snapshot", """
        select count(*) as rows_snap
        from DB_BI_P_SANDBOX.SANDBOX.FORECAST_BUDGET_PC_REASON_MTH_SNAP
        where run_id = (select run_id from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS order by triggered_at desc limit 1)
    """)
    
    # Additional validation queries for model pipeline
    run_query(session, "Model Dataset Row Count", """
        with last_run as (
          select run_id from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS 
          order by triggered_at desc limit 1
        )
        select 
          count(*) as dataset_rows,
          min(anchor_fiscal_yyyymm) as min_anchor,
          max(anchor_fiscal_yyyymm) as max_anchor,
          count(distinct horizon) as horizons
        from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_DATASET_PC_REASON_H_SNAP
        where run_id = (select run_id from last_run)
    """)
    
    run_query(session, "Model Runs Status", """
        with last_run as (
          select run_id from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS 
          order by triggered_at desc limit 1
        )
        select 
          model_scope,
          model_family,
          status,
          count(*) as model_count
        from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS
        where run_id = (select run_id from last_run)
        group by 1,2,3
        order by 1,2,3
    """)
    
    run_query(session, "Champion Models", """
        select 
          champion_scope,
          count(*) as champion_count
        from DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_CHAMPIONS
        where asof_fiscal_yyyymm = (
          select asof_fiscal_yyyymm from DB_BI_P_SANDBOX.SANDBOX.FORECAST_RUNS 
          order by triggered_at desc limit 1
        )
        group by 1
    """)
    
    print(f"\n{'='*80}")
    print("Validation queries complete!")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()
