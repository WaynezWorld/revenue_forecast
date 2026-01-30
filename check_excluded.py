import snowflake.snowpark as snowpark
from snowflake.snowpark import Session

def main():
    connection_parameters = {
        "account": "CONA-CCCI",
        "user": "waynez@coca-cola.com",
        "authenticator": "externalbrowser",
        "warehouse": "BI_P_QRY_PU_WH",
        "database": "DB_BI_P_SANDBOX",
        "schema": "SANDBOX",
        "role": "BI_P_ADMIN"
    }
    
    session = Session.builder.configs(connection_parameters).create()
    
    print("=" * 80)
    print("QUERY 1: Show excluded PCs with exclusion reasons")
    print("=" * 80)
    
    q1 = """
    SELECT 
        roll_up_shop,
        is_eligible,
        exclusion_reasons,
        months_present_lookback,
        avg_rev_lookback,
        total_rev_lookback
    FROM FORECAST_PC_ELIGIBILITY
    WHERE run_id = '5d0f5e35-b4e1-4af4-a592-e5f7f05f9686'
      AND is_eligible = FALSE
    ORDER BY roll_up_shop
    """
    
    df1 = session.sql(q1)
    df1.show(100)
    
    print("\n" + "=" * 80)
    print("QUERY 2: Count eligible vs excluded PCs")
    print("=" * 80)
    
    q2 = """
    SELECT 
        is_eligible,
        COUNT(*) as pc_count
    FROM FORECAST_PC_ELIGIBILITY
    WHERE run_id = '5d0f5e35-b4e1-4af4-a592-e5f7f05f9686'
    GROUP BY is_eligible
    ORDER BY is_eligible DESC
    """
    
    df2 = session.sql(q2)
    df2.show()
    
    print("\n" + "=" * 80)
    print("QUERY 3: Verify forecasted PCs match eligible list")
    print("=" * 80)
    
    q3 = """
    SELECT 
        COUNT(DISTINCT f.profit_center) as forecasted_pc_count,
        COUNT(DISTINCT e.roll_up_shop) as eligible_pc_count,
        COUNT(DISTINCT CASE WHEN e.roll_up_shop IS NULL THEN f.profit_center END) as pcs_forecasted_but_not_eligible
    FROM FORECAST_OUTPUT_PC_REASON_MTH f
    LEFT JOIN FORECAST_PC_ELIGIBILITY e
        ON f.profit_center = e.roll_up_shop
        AND e.run_id = '5d0f5e35-b4e1-4af4-a592-e5f7f05f9686'
        AND e.is_eligible = TRUE
    WHERE f.forecast_run_id = '418a2568-5027-4611-a64e-a2e741b9a90b'
    """
    
    df3 = session.sql(q3)
    df3.show()
    
    print("\n" + "=" * 80)
    print("QUERY 4: Identify PC×Reason with negative confidence bounds")
    print("=" * 80)
    
    q4 = """
    SELECT 
        profit_center,
        pc_reason,
        COUNT(*) as months_with_negative_lo,
        MIN(revenue_forecast_lo) as min_lo,
        AVG(revenue_forecast) as avg_forecast,
        AVG(revenue_forecast_lo) as avg_lo
    FROM FORECAST_OUTPUT_PC_REASON_MTH
    WHERE forecast_run_id = '418a2568-5027-4611-a64e-a2e741b9a90b'
      AND revenue_forecast_lo < 0
    GROUP BY profit_center, pc_reason
    ORDER BY min_lo ASC
    LIMIT 20
    """
    
    df4 = session.sql(q4)
    df4.show(100)
    
    session.close()

if __name__ == "__main__":
    main()
