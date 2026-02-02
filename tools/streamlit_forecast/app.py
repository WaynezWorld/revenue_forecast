"""
Revenue Forecast Streamlit App for Finance
===========================================

A simple UI for viewing monthly revenue forecasts, backtest results, and what-if scenarios.

Features:
- Filters by Year, Month, Area, Division, PC, Reason Code, Customer Group
- Model comparison (last 10 runs)
- PC715 tuned what-if scenario toggle
- Backtest view with accuracy metrics
- CSV export
- Dual mode: Snowflake (if credentials present) or CSV fallback

Author: Data Science Team
Created: 2026-01-31
"""

# ARTHUR PATCH - imports
import os
import traceback
import pandas as pd
import streamlit as st
from datetime import datetime
from pathlib import Path

# Conditional imports
try:
    import plotly.graph_objects as go
    import plotly.express as px
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    st.warning("Plotly not available. Charts will be disabled.")

try:
    import snowflake.connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False


# ═══════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════

REPO_ROOT = Path(__file__).parent.parent.parent
CSV_MODE = not (
    os.getenv("SNOWFLAKE_ACCOUNT") and 
    os.getenv("SNOWFLAKE_USER") and 
    SNOWFLAKE_AVAILABLE
)

MAX_ROWS_DISPLAY = 500
MAX_ROWS_DB = 100000  # Safety limit for DB queries


# ═══════════════════════════════════════════════════════════════════
# DATA LOADING FUNCTIONS
# ═══════════════════════════════════════════════════════════════════

@st.cache_data(ttl=3600)
def load_executive_summary():
    """Load executive summary from presentations folder."""
    try:
        summary_path = REPO_ROOT / "presentations" / "executive_summary.md"
        if summary_path.exists():
            with open(summary_path, 'r') as f:
                content = f.read()
                # Extract first non-empty line or first 200 chars
                lines = [l.strip() for l in content.split('\n') if l.strip() and not l.startswith('#')]
                return lines[0][:200] if lines else "FY2026 Revenue Forecast"
    except Exception as e:
        st.warning(f"Could not load executive summary: {e}")
    return "FY2026 Revenue Forecast"


@st.cache_data(ttl=3600)
def load_csv_data():
    """Load forecast data from CSV files (fallback mode)."""
    data = {}
    
    try:
        # Monthly forecast
        forecast_path = REPO_ROOT / "presentations" / "monthly_forecast_fy2026.csv"
        if forecast_path.exists():
            data['forecast'] = pd.read_csv(forecast_path)
        
        # Budget backtest
        backtest_path = REPO_ROOT / "presentations" / "budget_backtest.csv"
        if backtest_path.exists():
            data['backtest'] = pd.read_csv(backtest_path)
        
        # PC715 tuned what-if
        whatif_path = REPO_ROOT / "experiments" / "pc715" / "pc715_tuned_preds.csv"
        if whatif_path.exists():
            data['whatif'] = pd.read_csv(whatif_path)
        
        # Forecast accuracy (optional)
        accuracy_path = REPO_ROOT / "presentations" / "forecast_accuracy.md"
        if accuracy_path.exists():
            with open(accuracy_path, 'r') as f:
                data['accuracy_text'] = f.read()
        
    except Exception as e:
        st.error(f"Error loading CSV data: {e}")
    
    return data


# ARTHUR PATCH - robust Snowflake connection helper
def get_snowflake_connection():
    """Return Snowflake connection or None. Supports password and externalbrowser auth."""
    if not SNOWFLAKE_AVAILABLE:
        return None
    
    account = os.environ.get("SNOWFLAKE_ACCOUNT")
    user = os.environ.get("SNOWFLAKE_USER")
    password = os.environ.get("SNOWFLAKE_PASSWORD")
    role = os.environ.get("SNOWFLAKE_ROLE")
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")
    database = os.environ.get("SNOWFLAKE_DATABASE")
    schema = os.environ.get("SNOWFLAKE_SCHEMA")
    authenticator = os.environ.get("SNOWFLAKE_AUTHENTICATOR")  # optional, e.g., externalbrowser

    if not account or not user:
        return None

    kwargs = {
        "user": user,
        "account": account,
        "role": role,
        "warehouse": warehouse,
        "database": database,
        "schema": schema,
        "client_session_keep_alive": True,
    }
    if password:
        kwargs["password"] = password
    else:
        if authenticator and authenticator.lower() == "externalbrowser":
            kwargs["authenticator"] = "externalbrowser"
        else:
            # No interactive auth configured
            return None

    try:
        conn = snowflake.connector.connect(**{k: v for k, v in kwargs.items() if v is not None})
        # quick smoke test
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        return conn
    except Exception as e:
        # return None and let caller handle UI messaging
        return None


# ARTHUR PATCH - diagnostic function
def _sf_diag():
    """Return diagnostic info about Snowflake connection for troubleshooting."""
    info = {}
    info['SNOWFLAKE_ACCOUNT'] = bool(os.getenv('SNOWFLAKE_ACCOUNT'))
    info['SNOWFLAKE_USER'] = bool(os.getenv('SNOWFLAKE_USER'))
    info['SNOWFLAKE_PASSWORD_PRESENT'] = bool(os.getenv('SNOWFLAKE_PASSWORD'))
    info['SNOWFLAKE_AUTHENTICATOR'] = os.getenv('SNOWFLAKE_AUTHENTICATOR')
    info['SNOWFLAKE_WAREHOUSE'] = os.getenv('SNOWFLAKE_WAREHOUSE')
    info['SNOWFLAKE_DATABASE'] = os.getenv('SNOWFLAKE_DATABASE')
    info['SNOWFLAKE_SCHEMA'] = os.getenv('SNOWFLAKE_SCHEMA')
    info['SNOWFLAKE_AVAILABLE'] = SNOWFLAKE_AVAILABLE
    try:
        conn = get_snowflake_connection()
        if conn:
            cur = conn.cursor()
            cur.execute("SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
            result = cur.fetchone()
            cur.close()
            conn.close()
            info['CONN_OK'] = True
            info['CONN_MSG'] = f"Connected: {result}"
        else:
            info['CONN_OK'] = False
            info['CONN_MSG'] = "get_snowflake_connection() returned None - missing creds or externalbrowser not configured"
    except Exception as e:
        info['CONN_OK'] = False
        info['CONN_MSG'] = str(e)
        info['CONN_TRACE'] = traceback.format_exc()
    return info


# ARTHUR PATCH - load_model_runs_from_db
@st.cache_data(ttl=3600)
def load_model_runs_from_db(limit=20):
    """Load model runs from Snowflake using connector cursor."""
    conn = get_snowflake_connection()
    if not conn:
        return None
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT model_run_id,
                   COALESCE(params:candidate::string, RUN_ID) AS model_name,
                   model_family, asof_fiscal_yyyymm, created_at
            FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS
            ORDER BY created_at DESC
            LIMIT {limit}
        """)
        df = cur.fetch_pandas_all()
        cur.close()
        conn.close()
        return df
    except Exception as e:
        if cur: cur.close()
        try: conn.close()
        except: pass
        return None


# ARTHUR PATCH - load_forecast_from_db
@st.cache_data(ttl=3600)
def load_forecast_from_db(model_run_id, year=None, period=None):
    """Load forecast data from vw_forecast_report_mart."""
    conn = get_snowflake_connection()
    if not conn:
        return None
    cur = conn.cursor()
    try:
        filters = [f"model_run_id = '{model_run_id}'", "horizon = 1"]
        if year:
            filters.append(f"FORECAST_FISCAL_YEAR = {int(year)}")
        if period:
            filters.append(f"FORECAST_FISCAL_PERIOD = {int(period)}")
        where = " AND ".join(filters)
        cur.execute(f"""
            SELECT model_run_id, roll_up_shop AS pc, roll_up_shop_name,
                   customer_group, reason_code_group, anchor_fiscal_yyyymm, horizon,
                   target_fiscal_yyyymm AS forecast_month, y_true, y_pred AS forecast,
                   y_pred_lo AS forecast_lo, y_pred_hi AS forecast_hi, created_at
            FROM DB_BI_P_SANDBOX.SANDBOX.vw_forecast_report_mart
            WHERE {where}
            ORDER BY roll_up_shop, reason_code_group, forecast_month
            LIMIT {MAX_ROWS_DB}
        """)
        df = cur.fetch_pandas_all()
        cur.close()
        conn.close()
        return df
    except Exception as e:
        if cur: cur.close()
        try: conn.close()
        except: pass
        return None


# ARTHUR PATCH - load_filter_values_from_db
@st.cache_data(ttl=3600)
def load_filter_values_from_db():
    """Return filter values from champion view."""
    conn = get_snowflake_connection()
    if not conn:
        return None
    cur = conn.cursor()
    try:
        # Get distinct filters from champion view for stable keys
        cur.execute("""
            SELECT DISTINCT
              roll_up_shop,
              roll_up_shop_name,
              customer_group,
              reason_code_group,
              forecast_fiscal_year,
              forecast_fiscal_period
            FROM DB_BI_P_SANDBOX.SANDBOX.vw_forecast_for_report_champion
        """)
        fdf = cur.fetch_pandas_all()
        cur.close()
        conn.close()
        return {
            "pcs": sorted(fdf['ROLL_UP_SHOP'].dropna().unique().tolist()) if 'ROLL_UP_SHOP' in fdf.columns else [],
            "pc_names": {r['ROLL_UP_SHOP']: r['ROLL_UP_SHOP_NAME'] for _, r in fdf.drop_duplicates('ROLL_UP_SHOP').iterrows()} if 'ROLL_UP_SHOP_NAME' in fdf.columns else {},
            "customer_groups": sorted(fdf['CUSTOMER_GROUP'].dropna().unique().tolist()) if 'CUSTOMER_GROUP' in fdf.columns else [],
            "reason_groups": sorted(fdf['REASON_CODE_GROUP'].dropna().unique().tolist()) if 'REASON_CODE_GROUP' in fdf.columns else [],
            "years": sorted(fdf['FORECAST_FISCAL_YEAR'].dropna().unique().tolist()) if 'FORECAST_FISCAL_YEAR' in fdf.columns else []
        }
    except Exception as e:
        if cur: cur.close()
        try: conn.close()
        except: pass
        return None


# ═══════════════════════════════════════════════════════════════════
# UI HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════

def format_currency(value):
    """Format value as currency."""
    if pd.isna(value):
        return "N/A"
    return f"${value:,.0f}"


def compute_what_if(forecast_df, whatif_df):
    """Replace PC715 champion forecasts with tuned what-if forecasts."""
    if forecast_df is None or forecast_df.empty:
        return forecast_df
    if whatif_df is None or whatif_df.empty:
        st.warning("No what-if data available")
        return forecast_df
    
    # Create copy
    result = forecast_df.copy()
    
    # ARTHUR PATCH - Check if required columns exist
    if 'pc' not in result.columns:
        return result
    
    # Replace PC715 rows with what-if values
    pc715_mask = result['pc'] == '715'
    
    if 'forecast' in result.columns and 'forecast' in whatif_df.columns:
        # Merge on common keys
        merge_keys = ['pc', 'forecast_month'] if 'forecast_month' in whatif_df.columns else ['pc']
        whatif_agg = whatif_df[whatif_df['pc'] == '715'].groupby(merge_keys)['forecast'].sum().reset_index()
        whatif_agg.rename(columns={'forecast': 'forecast_whatif'}, inplace=True)
        
        result = result.merge(whatif_agg, on=merge_keys, how='left')
        result.loc[pc715_mask, 'forecast'] = result.loc[pc715_mask, 'forecast_whatif']
        result.drop(columns=['forecast_whatif'], inplace=True, errors='ignore')
    
    return result


# ═══════════════════════════════════════════════════════════════════
# MAIN APP
# ═══════════════════════════════════════════════════════════════════

def main():
    global CSV_MODE
    
    st.set_page_config(
        page_title="Revenue Forecast Viewer",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("📊 Revenue Forecast Viewer")
    st.caption("Data Science Team | Finance Dashboard")
    
    # ARTHUR PATCH - Add diagnostic expander
    with st.expander("🔧 Snowflake Diagnostic (DEBUG)", expanded=False):
        diag = _sf_diag()
        for k, v in diag.items():
            st.write(f"**{k}**: {v}")
    
    # Test Snowflake connection at startup and set mode
    test_conn = get_snowflake_connection()
    if test_conn:
        CSV_MODE = False
        test_conn.close()
    else:
        CSV_MODE = True
    
    # Show mode
    mode_indicator = "🔗 Snowflake Mode" if not CSV_MODE else "📂 CSV Mode (No Snowflake credentials)"
    st.sidebar.markdown(f"**Mode**: {mode_indicator}")
    
    if CSV_MODE:
        st.sidebar.info("Running in CSV Mode: Snowflake credentials not found or connection failed.")
    
    # ───────────────────────────────────────────────────────────────
    # SIDEBAR FILTERS
    # ───────────────────────────────────────────────────────────────
    
    st.sidebar.header("Filters")
    
    # Model selection
    selected_model_id = None
    forecast_df = pd.DataFrame()
    backtest_df = pd.DataFrame()
    whatif_df = pd.DataFrame()
    
    if CSV_MODE:
        # CSV mode - load from files
        data = load_csv_data()
        forecast_df = data.get('forecast', pd.DataFrame())
        backtest_df = data.get('backtest', pd.DataFrame())
        whatif_df = data.get('whatif', pd.DataFrame())
    else:
        # DB mode - load model runs from FORECAST_MODEL_RUNS
        st.sidebar.subheader("Model Selection")
        model_runs_df = load_model_runs_from_db()
        
        # ARTHUR PATCH - show diagnostic info
        if model_runs_df is not None:
            st.sidebar.caption(f"📊 Models loaded: {len(model_runs_df)}")
        
        if model_runs_df is None or model_runs_df.empty:
            st.sidebar.warning("No models found in FORECAST_MODEL_RUNS. Falling back to CSV mode.")
            CSV_MODE = True
            data = load_csv_data()
            forecast_df = data.get('forecast', pd.DataFrame())
            backtest_df = data.get('backtest', pd.DataFrame())
            whatif_df = data.get('whatif', pd.DataFrame())
        else:
            model_options = [
                f"{row['MODEL_NAME']} ({row['MODEL_RUN_ID'][:8]}...)"
                for _, row in model_runs_df.iterrows()
            ]
            sel_idx = st.sidebar.selectbox(
                "Select Model Run",
                list(range(len(model_options))),
                format_func=lambda i: model_options[i]
            )
            selected_model_id = model_runs_df.iloc[sel_idx]['MODEL_RUN_ID']
            
            # Load forecast for selected model
            forecast_df = load_forecast_from_db(selected_model_id)
            if forecast_df is None or forecast_df.empty:
                st.warning("Could not load forecast from DB. Check if vw_forecast_report_mart has data for this model.")
                forecast_df = pd.DataFrame()
    
    # Load filter values from data
    if not CSV_MODE and not forecast_df.empty:
        # Extract filter values from loaded forecast data
        pcs = sorted(forecast_df['pc'].dropna().unique().tolist()) if 'pc' in forecast_df.columns else []
        customer_groups = sorted(forecast_df['customer_group'].dropna().unique().tolist()) if 'customer_group' in forecast_df.columns else []
        reason_groups = sorted(forecast_df['reason_code_group'].dropna().unique().tolist()) if 'reason_code_group' in forecast_df.columns else []
        years = sorted(forecast_df['forecast_month'].astype(str).str[:4].unique().tolist()) if 'forecast_month' in forecast_df.columns else []
    elif CSV_MODE and not forecast_df.empty:
        # Extract from CSV data
        pcs = sorted(forecast_df['pc'].dropna().unique().tolist()) if 'pc' in forecast_df.columns else []
        customer_groups = sorted(forecast_df['customer_group'].dropna().unique().tolist()) if 'customer_group' in forecast_df.columns else []
        reason_groups = sorted(forecast_df['reason_code_group'].dropna().unique().tolist()) if 'reason_code_group' in forecast_df.columns else []
        years = sorted(forecast_df['forecast_month'].astype(str).str[:4].unique().tolist()) if 'forecast_month' in forecast_df.columns else []
    else:
        pcs = []
        customer_groups = []
        reason_groups = []
        years = []
    
    # Filter widgets
    pc_choice = st.sidebar.selectbox("Roll Up Shop / PC", ["All"] + pcs) if pcs else "All"
    cust_choice = st.sidebar.selectbox("Customer Group", ["All"] + customer_groups) if customer_groups else "All"
    reason_choice = st.sidebar.selectbox("Reason Code Group", ["All"] + reason_groups) if reason_groups else "All"
    year_choice = st.sidebar.selectbox("Year", ["All"] + years) if years else "All"
    
    # Month filter (derived from year)
    if not forecast_df.empty and 'forecast_month' in forecast_df.columns and year_choice != "All":
        months = sorted(forecast_df[forecast_df['forecast_month'].astype(str).str[:4] == str(year_choice)]['forecast_month'].unique())
        month_filter = st.sidebar.selectbox("Forecast Month", ["All"] + [str(m) for m in months]) if months else "All"
    else:
        month_filter = "All"
    
    # PC715 what-if toggle
    include_whatif = st.sidebar.checkbox(
        "Include PC715 tuned what-if",
        value=False,
        help="Replace PC715 champion forecast with tuned model (not deployed)"
    )
    
    # Apply button
    apply_filters = st.sidebar.button("Apply Filters", type="primary")
    
    # ───────────────────────────────────────────────────────────────
    # MAIN VIEW
    # ───────────────────────────────────────────────────────────────
    
    # Executive summary
    summary = load_executive_summary()
    st.info(f"**Summary**: {summary}")
    
    # Apply what-if if toggled
    if include_whatif and not whatif_df.empty:
        display_df = compute_what_if(forecast_df, whatif_df)
        st.caption("✏️ **What-if enabled**: PC715 forecasts replaced with tuned model")
    else:
        display_df = forecast_df.copy()
    
    # Apply filters to display_df
    if not display_df.empty:
        if pc_choice and pc_choice != "All" and 'pc' in display_df.columns:
            display_df = display_df[display_df['pc'] == pc_choice]
        if cust_choice and cust_choice != "All" and 'customer_group' in display_df.columns:
            display_df = display_df[display_df['customer_group'] == cust_choice]
        if reason_choice and reason_choice != "All" and 'reason_code_group' in display_df.columns:
            display_df = display_df[display_df['reason_code_group'] == reason_choice]
        if year_choice and year_choice != "All" and 'forecast_month' in display_df.columns:
            display_df = display_df[display_df['forecast_month'].astype(str).str[:4] == str(year_choice)]
        if month_filter and month_filter != "All" and 'forecast_month' in display_df.columns:
            display_df = display_df[display_df['forecast_month'] == int(month_filter)]
    
    # ───────────────────────────────────────────────────────────────
    # TAB 1: MONTHLY FORECAST
    # ───────────────────────────────────────────────────────────────
    
    tab1, tab2, tab3 = st.tabs(["📅 Monthly Forecast", "📊 Backtest", "🔍 Model Comparison"])
    
    with tab1:
        st.subheader("Next 12 Months Forecast")
        
        # ARTHUR PATCH - robust empty-frame handling
        if display_df is None or display_df.empty:
            st.warning("No forecast data available for selected filters")
        else:
            # Aggregate by month
            if 'forecast' in display_df.columns:
                agg_dict = {
                    'forecast': 'sum',
                    'forecast_lo': 'sum',
                    'forecast_hi': 'sum'
                }
                if 'y_true' in display_df.columns:
                    agg_dict['y_true'] = 'sum'
                
                monthly_agg = display_df.groupby('forecast_month').agg(agg_dict).reset_index()
                
                # Display table
                st.dataframe(
                    monthly_agg.head(MAX_ROWS_DISPLAY),
                    use_container_width=True,
                    hide_index=True
                )
                
                # Chart
                if PLOTLY_AVAILABLE:
                    fig = go.Figure()
                    
                    # Confidence band
                    fig.add_trace(go.Scatter(
                        x=monthly_agg['forecast_month'],
                        y=monthly_agg['forecast_hi'],
                        mode='lines',
                        line=dict(width=0),
                        showlegend=False,
                        hoverinfo='skip'
                    ))
                    fig.add_trace(go.Scatter(
                        x=monthly_agg['forecast_month'],
                        y=monthly_agg['forecast_lo'],
                        mode='lines',
                        line=dict(width=0),
                        fillcolor='rgba(68, 68, 68, 0.2)',
                        fill='tonexty',
                        name='80% Confidence',
                        hoverinfo='skip'
                    ))
                    
                    # Forecast line
                    fig.add_trace(go.Scatter(
                        x=monthly_agg['forecast_month'],
                        y=monthly_agg['forecast'],
                        mode='lines+markers',
                        name='Forecast',
                        line=dict(color='blue', width=2)
                    ))
                    
                    # Actuals line (if available)
                    if 'y_true' in monthly_agg.columns:
                        fig.add_trace(go.Scatter(
                            x=monthly_agg['forecast_month'],
                            y=monthly_agg['y_true'],
                            mode='lines+markers',
                            name='Actuals',
                            line=dict(color='green', width=2, dash='dot')
                        ))
                    
                    fig.update_layout(
                        title="Monthly Revenue Forecast",
                        xaxis_title="Forecast Month",
                        yaxis_title="Revenue ($)",
                        hovermode='x unified',
                        height=400
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
            
            # Download button
            csv = display_df.head(MAX_ROWS_DISPLAY).to_csv(index=False)
            year_for_file = year_choice if year_choice != "All" else "all"
            st.download_button(
                label="Download visible table as CSV",
                data=csv,
                file_name=f"forecast_{year_for_file}_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
    
    # ───────────────────────────────────────────────────────────────
    # TAB 2: BACKTEST
    # ───────────────────────────────────────────────────────────────
    
    with tab2:
        st.subheader("Backtest Results (Last 12 Anchors)")
        
        if backtest_df.empty:
            st.warning("No backtest data available")
        else:
            # Display backtest table
            display_cols = [c for c in ['anchor', 'pc', 'reason_group', 'y_true', 'y_pred', 'abs_err', 'budget'] if c in backtest_df.columns]
            if display_cols:
                st.dataframe(
                    backtest_df[display_cols].head(MAX_ROWS_DISPLAY),
                    use_container_width=True,
                    hide_index=True
                )
                
                # Compute metrics
                if 'abs_err' in backtest_df.columns and 'y_true' in backtest_df.columns:
                    wape = (backtest_df['abs_err'].sum() / backtest_df['y_true'].sum()) * 100
                    mae = backtest_df['abs_err'].mean()
                    
                    col1, col2 = st.columns(2)
                    col1.metric("WAPE", f"{wape:.2f}%")
                    col2.metric("MAE", format_currency(mae))
    
    # ───────────────────────────────────────────────────────────────
    # TAB 3: MODEL COMPARISON
    # ───────────────────────────────────────────────────────────────
    
    with tab3:
        st.subheader("Model Comparison")
        
        if CSV_MODE:
            st.info("Model comparison requires Snowflake connection")
        else:
            st.info("Model comparison feature - select multiple models to compare side-by-side")
            st.caption("(Feature in development)")


if __name__ == "__main__":
    main()
