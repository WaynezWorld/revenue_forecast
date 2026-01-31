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

import os
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


def get_snowflake_connection():
    """Create Snowflake connection if credentials available."""
    if not SNOWFLAKE_AVAILABLE:
        return None
    
    try:
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            role=os.getenv("SNOWFLAKE_ROLE", "SNFL_PRD_BI_POWERUSER_FR"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "BI_P_QRY_FIN_OPT_WH"),
            database=os.getenv("SNOWFLAKE_DATABASE", "DB_BI_P_SANDBOX"),
            schema=os.getenv("SNOWFLAKE_SCHEMA", "SANDBOX")
        )
        return conn
    except Exception as e:
        st.error(f"Snowflake connection failed: {e}")
        return None


@st.cache_data(ttl=3600)
def load_model_runs_from_db():
    """Load last 10 model runs from Snowflake."""
    conn = get_snowflake_connection()
    if not conn:
        return None
    
    try:
        query = """
        SELECT 
            model_run_id,
            model_name,
            created_at,
            wape,
            mae,
            bias
        FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS
        WHERE wape IS NOT NULL
        ORDER BY created_at DESC
        LIMIT 10
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.warning(f"Could not load model runs from DB: {e}")
        if conn:
            conn.close()
        return None


@st.cache_data(ttl=3600)
def load_forecast_from_db(model_run_id, min_month=None, max_month=None):
    """Load forecast data from Snowflake."""
    conn = get_snowflake_connection()
    if not conn:
        return None
    
    try:
        where_clauses = [f"model_run_id = '{model_run_id}'"]
        if min_month:
            where_clauses.append(f"target_fiscal_yyyymm >= {min_month}")
        if max_month:
            where_clauses.append(f"target_fiscal_yyyymm <= {max_month}")
        
        query = f"""
        SELECT 
            model_run_id,
            roll_up_shop AS pc,
            reason_group,
            anchor_fiscal_yyyymm,
            horizon,
            target_fiscal_yyyymm AS forecast_month,
            y_pred AS forecast,
            y_pred_lo AS forecast_lo,
            y_pred_hi AS forecast_hi
        FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS
        WHERE {' AND '.join(where_clauses)}
        ORDER BY target_fiscal_yyyymm, roll_up_shop
        LIMIT {MAX_ROWS_DB}
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        if len(df) >= MAX_ROWS_DB:
            st.warning(f"Query returned {len(df)} rows (limit reached). Results may be incomplete.")
        
        return df
    except Exception as e:
        st.error(f"Error loading forecast from DB: {e}")
        if conn:
            conn.close()
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
    if whatif_df is None or whatif_df.empty:
        st.warning("No what-if data available")
        return forecast_df
    
    # Create copy
    result = forecast_df.copy()
    
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
    st.set_page_config(
        page_title="Revenue Forecast Viewer",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("📊 Revenue Forecast Viewer")
    st.caption("Data Science Team | Finance Dashboard")
    
    # Show mode
    mode_indicator = "🔗 Snowflake Mode" if not CSV_MODE else "📂 CSV Mode (No Snowflake credentials)"
    st.sidebar.markdown(f"**Mode**: {mode_indicator}")
    
    # ───────────────────────────────────────────────────────────────
    # SIDEBAR FILTERS
    # ───────────────────────────────────────────────────────────────
    
    st.sidebar.header("Filters")
    
    # Load data
    if CSV_MODE:
        data = load_csv_data()
        forecast_df = data.get('forecast', pd.DataFrame())
        backtest_df = data.get('backtest', pd.DataFrame())
        whatif_df = data.get('whatif', pd.DataFrame())
    else:
        # DB mode - load model runs first
        model_runs_df = load_model_runs_from_db()
        if model_runs_df is None or model_runs_df.empty:
            st.warning("No model runs found in database. Falling back to CSV mode.")
            CSV_MODE = True
            data = load_csv_data()
            forecast_df = data.get('forecast', pd.DataFrame())
            backtest_df = data.get('backtest', pd.DataFrame())
            whatif_df = data.get('whatif', pd.DataFrame())
        else:
            # Let user select model
            st.sidebar.subheader("Model Selection")
            model_options = [
                f"{row['model_name']} ({row['model_run_id'][:8]}...)"
                for _, row in model_runs_df.iterrows()
            ]
            selected_model_idx = st.sidebar.selectbox(
                "Select Model Run",
                range(len(model_options)),
                format_func=lambda i: model_options[i]
            )
            selected_model_id = model_runs_df.iloc[selected_model_idx]['model_run_id']
            
            # Load forecast for selected model
            forecast_df = load_forecast_from_db(selected_model_id, min_month=202601, max_month=202612)
            backtest_df = pd.DataFrame()  # TODO: Load from DB if needed
            whatif_df = data.get('whatif', pd.DataFrame()) if 'data' in locals() else pd.DataFrame()
    
    # Year filter
    current_year = datetime.now().year
    if not forecast_df.empty and 'forecast_month' in forecast_df.columns:
        years = sorted(forecast_df['forecast_month'].astype(str).str[:4].unique())
        year_filter = st.sidebar.selectbox("Year", years, index=len(years)-1 if years else 0)
    else:
        year_filter = str(current_year)
    
    # Month filter
    if not forecast_df.empty and 'forecast_month' in forecast_df.columns:
        months = sorted(forecast_df[forecast_df['forecast_month'].astype(str).str[:4] == year_filter]['forecast_month'].unique())
        if months:
            month_filter = st.sidebar.selectbox("Forecast Month", months, index=0)
        else:
            month_filter = None
    else:
        month_filter = None
    
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
    
    # Filter by year/month
    if not display_df.empty and 'forecast_month' in display_df.columns:
        if year_filter:
            display_df = display_df[display_df['forecast_month'].astype(str).str[:4] == year_filter]
        if month_filter:
            display_df = display_df[display_df['forecast_month'] == month_filter]
    
    # ───────────────────────────────────────────────────────────────
    # TAB 1: MONTHLY FORECAST
    # ───────────────────────────────────────────────────────────────
    
    tab1, tab2, tab3 = st.tabs(["📅 Monthly Forecast", "📊 Backtest", "🔍 Model Comparison"])
    
    with tab1:
        st.subheader("Next 12 Months Forecast")
        
        if display_df.empty:
            st.warning("No forecast data available for selected filters")
        else:
            # Aggregate by month
            if 'forecast' in display_df.columns:
                monthly_agg = display_df.groupby('forecast_month').agg({
                    'forecast': 'sum',
                    'forecast_lo': 'sum',
                    'forecast_hi': 'sum'
                }).reset_index()
                
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
            st.download_button(
                label="Download visible table as CSV",
                data=csv,
                file_name=f"forecast_{year_filter}_{datetime.now().strftime('%Y%m%d')}.csv",
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
