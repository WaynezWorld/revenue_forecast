# Revenue Forecast Streamlit App

A simple UI for Finance to view monthly revenue forecasts, backtest results, and what-if scenarios.

## Features

- **Dual Mode**: Snowflake database (when credentials available) or CSV fallback
- **Filters**: Year, Month, Area, Division, PC, Reason Code, Customer Group
- **Model Selection**: View forecasts from last 10 model runs
- **What-If Analysis**: Toggle PC715 tuned scenario (clearly labeled as not deployed)
- **Backtest View**: Last 12 anchors with accuracy metrics (WAPE, MAE)
- **Charts**: Plotly visualizations with 80% confidence bands
- **Export**: Download displayed tables as CSV

## Installation

```bash
cd tools/streamlit_forecast
pip install -r requirements.txt
```

## Running the App

### Option 1: CSV Mode (No Database Required)

Default mode when Snowflake credentials are not provided. Reads from local CSV files:
- `presentations/monthly_forecast_fy2026.csv`
- `presentations/budget_backtest.csv`
- `experiments/pc715/pc715_tuned_preds.csv`

```bash
streamlit run app.py
```

The app will open in your browser at `http://localhost:8501`

### Option 2: Snowflake Mode (Database Access)

Set environment variables for Snowflake connection:

**PowerShell**:
```powershell
$env:SNOWFLAKE_ACCOUNT = "your_account"
$env:SNOWFLAKE_USER = "your_username"
$env:SNOWFLAKE_PASSWORD = "your_password"
$env:SNOWFLAKE_ROLE = "SNFL_PRD_BI_POWERUSER_FR"
$env:SNOWFLAKE_WAREHOUSE = "BI_P_QRY_FIN_OPT_WH"
$env:SNOWFLAKE_DATABASE = "DB_BI_P_SANDBOX"
$env:SNOWFLAKE_SCHEMA = "SANDBOX"

streamlit run app.py
```

**Bash/Linux**:
```bash
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_ROLE="SNFL_PRD_BI_POWERUSER_FR"
export SNOWFLAKE_WAREHOUSE="BI_P_QRY_FIN_OPT_WH"
export SNOWFLAKE_DATABASE="DB_BI_P_SANDBOX"
export SNOWFLAKE_SCHEMA="SANDBOX"

streamlit run app.py
```

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SNOWFLAKE_ACCOUNT` | No (CSV fallback) | - | Snowflake account identifier |
| `SNOWFLAKE_USER` | No (CSV fallback) | - | Snowflake username |
| `SNOWFLAKE_PASSWORD` | No (CSV fallback) | - | Snowflake password |
| `SNOWFLAKE_ROLE` | No | `SNFL_PRD_BI_POWERUSER_FR` | Snowflake role |
| `SNOWFLAKE_WAREHOUSE` | No | `BI_P_QRY_FIN_OPT_WH` | Snowflake warehouse |
| `SNOWFLAKE_DATABASE` | No | `DB_BI_P_SANDBOX` | Snowflake database |
| `SNOWFLAKE_SCHEMA` | No | `SANDBOX` | Snowflake schema |

## Safety & Constraints

### Read-Only Operations
- **No DDL/DML**: App only executes `SELECT` queries
- **Row Limits**: Max 100,000 rows per query (safety limit)
- **Display Limit**: Shows top 500 rows in UI tables by default
- **No Production Modifications**: Cannot alter database objects

### Query Limits
All Snowflake queries are limited:
- Forecast queries: `LIMIT 100000` and filtered by date range
- Model runs: `LIMIT 10`
- WHERE clauses restrict data scans

### CSV Fallback
If Snowflake credentials not present or connection fails, app automatically falls back to reading CSV files from the repository.

## UI Components

### Sidebar Filters
- **Year**: Select fiscal year
- **Forecast Month**: Select specific month
- **Model Selector**: Choose from last 10 model runs (DB mode only)
- **PC715 What-If Toggle**: Enable/disable tuned scenario
- **Apply Filters**: Refresh data with selected filters
- **Download**: Export visible table to CSV

### Main View Tabs

#### Tab 1: Monthly Forecast
- Next 12 months forecast table
- Aggregated by month (company totals)
- Line chart with 80% confidence bands
- Download button for CSV export

#### Tab 2: Backtest
- Last 12 anchors backtest results
- Columns: anchor, pc, reason_group, y_true, y_pred, abs_err, budget
- Summary metrics: WAPE, MAE
- Performance by horizon (if available)

#### Tab 3: Model Comparison
- Side-by-side comparison of multiple models (DB mode only)
- Highlight best WAPE model
- Feature in development

## What-If Scenario

When **"Include PC715 tuned what-if"** checkbox is enabled:
- Replaces PC715 champion forecasts with tuned model predictions
- Clearly labeled as **"what-if — PC715 tuned"** (not deployed)
- Uses `experiments/pc715/pc715_tuned_preds.csv` or database equivalent
- Does NOT modify production forecasts

## Troubleshooting

### "No forecast data available"
- **CSV Mode**: Ensure CSV files exist in `presentations/` folder
- **DB Mode**: Check Snowflake credentials and network access
- Verify filters are not too restrictive

### "Snowflake connection failed"
- Verify environment variables are set correctly
- Check network connectivity to Snowflake
- App will automatically fall back to CSV mode

### Charts not displaying
- Ensure `plotly` is installed: `pip install plotly`
- Charts require data to be loaded successfully

### Import errors
- Run `pip install -r requirements.txt`
- For Snowflake mode: ensure `snowflake-connector-python` is installed

## Data Sources

### CSV Mode Files
```
presentations/
  ├── monthly_forecast_fy2026.csv       # Champion forecasts
  ├── budget_backtest.csv                # Backtest with actuals vs budget
  └── executive_summary.md               # One-line summary

experiments/pc715/
  └── pc715_tuned_preds.csv              # PC715 what-if scenario
```

### Snowflake Mode Tables
```sql
-- Model runs (last 10)
SELECT * FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS
WHERE wape IS NOT NULL
ORDER BY created_at DESC
LIMIT 10;

-- Forecast data
SELECT * FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS
WHERE model_run_id = '...'
  AND target_fiscal_yyyymm BETWEEN 202601 AND 202612
LIMIT 100000;
```

## Performance

- **Caching**: Data cached for 1 hour (`@st.cache_data(ttl=3600)`)
- **Pagination**: Only top 500 rows displayed by default
- **Query Limits**: All DB queries have `LIMIT` clauses
- **Lazy Loading**: Data loaded only when needed

## Security

- **No Credentials in Code**: All credentials via environment variables
- **No Credentials in Repo**: `.gitignore` prevents credential files
- **Read-Only**: App cannot modify database
- **Session Isolation**: Each user session is isolated

## Support

**Contact**: Data Science Team  
**Slack**: #revenue-forecasting  
**Issues**: Report in GitHub repo

## Version History

- **v1.0** (2026-01-31): Initial release
  - CSV and Snowflake dual mode
  - Monthly forecast view
  - Backtest view
  - PC715 what-if toggle
  - CSV export
