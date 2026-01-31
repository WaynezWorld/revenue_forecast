"""
Error Decomposition Analysis - Complete
Generates revenue-weighted error analysis by customer group and reason code group
"""

from snowflake.snowpark import Session
import pandas as pd
import numpy as np
from datetime import datetime
import os

# Create session
connection_params = {
    'account': 'CONA-CCCI',
    'user': 'NBALJE',
    'authenticator': 'externalbrowser',
    'database': 'DB_BI_P_SANDBOX',
    'schema': 'SANDBOX',
    'role': 'SNFL_PRD_BI_POWERUSER_FR'
}

print("Creating Snowflake session...")
session = Session.builder.configs(connection_params).create()
session.sql('USE WAREHOUSE BI_P_QRY_FIN_OPT_WH').collect()
print(f"[OK] Connected to {session.get_current_database()}.{session.get_current_schema()}")

# Champion model ID (from previous run)
champion_mrid = '3c4074b1-ba9d-47aa-ab45-1d16adb1bfef'
print(f"[OK] Using champion model: {champion_mrid}")

# COMPUTE PER-SERIES METRICS
print("\n=== Computing per-series metrics (horizon=1 only) ===")
per_series_sql = f"""
WITH h1_preds AS (
  SELECT 
    p.roll_up_shop,
    p.reason_group,
    p.y_true,
    p.y_pred
  FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS p
  WHERE p.model_run_id = '{champion_mrid}'
    AND p.horizon = 1
),
series_metrics AS (
  SELECT
    roll_up_shop,
    reason_group,
    COUNT(*) as n_months,
    AVG(y_true) as avg_monthly_revenue,
    SUM(ABS(y_pred - y_true)) as total_abs_error,
    SUM(ABS(y_pred - y_true)) / NULLIF(SUM(ABS(y_true)), 0) as wape_series,
    AVG(ABS(y_pred - y_true)) as mae_series,
    SUM(y_pred - y_true) / NULLIF(SUM(ABS(y_true)), 0) as bias_series,
    STDDEV_SAMP(y_true) as sd_y,
    SUM(CASE WHEN y_true = 0 THEN 1 ELSE 0 END) / COUNT(*) as pct_zero_months
  FROM h1_preds
  GROUP BY roll_up_shop, reason_group
)
SELECT
  s.*,
  s.sd_y / NULLIF(ABS(s.avg_monthly_revenue), 0) as cv,
  m.cust_grp as customer_group,
  s.reason_group as reason_code_group
FROM series_metrics s
LEFT JOIN DB_BI_P_SANDBOX.SANDBOX.FORECAST_CUST_MIX_PC_REASON m
  ON m.roll_up_shop = s.roll_up_shop
 AND m.reason_group = s.reason_group
 AND m.asof_fiscal_yyyymm = 202512
ORDER BY s.avg_monthly_revenue DESC
"""

print("Running per-series query...")
series_df = session.sql(per_series_sql).to_pandas()
print(f"[OK] Computed metrics for {len(series_df)} series")

# Handle NULL customer_group
series_df['CUSTOMER_GROUP'] = series_df['CUSTOMER_GROUP'].fillna('UNKNOWN')

# Top 50 by revenue
print("\n=== Generating top_series_by_revenue.csv ===")
top_revenue = series_df.nlargest(50, 'AVG_MONTHLY_REVENUE')
top_revenue.to_csv('analysis/top_series_by_revenue.csv', index=False)
print(f"[OK] Saved {len(top_revenue)} series")

# Top 50 by error contribution
print("\n=== Generating top_series_by_error_contribution.csv ===")
top_error = series_df.nlargest(50, 'TOTAL_ABS_ERROR')
top_error.to_csv('analysis/top_series_by_error_contribution.csv', index=False)
print(f"[OK] Saved {len(top_error)} series")

# COMPANY-LEVEL METRICS
print("\n=== Computing company-level metrics ===")
company_sql = f"""
SELECT
  SUM(ABS(y_pred - y_true)) / NULLIF(SUM(ABS(y_true)), 0) as wape,
  AVG(ABS(y_pred - y_true)) as mae,
  SUM(y_pred - y_true) / NULLIF(SUM(ABS(y_true)), 0) as bias,
  COUNT(DISTINCT roll_up_shop || '_' || reason_group) as n_series,
  AVG(y_true) as avg_monthly_revenue
FROM DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS
WHERE model_run_id = '{champion_mrid}'
  AND horizon = 1
"""
company_df = session.sql(company_sql).to_pandas()
company_wape = company_df.iloc[0]['WAPE'] * 100
company_mae = company_df.iloc[0]['MAE']
company_bias = company_df.iloc[0]['BIAS'] * 100
n_series = int(company_df.iloc[0]['N_SERIES'])
company_avg_rev = company_df.iloc[0]['AVG_MONTHLY_REVENUE']

print(f"  WAPE: {company_wape:.2f}%")
print(f"  MAE: ${company_mae:,.0f}")
print(f"  BIAS: {company_bias:.2f}%")
print(f"  Series: {n_series}")

# CUSTOMER GROUP SUMMARY
print("\n=== Computing customer_group summary ===")
cust_group_summary = series_df.groupby('CUSTOMER_GROUP').agg({
    'AVG_MONTHLY_REVENUE': 'sum',
    'TOTAL_ABS_ERROR': 'sum',
    'WAPE_SERIES': 'mean',
    'CV': 'mean',
    'PCT_ZERO_MONTHS': 'mean',
    'ROLL_UP_SHOP': 'count'
}).rename(columns={'ROLL_UP_SHOP': 'n_series'}).reset_index()
cust_group_summary['wape_group'] = (cust_group_summary['TOTAL_ABS_ERROR'] / 
                                     cust_group_summary['AVG_MONTHLY_REVENUE'])

# REASON CODE GROUP SUMMARY
print("=== Computing reason_code_group summary ===")
reason_group_summary = series_df.groupby('REASON_CODE_GROUP').agg({
    'AVG_MONTHLY_REVENUE': 'sum',
    'TOTAL_ABS_ERROR': 'sum',
    'WAPE_SERIES': 'mean',
    'CV': 'mean',
    'PCT_ZERO_MONTHS': 'mean',
    'ROLL_UP_SHOP': 'count'
}).rename(columns={'ROLL_UP_SHOP': 'n_series'}).reset_index()
reason_group_summary['wape_group'] = (reason_group_summary['TOTAL_ABS_ERROR'] / 
                                       reason_group_summary['AVG_MONTHLY_REVENUE'])

# Generate markdown summary
print("\n=== Generating markdown summary ===")
total_revenue = series_df['AVG_MONTHLY_REVENUE'].sum()
total_abs_error = series_df['TOTAL_ABS_ERROR'].sum()

# Top 5 revenue drivers
top5_revenue = series_df.nlargest(5, 'AVG_MONTHLY_REVENUE')
# Top 5 error contributors
top5_error = series_df.nlargest(5, 'TOTAL_ABS_ERROR')

# Find intersection (high revenue AND high error)
top20_revenue_ids = set(series_df.nlargest(20, 'AVG_MONTHLY_REVENUE').apply(
    lambda x: f"{x['ROLL_UP_SHOP']}_{x['REASON_GROUP']}", axis=1))
top20_error_ids = set(series_df.nlargest(20, 'TOTAL_ABS_ERROR').apply(
    lambda x: f"{x['ROLL_UP_SHOP']}_{x['REASON_GROUP']}", axis=1))
intersection_ids = top20_revenue_ids & top20_error_ids
intersection_df = series_df[series_df.apply(
    lambda x: f"{x['ROLL_UP_SHOP']}_{x['REASON_GROUP']}" in intersection_ids, axis=1)]

# Volatility flags
series_df['volatile'] = (series_df['CV'] >= 1.0) | (series_df['PCT_ZERO_MONTHS'] >= 0.3)
series_df['stable'] = (series_df['CV'] < 0.5) & (series_df['PCT_ZERO_MONTHS'] < 0.05)
series_df['material'] = (series_df['AVG_MONTHLY_REVENUE'] >= 0.01 * company_avg_rev)

# Build markdown
md_content = f"""# Error Decomposition & Series Health Summary

**Analysis Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  
**Champion Model**: {champion_mrid}  
**Horizon**: 1 month ahead  
**Data**: GBR GLOBAL champion model backtest predictions

---

## Company-Level Performance

| Metric | Value |
|--------|-------|
| **WAPE** | {company_wape:.2f}% |
| **MAE** | ${company_mae:,.0f} |
| **BIAS** | {company_bias:+.2f}% |
| **Series Count** | {n_series} |
| **Avg Monthly Revenue/Series** | ${company_avg_rev:,.0f} |

---

## Top 5 Revenue Drivers

| Rank | PC | Reason Group | Avg Monthly Revenue | % of Total Revenue | Customer Group |
|------|---|--|---------------------|-------------------|----------------|
"""

for idx, row in enumerate(top5_revenue.itertuples(), 1):
    pct_total = (row.AVG_MONTHLY_REVENUE / total_revenue) * 100
    md_content += f"| {idx} | {row.ROLL_UP_SHOP} | {row.REASON_GROUP} | ${row.AVG_MONTHLY_REVENUE:,.0f} | {pct_total:.1f}% | {row.CUSTOMER_GROUP} |\n"

md_content += f"""
**Total Revenue (Top 5)**: ${top5_revenue['AVG_MONTHLY_REVENUE'].sum():,.0f} ({(top5_revenue['AVG_MONTHLY_REVENUE'].sum()/total_revenue)*100:.1f}% of total)

---

## Top 5 Error Contributors

| Rank | PC | Reason Group | Total Abs Error | % of Total Error | WAPE | Customer Group |
|------|---|--|-----------------|-----------------|------|----------------|
"""

for idx, row in enumerate(top5_error.itertuples(), 1):
    pct_total_err = (row.TOTAL_ABS_ERROR / total_abs_error) * 100
    md_content += f"| {idx} | {row.ROLL_UP_SHOP} | {row.REASON_GROUP} | ${row.TOTAL_ABS_ERROR:,.0f} | {pct_total_err:.1f}% | {row.WAPE_SERIES*100:.1f}% | {row.CUSTOMER_GROUP} |\n"

md_content += f"""
**Total Error (Top 5)**: ${top5_error['TOTAL_ABS_ERROR'].sum():,.0f} ({(top5_error['TOTAL_ABS_ERROR'].sum()/total_abs_error)*100:.1f}% of total)

---

## High Revenue AND High Error Series (Prioritization)

**Count**: {len(intersection_df)} series in both top-20 revenue and top-20 error

| PC | Reason Group | Avg Revenue | Total Error | WAPE | CV | Zero% | Health Note | Recommended Action |
|----|--------------|-------------|-------------|------|----|----|-------------|-------------------|
"""

for row in intersection_df.nlargest(10, 'AVG_MONTHLY_REVENUE').itertuples():
    if row.CV >= 1.0:
        health = "High volatility"
        action = "Consider aggregate-first or contract split"
    elif row.PCT_ZERO_MONTHS >= 0.3:
        health = "Sparse/intermittent"
        action = "Use aggregate forecast or intermittent method"
    elif row.WAPE_SERIES > 0.5:
        health = "Model struggling"
        action = "Feature engineering or ensemble"
    else:
        health = "Stable, improvable"
        action = "Hyperparameter tuning"
    
    md_content += f"| {row.ROLL_UP_SHOP} | {row.REASON_GROUP} | ${row.AVG_MONTHLY_REVENUE:,.0f} | ${row.TOTAL_ABS_ERROR:,.0f} | {row.WAPE_SERIES*100:.1f}% | {row.CV:.2f} | {row.PCT_ZERO_MONTHS*100:.0f}% | {health} | {action} |\n"

md_content += """
---

## Customer Group Analysis

| Customer Group | Series Count | Total Revenue | Avg WAPE | Avg CV | Avg Zero% | Group WAPE |
|----------------|--------------|---------------|----------|--------|-----------|------------|
"""

for row in cust_group_summary.sort_values('AVG_MONTHLY_REVENUE', ascending=False).itertuples():
    md_content += f"| {row.CUSTOMER_GROUP} | {row.n_series} | ${row.AVG_MONTHLY_REVENUE:,.0f} | {row.WAPE_SERIES*100:.1f}% | {row.CV:.2f} | {row.PCT_ZERO_MONTHS*100:.0f}% | {row.wape_group*100:.1f}% |\n"

md_content += """
---

## Reason Code Group Analysis

| Reason Group | Series Count | Total Revenue | Avg WAPE | Avg CV | Avg Zero% | Group WAPE |
|--------------|--------------|---------------|----------|--------|-----------|------------|
"""

for row in reason_group_summary.sort_values('AVG_MONTHLY_REVENUE', ascending=False).itertuples():
    md_content += f"| {row.REASON_CODE_GROUP} | {row.n_series} | ${row.AVG_MONTHLY_REVENUE:,.0f} | {row.WAPE_SERIES*100:.1f}% | {row.CV:.2f} | {row.PCT_ZERO_MONTHS*100:.0f}% | {row.wape_group*100:.1f}% |\n"

md_content += f"""
---

## Volatility Analysis

**Definitions**:
- **Volatile**: CV >= 1.0 OR Zero% >= 30%
- **Stable**: CV < 0.5 AND Zero% < 5%
- **Material**: Avg Monthly Revenue >= ${company_avg_rev * 0.01:,.0f} (1% of company avg)

| Category | Count | % of Series |
|----------|-------|-------------|
| Volatile | {series_df['volatile'].sum()} | {(series_df['volatile'].sum()/len(series_df))*100:.1f}% |
| Stable | {series_df['stable'].sum()} | {(series_df['stable'].sum()/len(series_df))*100:.1f}% |
| Material | {series_df['material'].sum()} | {(series_df['material'].sum()/len(series_df))*100:.1f}% |
| Volatile + Material | {(series_df['volatile'] & series_df['material']).sum()} | {((series_df['volatile'] & series_df['material']).sum()/len(series_df))*100:.1f}% |

---

## Key Findings & Recommendations

1. **Revenue Concentration**: Top 5 series account for {(top5_revenue['AVG_MONTHLY_REVENUE'].sum()/total_revenue)*100:.1f}% of revenue
2. **Error Concentration**: Top 5 error contributors account for {(top5_error['TOTAL_ABS_ERROR'].sum()/total_abs_error)*100:.1f}% of total error
3. **Volatile Series**: {series_df['volatile'].sum()} series ({(series_df['volatile'].sum()/len(series_df))*100:.1f}%) are highly volatile
4. **High-Priority Series**: {len(intersection_df)} series are both high-revenue and high-error

**Recommended Actions**:
- **Immediate**: Focus model improvements on the {len(intersection_df)} high-revenue + high-error series
- **Short-term**: Implement ensemble methods for volatile material series
- **Medium-term**: Consider customer-group-specific models (current avg WAPE varies by group)
- **Long-term**: Investigate contract-level forecasting for top revenue drivers with high CV

---

**Analysis Artifacts**:
- `analysis/top_series_by_revenue.csv` - Top 50 series by revenue
- `analysis/top_series_by_error_contribution.csv` - Top 50 series by error
- `analysis/error_decomp_summary.md` - This summary

**Metadata Notes**:
- `customer_group` joined from FORECAST_CUST_MIX_PC_REASON.CUST_GRP
- `reason_code_group` = REASON_GROUP (already at group level)
- ASOF date: 202512
"""

# Save markdown
with open('analysis/error_decomp_summary.md', 'w') as f:
    f.write(md_content)

print("[OK] Saved analysis/error_decomp_summary.md")

session.close()
print("\n[OK] Analysis complete!")
print("\nGenerated files:")
print("  - analysis/top_series_by_revenue.csv")
print("  - analysis/top_series_by_error_contribution.csv")
print("  - analysis/error_decomp_summary.md")
