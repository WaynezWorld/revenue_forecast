# Error Decomposition & Series Health Summary

**Analysis Date**: 2026-01-31 09:38:05  
**Champion Model**: 3c4074b1-ba9d-47aa-ab45-1d16adb1bfef  
**Horizon**: 1 month ahead  
**Data**: GBR GLOBAL champion model backtest predictions

---

## Company-Level Performance

| Metric | Value |
|--------|-------|
| **WAPE** | 27.36% |
| **MAE** | $11,116 |
| **BIAS** | -13.16% |
| **Series Count** | 201 |
| **Avg Monthly Revenue/Series** | $40,582 |

---

## Top 5 Revenue Drivers

| Rank | PC | Reason Group | Avg Monthly Revenue | % of Total Revenue | Customer Group |
|------|---|--|---------------------|-------------------|----------------|
| 1 | 555 | Routine | $611,846 | 1.5% | CCCI |
| 2 | 555 | Routine | $611,846 | 1.5% | CAPX |
| 3 | 555 | Routine | $611,846 | 1.5% | UNKNOWN |
| 4 | 555 | Routine | $611,846 | 1.5% | TRANS |
| 5 | 555 | Routine | $611,846 | 1.5% | EXT |

**Total Revenue (Top 5)**: $3,059,232 (7.5% of total)

---

## Top 5 Error Contributors

| Rank | PC | Reason Group | Total Abs Error | % of Total Error | WAPE | Customer Group |
|------|---|--|-----------------|-----------------|------|----------------|
| 1 | 555 | Routine | $3,297,917 | 2.5% | 44.9% | CCCI |
| 2 | 555 | Routine | $3,297,917 | 2.5% | 44.9% | CAPX |
| 3 | 555 | Routine | $3,297,917 | 2.5% | 44.9% | UNKNOWN |
| 4 | 555 | Routine | $3,297,917 | 2.5% | 44.9% | TRANS |
| 5 | 555 | Routine | $3,297,917 | 2.5% | 44.9% | EXT |

**Total Error (Top 5)**: $16,489,585 (12.3% of total)

---

## High Revenue AND High Error Series (Prioritization)

**Count**: 15 series in both top-20 revenue and top-20 error

| PC | Reason Group | Avg Revenue | Total Error | WAPE | CV | Zero% | Health Note | Recommended Action |
|----|--------------|-------------|-------------|------|----|----|-------------|-------------------|
| 555 | Routine | $611,846 | $3,297,917 | 44.9% | 0.23 | 0% | Stable, improvable | Hyperparameter tuning |
| 555 | Routine | $611,846 | $3,297,917 | 44.9% | 0.23 | 0% | Stable, improvable | Hyperparameter tuning |
| 555 | Routine | $611,846 | $3,297,917 | 44.9% | 0.23 | 0% | Stable, improvable | Hyperparameter tuning |
| 555 | Routine | $611,846 | $3,297,917 | 44.9% | 0.23 | 0% | Stable, improvable | Hyperparameter tuning |
| 555 | Routine | $611,846 | $3,297,917 | 44.9% | 0.23 | 0% | Stable, improvable | Hyperparameter tuning |
| 715 | Routine | $344,890 | $836,130 | 20.2% | 0.18 | 0% | Stable, improvable | Hyperparameter tuning |
| 715 | Routine | $344,890 | $836,130 | 20.2% | 0.18 | 0% | Stable, improvable | Hyperparameter tuning |
| 715 | Routine | $344,890 | $836,130 | 20.2% | 0.18 | 0% | Stable, improvable | Hyperparameter tuning |
| 715 | Routine | $344,890 | $836,130 | 20.2% | 0.18 | 0% | Stable, improvable | Hyperparameter tuning |
| 715 | Routine | $344,890 | $836,130 | 20.2% | 0.18 | 0% | Stable, improvable | Hyperparameter tuning |

---

## Customer Group Analysis

| Customer Group | Series Count | Total Revenue | Avg WAPE | Avg CV | Avg Zero% | Group WAPE |
|----------------|--------------|---------------|----------|--------|-----------|------------|
| CAPX | 201 | $8,157,082 | 62.4% | 1.59 | 21% | 328.7% |
| CCCI | 201 | $8,157,082 | 62.4% | 1.59 | 21% | 328.7% |
| EXT | 201 | $8,157,082 | 62.4% | 1.59 | 21% | 328.7% |
| TRANS | 201 | $8,157,082 | 62.4% | 1.59 | 21% | 328.7% |
| UNKNOWN | 201 | $8,157,082 | 62.4% | 1.59 | 21% | 328.7% |

---

## Reason Code Group Analysis

| Reason Group | Series Count | Total Revenue | Avg WAPE | Avg CV | Avg Zero% | Group WAPE |
|--------------|--------------|---------------|----------|--------|-----------|------------|
| Routine | 335 | $33,639,345 | 31.7% | 0.51 | 3% | 278.1% |
| Commercial | 335 | $5,316,205 | 70.2% | 2.83 | 26% | 484.9% |
| Non-Routine | 335 | $1,829,859 | 87.6% | 1.58 | 32% | 804.2% |

---

## Volatility Analysis

**Definitions**:
- **Volatile**: CV >= 1.0 OR Zero% >= 30%
- **Stable**: CV < 0.5 AND Zero% < 5%
- **Material**: Avg Monthly Revenue >= $406 (1% of company avg)

| Category | Count | % of Series |
|----------|-------|-------------|
| Volatile | 410 | 40.8% |
| Stable | 410 | 40.8% |
| Material | 820 | 81.6% |
| Volatile + Material | 225 | 22.4% |

---

## Key Findings & Recommendations

1. **Revenue Concentration**: Top 5 series account for 7.5% of revenue
2. **Error Concentration**: Top 5 error contributors account for 12.3% of total error
3. **Volatile Series**: 410 series (40.8%) are highly volatile
4. **High-Priority Series**: 15 series are both high-revenue and high-error

**Recommended Actions**:
- **Immediate**: Focus model improvements on the 15 high-revenue + high-error series
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
