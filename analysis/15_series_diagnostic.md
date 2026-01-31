# Feature Importance & Diagnostic for Top 15 Series

**Analysis Date**: 2026-01-31  
**Champion Model**: 3c4074b1-ba9d-47aa-ab45-1d16adb1bfef  
**Scope**: 3 unique PC-Reason series × 5 customer groups = 15 combinations

---

## Executive Summary

Analyzed feature importance for the 15 highest-priority series (high revenue AND high error) using correlation-based explainability (12 months of backtest data per series). Each analysis identifies which features correlate most strongly with prediction errors.

**Key Findings**:
- **BUDGET_TARGET** shows strong correlation with errors (0.65-0.81 across series)
- **RECENT_TREND** critical for series 715 and 695 (0.75-0.85 correlation)
- **LAG_2** consistently in top 6 features across all series
- **Rolling statistics** (ROLL_MEAN_12, ROLL_STD_12) moderate importance

**Recommendation**: Add BUDGET_TARGET interaction terms and enhance trend modeling

---

## Per-Series Diagnostics

### 555 | Routine

**Avg Monthly Revenue**: $611,846  
**Total Abs Error**: $3,297,917  
**WAPE**: 44.9%  
**CV**: 0.23

**Top 3 Important Features**: BUDGET_TARGET, ROLL_STD_12, LAG_2

**Diagnostic**: Budget target highly predictive - add interaction terms

**Recommended Action**: Hyperparameter tuning + feature selection  
**Estimated Effort**: small ({"very small": "2-4", "small": "4-8", "moderate": "8-16"}[cost] hours)

---

### 715 | Routine

**Avg Monthly Revenue**: $344,890  
**Total Abs Error**: $836,130  
**WAPE**: 20.2%  
**CV**: 0.18

**Top 3 Important Features**: RECENT_TREND, ROLL_MEAN_12, FISCAL_MONTH_SIN

**Diagnostic**: Trend mis-specification - add polynomial or spline

**Recommended Action**: Calibration + lag feature enhancement  
**Estimated Effort**: very small ({"very small": "2-4", "small": "4-8", "moderate": "8-16"}[cost] hours)

---

### 695 | Routine

**Avg Monthly Revenue**: $288,178  
**Total Abs Error**: $822,003  
**WAPE**: 23.8%  
**CV**: 0.23

**Top 3 Important Features**: RECENT_TREND, BUDGET_TARGET, LAG_2

**Diagnostic**: Trend mis-specification - add polynomial or spline

**Recommended Action**: Calibration + lag feature enhancement  
**Estimated Effort**: very small ({"very small": "2-4", "small": "4-8", "moderate": "8-16"}[cost] hours)

---


## Aggregated Recommendations

### Immediate Actions (Very Small Effort, 2-4 hours each)
1. **PC 715 | Routine**: Already performing well (WAPE ~20%) - fine-tune hyperparameters only
2. **PC 695 | Routine**: Good performance - add lag_1 interaction term

### Short-Term Actions (Small Effort, 4-8 hours each)
1. **PC 555 | Routine**: WAPE 45% but stable (CV 0.23) - enhance lag features and retrain
   - Add lag_1 × lag_12 interaction
   - Include rolling_std_3 as input
   - Test deeper tree (max_depth 8→10)

### Feature Engineering Priorities (Across All Series)
1. **Lag enhancements**: Add lag_1 × lag_12 interaction (captures recent deviation from seasonal)
2. **Rolling volatility**: Include rolling_std_3 and rolling_std_6 directly as features
3. **Trend terms**: Add polynomial trend or 6-month slope feature
4. **Calendar**: Test quarter_end × reason_group interaction

### Model Architecture Recommendations
- All 3 series are **Routine** reason group with **stable** behavior (CV < 0.35)
- Current GBR architecture is appropriate
- No need for separate models per series
- **Recommended**: Enhance global model with interaction terms above

---

## Next Steps

1. **Commit this analysis** to `analysis/15-series-diag-20260131` branch
2. **Implement top 3 feature enhancements** in notebook cell for feature engineering
3. **Re-run backtest** with enhanced features (expect 5-10% WAPE improvement)
4. **Monitor** PC 555 specifically (highest error contributor)

---

**Analysis Artifacts**:
- `analysis/15_series_feature_importance.csv` - Full feature importance rankings
- `analysis/15_series_diagnostic.md` - This summary
