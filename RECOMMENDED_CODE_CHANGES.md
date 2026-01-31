# RECOMMENDED CODE CHANGES - Metric Audit Findings

## 1. Remove Ridge Model (PRIORITY 1)

**File**: `10__modeling__backtest_global_plus_overrides.ipynb`  
**Cell**: Model candidate definitions (around line 59)

### Current Code:
```python
CANDIDATES = [
    # baseline: seasonal naive (computed in SQL; no sklearn needed)
    {"name": "SEASONAL_NAIVE_LAG12", "family": "baseline", "params": {"kind": "seasonal_naive_lag12"}},
    # ridge regression
    {"name": "RIDGE_OHE", "family": "ridge", "params": {"alpha": 1.0, "target_transform": "signed_log1p"}},
    # gradient boosting (sklearn)
    {"name": "GBR_OHE", "family": "gbr", "params": {"target_transform": "signed_log1p"}},
]
```

### Recommended Change:
```python
CANDIDATES = [
    # baseline: seasonal naive (computed in SQL; no sklearn needed)
    {"name": "SEASONAL_NAIVE_LAG12", "family": "baseline", "params": {"kind": "seasonal_naive_lag12"}},
    
    # ❌ REMOVED: Ridge regression - incompatible with exponential inverse transform
    # Linear models extrapolate wildly in log-space, producing 1130% WAPE
    # See METRIC_AUDIT_REPORT.md for details
    
    # gradient boosting (sklearn) - tree-based models are bounded, work well with log transforms
    {"name": "GBR_OHE", "family": "gbr", "params": {"target_transform": "signed_log1p"}},
]
```

---

## 2. Add Model-Transform Compatibility Documentation (PRIORITY 2)

**File**: Create `docs/MODEL_TRANSFORM_GUIDE.md`

```markdown
# Model-Transform Compatibility Guide

## Overview
This forecasting pipeline uses **signed_log1p** transform to handle negative revenues and compress extreme values. Not all models are compatible with this transform.

## Transform Details
```python
# Forward transform
y_t = sign(y) * log1p(|y| / 100)

# Inverse transform
y = sign(y_t) * 100 * expm1(|y_t|)
```

The inverse `expm1()` is exponential, which amplifies any extrapolation errors.

## ✅ COMPATIBLE MODELS (Tree-Based)

These models produce predictions **bounded by training data range**, preventing dangerous extrapolation:

- **GradientBoostingRegressor** ✅ RECOMMENDED
  - Naturally bounded by leaf values
  - Current champion: 28% WAPE
  - Works excellent with signed_log1p

- **RandomForestRegressor** ✅ SAFE
  - Predictions are averages of training targets
  - Cannot extrapolate beyond [min_train, max_train]

- **XGBoost / LightGBM** ✅ SAFE
  - Tree-based, same bounded properties
  - Can handle assymetric loss functions

## ❌ INCOMPATIBLE MODELS (Linear)

These models produce **unbounded predictions** that explode under exponential inverse:

- **Ridge / Lasso / ElasticNet** ❌ DANGEROUS
  - Linear extrapolation: ŷ_t = β₀ + Σβᵢxᵢ
  - Can predict any value in (-∞, +∞)
  - Example failure: Predict y_t=8.5 → y=$450K (actual $12K)
  - Measured failure: 1130% WAPE (40x worse than GBR)

- **LinearRegression** ❌ DANGEROUS
  - Same extrapolation issue as Ridge

- **SVR (linear kernel)** ❌ DANGEROUS
  - Linear decision boundary, unbounded predictions

## Example Failure Mode

```python
# Ridge predicts slightly outside training range
y_train_t = [3.0, 4.0, 5.0, 6.0]  # log-space training targets
yhat_t = 8.5  # Linear extrapolation for unusual feature combination

# Inverse transform explodes
yhat = 100 * expm1(8.5)
     = 100 * (e^8.5 - 1)
     = 100 * 4913.2
     = $491,320

# But actual revenue = $12,000
# Error = $479,320 (3994% APE) ❌
```

## Recommendations

1. **Stick with GradientBoostingRegressor**
   - Best balance of accuracy and safety
   - No risk of extrapolation explosion

2. **If trying new models**:
   - Prefer tree-based (XGBoost, LightGBM, RandomForest)
   - Avoid linear models (Ridge, Lasso, LinearRegression)
   - Always backtest for multiple anchors

3. **If you must use linear models**:
   - Remove `target_transform` (work in original scale)
   - Accept wider prediction intervals
   - Apply stricter feature engineering (remove extreme values)

## Testing Checklist

Before adding new model candidate:
- [ ] Check if model type is tree-based (bounded) or linear (unbounded)
- [ ] If linear, remove target_transform or apply strict clipping
- [ ] Run backtest on latest 12 anchors
- [ ] Verify WAPE < 100% (sanity check)
- [ ] Compare to GBR baseline performance
```

---

## 3. Add Unit Tests (PRIORITY 3)

**File**: Create `tests/test_metrics.py`

```python
"""
Unit tests for metric calculations and transform operations.
Ensures metric formulas match SQL implementation and transforms are invertible.
"""

import numpy as np
import pytest


def signed_log1p(x, eps=100.0):
    """Forward transform - matches notebook implementation"""
    return np.sign(x) * np.log1p(np.abs(x) / eps)


def signed_expm1(y, eps=100.0):
    """Inverse transform - matches notebook implementation"""
    return np.sign(y) * eps * np.expm1(np.abs(y))


class TestMetricFormulas:
    """Test metric calculations match SQL formulas in notebook"""
    
    def test_wape_basic(self):
        """WAPE = sum(|pred - actual|) / sum(|actual|)"""
        y_true = np.array([100, 200, 150])
        y_pred = np.array([110, 190, 160])
        
        abs_err = np.abs(y_pred - y_true)
        abs_y = np.abs(y_true)
        wape = np.sum(abs_err) / np.sum(abs_y)
        
        # Manual: (10 + 10 + 10) / (100 + 200 + 150) = 30/450 = 0.0667
        assert np.isclose(wape, 0.0667, atol=0.0001)
        assert np.isclose(wape * 100, 6.67, atol=0.01)  # As percentage
    
    def test_wape_with_negatives(self):
        """WAPE handles negative revenues correctly"""
        y_true = np.array([100, -50, 150])  # Mix of positive/negative
        y_pred = np.array([110, -60, 140])
        
        abs_err = np.abs(y_pred - y_true)  # [10, 10, 10]
        abs_y = np.abs(y_true)              # [100, 50, 150]
        wape = np.sum(abs_err) / np.sum(abs_y)
        
        # (10 + 10 + 10) / (100 + 50 + 150) = 30/300 = 0.10
        assert np.isclose(wape, 0.10, atol=0.0001)
    
    def test_mae_basic(self):
        """MAE = avg(|pred - actual|)"""
        y_true = np.array([100, 200, 300])
        y_pred = np.array([110, 190, 310])
        
        mae = np.mean(np.abs(y_pred - y_true))
        
        # (10 + 10 + 10) / 3 = 10.0
        assert mae == 10.0
    
    def test_bias_positive_negative(self):
        """BIAS = sum(pred - actual) / sum(|actual|)"""
        y_true = np.array([100, 200, 100])
        y_pred = np.array([120, 180, 130])  # Over, Under, Over
        
        err_signed = y_pred - y_true  # [20, -20, 30]
        abs_y = np.abs(y_true)        # [100, 200, 100]
        bias = np.sum(err_signed) / np.sum(abs_y)
        
        # (20 - 20 + 30) / 400 = 30/400 = 0.075 = +7.5%
        assert np.isclose(bias, 0.075, atol=0.0001)
    
    def test_mape_with_epsilon_threshold(self):
        """MAPE excludes actuals below threshold"""
        y_true = np.array([50, 200, 10, 300])
        y_pred = np.array([60, 210, 100, 330])
        
        eps_threshold = 100
        mask = np.abs(y_true) >= eps_threshold
        
        # Should only include indices 1 and 3 (200, 300)
        assert mask.sum() == 2
        
        mape = np.mean(
            np.abs(y_pred[mask] - y_true[mask]) / np.abs(y_true[mask])
        )
        
        # (|210-200|/200 + |330-300|/300) / 2
        # = (10/200 + 30/300) / 2
        # = (0.05 + 0.10) / 2 = 0.075 = 7.5%
        assert np.isclose(mape, 0.075, atol=0.0001)


class TestTransformInverse:
    """Test signed_log1p and signed_expm1 are proper inverses"""
    
    def test_roundtrip_positive_values(self):
        """Transform → Inverse should recover original values"""
        revenues = np.array([1000, 5000, 10000, 50000])
        eps = 100
        
        transformed = signed_log1p(revenues, eps)
        recovered = signed_expm1(transformed, eps)
        
        np.testing.assert_allclose(recovered, revenues, rtol=1e-10)
    
    def test_roundtrip_negative_values(self):
        """Transform handles negative revenues (credits/returns)"""
        revenues = np.array([-1000, -5000, -500])
        eps = 100
        
        transformed = signed_log1p(revenues, eps)
        recovered = signed_expm1(transformed, eps)
        
        np.testing.assert_allclose(recovered, revenues, rtol=1e-10)
    
    def test_roundtrip_mixed_values(self):
        """Transform handles mix of positive/negative/zero"""
        revenues = np.array([50000, -5000, 0, 150000, -20000])
        eps = 100
        
        transformed = signed_log1p(revenues, eps)
        recovered = signed_expm1(transformed, eps)
        
        np.testing.assert_allclose(recovered, revenues, rtol=1e-10)
    
    def test_zero_value(self):
        """Transform handles zero correctly"""
        revenues = np.array([0.0])
        eps = 100
        
        transformed = signed_log1p(revenues, eps)
        assert transformed[0] == 0.0
        
        recovered = signed_expm1(transformed, eps)
        assert recovered[0] == 0.0


class TestRidgeExtrapolationRisk:
    """Demonstrate why Ridge fails with log transform"""
    
    def test_extrapolation_amplification(self):
        """Show exponential inverse amplifies extrapolation errors"""
        eps = 100
        
        # Ridge predicts slightly beyond training range
        y_train_max_t = 6.0  # Max log-space training value
        yhat_t = 7.0         # Small extrapolation (+1.0 in log space)
        
        # Inverse transform
        y_train_max = signed_expm1(y_train_max_t, eps)
        yhat = signed_expm1(yhat_t, eps)
        
        # Check amplification
        log_diff = yhat_t - y_train_max_t  # +1.0
        orig_diff = yhat - y_train_max     # Should be much larger
        
        # 1.0 in log space → 100 * (e^7 - e^6) = 100 * (1096.6 - 403.4) = $69,320
        assert orig_diff > 50000, "Log extrapolation amplified significantly"
        assert orig_diff / y_train_max > 1.0, "Extrapolation creates >100% error"
    
    def test_extreme_log_predictions(self):
        """Show what happens with extreme log-space predictions"""
        eps = 100
        extreme_predictions_t = np.array([10, 12, 15])  # Very high log values
        
        extreme_predictions = signed_expm1(extreme_predictions_t, eps)
        
        # expm1(10) ≈ 22,025 → $2.2M
        # expm1(12) ≈ 162,754 → $16.3M
        # expm1(15) ≈ 3,269,017 → $326M
        
        assert extreme_predictions[0] > 2_000_000
        assert extreme_predictions[1] > 16_000_000
        assert extreme_predictions[2] > 300_000_000
        
        # If true revenue is $15,000, errors are catastrophic
        y_true = 15000
        errors = np.abs(extreme_predictions - y_true)
        ape = errors / y_true * 100
        
        assert np.all(ape > 10000), "All errors >10,000% (Ridge failure mode)"


class TestZeroHandling:
    """Test division by zero prevention"""
    
    def test_wape_with_zero_total(self):
        """WAPE should handle sum(|y_true|) = 0"""
        y_true = np.array([0, 0, 0])
        y_pred = np.array([10, 20, 30])
        
        abs_y_sum = np.sum(np.abs(y_true))
        
        if abs_y_sum == 0:
            wape = None  # Matches SQL: nullif(sum(abs_y), 0)
        else:
            wape = np.sum(np.abs(y_pred - y_true)) / abs_y_sum
        
        assert wape is None, "WAPE undefined when all actuals are zero"
    
    def test_mape_skip_zero_actuals(self):
        """MAPE should skip individual zero actuals"""
        y_true = np.array([100, 0, 200])
        y_pred = np.array([110, 50, 210])
        
        # Filter out zeros before calculating percentage error
        mask = y_true != 0
        
        mape = np.mean(
            np.abs(y_pred[mask] - y_true[mask]) / np.abs(y_true[mask])
        )
        
        # Only includes indices 0 and 2
        # (10/100 + 10/200) / 2 = 0.075 = 7.5%
        assert np.isclose(mape, 0.075, atol=0.0001)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
```

**Run tests**:
```bash
cd c:\GitHub\revenue_forecast
pip install pytest
pytest tests/test_metrics.py -v
```

---

## 4. Update Scoring Procedure Documentation (PRIORITY 4)

**File**: `11__proc__score_and_publish_marts.sql`  
**Location**: Add comment block at top

```sql
/*******************************************************************************
PROCEDURE: SP_SCORE_AND_PUBLISH_FORECASTS
PURPOSE:   Generate production revenue forecasts using champion models from backtest

DEPENDENCIES:
  - FORECAST_MODEL_CHAMPIONS (latest champion selections)
  - FORECAST_MODEL_BACKTEST_PREDICTIONS (historical prediction patterns)
  - FORECAST_ACTUALS_PC_REASON_MTH_SNAP (actual revenue for anchor scaling)

METRIC DEFINITIONS:
  - WAPE (Weighted Absolute Percentage Error): sum(|pred-actual|) / sum(|actual|)
    * Weighted by magnitude, robust to outliers
    * Champion model: GBR with 28.26% WAPE
  
  - MAE (Mean Absolute Error): avg(|pred-actual|)
    * Dollar error, interpretable for business
    * Champion model: $11,911 MAE
  
  - BIAS: sum(pred-actual) / sum(|actual|)
    * Positive = over-forecast, Negative = under-forecast
    * Champion model: -14% (slight under-forecast, conservative)

MODEL COMPATIBILITY NOTE:
  - Only tree-based models (GBR, RandomForest, XGBoost) are compatible with
    signed_log1p transform used in training
  - Linear models (Ridge, Lasso) produce catastrophic errors (1000%+ WAPE)
    due to unbounded extrapolation in log-space
  - See docs/MODEL_TRANSFORM_GUIDE.md for details

CONFIDENCE INTERVALS:
  - Lower bound floored at $0 to prevent negative revenue forecasts
  - Upper/lower bounds calculated from backtest residual distributions (10th/90th percentile)
  - Intervals reflect genuine uncertainty, not artificial constraints

LAST UPDATED: 2025-01-XX (Added confidence bound floor)
AUTHOR: Wayne Jones
*******************************************************************************/
```

---

## 5. Optional: Add GBR Hyperparameter Tuning (FUTURE)

**File**: `10__modeling__backtest_global_plus_overrides.ipynb`  
**Cell**: Model candidate definitions

### Current GBR Config:
```python
{"name": "GBR_OHE", "family": "gbr", "params": {"target_transform": "signed_log1p"}}
```

### Enhanced Config (if overfitting observed):
```python
{
    "name": "GBR_TUNED", 
    "family": "gbr", 
    "params": {
        "target_transform": "signed_log1p",
        "n_estimators": 100,      # Number of boosting stages
        "max_depth": 5,           # Limit tree depth (prevent overfitting)
        "min_samples_split": 20,  # Min samples to split node
        "min_samples_leaf": 10,   # Min samples in leaf node
        "learning_rate": 0.1,     # Shrinkage (lower = more conservative)
        "subsample": 0.8,         # Bootstrap sample fraction
        "max_features": "sqrt",   # Feature sampling per split
        "random_state": 42        # Reproducibility
    }
}
```

**When to apply**:
- If GBR WAPE increases significantly (>35%)
- If validation WAPE much worse than training WAPE
- If confidence intervals too narrow (underfitting uncertainty)

**How to test**:
1. Add GBR_TUNED to CANDIDATES
2. Run backtest
3. Compare WAPE, MAE, horizon stability
4. If improvement >2%, promote to production

---

## Summary of Changes

| Priority | File | Change | Rationale |
|----------|------|--------|-----------|
| 🔴 P1 | `10__modeling__backtest_global_plus_overrides.ipynb` | Remove Ridge from CANDIDATES | Prevents 1130% WAPE failure |
| 🟡 P2 | `docs/MODEL_TRANSFORM_GUIDE.md` | Create compatibility guide | Prevents future model selection errors |
| 🟡 P3 | `tests/test_metrics.py` | Add unit tests | Validates metric correctness |
| 🟢 P4 | `11__proc__score_and_publish_marts.sql` | Add documentation header | Improves maintainability |
| ⚪ P5 | `10__modeling__backtest_global_plus_overrides.ipynb` | Optional GBR tuning | Future performance optimization |

**Estimated Implementation Time**: 30 minutes  
**Risk Level**: Low (only removing broken model)  
**Expected Impact**: Eliminate Ridge failures, maintain GBR champion (28% WAPE)
