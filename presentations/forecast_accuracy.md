# Forecast Accuracy Report

**Model**: GradientBoostingRegressor (3c4074b1-ba9d-47aa-ab45-1d16adb1bfef)

## Company-Level Metrics

- **WAPE**: 28.26%
- **MAE**: $11,911
- **BIAS**: -14.14%

## Accuracy by Horizon

|   horizon |   wape_pct |   mae_dollars |   bias_pct |   n_forecasts |
|----------:|-----------:|--------------:|-----------:|--------------:|
|         1 |      27.36 |         11116 |     -13.16 |          2412 |
|         2 |      27.49 |         11449 |     -14.43 |          2211 |
|         3 |      27.59 |         11565 |     -14.45 |          2010 |
|         4 |      28.2  |         11632 |     -12.93 |          1809 |
|         5 |      28.74 |         12027 |     -13.43 |          1608 |
|         6 |      28.51 |         12227 |     -14.75 |          1407 |
|         7 |      28.9  |         12318 |     -14.19 |          1206 |
|         8 |      28.86 |         12577 |     -14.86 |          1005 |
|         9 |      29.5  |         13123 |     -16.11 |           804 |
|        10 |      29.62 |         12767 |     -14.33 |           603 |
|        11 |      28.9  |         12733 |     -14.11 |           402 |
|        12 |      30.15 |         14558 |     -17.83 |           201 |

**Notes**:
- WAPE = Weighted Absolute Percentage Error
- MAE = Mean Absolute Error (dollars)
- BIAS = (Sum of signed errors) / (Sum of |actuals|)
