# Executive Summary: FY2026 Revenue Forecast

**Champion Model**: GradientBoostingRegressor

**Company WAPE (H1)**: 27.36% (Champion) → 27.15% (What-if with PC715 tuned)

## FY2026 Forecast

| Scenario | Total Forecast | 80% CI Range | Delta vs Champion |
|----------|---------------|--------------|-------------------|
| **Champion (GBR)** | $84,992,132 | ($84,715,755, $85,106,026) | - |
| **What-if (PC715 tuned)** | $85,604,072 | - | +$611,940 (+0.72%) |

**What-if Impact**: Replacing PC715 champion predictions with tuned LightGBM reduces company WAPE from 27.36% to 27.15% (-0.8% relative) and increases FY2026 total forecast by $611,940 (+0.72%), while improving bias from -13.16% to -12.53% (reducing systematic underforecasting).

## Model Performance

The champion GradientBoostingRegressor model demonstrates a 28.26% WAPE across 15,678 forecasts. Horizon-1 forecasts (next month) show 27.4% WAPE, with performance degrading to 30.1% at horizon 12. Recent PC 715 tuning experiment achieved 24.7% WAPE improvement using per-series LightGBM, suggesting significant upside potential.

## Top Recommendations

1. **PC 715 Tuning Success**: Experimental LightGBM tuning achieved 24.7% WAPE improvement for PC 715. Recommend deploying as override and extending methodology to PC 555 and PC 695.

2. **High-Error Series Focus**: Top 3 series (PC 555, 715, 695) contribute significant revenue and forecast error. Prioritize per-series modeling or ensemble approaches.

3. **Horizon Degradation**: Forecast accuracy degrades from 27.4% WAPE (H1) to 30.1% (H12.0). Consider refreshing forecasts more frequently for long-horizon planning.

## Key Insights

- **High-Revenue Series**: Top 10 product-customer combinations drive $237,727,790 in revenue
- **Forecast Confidence**: 80% CI width averages �0.5% of point forecast
- **Model Selection**: Champion selected from 2 candidates (GBR, LightGBM) based on backtesting performance

---
*Generated: 2026-01-31 11:46:12*
