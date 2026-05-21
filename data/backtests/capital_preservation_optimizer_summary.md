# MT5 Capital Preservation Optimizer Summary

Safety: `broker_touched=false`, `order_executed=false`, `order_policy=journal_only_no_broker`.

Recommendation: **reject**

This report is paper-only. It never recommends real trading.

## Candidates
No profile passed capital-preservation gates. Recommendation: reject/observation_only.

## Top Results
- `M15 trend_continuation_v1` PF `0.0`, WR `0.0`, DD `0.0`, score `-935.68`, recommendation `observation_only`. Reasons: sample_too_small, pf_below_1_20, expectancy_not_positive, win_rate_below_45, monte_carlo_stressed_pf_below_1_05.
- `M15 breakout_pullback_v1` PF `0.0`, WR `0.0`, DD `0.0`, score `-1166.0`, recommendation `reject`. Reasons: sample_too_small, pf_below_1_20, expectancy_not_positive, win_rate_below_45, monte_carlo_no_closed_trades.
- `M15 capital_preservation_v2` PF `0.0`, WR `0.0`, DD `0.0`, score `-1166.0`, recommendation `reject`. Reasons: sample_too_small, pf_below_1_20, expectancy_not_positive, win_rate_below_45, monte_carlo_no_closed_trades.
- `M15 capital_preservation_v2` PF `0.0`, WR `0.0`, DD `0.0`, score `-1166.0`, recommendation `reject`. Reasons: sample_too_small, pf_below_1_20, expectancy_not_positive, win_rate_below_45, monte_carlo_no_closed_trades.
- `M15 capital_preservation_v2` PF `0.0`, WR `0.0`, DD `0.0`, score `-1166.0`, recommendation `reject`. Reasons: sample_too_small, pf_below_1_20, expectancy_not_positive, win_rate_below_45, monte_carlo_no_closed_trades.
- `M15 capital_preservation_v2` PF `0.0`, WR `0.0`, DD `0.0`, score `-1166.0`, recommendation `reject`. Reasons: sample_too_small, pf_below_1_20, expectancy_not_positive, win_rate_below_45, monte_carlo_no_closed_trades.

## Risk Position
- No real trading.
- No martingale, no grid, no averaging losses, no size increase after losses.
- MaxOpenTrades remains 1 inside the simulator.
- If there is doubt, Genesis should choose `NO_TRADE`.
