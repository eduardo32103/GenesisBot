# MT5 Robust Optimizer Summary

Safety: `broker_touched=false`, `order_executed=false`, `order_policy=journal_only_no_broker`.

Recommendation: **reject**

This report never recommends real trading. Passing profiles are paper-forward candidates only.

## Candidates
No profile passed the institutional robustness gates. Keep `observation_only`.

## Top Results
- `M30 quality_v3_conservative` PF `1.6991`, WR `52.0`, DD `2030.14`, recommendation `reject`. Reasons: sample_too_small.
- `M30 quality_v3_conservative` PF `1.7025`, WR `47.83`, DD `2030.14`, recommendation `reject`. Reasons: sample_too_small, monte_carlo_stressed_pf_below_1_05, monte_carlo_stressed_expectancy_negative.
- `M30 quality_v3_conservative` PF `1.5864`, WR `50.0`, DD `2030.14`, recommendation `reject`. Reasons: sample_too_small, monte_carlo_stressed_pf_below_1_05, monte_carlo_stressed_expectancy_negative.
- `M15 anti_chop_v1` PF `1.4792`, WR `55.0`, DD `2268.1098`, recommendation `reject`. Reasons: sample_too_small, monte_carlo_stressed_pf_below_1_05, monte_carlo_stressed_expectancy_negative.
- `M15 anti_chop_v1` PF `1.2672`, WR `52.38`, DD `3184.0698`, recommendation `reject`. Reasons: sample_too_small, first_half_pf_below_1, monte_carlo_stressed_pf_below_1_05, monte_carlo_stressed_expectancy_negative.
- `M15 anti_chop_v1` PF `1.1488`, WR `50.0`, DD `3184.0698`, recommendation `reject`. Reasons: sample_too_small, pf_below_1_20, first_half_pf_below_1, monte_carlo_risk_of_ruin_high.
- `M15 quality_strict` PF `0.8955`, WR `42.86`, DD `741.04`, recommendation `reject`. Reasons: sample_too_small, pf_below_1_20, expectancy_not_positive, win_rate_below_45.
- `M15 quality_strict` PF `0.8955`, WR `42.86`, DD `741.04`, recommendation `reject`. Reasons: sample_too_small, pf_below_1_20, expectancy_not_positive, win_rate_below_45.
- `M15 quality_strict` PF `0.8955`, WR `42.86`, DD `741.04`, recommendation `reject`. Reasons: sample_too_small, pf_below_1_20, expectancy_not_positive, win_rate_below_45.
- `M15 anti_chop_v1` PF `1.2284`, WR `50.0`, DD `2532.68`, recommendation `reject`. Reasons: sample_too_small, monte_carlo_stressed_pf_below_1_05, monte_carlo_stressed_expectancy_negative.

## Risk Position
- Real trading remains disabled.
- No martingale, no grid escalation, no averaging losses.
- If there is doubt, Genesis must return `NO_TRADE`.
