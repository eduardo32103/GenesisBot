# MT5 Robust Optimizer Summary

Safety: `broker_touched=false`, `order_executed=false`, `order_policy=journal_only_no_broker`.

Recommendation: **reject**

This report never recommends real trading. Passing profiles are paper-forward candidates only.

## Candidates
No profile passed the institutional robustness gates. Keep `observation_only`.

## Top Results
- `H1 quality_v3_conservative` PF `3.872`, WR `50.0`, DD `459.1`, recommendation `reject`. Reasons: sample_too_small.
- `H1 capital_preservation_v1` PF `9.8872`, WR `50.0`, DD `0.0`, recommendation `reject`. Reasons: sample_too_small.
- `H1 momentum_v2_filtered` PF `3.872`, WR `50.0`, DD `459.1`, recommendation `reject`. Reasons: sample_too_small.
- `H1 trend_v2_drawdown_guard` PF `3.872`, WR `50.0`, DD `459.1`, recommendation `reject`. Reasons: sample_too_small, test_pf_below_1.
- `H1 anti_chop_v2_safe` PF `3.6535`, WR `50.0`, DD `459.1`, recommendation `reject`. Reasons: sample_too_small.
- `H1 quality_strict` PF `3.6535`, WR `50.0`, DD `459.1`, recommendation `reject`. Reasons: sample_too_small.
- `M15 rsi_reversal_v2_confirmed` PF `1.3053`, WR `50.0`, DD `825.58`, recommendation `reject`. Reasons: sample_too_small.
- `M30 anti_chop_v1` PF `1.4289`, WR `56.52`, DD `4038.88535`, recommendation `reject`. Reasons: sample_too_small, test_pf_below_1, monte_carlo_risk_of_ruin_high, monte_carlo_drawdown_p95_above_limit.
- `M30 rsi_reversal_v2_confirmed` PF `1.5627`, WR `58.82`, DD `3915.78535`, recommendation `reject`. Reasons: sample_too_small, monte_carlo_risk_of_ruin_high, monte_carlo_drawdown_p95_above_limit, monte_carlo_stressed_pf_below_1_05.
- `M15 anti_chop_v1` PF `1.3037`, WR `57.14`, DD `1778.3`, recommendation `reject`. Reasons: sample_too_small, monte_carlo_stressed_pf_below_1_05, monte_carlo_stressed_expectancy_negative.

## Risk Position
- Real trading remains disabled.
- No martingale, no grid escalation, no averaging losses.
- If there is doubt, Genesis must return `NO_TRADE`.
