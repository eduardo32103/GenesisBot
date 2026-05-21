# MT5 Capital Preservation Optimizer Summary

Safety: `broker_touched=false`, `order_executed=false`, `order_policy=journal_only_no_broker`.

Recommendation: **reject**

This report is paper-only. It never recommends real trading.

## Candidates
No profile passed capital-preservation gates. Recommendation: reject/observation_only.

## Top Results
- `M15 liquidity_sweep_reversal_v1` PF `302.02`, WR `100.0`, DD `0.0`, score `200.87`, recommendation `observation_only`. Reasons: sample_too_small, single_trade_dependency.
- `M30 breakout_pullback_v1` PF `619.0155`, WR `100.0`, DD `0.0`, score `185.855`, recommendation `observation_only`. Reasons: sample_too_small, single_trade_dependency.
- `M30 trend_continuation_v2_low_drawdown` PF `5.2039`, WR `77.78`, DD `576.45`, score `90.5431`, recommendation `observation_only`. Reasons: sample_too_small.
- `H1 low_drawdown_v2` PF `5.4751`, WR `77.78`, DD `474.6`, score `69.1899`, recommendation `observation_only`. Reasons: sample_too_small.
- `H1 low_drawdown_v2` PF `7.7284`, WR `66.67`, DD `234.38`, score `66.9445`, recommendation `observation_only`. Reasons: sample_too_small.
- `M30 trend_continuation_v2_low_drawdown` PF `2.8061`, WR `75.0`, DD `833.018358`, score `56.3242`, recommendation `observation_only`. Reasons: sample_too_small.
- `M30 trend_continuation_v2_low_drawdown` PF `2.8061`, WR `75.0`, DD `833.018358`, score `56.3242`, recommendation `observation_only`. Reasons: sample_too_small.
- `H1 low_drawdown_v2` PF `7.251`, WR `66.67`, DD `185.28`, score `51.7173`, recommendation `observation_only`. Reasons: sample_too_small.
- `H1 low_drawdown_v2` PF `7.5613`, WR `66.67`, DD `234.38`, score `38.3045`, recommendation `observation_only`. Reasons: sample_too_small, single_trade_dependency.
- `H1 low_drawdown_v2` PF `7.5613`, WR `66.67`, DD `234.38`, score `38.3045`, recommendation `observation_only`. Reasons: sample_too_small, single_trade_dependency.
- `H1 capital_preservation_v2` PF `6.9918`, WR `83.33`, DD `234.38`, score `34.3725`, recommendation `observation_only`. Reasons: sample_too_small, single_trade_dependency.
- `H1 mean_reversion_v1_safe` PF `3.1133`, WR `85.71`, DD `498.959357`, score `34.121`, recommendation `observation_only`. Reasons: sample_too_small, single_trade_dependency.
- `H1 low_drawdown_v2` PF `5.2499`, WR `66.67`, DD `474.6`, score `28.0869`, recommendation `observation_only`. Reasons: sample_too_small, single_trade_dependency.
- `H1 low_drawdown_v2` PF `7.3949`, WR `55.56`, DD `234.38`, score `25.8415`, recommendation `observation_only`. Reasons: sample_too_small, single_trade_dependency.
- `H1 capital_preservation_v2` PF `3.5684`, WR `83.33`, DD `474.6`, score `11.2199`, recommendation `observation_only`. Reasons: sample_too_small, single_trade_dependency.
- `H1 capital_preservation_v2` PF `4.9735`, WR `83.33`, DD `234.38`, score `10.6225`, recommendation `observation_only`. Reasons: sample_too_small.
- `H1 capital_preservation_v2` PF `4.9735`, WR `83.33`, DD `234.38`, score `10.6225`, recommendation `observation_only`. Reasons: sample_too_small.
- `H1 low_drawdown_v2` PF `6.9076`, WR `55.56`, DD `185.28`, score `10.6143`, recommendation `observation_only`. Reasons: sample_too_small, single_trade_dependency.
- `M30 low_drawdown_v2` PF `2.87`, WR `66.67`, DD `985.5186`, score `7.3225`, recommendation `observation_only`. Reasons: sample_too_small, single_trade_dependency.
- `H1 low_drawdown_v2` PF `4.587`, WR `66.67`, DD `474.6`, score `6.4419`, recommendation `observation_only`. Reasons: sample_too_small, single_trade_dependency.

## Risk Position
- No real trading.
- No martingale, no grid, no averaging losses, no size increase after losses.
- MaxOpenTrades remains 1 inside the simulator.
- If there is doubt, Genesis should choose `NO_TRADE`.
