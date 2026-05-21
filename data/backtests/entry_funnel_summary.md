# MT5 Entry Funnel Diagnostics

Paper-only diagnostics. No broker touched, no live orders, no promoted profile mutation.

## Timeframes With Most Opportunity

- `H1 trend_continuation_v5_defense_aware` generated `459` signals, actionable `459`, opened `5`.
- `M30 trend_continuation_v5_defense_aware` generated `391` signals, actionable `391`, opened `12`.
- `H1 low_drawdown_v5_session_filtered` generated `320` signals, actionable `320`, opened `4`.
- `H1 capital_preservation_v4_side_filtered` generated `282` signals, actionable `282`, opened `2`.
- `M30 low_drawdown_v5_session_filtered` generated `258` signals, actionable `258`, opened `12`.

## Most Restrictive Profiles

- `M30 liquidity_sweep_v3_session_confirmed` restrictiveness `36.8344`; rsi_filter=4999, score_threshold=4648, pullback_filter=4378.
- `H1 liquidity_sweep_v3_session_confirmed` restrictiveness `34.6257`; rsi_filter=4999, pullback_filter=4357, score_threshold=4216.
- `M30 breakout_pullback_v5_fast_exit` restrictiveness `27.6923`; pullback_filter=4092, score_threshold=3670, regime_filter=1491.
- `H1 breakout_pullback_v5_fast_exit` restrictiveness `27.1038`; pullback_filter=4119, score_threshold=3381, regime_filter=876.
- `H1 capital_preservation_v4_side_filtered` restrictiveness `24.3233`; score_threshold=3731, pullback_filter=1635, rsi_filter=1086.

## Recommendations

- Timeframe with most raw opportunity: `H1`.
- Most restrictive filter: `score_threshold` with `37497` failures.
- Most common no-trade reason: `session_filter` with `21537` bars.
- Build balanced variants by easing the dominant blocker one step at a time, then rerun capital preservation gates.
- Do not promote profiles from funnel counts alone; funnel only explains opportunity loss.

## Safety
- No martingale.
- No grid.
- MaxOpenTrades remains 1 in simulation.
- Recommendation: no real trading. Use diagnostics to design more paper-only variants.
- broker_touched=false
- order_executed=false
- order_policy=journal_only_no_broker
