# MT5 Entry Funnel Diagnostics

Paper-only diagnostics. No broker touched, no live orders, no promoted profile mutation.

## Timeframes With Most Opportunity

- `H1 trend_continuation_v4_expanded` generated `835` signals, actionable `835`, opened `5`.
- `M30 trend_continuation_v4_expanded` generated `783` signals, actionable `783`, opened `12`.
- `H1 low_drawdown_v4_expanded` generated `599` signals, actionable `599`, opened `6`.
- `M30 low_drawdown_v4_expanded` generated `504` signals, actionable `504`, opened `13`.
- `H1 breakout_pullback_v4_expanded` generated `108` signals, actionable `108`, opened `3`.

## Most Restrictive Profiles

- `H1 breakout_pullback_v4_expanded` restrictiveness `25.9618`; pullback_filter=4119, score_threshold=3381, regime_filter=876.
- `M30 breakout_pullback_v4_expanded` restrictiveness `25.9064`; pullback_filter=4092, score_threshold=3670, regime_filter=1491.
- `H1 low_drawdown_v4_expanded` restrictiveness `18.963`; score_threshold=3481, pullback_filter=1635, regime_filter=876.
- `H1 trend_continuation_v4_expanded` restrictiveness `18.2927`; score_threshold=3143, pullback_filter=1635, regime_filter=876.
- `M30 low_drawdown_v4_expanded` restrictiveness `18.1636`; score_threshold=3798, pullback_filter=1555, regime_filter=1491.

## Recommendations

- Timeframe with most raw opportunity: `H1`.
- Most restrictive filter: `score_threshold` with `20578` failures.
- Most common no-trade reason: `drawdown_accelerating` with `9868` bars.
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
