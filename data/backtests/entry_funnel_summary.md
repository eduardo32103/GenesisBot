# MT5 Entry Funnel Diagnostics

Paper-only diagnostics. No broker touched, no live orders, no promoted profile mutation.

## Timeframes With Most Opportunity

- `H1 trend_continuation_v3_balanced` generated `523` signals, actionable `523`, opened `2`.
- `H1 anti_chop_v4_balanced` generated `370` signals, actionable `370`, opened `13`.
- `H1 trend_continuation_v1` generated `327` signals, actionable `327`, opened `2`.
- `M30 trend_continuation_v3_balanced` generated `315` signals, actionable `315`, opened `12`.
- `H1 low_drawdown_v3_more_trades` generated `287` signals, actionable `287`, opened `4`.

## Most Restrictive Profiles

- `M15 breakout_pullback_v2_safe` restrictiveness `82.2304`; spread_filter=4999, volatility_filter=4990, score_threshold=4951.
- `M15 liquidity_sweep_reversal_v1` restrictiveness `78.8878`; spread_filter=4999, rsi_filter=4999, score_threshold=4989.
- `M15 capital_preservation_v2` restrictiveness `78.3277`; spread_filter=4999, volatility_filter=4999, score_threshold=4999.
- `M15 trend_continuation_v2_low_drawdown` restrictiveness `77.3355`; spread_filter=4999, volatility_filter=4984, score_threshold=4914.
- `M15 mean_reversion_v1_safe` restrictiveness `77.3135`; spread_filter=4999, rsi_filter=4999, score_threshold=4983.

## Recommendations

- Timeframe with most raw opportunity: `H1`.
- Most restrictive filter: `score_threshold` with `408523` failures.
- Most common no-trade reason: `volatility_too_low` with `371456` bars.
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
