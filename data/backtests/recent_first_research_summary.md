# MT5 Recent-First Research Summary

Recent-first signal discovery. Paper/offline only; no broker, no order execution, no automatic promotion.

Evaluations: `180`.
Max evaluations requested: `180`.
Candidates: `0`.

## Top 20
- `H1_30000` `H1` `recent_liquidity_sweep` side `both` session `all` recent `17` recent PF `2.2109`, recent exp `0.2703`, total `46` total PF `1.2874`, MC PF `0.6525`, recommendation `observation_only`.
- `H1_30000` `H1` `recent_failed_breakout_reversal` side `both` session `all` recent `17` recent PF `2.2109`, recent exp `0.2703`, total `46` total PF `1.2874`, MC PF `0.6525`, recommendation `observation_only`.
- `H1_30000` `H1` `recent_range_reversion` side `both` session `all` recent `14` recent PF `3.4005`, recent exp `0.3515`, total `49` total PF `1.3508`, MC PF `0.6849`, recommendation `observation_only`.
- `M30_40000` `M30` `recent_london_us_breakout` side `both` session `all` recent `32` recent PF `2.0626`, recent exp `0.3254`, total `60` total PF `1.5384`, MC PF `0.8893`, recommendation `observation_only`.
- `M30_40000` `M30` `recent_london_us_breakout` side `both` session `london_us` recent `32` recent PF `2.0626`, recent exp `0.3254`, total `60` total PF `1.5384`, MC PF `0.8893`, recommendation `observation_only`.
- `M30_60000` `M30` `recent_london_us_breakout` side `both` session `all` recent `40` recent PF `1.8104`, recent exp `0.276`, total `85` total PF `1.5572`, MC PF `0.9435`, recommendation `observation_only`.
- `M30_60000` `M30` `recent_london_us_breakout` side `both` session `london_us` recent `40` recent PF `1.8104`, recent exp `0.276`, total `85` total PF `1.5572`, MC PF `0.9435`, recommendation `observation_only`.
- `H1_30000` `H1` `recent_liquidity_sweep` side `both` session `all` recent `11` recent PF `2.6936`, recent exp `0.3068`, total `56` total PF `1.1574`, MC PF `0.4938`, recommendation `observation_only`.
- `H1_30000` `H1` `recent_failed_breakout_reversal` side `both` session `all` recent `11` recent PF `2.6936`, recent exp `0.3068`, total `56` total PF `1.1574`, MC PF `0.4938`, recommendation `observation_only`.
- `M15_20000` `M15` `recent_session_open_continuation` side `both` session `all` recent `20` recent PF `1.2996`, recent exp `0.0825`, total `55` total PF `1.3717`, MC PF `0.6558`, recommendation `observation_only`.
- `M15_20000` `M15` `recent_session_open_continuation` side `both` session `all` recent `20` recent PF `1.2996`, recent exp `0.0825`, total `55` total PF `1.3717`, MC PF `0.6558`, recommendation `observation_only`.
- `M30_60000` `M30` `recent_failed_breakout_reversal` side `both` session `all` recent `12` recent PF `1.2881`, recent exp `0.1104`, total `50` total PF `1.1745`, MC PF `0.5047`, recommendation `observation_only`.
- `H1_30000` `H1` `recent_range_reversion` side `both` session `all` recent `19` recent PF `1.9093`, recent exp `0.2208`, total `50` total PF `1.1565`, MC PF `0.5122`, recommendation `observation_only`.
- `H1_30000` `H1` `recent_range_reversion` side `both` session `all` recent `17` recent PF `2.0657`, recent exp `0.2363`, total `47` total PF `1.1063`, MC PF `0.527`, recommendation `observation_only`.
- `M30_40000` `M30` `recent_failed_breakout_reversal` side `both` session `all` recent `10` recent PF `1.0339`, recent exp `-0.0424`, total `48` total PF `1.5912`, MC PF `0.8872`, recommendation `reject`.
- `M30_60000` `M30` `recent_range_reversion` side `both` session `all` recent `9` recent PF `1.1306`, recent exp `-0.0108`, total `52` total PF `1.7639`, MC PF `0.8812`, recommendation `reject`.
- `H1_30000` `H1` `recent_liquidity_sweep` side `both` session `all` recent `23` recent PF `1.0562`, recent exp `0.1364`, total `63` total PF `0.8389`, MC PF `0.4118`, recommendation `observation_only`.
- `M30_40000` `M30` `recent_range_reversion` side `both` session `all` recent `9` recent PF `1.1306`, recent exp `-0.0108`, total `40` total PF `1.6542`, MC PF `0.8923`, recommendation `reject`.
- `M30_60000` `M30` `recent_volatility_breakout` side `buy` session `all` recent `12` recent PF `1.0031`, recent exp `-0.0062`, total `54` total PF `1.2398`, MC PF `0.6091`, recommendation `reject`.
- `M15_20000` `M15` `recent_failed_breakout_reversal` side `both` session `all` recent `15` recent PF `1.6625`, recent exp `0.159`, total `53` total PF `0.8531`, MC PF `0.3985`, recommendation `reject`.

## Answers
1. Families generating recent trades: H1_30000 H1 recent_liquidity_sweep both all recent=17 total=46; H1_30000 H1 recent_failed_breakout_reversal both all recent=17 total=46; H1_30000 H1 recent_range_reversion both all recent=14 total=49; M30_40000 M30 recent_london_us_breakout both all recent=32 total=60; M30_40000 M30 recent_london_us_breakout both london_us recent=32 total=60; M30_60000 M30 recent_london_us_breakout both all recent=40 total=85; M30_60000 M30 recent_london_us_breakout both london_us recent=40 total=85; H1_30000 H1 recent_liquidity_sweep both all recent=11 total=56
2. Best recent timeframe: H1 (506 recent closed, 1850 total closed across variants)
3. Best recent side: both (1797 recent closed, 7679 total closed across variants)
4. Best recent session/hour: all (1364 recent closed, 6056 total closed across variants)
5. Families surviving backward validation: H1_30000 H1 recent_liquidity_sweep both all recent=17 total=46; H1_30000 H1 recent_failed_breakout_reversal both all recent=17 total=46; M15_20000 M15 recent_session_open_continuation both all recent=20 total=55; M15_20000 M15 recent_session_open_continuation both all recent=20 total=55; M30_60000 M30 recent_failed_breakout_reversal both all recent=12 total=50
6. Recent-overfit families: none
7. Monte Carlo failures: recent_atr_expansion_scalp, recent_chop_avoidance_reversal, recent_ema_reclaim, recent_failed_breakout_reversal, recent_liquidity_sweep, recent_london_us_breakout, recent_momentum_pullback, recent_range_reversion, recent_session_open_continuation, recent_volatility_breakout
8. Top 3 for capital preservation optimizer: none; no profile should pass to capital preservation optimizer
9. No automatic promotion.

## Safety
- No real trading.
- No order_send.
- No broker credentials.
- MaxOpenTrades=1.
- No martingale, no grid, no averaging down, no size increase after loss.
- broker_touched=false
- order_executed=false
- order_policy=journal_only_no_broker
