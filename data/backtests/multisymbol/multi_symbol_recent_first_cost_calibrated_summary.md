# MT5 Multi-Symbol Recent-First Summary

Multi-symbol recent-first edge discovery. Paper/offline/read-only only; no broker, no order execution, no automatic promotion.

Evaluations: `252`.
Candidates: `0`.
Skipped symbols: `0`.

## Top Results
- `NAS100` `H1` `recent_volatility_breakout` side `both` session `all` hardening `trailing_defensive` recent `2` recent PF `379.29`, total `13` total PF `2.7966`, MC PF `1.6096`, spread x2 PF `2.7655`, recommendation `reject`.
- `NAS100` `H1` `recent_volatility_breakout` side `both` session `all` hardening `baseline` recent `2` recent PF `379.29`, total `13` total PF `2.3295`, MC PF `1.3365`, spread x2 PF `2.3026`, recommendation `reject`.
- `NAS100` `H1` `recent_volatility_breakout` side `both` session `all` hardening `mae_guard` recent `2` recent PF `379.29`, total `13` total PF `2.3295`, MC PF `1.3365`, spread x2 PF `2.3026`, recommendation `reject`.
- `NAS100` `H1` `recent_volatility_breakout` side `both` session `all` hardening `fast_loss_cut` recent `2` recent PF `379.29`, total `13` total PF `2.3295`, MC PF `1.3365`, spread x2 PF `2.3026`, recommendation `reject`.
- `US500` `H1` `recent_volatility_breakout` side `both` session `all` hardening `baseline` recent `1` recent PF `46.725`, total `7` total PF `10.1147`, MC PF `7.8159`, spread x2 PF `9.1645`, recommendation `reject`.
- `US500` `H1` `recent_volatility_breakout` side `both` session `all` hardening `mae_guard` recent `1` recent PF `46.725`, total `7` total PF `10.1147`, MC PF `7.8159`, spread x2 PF `9.1645`, recommendation `reject`.
- `US500` `H1` `recent_volatility_breakout` side `both` session `all` hardening `fast_loss_cut` recent `1` recent PF `46.725`, total `7` total PF `10.1147`, MC PF `7.8159`, spread x2 PF `9.1645`, recommendation `reject`.
- `US500` `H1` `recent_volatility_breakout` side `both` session `all` hardening `trailing_defensive` recent `1` recent PF `46.725`, total `7` total PF `10.1147`, MC PF `7.8159`, spread x2 PF `9.1645`, recommendation `reject`.
- `EURUSD` `H1` `recent_session_open_continuation` side `both` session `all` hardening `trailing_defensive` recent `1` recent PF `0.0`, total `12` total PF `3.2582`, MC PF `2.1012`, spread x2 PF `3.1567`, recommendation `reject`.
- `XAUUSD` `M15` `recent_session_open_continuation` side `both` session `all` hardening `baseline` recent `17` recent PF `1.704`, total `52` total PF `1.4322`, MC PF `0.6738`, spread x2 PF `1.4192`, recommendation `observation_only`.
- `XAUUSD` `M15` `recent_session_open_continuation` side `both` session `all` hardening `fast_loss_cut` recent `17` recent PF `1.704`, total `52` total PF `1.4322`, MC PF `0.6738`, spread x2 PF `1.4192`, recommendation `observation_only`.
- `XAUUSD` `M15` `recent_session_open_continuation` side `both` session `all` hardening `trailing_defensive` recent `17` recent PF `1.704`, total `53` total PF `1.4046`, MC PF `0.662`, spread x2 PF `1.3932`, recommendation `observation_only`.
- `ETHUSD` `M30` `recent_volatility_breakout` side `both` session `all` hardening `trailing_defensive` recent `28` recent PF `1.1394`, total `85` total PF `1.5895`, MC PF `0.9422`, spread x2 PF `1.4401`, recommendation `observation_only`.
- `EURUSD` `H1` `recent_session_open_continuation` side `both` session `all` hardening `baseline` recent `1` recent PF `0.0`, total `12` total PF `2.8284`, MC PF `1.8217`, spread x2 PF `2.7285`, recommendation `reject`.
- `EURUSD` `H1` `recent_session_open_continuation` side `both` session `all` hardening `fast_loss_cut` recent `1` recent PF `0.0`, total `12` total PF `2.8284`, MC PF `1.8217`, spread x2 PF `2.7285`, recommendation `reject`.
- `ETHUSD` `M30` `recent_liquidity_sweep` side `both` session `all` hardening `fast_loss_cut` recent `14` recent PF `2.7797`, total `50` total PF `0.9972`, MC PF `0.4705`, spread x2 PF `0.9866`, recommendation `reject`.
- `US500` `H1` `recent_session_open_continuation` side `both` session `all` hardening `baseline` recent `30` recent PF `1.1829`, total `76` total PF `1.227`, MC PF `0.7222`, spread x2 PF `1.4057`, recommendation `observation_only`.
- `EURUSD` `H1` `recent_session_open_continuation` side `both` session `all` hardening `mae_guard` recent `1` recent PF `0.0`, total `12` total PF `2.7365`, MC PF `1.7625`, spread x2 PF `2.6425`, recommendation `reject`.
- `XAUUSD` `H1` `recent_volatility_breakout` side `both` session `all` hardening `baseline` recent `12` recent PF `1.3037`, total `19` total PF `1.572`, MC PF `1.0511`, spread x2 PF `1.57`, recommendation `reject`.
- `XAUUSD` `H1` `recent_volatility_breakout` side `both` session `all` hardening `fast_loss_cut` recent `12` recent PF `1.3037`, total `19` total PF `1.572`, MC PF `1.0511`, spread x2 PF `1.57`, recommendation `reject`.

## Answers
1. Spread x2 zero verdict: mixed; some profiles collapse under spread x2 while others survive
2. Symbols with reliable costs: BTCUSD, ETHUSD, EURUSD, GBPUSD, NAS100, US500, XAUUSD
3. Alias/export status: exported/evaluated: BTCUSD, ETHUSD, EURUSD, GBPUSD, NAS100, US500, XAUUSD; skipped/no local CSV: none
4. Symbols with recent edge: NAS100 best=H1 recent_volatility_breakout both recent=2 recent_pf=379.29; US500 best=H1 recent_volatility_breakout both recent=1 recent_pf=46.725; BTCUSD best=M30 recent_liquidity_sweep both recent=15 recent_pf=1.782
5. Evaluated symbols without edge / NO_TRADE: ETHUSD, EURUSD, GBPUSD, XAUUSD
6. Best timeframe by symbol: BTCUSD:M30; ETHUSD:M30; EURUSD:H1; GBPUSD:M15; NAS100:H1; US500:H1; XAUUSD:M15
7. Best family by symbol: BTCUSD:recent_liquidity_sweep; ETHUSD:recent_volatility_breakout; EURUSD:recent_session_open_continuation; GBPUSD:recent_liquidity_sweep; NAS100:recent_volatility_breakout; US500:recent_volatility_breakout; XAUUSD:recent_session_open_continuation
8. Best side by symbol: BTCUSD:both; ETHUSD:both; EURUSD:both; GBPUSD:both; NAS100:both; US500:both; XAUUSD:both
9. Best session by symbol: BTCUSD:all; ETHUSD:all; EURUSD:all; GBPUSD:all; NAS100:all; US500:all; XAUUSD:all
10. Profiles passing Monte Carlo: NAS100 H1 recent_volatility_breakout both recent=2 total=13 MC=1.6096 spread2=2.7655; NAS100 H1 recent_volatility_breakout both recent=2 total=13 MC=1.3365 spread2=2.3026; NAS100 H1 recent_volatility_breakout both recent=2 total=13 MC=1.3365 spread2=2.3026; NAS100 H1 recent_volatility_breakout both recent=2 total=13 MC=1.3365 spread2=2.3026; US500 H1 recent_volatility_breakout both recent=1 total=7 MC=7.8159 spread2=9.1645; US500 H1 recent_volatility_breakout both recent=1 total=7 MC=7.8159 spread2=9.1645; US500 H1 recent_volatility_breakout both recent=1 total=7 MC=7.8159 spread2=9.1645; US500 H1 recent_volatility_breakout both recent=1 total=7 MC=7.8159 spread2=9.1645
11. Profiles failing spread/slippage stress: ETHUSD M30 recent_liquidity_sweep both recent=14 total=50 MC=0.4705 spread2=0.9866; US500 H1 recent_liquidity_sweep both recent=23 total=60 MC=0.6469 spread2=0.9224; US500 H1 recent_liquidity_sweep both recent=23 total=60 MC=0.6469 spread2=0.9224; EURUSD H1 recent_liquidity_sweep both recent=14 total=54 MC=0.4722 spread2=0.9488; EURUSD H1 recent_liquidity_sweep both recent=14 total=54 MC=0.4722 spread2=0.9488; EURUSD H1 recent_liquidity_sweep both recent=14 total=54 MC=0.4722 spread2=0.9488; BTCUSD H1 recent_liquidity_sweep both recent=20 total=50 MC=0.368 spread2=0.8768; BTCUSD H1 recent_liquidity_sweep both recent=20 total=50 MC=0.368 spread2=0.8768
12. Top 3 for capital preservation optimizer: none; no profile should pass to capital preservation optimizer
13. Final recommendation: observation_only
14. No automatic promotion.

## Safety
- No real trading.
- No order_send.
- No broker credentials.
- MaxOpenTrades=1.
- No martingale, no grid, no averaging down, no size increase after loss.
- broker_touched=false
- order_executed=false
- order_policy=journal_only_no_broker
