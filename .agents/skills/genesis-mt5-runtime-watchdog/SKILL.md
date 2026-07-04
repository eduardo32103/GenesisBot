---
name: genesis-mt5-runtime-watchdog
description: "Use when validating GENESIS MT5 bridge, EA attachment, tick/bars freshness, runtime context, XAUUSD.b aliasing, and XAUUSD M15 readiness."
---

# Genesis MT5 Runtime Watchdog

## Global Contract

- Work only on GENESIS / GenesisBot.
- Validate runtime context without opening trades.
- Do not touch broker execution or call `order_send`.
- Do not push unless explicitly requested.

## Runtime Checks

Inspect:

- `latest_tick_at`
- `latest_bars_at`
- `bars_count`
- `runtime_context_available`
- `runtime_context_recent`
- `runtime_snapshot_context`
- `symbol_alias_used`
- XAUUSD vs XAUUSD.b normalization

If MT5 or the EA is not sending recent data, block the supervisor with `configure_mt5_bridge_for_xauusd_m15`. Confirm `SendBars` and `SendTick` are alive with HTTP 200 when live logs are available.

## Required Output

Always report:

- `runtime_status`
- `latest_tick_at`
- `latest_bars_at`
- `bars_count`
- `readiness_state`
- `recommendation`
- `broker_touched`
- `order_executed`
- `order_policy`
- `candidate_activated`
- `paper_forward_onboarding_started`
