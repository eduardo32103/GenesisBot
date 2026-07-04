---
name: genesis-parameter-sweep-lab
description: "Use when optimizing GENESIS XAUUSD M15 fast observation parameters from historical bars and paper-only results, including cooldown and filter experiments."
---

# Genesis Parameter Sweep Lab

## Global Contract

- Work only on GENESIS / GenesisBot.
- Use offline or paper-only data.
- Do not touch broker, activate real trading, or promote candidates.
- Do not push unless explicitly requested.

## Sweep Scope

Evaluate combinations of:

- `time_stop_bars`
- `min_r_to_arm_trailing`
- `giveback_r`
- `fast_loss_cut_r`
- entry cooldown
- direction filter
- volatility filter
- `recent_edge_negative` cooldown

Use historical/offline data when available. Propose parameters only for paper-only supervisor review. Flag overfit risk when sample size is small, dependency is concentrated, or parameter changes are too narrow.

## Required Output

Always report:

- `evaluations_count`
- `recommended_time_stop_bars`
- `recommended_min_r_to_arm_trailing`
- `recommended_giveback_r`
- `recommended_fast_loss_cut_r`
- `expected_win_rate`
- `expected_profit_factor`
- `warning_if_overfit`
- `broker_touched`
- `order_executed`
- `order_policy`
- `candidate_activated`
- `paper_forward_onboarding_started`
