---
name: genesis-autopilot-orchestrator
description: "Use when coordinating GENESIS XAUUSD M15 paper-only test automation, supervisor runs, DB checks, runtime checks, strict paper probes, and winrate loops."
---

# Genesis Autopilot Orchestrator

## Global Contract

- Work only on GENESIS / GenesisBot.
- Treat XAUUSD M15 as the primary symbol/timeframe unless the user says otherwise.
- Keep every action paper-only.
- Do not touch the real broker, call `order_send`, enable real trading, set `candidate_activated=true`, or start `paper_forward_onboarding_started`.
- Do not hide errors, and do not push unless the user explicitly asks.
- Do not commit runtime files under `data/research_outputs` unless explicitly requested.

## Workflow

Before opening any paper shadow, check in order:

1. Persistent DB status.
2. Open shadows.
3. XAUUSD M15 readiness.
4. MT5 runtime freshness for tick and bars.
5. History reconciliation.

If `queue_depth > 0`, do not open a shadow; drain or request a drain path first. If `open_count > 1`, block. If `open_count == 1`, monitor or close that paper shadow before opening another.

If `open_count == 0` and readiness is ready, allow only a confirmed paper-only open. If readiness is blocked by `recent_edge_negative`, do not force entry; use adaptive cooldown and strict paper probe behavior.

For session targets, count `session_trades_closed`, not `historical_closed_count`. Preserve `session_id`, `session_started_at`, `session_trades_opened`, and `session_trades_closed`.

## Required Output

Always report:

- `supervisor_state`
- `stop_reason`
- `next_action`
- `session_id`
- `session_trades_opened`
- `session_trades_closed`
- `open_count`
- `merged_open_count`
- `queue_depth`
- `readiness_state`
- `broker_touched`
- `order_executed`
- `order_policy`
- `candidate_activated`
- `paper_forward_onboarding_started`
