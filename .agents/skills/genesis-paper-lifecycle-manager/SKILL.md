---
name: genesis-paper-lifecycle-manager
description: "Use when implementing or debugging GENESIS XAUUSD M15 paper shadow open, monitor, close, backfill, reconciliation, duplicate handling, and orphan handling."
---

# Genesis Paper Lifecycle Manager

## Global Contract

- Work only on GENESIS / GenesisBot.
- Never open real trades or touch the broker.
- Never create a second paper shadow while one is open.
- Do not push unless explicitly requested.

## Lifecycle Rules

Manage:

- atomic open
- open persistence
- merged runtime plus persistent open count
- duplicate detection
- monitor
- paper close
- atomic close persistence
- history reconciliation
- orphan detection
- valid runtime open backfill

Do not delete a runtime shadow if close persistence failed. Do not report `paper_shadow_created=true` if critical open persistence failed. Do not revive closed shadows. Treat runtime and persistent rows with the same `shadow_trade_id` as one shadow.

## Required Output

Always report:

- `lifecycle_state`
- `shadow_trade_id`
- `open_source`
- `runtime_open_count`
- `persistent_open_count`
- `merged_open_count`
- `duplicate_detected`
- `paper_shadow_created`
- `paper_close_applied`
- `close_reason`
- `reconciliation_status`
- `broker_touched`
- `order_executed`
- `order_policy`
- `candidate_activated`
- `paper_forward_onboarding_started`
