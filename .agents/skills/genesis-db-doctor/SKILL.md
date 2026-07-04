---
name: genesis-db-doctor
description: "Use when debugging GENESIS Persistent Intelligence, Railway Postgres, DB queue/backpressure, schema fallback, history, and paper shadow persistence."
---

# Genesis DB Doctor

## Global Contract

- Work only on GENESIS / GenesisBot.
- Keep all work paper-only and broker-free.
- Do not push unless explicitly requested.
- Do not let read-only endpoints create DB queue pressure.

## Diagnostic Workflow

Review:

- `persistent-intelligence/status`
- `queue_depth`, `queued_writes`, `failed_writes`
- `queue_drain_succeeded`
- `tables_ready`, `db_degraded`
- schema fallback and optional-column fallback
- `mt5_shadow_trades`
- history endpoint
- open endpoint persistent fallback

If `queue_depth > 0`, recommend or implement a safe drain before opening a new paper shadow. If an optional column fails, implement a minimal fallback without degrading DB health or enqueuing noncritical writes. Separate critical writes from noncritical writes.

## Required Output

Always report:

- `db_status`
- `queue_status`
- `schema_status`
- `failed_write_root_cause`
- `recommended_next_action`
- `broker_touched`
- `order_executed`
- `order_policy`
- `candidate_activated`
- `paper_forward_onboarding_started`
