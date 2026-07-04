---
name: genesis-qa-red-team
description: "Use when running GENESIS regression tests, paper-only safety gates, git diff checks, adversarial checks, and readiness review before commit or push."
---

# Genesis QA Red Team

## Global Contract

- Work only on GENESIS / GenesisBot.
- Verify paper-only safety before any commit or push.
- Do not push unless explicitly requested.

## Required Checks

Run or request:

```powershell
python -m unittest tests.unit.test_mt5_xau_m15_paper_test_supervisor
python -m unittest tests.unit.test_mt5_xau_m15_paper_observation_batch_runner
python -m unittest tests.unit.test_mt5_xau_m15_paper_shadow_monitor
python -m unittest tests.unit.test_mt5_xau_m15_paper_observation_readiness
python -m unittest tests.unit.test_mt5_xau_m15_runtime_open_shadow_backfill
python -m unittest tests.unit.test_mt5_persistent_intelligence_store
python -m unittest tests.unit.test_mt5_capital_protection_governor
python -m unittest tests.unit.test_mt5_risk_recovery
node --check app/dashboard/app.js
git diff --check
powershell -ExecutionPolicy Bypass -File scripts/run_genesis_agent_gate.ps1
powershell -ExecutionPolicy Bypass -File scripts/run_genesis_subagent_gate.ps1
```

Search for duplicate `shadow_trade_id`, orphan opened shadow missing close record, close without persistence, open without persistence, read-only endpoint queue generation, and target trades counting old historical trades.

## Required Output

Always report:

- `tests_passed`
- `tests_failed`
- `blockers`
- `safety_status`
- `git_status_summary`
- `ready_to_commit`
- `ready_to_push`
- `broker_touched`
- `order_executed`
- `order_policy`
- `candidate_activated`
- `paper_forward_onboarding_started`
