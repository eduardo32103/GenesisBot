# Objective

Describe the user-facing goal and why this change is needed.

# Files Touched

-

# Scope

- [ ] Docs/checks only
- [ ] DB Doctor
- [ ] DB / Infra
- [ ] Safety / Risk
- [ ] Learning / Tournament
- [ ] Research
- [ ] Dashboard / Telemetry
- [ ] QA / Tests

# Safety Output

- [ ] `broker_touched=false`
- [ ] `order_executed=false`
- [ ] `order_policy=journal_only_no_broker`
- [ ] No `order_send`
- [ ] No real trading
- [ ] No broker action
- [ ] No live trading
- [ ] No promoted-profile mutation
- [ ] No automatic real promotion
- [ ] No martingale
- [ ] No grid
- [ ] No averaging down
- [ ] No increasing lot after loss

# Tests / Checks

Paste exact commands and results:

```text
python -m unittest tests.unit.test_mt5_persistent_intelligence_store
python -m unittest tests.unit.test_mt5_capital_protection_governor
python -m unittest tests.unit.test_mt5_strategy_tournament
python -m unittest tests.unit.test_mt5_adaptive_strategy_governor
python -m unittest tests.unit.test_mt5_risk_recovery
node --check app/dashboard/app.js
git diff --check
powershell -ExecutionPolicy Bypass -File scripts/run_genesis_agent_gate.ps1
```

# Railway / DB Status

Fill when relevant:

- provider:
- db_available:
- db_degraded:
- tables_ready:
- missing_tables:
- recommendation:

# Risk Review

- RiskGovernor behavior changed? `no`
- Capital protection behavior changed? `no`
- Runtime trading behavior changed? `no`
- Broker/execution files touched? `no`

# Rollback Plan

Explain the fastest safe rollback. Include whether rollback is code-only, DB-only,
or both. Destructive DB rollback is not allowed without explicit human approval.

# Notes / Blockers

-
