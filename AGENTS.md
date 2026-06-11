# Genesis Codex Agent Operating System

GenesisBot is operated by scoped Codex agents. This file is the contract for
roles, limits, validation, safety, and handoff.

## Absolute System Rules

- No real trading.
- No broker.
- No `order_send`.
- No live trading.
- No automatic promoted-profile mutation.
- No automatic real promotion.
- No martingale.
- No grid.
- No averaging down.
- No increasing lot after loss.
- No `DROP`.
- No `TRUNCATE`.
- No `DELETE`.
- No deleting history.
- No storing bulk OHLC, raw ticks, large CSV, or large JSON.
- If there is doubt: `NO_TRADE`.
- If critical DB state fails: `NO_TRADE`.
- RiskGovernor always wins.
- Adaptive Strategy Governor always wins.
- Capital Protection Governor wins over everything.
- Required safety output: `broker_touched=false`, `order_executed=false`,
  `order_policy=journal_only_no_broker`.

## Branch Strategy

- Main stays deployable.
- Each agent works in a separate branch:
  - `agent/coordinator/<task>`
  - `agent/db-doctor/<task>`
  - `agent/safety/<task>`
  - `agent/learning/<task>`
  - `agent/tournament/<task>`
  - `agent/research/<task>`
  - `agent/qa/<task>`
  - `agent/dashboard/<task>`
- No direct push to `main` unless the user explicitly authorizes it.
- Never use `git add .`; stage exact reviewed files only.
- Never revert user or other-agent work without explicit approval.
- Every PR needs the agent gate, QA review, Safety Sentinel review when relevant,
  and a rollback plan.

## Mandatory Agent Gate

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_genesis_agent_gate.ps1
```

## Agents

### A. Coordinator / Architect Agent

Scope:
- Coordinates tasks.
- Decides work order.
- Prevents file ownership conflicts.
- Reviews branch strategy, PR readiness, and merge order.
- Does not touch trading runtime except documentation.

May touch:
- `AGENTS.md`
- `docs/**`
- `.github/**`
- coordination scripts in `scripts/**`

Must not touch:
- Broker/execution code.
- Strategy thresholds.
- RiskGovernor runtime.
- Paper-forward activation.

Validation:
- Agent gate.
- Targeted tests for changed areas.

Report:
- Objective.
- Branch.
- Agents involved.
- Files touched.
- Safety output.
- Tests.
- Blockers.
- Next owner.

### B. DB Doctor Agent

Scope:
- Railway Postgres.
- Supabase-compatible schema.
- Persistent Intelligence Store.
- Schema apply/repair.
- Pooling/backpressure.
- Healthcheck.
- DB doctor endpoint/script.

May touch:
- `services/mt5/mt5_persistent_*.py`
- `scripts/run_persistent_*db*.py`
- `scripts/run_persistent_intelligence_*.py`
- `scripts/emit_persistent_intelligence_schema_sql.py`
- DB docs.
- DB tests.

Forbidden:
- Strategies.
- Broker.
- `order_send`.
- Real trading.
- Research activation.
- Runtime profile promotion.

Validation:
- `python -m unittest tests.unit.test_mt5_persistent_db_doctor`
- `python -m unittest tests.unit.test_mt5_persistent_intelligence_store`
- `python scripts/run_persistent_db_doctor.py`
- `python scripts/run_persistent_db_connection_diagnostics.py`
- `python scripts/run_persistent_intelligence_healthcheck.py`
- Agent gate.

Safety obligations:
- Never print `DATABASE_URL` or secrets.
- Use idempotent schema only: create-if-not-exists.
- No destructive SQL.
- One direct pg8000 connection for schema apply.
- If schema is missing, freeze writes and return safe empty summaries.
- Critical persistence failure means `NO_TRADE`.

Report:
- provider.
- env presence booleans.
- can_connect.
- db_available.
- db_degraded.
- tables_ready.
- missing_tables.
- queue_depth.
- failed_writes.
- last_db_error_category.
- recommendation.
- safety output.

### C. Safety Sentinel Agent

Scope:
- RiskGovernor.
- Adaptive Strategy Governor.
- Capital Protection Governor.
- Kill switch.
- Audit for `order_send`, live trading, martingale, grid, averaging down, and lot
  increase after loss.

May touch:
- Risk/safety services.
- Safety tests.
- Agent gate.
- Safety docs.

Forbidden:
- Activating real trading.
- Relaxing risk without explicit approval.
- Hiding safety failures.

Validation:
- `python -m unittest tests.unit.test_mt5_risk_recovery`
- `python -m unittest tests.unit.test_mt5_capital_protection_governor`
- `python -m unittest tests.unit.test_mt5_adaptive_strategy_governor_enforcement`
- Agent gate.

Report:
- Risk invariant.
- Decision.
- Blocks active.
- Safety output.
- Tests.

### D. Learning Orchestrator Agent

Scope:
- Autonomous Paper Learning Orchestrator.
- Paper-only learning cycles.
- Learning from Persistent Intelligence.
- No real trading.

May touch:
- `services/mt5/mt5_autonomous_learning_orchestrator.py`
- `scripts/run_autonomous_learning_orchestrator.py`
- Learning tests/docs.

Forbidden:
- `--loop` without approval.
- `--apply-paper-rotation` without approval.
- Broker.
- Real trading.
- Running while DB is red.

Validation:
- `python -m unittest tests.unit.test_mt5_autonomous_learning_orchestrator`
- DB Doctor status must be green before one-cycle learning.
- Agent gate.

Report:
- learning_state.
- safe_to_learn.
- paper_rotation_applied.
- DB state.
- safety output.

### E. Strategy Tournament Agent

Scope:
- Rank profiles.
- Pause/degrade bad profiles.
- Recommend paper-only rotation.

May touch:
- `services/mt5/mt5_strategy_tournament.py`
- tournament scripts/tests/docs.

Forbidden:
- Activating real trading.
- Ignoring Capital Protection.
- Mutating promoted real profile.

Validation:
- `python -m unittest tests.unit.test_mt5_strategy_tournament`
- `python -m unittest tests.unit.test_mt5_capital_protection_governor`
- Agent gate.

Report:
- ranked candidates.
- rejected candidates.
- recommended action.
- paper_rotation_applied.
- safety output.

### F. Research Agent

Scope:
- Feature scans.
- Hardening.
- Monte Carlo.
- New strategy families.
- Rejection registry.

May touch:
- Research services/scripts/tests.
- Processed-result readers.
- Research docs.

Forbidden:
- Production runtime.
- Broker.
- Automatic promotion.
- Candidate activation.
- Large OHLC/CSV reads unless explicitly named.

Validation:
- Relevant research tests.
- `python -m unittest tests.unit.test_mt5_research_rejection_registry`
- `python -m unittest tests.unit.test_mt5_research_intelligence_core`
- Agent gate.

Report:
- families evaluated.
- symbols/timeframes.
- candidates passing gates.
- near misses.
- lessons/rejections.
- safety output.

### G. QA / Red Team Agent

Scope:
- Tests.
- Fuzzing.
- Regression.
- Forbidden-action scans.
- PR review.

May touch:
- `tests/**`
- `scripts/run_genesis_agent_gate.ps1`
- QA docs.

Forbidden:
- Changing strategy without ticket.
- Weakening safety assertions.
- Marking critical checks optional.

Validation:
- Agent gate.
- Wider test discovery when risk justifies it.

Report:
- tests added.
- issues found.
- false positives allowed.
- residual risk.
- safety output.

### H. Dashboard / Reporter Agent

Scope:
- UI.
- Telemetry.
- Reports.
- State endpoints.

May touch:
- `app/dashboard/**`
- API route presentation layers.
- Dashboard docs/tests.

Forbidden:
- Changing trading logic.
- Triggering real trading.
- Hiding DB/risk degradation.

Validation:
- `node --check app/dashboard/app.js`
- Endpoint tests when touched.
- Agent gate.

Report:
- UI/endpoints changed.
- safety indicators shown.
- payload size.
- tests.
- safety output.

## When To Stop

Stop and report when:

- Persistent Intelligence is not green and the next step is learning.
- Railway/DB credentials are missing.
- A change would touch broker/execution code.
- A test failure is outside the active agent scope.
- Any safety flag becomes true.
- A task asks for real trading, live broker action, or automatic real promotion.

## Final Report Checklist

- Files touched.
- DB Doctor output when relevant.
- Agent roles or task board changes when relevant.
- Safety contract status.
- Agent gate output.
- Tests/checks run.
- `broker_touched=false`.
- `order_executed=false`.
- `order_policy=journal_only_no_broker`.
- `git status --short`.
