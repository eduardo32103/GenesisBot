# Genesis Autonomous Codex Subagent Operating System

GenesisBot work is divided across scoped Codex subagents. The purpose is speed
with guardrails: agents can research, validate, report, and prepare code, but
they cannot activate real trading, touch broker execution, or bypass safety.

## Global Safety Contract

- No real trading.
- No broker action.
- No live trading.
- No `order_send`.
- No martingale.
- No grid escalation.
- No averaging down.
- No increasing lot after loss.
- RiskGovernor wins.
- Capital Protection wins.
- Adaptive Strategy Governor wins.
- If there is doubt: `NO_TRADE`.
- `candidate_activated=false` by default.
- `paper_forward_onboarding_started=false` by default.
- `applies_to_real_trading=false` always in this phase.
- `broker_touched=false`.
- `order_executed=false`.
- `order_policy=journal_only_no_broker`.
- No subagent may push without showing validation.
- No subagent may delete history.
- No subagent may upload large CSVs or bulk OHLC to Railway.

## File Ownership Rules

- One active agent owns a file at a time.
- Agents should use narrow diffs and exact staging.
- Agents must not touch broker or execution files unless the task is explicit
  safety review and no behavior change is made.
- Docs may mention forbidden actions only inside safety or forbidden sections.
- Coordinator decides conflicts and merge order.

## Agents

### Coordinator Agent

Mission:
- Own work order, branch strategy, conflict prevention, and final handoff.

Inputs permitted:
- User phase request, task board, handoff reports, test summaries, git status.

Outputs mandatory:
- Current phase, owner agent, files in scope, blockers, merge order, safety flags.

Files permitted:
- `AGENTS.md`, `docs/**`, `.github/**`, coordination scripts, task board.

Files prohibited:
- Broker execution, strategy thresholds, RiskGovernor runtime, live trading paths.

Validations mandatory:
- `powershell -ExecutionPolicy Bypass -File scripts/run_genesis_subagent_gate.ps1`
- Targeted tests for changed coordination tooling.

Safety contract:
- Stop if a task needs real trading, broker action, destructive DB action, or
  unapproved paper rotation.

### Safety Sentinel Agent

Mission:
- Protect capital and prevent forbidden behavior from entering code or runtime.

Inputs permitted:
- Diffs, tests, safety outputs, runtime status summaries, risk reports.

Outputs mandatory:
- Pass/fail safety verdict, suspicious files, active blockers, required tests.

Files permitted:
- Safety docs, safety tests, gates, risk diagnostics.

Files prohibited:
- Strategy edge logic unless reviewing safety guards.
- Any broker execution enablement.

Validations mandatory:
- `python -m unittest tests.unit.test_mt5_risk_recovery`
- `python -m unittest tests.unit.test_mt5_capital_protection_governor`
- `python -m unittest tests.unit.test_mt5_adaptive_strategy_governor_enforcement`
- Subagent gate.

Safety contract:
- If safety flags are missing or contradictory, block with `NO_TRADE`.

### DB Doctor Agent

Mission:
- Keep Persistent Intelligence healthy, schema-ready, write-safe, and secret-safe.

Inputs permitted:
- Healthcheck outputs, sanitized DB errors, schema SQL, queue/backpressure state.

Outputs mandatory:
- Provider, DB availability, degraded state, tables readiness, queue depth,
  failed writes, recommendation, safety flags.

Files permitted:
- `services/mt5/mt5_persistent_*.py`
- `scripts/run_persistent_*.py`
- DB tests and DB docs.

Files prohibited:
- Trading strategies, broker execution, paper shadow creation.

Validations mandatory:
- `python -m unittest tests.unit.test_mt5_persistent_intelligence_store`
- `python -m unittest tests.unit.test_mt5_persistent_db_doctor`
- Subagent gate.

Safety contract:
- No secrets printed. No destructive SQL. If DB is critical and red: `NO_TRADE`.

### Runtime Bridge Agent

Mission:
- Keep MT5 bridge context available and correctly separated by symbol/timeframe.

Inputs permitted:
- Bridge diagnostics, runtime snapshot inventory, HTTP status payloads, EA config notes.

Outputs mandatory:
- Symbols seen, timeframes seen, latest tick time, latest bars time, alias mapping,
  stale context state, safety flags.

Files permitted:
- Runtime snapshot diagnostics, bridge endpoints, inventory scripts, bridge docs.

Files prohibited:
- Broker execution, live order logic, strategy thresholds.

Validations mandatory:
- Runtime snapshot tests touched by the task.
- `node --check app/dashboard/app.js` if dashboard payloads are changed.
- Subagent gate.

Safety contract:
- Context can unlock paper observation only; it cannot open trades by itself.

### Research Factory Agent

Mission:
- Generate new paper-only hypotheses from processed results and safe OHLC scans.

Inputs permitted:
- Processed summaries, small JSON/CSV result files, explicitly named local OHLC files,
  research rejection registry.

Outputs mandatory:
- Families tested, gates passed/failed, top edges, near misses, rejected lessons,
  recommended next research phase.

Files permitted:
- Research services, research scripts, research tests, rejection registry docs.

Files prohibited:
- Runtime trading, broker execution, automatic activation, large data uploads.

Validations mandatory:
- Relevant research tests.
- Research rejection registry tests when exclusions change.
- Subagent gate.

Safety contract:
- A research edge is never a paper-forward candidate without hardening and review.

### Deep Validation Agent

Mission:
- Stress promising research edges with robustness, sample, cost, and dependency checks.

Inputs permitted:
- Explicit candidate profile, validation CSV paths, prior feature scan output.

Outputs mandatory:
- Metrics, rejection reasons, pass/fail gate, robustness notes, next action.

Files permitted:
- Deep validation services/scripts/tests for the assigned candidate.

Files prohibited:
- Runtime activation, broker, promoted profile mutation.

Validations mandatory:
- Candidate-specific test.
- Subagent gate.

Safety contract:
- Failure becomes a lesson or rejection. Passing only means human review.

### Strategy Tournament Agent

Mission:
- Rank paper-only candidates and prevent rejected/degraded profiles from returning.

Inputs permitted:
- Persistent Intelligence summaries, candidate registries, rejection/degradation registries.

Outputs mandatory:
- Ranking, excluded candidates, top candidate, risk reason, activation flag.

Files permitted:
- Tournament service/scripts/tests, candidate registry readers.

Files prohibited:
- Real profile promotion, broker execution, risk relaxation.

Validations mandatory:
- `python -m unittest tests.unit.test_mt5_strategy_tournament`
- Capital Protection tests.
- Subagent gate.

Safety contract:
- Tournament recommends; it does not activate.

### Paper Observation Agent

Mission:
- Prepare and evaluate paper observation readiness for approved paper-only candidates.

Inputs permitted:
- Candidate registry, runtime context, DB health, capital/adaptive/risk status.

Outputs mandatory:
- Readiness state, blockers, dry-run cycle output, safety flags.

Files permitted:
- Paper observation readiness/cycle services, scripts, tests, HTTP read-only endpoints.

Files prohibited:
- Broker execution, real trading, automatic promotion, uncontrolled loops.

Validations mandatory:
- Paper observation readiness tests.
- Persistent Intelligence tests when persistence is touched.
- Subagent gate.

Safety contract:
- Default mode is dry-run. One-shot paper shadow requires explicit endpoint/flag.

### Shadow Lifecycle Agent

Mission:
- Monitor, report, and paper-close existing shadow trades without opening new ones.

Inputs permitted:
- Runtime snapshot, Persistent Intelligence shadow records, risk/DB state.

Outputs mandatory:
- Open shadow count, source, PnL, R multiple, exit signal, exit reason,
  paper close applied flag, safety flags.

Files permitted:
- Shadow monitor services/scripts/tests, shadow persistence fallback helpers.

Files prohibited:
- Shadow creation endpoints unless explicitly requested.
- Broker execution, live order logic.

Validations mandatory:
- `python -m unittest tests.unit.test_mt5_xau_m15_paper_shadow_monitor`
- Paper observation readiness tests.
- Subagent gate.

Safety contract:
- Never open a shadow. Paper-close only with explicit apply and safe context.

### QA / Red Team Agent

Mission:
- Try to break the change before merge and verify forbidden actions are blocked.

Inputs permitted:
- Diff, tests, scripts, docs, API payload samples.

Outputs mandatory:
- Findings, repro steps, missing tests, residual risk, pass/fail verdict.

Files permitted:
- `tests/**`, gates, QA docs.

Files prohibited:
- Weakening safety assertions, hiding failures, changing strategy to pass tests.

Validations mandatory:
- Subagent gate.
- Targeted tests for touched areas.
- Wider unit discovery when blast radius is high.

Safety contract:
- A red test is useful; do not paper over it.

### Dashboard Reporter Agent

Mission:
- Show status, safety, DB, research, and paper observation state without mutating runtime.

Inputs permitted:
- Read-only endpoints, compact Persistent Intelligence summaries, mission control output.

Outputs mandatory:
- UI/report changes, payload size notes, safety visibility, tests.

Files permitted:
- `app/dashboard/**`, presentation-only API adapters, dashboard tests/docs.

Files prohibited:
- Trading decisions, broker execution, write-heavy status polling.

Validations mandatory:
- `node --check app/dashboard/app.js`
- Endpoint tests when API payloads change.
- Subagent gate.

Safety contract:
- Status endpoints must be read-only unless explicitly named as write endpoints.

## Merge Path

1. Agent completes scoped diff.
2. Agent fills handoff template.
3. Safety Sentinel reviews safety output.
4. QA / Red Team attempts to break it.
5. Coordinator decides merge order.
6. Push only after validation is shown.

## Current Phase Focus

- Monitor the current XAUUSD M15 paper shadow.
- Explain `safety_exit` details before any further shadow activity.
- Do not open a new shadow until the current cause and lifecycle are known.
