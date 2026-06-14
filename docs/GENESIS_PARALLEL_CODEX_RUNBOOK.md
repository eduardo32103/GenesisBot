# Genesis Parallel Codex Runbook

This runbook explains how to run Genesis Codex subagents in parallel without
stepping on safety, DB, or runtime ownership.

## Operating Model

- One task per agent.
- One owner per file at a time.
- Each agent uses a logical branch or returns an isolated diff.
- Coordinator owns branch naming and merge order.
- Safety Sentinel reviews every task before merge.
- QA / Red Team tries to break the change before push.

## Branch Strategy

Suggested branch names:

- `agent/coordinator/<task>`
- `agent/safety/<task>`
- `agent/db-doctor/<task>`
- `agent/runtime-bridge/<task>`
- `agent/research-factory/<task>`
- `agent/deep-validation/<task>`
- `agent/strategy-tournament/<task>`
- `agent/paper-observation/<task>`
- `agent/shadow-lifecycle/<task>`
- `agent/dashboard/<task>`
- `agent/qa/<task>`

Do not branch from a dirty working tree unless the Coordinator explicitly owns
the dirty files. Do not use broad staging.

## Parallel Rules

- Split tasks by file ownership.
- Avoid two agents touching the same service.
- Prefer docs/prompts/tests in separate commits from runtime changes.
- Status endpoints must be read-only unless the route explicitly says write/apply.
- No agent may run a loop by default.
- No agent may apply paper rotation without explicit human approval.

## Safety Sentinel Review

Safety Sentinel checks:

- No real trading.
- No broker action.
- No `order_send`.
- No live trading enablement.
- No martingale.
- No grid escalation.
- No averaging down.
- No increasing lot after loss.
- `broker_touched=false`.
- `order_executed=false`.
- `order_policy=journal_only_no_broker`.

Run:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_genesis_subagent_gate.ps1
```

## Coordinator Merge Order

1. DB/safety foundation.
2. Read-only diagnostics.
3. Tests.
4. Runtime read paths.
5. Paper-only apply paths.
6. Dashboard/reporting.
7. Research.

Never merge research activation before DB and safety are green.

## QA / Red Team Checklist

- Try stale runtime context.
- Try missing DB.
- Try duplicated shadow records.
- Try no open shadow.
- Try a blocked governor.
- Check that dry-run remains dry-run.
- Check that status endpoints do not write.
- Check that all reports include safety flags.

## When To Stop

Stop when:

- DB is degraded and the next step would write or learn.
- Capital Protection blocks.
- Adaptive Governor blocks.
- RiskGovernor blocks.
- Queue depth rises unexpectedly.
- A task asks for broker action.
- A change would hide safety flags.

## Reporting

Every agent report must include:

- Files touched.
- Decision.
- Metrics.
- Risks.
- Tests.
- Git status.
- Safety flags.
- Next action.
