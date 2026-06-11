# Genesis Autonomous Workflow

Genesis uses specialized Codex agents. The goal is faster work without unsafe
overlap or trading risk.

## Workflow Rules

1. Each agent works in a separate branch.
2. Each agent has limited scope.
3. No agent pushes directly to `main` unless the user explicitly authorizes it.
4. Every change requires the agent gate.
5. QA / Red Team Agent reviews risky changes.
6. Safety Sentinel Agent reviews MT5, risk, learning, broker-adjacent, and DB
   critical changes.
7. Only after review may the Coordinator merge/deploy.
8. No learning loop runs until DB is green.
9. Research Agent may run offline scans without touching runtime.
10. Strategy Tournament only recommends paper rotation.

## Branch Naming

- `agent/coordinator/<task>`
- `agent/db-doctor/<task>`
- `agent/safety/<task>`
- `agent/learning/<task>`
- `agent/tournament/<task>`
- `agent/research/<task>`
- `agent/qa/<task>`
- `agent/dashboard/<task>`

## Required Gate

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_genesis_agent_gate.ps1
```

The gate must pass before PR or push.

## Current Blocker

Persistent Intelligence must be green before learning:

- `db_available=true`
- `db_degraded=false`
- `tables_ready=true`
- `missing_tables=[]`
- `recommendation=persistent_intelligence_ready`

## After DB Green

Prepare, but do not execute without explicit approval:

```powershell
python scripts/run_autonomous_learning_orchestrator.py
python scripts/run_strategy_tournament.py
python scripts/run_capital_protection_governor.py
```

Only after the single approved cycle is healthy may the team prepare a
paper-only controlled loop. Real trading remains forbidden.
