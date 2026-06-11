# Genesis Agent Runbook

Use this runbook before changing code, before handing work to another agent, and
before opening a PR.

## 1. Start A Branch

```powershell
git status -sb
git switch -c agent/<role>/<short-task>
```

Use one of these role prefixes:

- `agent/db/`
- `agent/db-doctor/`
- `agent/safety/`
- `agent/learning/`
- `agent/research/`
- `agent/qa/`
- `agent/dashboard/`
- `agent/docs/`

Do not create a branch if the working tree has unrelated user changes that would
be mixed into the task. Ask the Coordinator / Architect Agent to split the work.

## 2. Before Editing

- Read only the files in scope.
- Do not scan large `data/backtests` files unless the task explicitly names them.
- Do not open big CSV/JSON files for routine validation.
- Confirm the task does not require broker, live trading, or order execution.
- If DB is required, check Persistent Intelligence status first.

## 3. Run Tests

Fast safety gate:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_genesis_agent_gate.ps1
```

MT5 core tests:

```powershell
python -m unittest tests.unit.test_mt5_persistent_intelligence_store
python -m unittest tests.unit.test_mt5_persistent_db_doctor
python -m unittest tests.unit.test_mt5_autonomous_learning_orchestrator
python -m unittest tests.unit.test_mt5_capital_protection_governor
python -m unittest tests.unit.test_mt5_strategy_tournament
python -m unittest tests.unit.test_mt5_adaptive_strategy_governor
python -m unittest tests.unit.test_mt5_adaptive_strategy_governor_enforcement
python -m unittest tests.unit.test_mt5_shadow_trade_hygiene
python -m unittest tests.unit.test_mt5_risk_recovery
node --check app/dashboard/app.js
git diff --check
```

Run extra targeted tests for the files touched.

## 4. Validate Railway Persistent Intelligence

Status endpoint:

```powershell
Invoke-RestMethod -Uri "https://genesisbot-production.up.railway.app/api/genesis/mt5/persistent-intelligence/status" -TimeoutSec 30
```

Recent events endpoint:

```powershell
Invoke-RestMethod -Uri "https://genesisbot-production.up.railway.app/api/genesis/mt5/persistent-intelligence/recent-events?limit=20" -TimeoutSec 30
```

Expected green state:

- `provider=railway_postgres`
- `db_available=true`
- `db_degraded=false`
- `tables_ready=true`
- `missing_tables=[]`
- `recommendation=persistent_intelligence_ready`
- `broker_touched=false`
- `order_executed=false`
- `order_policy=journal_only_no_broker`

If Railway CLI is authenticated or `DATABASE_URL` is available, schema repair can
be run with:

```powershell
python scripts/run_persistent_db_doctor.py --repair --wait-for-connection --max-connect-attempts 10
```

Never print `DATABASE_URL` or token values.

DB Doctor dry run:

```powershell
python scripts/run_persistent_db_doctor.py
```

DB Doctor direct schema apply:

```powershell
python scripts/run_persistent_db_doctor.py --apply-schema --wait-for-connection --max-connect-attempts 10
```

## 5. Validate MT5 Safety

For any MT5 change, verify the final output includes:

- `decision=NO_TRADE` when DB/risk/safety is uncertain.
- `broker_touched=false`
- `order_executed=false`
- `order_policy=journal_only_no_broker`
- no `order_send`
- no real trading
- no automatic promotion

Do not continue to learning loops if Persistent Intelligence is degraded.

## 6. Stop Local Learning Loops

Inspect Python processes:

```powershell
Get-CimInstance Win32_Process -Filter "name='python.exe'" | Select-Object ProcessId,CommandLine | Format-List
```

Stop only processes that are clearly Genesis paper learning loops, such as:

- `run_autonomous_learning_orchestrator.py --loop`
- `mt5_learning_loop`

Example:

```powershell
Stop-Process -Id <PID> -Force
```

Do not stop unrelated Python processes.

## 7. When Not To Advance

Stop and report when:

- Persistent Intelligence is not green and the next step is learning.
- Schema is missing or DB backpressure is active.
- Railway CLI is not authenticated and DB repair requires Railway env.
- A change would touch broker or execution logic.
- A strategy/risk threshold change is needed but not explicitly authorized.
- Tests fail outside your scope.

## 8. Report Changes

Use this format:

- Objective:
- Branch:
- Files touched:
- Summary:
- Safety output:
- Tests/checks:
- Railway status, if relevant:
- What was not run:
- Blockers:
- Next owner:

## 9. Commit And PR

```powershell
git status -sb
git add <exact-file-1> <exact-file-2>
git commit -m "<short imperative summary>"
git push origin <branch>
```

Use the PR template. Include a rollback plan and safety output.
