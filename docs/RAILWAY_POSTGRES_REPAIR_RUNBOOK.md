# Railway Postgres Repair Runbook

Use this runbook when Genesis Persistent Intelligence reports:

- `provider=railway_postgres`
- `db_degraded=true`
- `tables_ready=false`
- `missing_tables` is not empty
- `last_db_error_category=max_connections` or `missing_schema`

## Safety Rules

- No real trading.
- No broker.
- No `order_send`.
- No loops.
- No Autonomous Paper Learning until DB is green.
- No `DROP`, `TRUNCATE`, or `DELETE`.
- Do not print `DATABASE_URL`, `DATABASE_PUBLIC_URL`, passwords, or tokens.
- Required output: `broker_touched=false`, `order_executed=false`,
  `order_policy=journal_only_no_broker`.

## Do Not Paste SQL Into Bash

The schema SQL must be executed in a Postgres query console, not in the
GenesisBot bash console.

Good options:

- Railway Postgres Query Console.
- A Postgres client connected to the Railway database.
- The Python apply script from a Railway shell where DB env vars exist.

Bad option:

- Pasting `CREATE TABLE ...` directly into a bash prompt.

## Fast Diagnosis

From Railway shell or any environment with DB env vars:

```powershell
python scripts/run_persistent_db_connection_diagnostics.py
```

If the internal URL is saturated, try:

```powershell
python scripts/run_persistent_db_connection_diagnostics.py --prefer-public-url
```

The script prints only env presence booleans, never URL or password values.

## Apply Schema With Python

Default private/internal URL:

```powershell
python scripts/run_persistent_intelligence_apply_schema.py --apply --no-rls --wait-for-connection --max-connect-attempts 10 --connect-backoff-seconds 5 --statement-timeout-ms 30000
```

Prefer public URL if `DATABASE_PUBLIC_URL` exists:

```powershell
python scripts/run_persistent_intelligence_apply_schema.py --apply --no-rls --prefer-public-url --wait-for-connection --max-connect-attempts 10 --connect-backoff-seconds 5 --statement-timeout-ms 30000
```

Use only public URL:

```powershell
python scripts/run_persistent_intelligence_apply_schema.py --apply --no-rls --use-public-url --wait-for-connection --max-connect-attempts 10 --connect-backoff-seconds 5 --statement-timeout-ms 30000
```

## Apply Schema With Query Console

Generate the Railway SQL file:

```powershell
python scripts/emit_persistent_intelligence_schema_sql.py --railway-file
```

Open `persistent_intelligence_schema_railway.sql`, paste it into the Railway
Postgres Query Console, and run it there.

## If Max Connections Persists

1. Stop any local or Railway learning loops.
2. Keep GenesisBot deployed with schema-missing freeze enabled.
3. Restart the Postgres service.
4. Restart GenesisBot.
5. Wait 60 seconds.
6. Run connection diagnostics.
7. Apply schema with `--prefer-public-url`.

## Final Validation

Status endpoint:

```powershell
Invoke-RestMethod -Uri "https://genesisbot-production.up.railway.app/api/genesis/mt5/persistent-intelligence/status" -TimeoutSec 30
```

Expected:

- `db_available=true`
- `db_degraded=false`
- `tables_ready=true`
- `missing_tables=[]`
- `schema_missing_write_freeze=false`
- `writes_frozen=false`
- `recommendation=persistent_intelligence_ready`
- `broker_touched=false`
- `order_executed=false`
- `order_policy=journal_only_no_broker`

Recent events endpoint:

```powershell
Invoke-RestMethod -Uri "https://genesisbot-production.up.railway.app/api/genesis/mt5/persistent-intelligence/recent-events?limit=20" -TimeoutSec 30
```

Learning stays blocked until the DB is green. After DB is green, run only one
manual autonomous cycle if explicitly approved. Do not run loop mode.
