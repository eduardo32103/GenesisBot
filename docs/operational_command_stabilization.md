## Operational command stabilization note

- Root cause: the original `/check_db` handler only printed database results to logs and never sent a Telegram reply back to the user.
- Active route: in this repo there is no `railway.json`, `railway.toml`, or `nixpacks.toml`; the only declared entrypoint is `Procfile` with `worker: python main.py`.
- Active runtime: the deployed worker entrypoint resolves to `main.py`.
- Fix applied: `/check_db` was converted into a Telegram response flow that probes the database in a worker thread, returns a clean `DB OK` message on success, returns `DB ERROR: <detalle corto>` on failure, and falls back to `send_message` if `reply_to` fails.
- Guardrail learned: validate fixes against the real deploy path and not only against local copies or logs.
