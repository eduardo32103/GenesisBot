# Genesis Agent Task Board

This board is the shared operating surface for Codex agents. The Coordinator
Agent owns ordering. Agents move cards only after reporting safety output and
validation results.

## Urgent

1. DB Doctor: repair Persistent Intelligence schema until `tables_ready=true`.
2. DB Doctor: ensure `writes_frozen` prevents queue growth.
3. Safety Sentinel: verify no broker or `order_send` path.
4. Coordinator: do not allow learning loop until DB green.

## Active

1. Build Agent OS docs and gates.
2. Add DB Doctor endpoint/script.
3. Prepare one-cycle Autonomous Learning after DB green.

## Blocked

1. Autonomous paper loop blocked until `db_degraded=false`.
2. Strategy research blocked until DB stable.

## Review

- Persistent DB Doctor.
- Safety contract.
- Agent gate.

## Done

- RiskGovernor.
- Adaptive Strategy Governor.
- Capital Protection Governor.
- Strategy Tournament.
- Persistent Store base.
- Backpressure base.

## Card Template

- Task:
- Owner agent:
- Branch:
- Scope:
- Files allowed:
- Files forbidden:
- Safety gates:
- Current blocker:
- Latest result:
- Next action:
