# Handoff Requirements

Use this checklist to decide whether a GENESIS subagent report is complete enough to brief Eduardo or ChatGPT.

## Required Per-Subagent Fields

Each subagent report should include:

- `agent_or_skill_name`
- `role`
- `scope`
- `files_read`
- `files_changed`
- `commands_run`
- `outputs_verified`
- `tests_run`
- `blockers_found`
- `risks_found`
- `recommended_next_action`
- `safety_flags`

## Required Safety Flags

Every report must state:

- `broker_touched=false`
- `order_executed=false`
- `order_policy=journal_only_no_broker`
- `candidate_activated=false`
- `paper_forward_onboarding_started=false`

If any flag is missing, mark it `no encontrado`.
If any flag is true or unknown in a trading-sensitive path, mark it as a blocker.

## Missing Reports

If a report is missing for an expected role, write:

`Falta handoff de <agent_or_skill_name>; pedir a ese hilo que escriba reporte con files_read, files_changed, commands_run, outputs_verified, blockers, risks y safety_flags.`

Expected GENESIS roles commonly include:

- Safety Sentinel
- DB Doctor
- Runtime Watchdog
- Paper Lifecycle Manager
- Winrate Analyst
- Autopilot Orchestrator
- QA / Red Team
- Research Agent

## Contradictions

When reports disagree:

1. Quote the conflicting field names and values.
2. Prefer the newest live command output over older snapshots.
3. Prefer command/file evidence over memory.
4. Do not resolve by guessing.
5. State the next command needed to settle the contradiction.

## Read-Only Boundaries

This skill must not:

- Call POST endpoints by default.
- Open or close paper shadows.
- Touch broker or execution logic.
- Modify trading strategy logic.
- Push commits.

If the user asks for an action outside read-only briefing, state that it requires explicit confirmation and a different operational skill.
