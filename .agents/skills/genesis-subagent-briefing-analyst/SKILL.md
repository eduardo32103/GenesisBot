---
name: genesis-subagent-briefing-analyst
description: "Use when Codex needs to read GENESIS task boards, handoffs, subagent reports, research outputs, git state, and recent validation results, then produce an exact Spanish briefing for Eduardo and a short handoff message for ChatGPT before choosing the next step."
---

# Jefe de gabinete

## Mission

Produce a read-only GENESIS briefing from repository evidence and recent agent work. Use this skill before deciding the next engineering, DB, runtime, safety, paper lifecycle, or research action when multiple GENESIS skills/subagents have contributed.

## Safety Contract

- Work only on GENESIS / GenesisBot.
- Default to read-only.
- Do not touch broker, call `order_send`, open shadows, close shadows, activate candidates, start paper-forward onboarding, or push.
- Do not call POST endpoints unless the user explicitly asks for that exact action.
- Do not modify trading logic.
- If evidence is missing or uncertain, write `no encontrado` or `no verificable`; do not invent.
- If subagent reports contradict each other, list the contradiction and cite both sides.
- If a required subagent report is missing, state which report is missing and ask that thread to write a handoff.

## Evidence To Inspect

Prefer targeted reads and summaries. Avoid opening large CSV files or large raw JSON dumps unless the user explicitly asks.

Read or check, when present:

- `docs/GENESIS_AGENT_TASK_BOARD.md`
- `docs/GENESIS_SUBAGENT_OPERATING_SYSTEM.md`
- `docs/GENESIS_AGENT_HANDOFF_TEMPLATE.md`
- `docs/agent_tasks/*.md`
- `docs/agent_reports/*.md`
- `data/research_outputs/*.json` using targeted filenames and compact summaries
- `git status -sb`
- `git diff --name-status`
- `git diff --check`
- `git log -8 --oneline`
- Recent test results from files, terminal output, or the conversation prompt

For optional live checks, use GET-only endpoints or existing scripts in dry-run/read-only mode. Never use POST by default.

## Workflow

1. Establish repo state:
   - Run `git status -sb`, `git diff --name-status`, `git diff --check`, and `git log -8 --oneline`.
   - Separate user/prior dirty work from newly observed facts.

2. Gather agent evidence:
   - Read task board and operating system docs.
   - Read agent task files and agent report files if present.
   - Check recent research output JSON snapshots for DB, runtime, open shadows, history, readiness, supervisor, and winrate evidence.
   - Use `no encontrado` for missing paths.

3. Reconstruct subagent work:
   - Group findings by skill/subagent name.
   - Record claimed work, touched files, outputs, tests, blockers, and safety flags.
   - Mark anything based only on conversation memory as `no verificable en archivos` unless the prompt provides exact output.

4. Build the briefing:
   - Follow `references/BRIEFING_SCHEMA.md`.
   - Apply `references/HANDOFF_REQUIREMENTS.md` when reports are missing or incomplete.

5. Close with a ready-to-paste message:
   - Write a short Spanish `Mensaje listo para ChatGPT` with current state, blocker, next safe command/action, and safety constraints.

## Required Output

Always produce:

1. Resumen ejecutivo en espanol.
2. Que hizo cada skill/subagente.
3. Archivos tocados por cada uno.
4. Outputs verificables.
5. Bloqueadores activos.
6. Riesgos de integracion.
7. Estado actual de DB, MT5 runtime, open shadows, history y supervisor.
8. Safety final:
   - `broker_touched`
   - `order_executed`
   - `order_policy`
   - `candidate_activated`
   - `paper_forward_onboarding_started`
9. `Mensaje listo para ChatGPT`, corto y exacto.

## Reference Files

- Read `references/BRIEFING_SCHEMA.md` before writing the final briefing.
- Read `references/HANDOFF_REQUIREMENTS.md` when checking whether subagent reports are complete.
