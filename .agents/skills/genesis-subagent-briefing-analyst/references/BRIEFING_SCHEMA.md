# Briefing Schema

Use this schema for GENESIS subagent briefings. Keep the briefing compact, factual, and in Spanish.

## Required Sections

1. `Resumen ejecutivo`
   - State the current GENESIS phase, main blocker, and safest next action.
   - Mention whether the evidence is live, file-based, or prompt-based.

2. `Trabajo por skill/subagente`
   - For each skill/subagent, list:
     - `nombre`
     - `rol`
     - `que hizo`
     - `archivos tocados`
     - `outputs verificables`
     - `tests/validaciones`
     - `estado`

3. `Archivos tocados`
   - Use `git diff --name-status`.
   - If ownership by subagent is unknown, write `no verificable`.

4. `Outputs verificables`
   - Include exact values only when found in files, command output, endpoint output, or prompt text.
   - Use `no encontrado` for missing values.

5. `Bloqueadores activos`
   - Separate blockers into DB, MT5 runtime, RiskGovernor, lifecycle, tests, git, and deployment.

6. `Riesgos de integracion`
   - Include dirty worktree risk, stale deployment risk, missing reports, failing tests, schema gaps, duplicate/orphan shadow risk, and safety flag risk.

7. `Estado operativo`
   - Report:
     - DB availability, degraded state, tables readiness, queue depth
     - MT5 runtime availability, freshness, bars count, latest tick/bars
     - open shadow counts: runtime, persistent, merged
     - history closed count and sample quality
     - supervisor state, stop reason, next action

8. `Safety final`
   - Always include:
     - `broker_touched`
     - `order_executed`
     - `order_policy`
     - `candidate_activated`
     - `paper_forward_onboarding_started`

9. `Mensaje listo para ChatGPT`
   - Keep it short.
   - Include current state, blocker, next safe step, and non-negotiable safety constraints.

## Evidence Labels

Use these labels when precision matters:

- `verificado`: observed in a command, file, endpoint, or prompt output.
- `no encontrado`: path/value/report is absent.
- `no verificable`: claim exists but no direct evidence is available.
- `contradiccion`: two evidence sources disagree.
- `stale`: evidence is likely outdated compared with current runtime.

## Style Rules

- Write in Spanish.
- Use exact field names for safety and runtime values.
- Do not overstate confidence.
- Do not invent test results.
- Prefer short tables or flat bullets.
