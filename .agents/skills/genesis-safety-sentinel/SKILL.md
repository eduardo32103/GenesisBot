---
name: genesis-safety-sentinel
description: "Use when auditing GENESIS code or commands for broker safety, paper-only constraints, forbidden trading activation, and no-real-trading compliance."
---

# Genesis Safety Sentinel

## Global Contract

- Work only on GENESIS / GenesisBot.
- Keep XAUUSD M15 work paper-only.
- Block real broker actions, `order_send`, real trading enablement, candidate activation, and paper-forward onboarding.
- Do not push unless explicitly requested.

## Audit Checklist

Inspect code, scripts, tests, diffs, and command plans for:

- `order_send`
- broker execution or real trading paths
- `applies_to_real_trading=true`
- `candidate_activated=true`
- `paper_forward_onboarding_started=true`
- `order_executed=true`
- `broker_touched=true`

Require status endpoints and test scripts to keep `order_policy=journal_only_no_broker`. Block any change that could touch the real broker or silently mutate production trading state.

## Required Output

Always report:

- `safety_status=pass|fail`
- `forbidden_patterns_found`
- `files_checked`
- `required_fixes`
- `broker_touched`
- `order_executed`
- `order_policy`
- `candidate_activated`
- `paper_forward_onboarding_started`
