# Genesis Safety Contract

## Absolute Contract

- Survival first.
- Capital protection over profit.
- No real trading until explicit human approval.
- No broker action in current phase.
- No `order_send`.
- No martingale.
- No grid.
- No averaging down.
- No increasing lot after loss.
- Every automated decision must be explainable.
- Every profile can be paused/degraded.
- Every research failure becomes a lesson.
- If data is missing, stale, degraded, or contradictory: `NO_TRADE`.
- If DB critical state is degraded: `NO_TRADE`.
- If Capital Protection Governor blocks: `NO_TRADE`.
- If Adaptive Strategy Governor blocks: `NO_TRADE`.
- If RiskGovernor blocks: `NO_TRADE`.

## Current Operating Mode

- `broker_touched=false`
- `order_executed=false`
- `order_policy=journal_only_no_broker`
- Paper-only research and diagnostics are allowed.
- Real trading, broker execution, and automatic real promotion are not allowed.

## Persistence Safety

- Persistent Intelligence must degrade safely when DB is missing, overloaded, or
  missing schema.
- Missing schema freezes normal writes.
- Critical persistence failure blocks unsafe action with `NO_TRADE`.
- No OHLC bulk storage.
- No raw tick storage.
- No large CSV or JSON payloads.
- No secrets printed.
- No destructive schema operations.

## Human Approval Required

Explicit human approval is required for:

- Any real trading.
- Any broker order.
- Any profile promotion that could affect real trading.
- Any loop mode.
- Any paper rotation application.
- Any destructive DB action.

## Required Safety Output

Every agent report and runtime safety payload must include:

- `broker_touched=false`
- `order_executed=false`
- `order_policy=journal_only_no_broker`
