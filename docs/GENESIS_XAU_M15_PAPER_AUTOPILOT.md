# Genesis XAUUSD M15 Paper Autopilot

This phase runs XAUUSD M15 as a controlled paper-only batch process:

1. Check Persistent Intelligence, open shadows, readiness, and monitor state.
2. Keep at most one XAUUSD M15 paper shadow open.
3. Monitor the open shadow.
4. Close paper-only only when the monitor returns `should_close_paper=true` with an allowed paper exit reason.
5. Save compact local batch state/results.
6. Re-check gates before the next paper shadow.

No real trading is allowed in this mode.

## Reconciliation First

Before opening a new paper shadow, the runner checks the local state file. If it has
`current_open_shadow_id` or `pending_reconciliation_shadow_id`, or if
`trades_opened > trades_closed`, the runner reconciles first:

- If live `open_count=1` and the ID matches, it monitors that shadow.
- If live `open_count>1`, it stops with `stopped_by_duplicate_shadow`.
- If live `open_count=0`, it queries the read-only history endpoint:
  `/api/genesis/mt5/shadow-trades/history?symbol=XAUUSD&timeframe=M15&limit=20`.
- If the closed ID is found, it imports the compact closed trade and updates stats.
- If no closed record is found, it stops with
  `stopped_by_orphaned_shadow_missing_close_record` and does not invent PnL.

## Safety Contract

- `broker_touched=false`
- `order_executed=false`
- `order_policy=journal_only_no_broker`
- `candidate_activated=false`
- `paper_forward_onboarding_started=false`
- `applies_to_real_trading=false`
- No real trading.
- No broker execution.
- No market orders.
- No promotion.
- No martingale, grid, averaging down, or lot increase after loss.
- No loop without `--max-cycles`.
- No new shadow if runtime or Persistent Intelligence already reports one open.
- No close when the monitor says `entry_block_only`, `caution_watch`, or `should_watch_only=true`.

## Commands

Dry-run status, no open, no close:

```powershell
python scripts/run_xau_m15_paper_observation_batch_runner.py --dry-run --target-trades 3 --max-cycles 5
```

One local dry-run step:

```powershell
python scripts/run_xau_m15_paper_observation_batch_runner.py --once --dry-run
```

One live HTTP paper-only step against the Railway web process:

```powershell
python scripts/run_xau_m15_paper_observation_batch_runner.py --once --paper-only-confirmed --base-url https://genesisbot-production.up.railway.app
```

Controlled worker or one-off batch, never inside dashboard polling:

```powershell
python scripts/run_xau_m15_paper_observation_batch_runner.py --base-url https://genesisbot-production.up.railway.app --target-trades 20 --max-cycles 200 --interval-seconds 60 --paper-only-confirmed
```

Fast paper observation, one step only:

```powershell
python scripts/run_xau_m15_paper_observation_batch_runner.py --base-url https://genesisbot-production.up.railway.app --once --paper-only-confirmed --exit-policy fast_observation --time-stop-bars 2 --target-trades 3 --max-cycles 5
```

## Gates Before Opening

- Persistent Intelligence healthy.
- `queue_depth=0`.
- `queued_writes=0`.
- `open_shadow_count=0`.
- XAUUSD M15 candidate is `paper_observation_review`.
- Runtime context is available and recent.
- `bars_count>=100`.
- Latest tick is available.
- Capital, Adaptive, and Risk governors allow paper observation.
- All payloads preserve broker/order safety flags.

## Close Rules

The runner only applies a paper close when all are true:

- `--paper-only-confirmed` is present.
- A current XAUUSD M15 shadow is open.
- The monitor says `should_close_paper=true`.
- Exit reason is one of:
  - `take_profit_hit`
  - `stop_loss_hit`
  - `trailing_defensive_exit`
  - `critical_safety_exit`
  - `safety_exit`
  - `paper_timebox_exit`
  - `paper_stagnation_exit`
  - `paper_fast_trailing_exit`

The runner does not close for entry-block-only conditions. `max_open_trades_reached` blocks new entries, but does not close the current single monitored shadow.

## Fast Observation Policy

`--exit-policy fast_observation` is paper-only and requires
`--paper-only-confirmed`.

- `paper_timebox_exit`: `bars_since_entry >= --time-stop-bars` and `r_multiple <= 0.05`.
- `paper_stagnation_exit`: optional `--max-hold-minutes` is reached and absolute R is near flat.
- `paper_fast_trailing_exit`: R reached `--min-r-to-arm-trailing` and gives back `--giveback-r`.

It never applies to real trading, never touches broker, and never closes
`entry_block_only` or `caution_watch` unless the explicit fast policy condition is met.

## Restart Safety

The runner reads:

- local state file, default `data/research_outputs/xau_m15_paper_batch_state.json`
- local results file, default `data/research_outputs/xau_m15_paper_batch_results.json`
- live open-shadow endpoint
- monitor fallback, including Persistent Intelligence fallback handled by the monitor

If state says a shadow is open but runtime and DB fallback do not find it, the runner marks `orphaned_or_runtime_lost`, does not invent PnL, and does not create a duplicate in that step.

## Subagent Responsibilities

- Coordinator Agent: keeps this phase scoped to XAUUSD M15 paper observation.
- Safety Sentinel: runs gates and forbidden activation scans.
- Runtime Bridge Agent: validates ticks, bars, readiness, and stale runtime.
- Shadow Lifecycle Agent: handles open, monitor, and paper-only close.
- Persistent Intelligence Agent: blocks on queue pressure and avoids status writes.
- Results Analyst Agent: computes batch metrics.
- QA / Red Team Agent: covers degraded DB, duplicate shadows, stale runtime, watch-only, and broker/order flag failures.
