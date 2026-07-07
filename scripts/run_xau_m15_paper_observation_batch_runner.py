from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_xau_m15_paper_observation_batch_runner import (  # noqa: E402
    DEFAULT_RESULTS_FILE,
    DEFAULT_STATE_FILE,
    run_xau_m15_paper_observation_batch_runner,
)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    dry_run = bool(args.dry_run) if args.dry_run is not None else not bool(args.paper_only_confirmed)
    asset_configs = _load_asset_configs(args)
    result = run_xau_m15_paper_observation_batch_runner(
        base_url=args.base_url,
        symbol=args.symbol,
        broker_symbol=args.broker_symbol,
        timeframe=args.timeframe,
        target_trades=args.target_trades,
        max_cycles=args.max_cycles,
        interval_seconds=args.interval_seconds,
        max_runtime_minutes=args.max_runtime_minutes,
        dry_run=dry_run,
        paper_only_confirmed=args.paper_only_confirmed,
        once=args.once,
        exit_policy=args.exit_policy,
        time_stop_bars=args.time_stop_bars,
        max_hold_minutes=args.max_hold_minutes,
        min_r_to_arm_trailing=args.min_r_to_arm_trailing,
        giveback_r=args.giveback_r,
        fast_loss_cut_r=args.fast_loss_cut_r,
        state_file=args.state_file,
        results_file=args.results_file,
        timeout_seconds=args.timeout_seconds,
        allowed_symbols=args.allow_symbol,
        asset_configs=asset_configs,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True, default=str))
    else:
        print(_human_summary(result))
    return 0


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a controlled XAUUSD M15 paper-only batch step/runner.")
    parser.add_argument("--base-url", default="", help="Optional live Genesis base URL. Uses live HTTP process endpoints.")
    parser.add_argument("--symbol", default="XAUUSD")
    parser.add_argument("--broker-symbol", default="XAUUSD.b")
    parser.add_argument("--timeframe", default="M15")
    parser.add_argument("--allow-symbol", action="append", default=None, help="Explicit paper-test symbol allowlist shorthand. Repeatable. No default; omitted allowlist fails closed.")
    parser.add_argument("--asset-config-json", default="", help="Explicit JSON object/list with enabled paper-only asset config(s).")
    parser.add_argument("--asset-config-file", default="", help="Path to explicit JSON object/list with enabled paper-only asset config(s).")
    parser.add_argument("--target-trades", type=int, default=20)
    parser.add_argument("--max-cycles", type=int, default=200)
    parser.add_argument("--interval-seconds", type=float, default=60.0)
    parser.add_argument("--max-runtime-minutes", type=float, default=None)
    parser.add_argument("--dry-run", action="store_true", default=None, help="Force dry-run mode. Default unless --paper-only-confirmed is present.")
    parser.add_argument("--paper-only-confirmed", action="store_true", help="Required to open or close paper shadows.")
    parser.add_argument("--once", action="store_true", help="Run exactly one step.")
    parser.add_argument("--exit-policy", choices=["default", "fast_observation"], default="default")
    parser.add_argument("--time-stop-bars", type=int, default=1)
    parser.add_argument("--max-hold-minutes", type=float, default=None)
    parser.add_argument("--min-r-to-arm-trailing", type=float, default=0.15)
    parser.add_argument("--giveback-r", type=float, default=0.10)
    parser.add_argument("--fast-loss-cut-r", type=float, default=-0.25)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_FILE))
    parser.add_argument("--results-file", default=str(DEFAULT_RESULTS_FILE))
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    return parser.parse_args(argv)


def _load_asset_configs(args: argparse.Namespace) -> list[dict[str, Any]] | None:
    payloads: list[dict[str, Any]] = []
    if args.asset_config_file:
        path = Path(args.asset_config_file)
        decoded = json.loads(path.read_text(encoding="utf-8"))
        payloads.extend(_as_config_list(decoded))
    if args.asset_config_json:
        payloads.extend(_as_config_list(json.loads(args.asset_config_json)))
    return payloads if payloads else None


def _as_config_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        return [dict(payload)]
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, dict)]
    raise ValueError("asset config must be a JSON object or list of objects")


def _human_summary(result: dict[str, Any]) -> str:
    last = result.get("last_cycle") if isinstance(result.get("last_cycle"), dict) else {}
    stats = result.get("batch_stats") if isinstance(result.get("batch_stats"), dict) else {}
    db = last.get("db_state") if isinstance(last.get("db_state"), dict) else {}
    return "\n".join(
        [
            "MT5 XAUUSD M15 Paper Observation Batch Runner",
            f"symbol={result.get('symbol')}",
            f"broker_symbol={result.get('broker_symbol')}",
            f"timeframe={result.get('timeframe')}",
            f"status={result.get('status')}",
            f"mode={result.get('mode')}",
            f"exit_policy={result.get('exit_policy')}",
            f"client_source={result.get('client_source')}",
            f"runner_state={result.get('runner_state')}",
            f"cycle_number={last.get('cycle_number')}",
            f"cycles_requested={result.get('cycles_requested')}",
            f"cycles_completed={result.get('cycles_completed')}",
            f"target_trades={result.get('target_trades')}",
            f"session_id={result.get('session_id')}",
            f"target_scope={result.get('target_scope')}",
            f"session_trades_opened={result.get('session_trades_opened')}",
            f"session_trades_closed={result.get('session_trades_closed')}",
            f"valid_trades_closed={stats.get('valid_trades_closed')}",
            f"invalid_samples={stats.get('invalid_samples')}",
            f"historical_closed_count={result.get('historical_closed_count')}",
            f"stop_reason={result.get('stop_reason')}",
            f"db_state={_compact_json(db)}",
            f"readiness_state={last.get('readiness_state')}",
            f"open_shadow_count={last.get('open_shadow_count')}",
            f"current_shadow_id={last.get('current_shadow_id')}",
            f"monitor_state={last.get('monitor_state')}",
            f"exit_signal={last.get('exit_signal')}",
            f"exit_reason={last.get('exit_reason')}",
            f"should_close_paper={last.get('should_close_paper')}",
            f"should_watch_only={last.get('should_watch_only')}",
            f"paper_shadow_created={last.get('paper_shadow_created')}",
            f"paper_close_applied={last.get('paper_close_applied')}",
            f"next_action={last.get('next_action')}",
            f"batch_stats={_compact_json(stats)}",
            f"candidate_activated={result.get('candidate_activated')}",
            f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
            f"broker_touched={result.get('broker_touched')}",
            f"order_executed={result.get('order_executed')}",
            f"order_policy={result.get('order_policy')}",
        ]
    )


def _compact_json(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":"), default=str)


if __name__ == "__main__":
    raise SystemExit(main())
