from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_xau_m15_paper_observation_batch_runner import DEFAULT_RESULTS_FILE, DEFAULT_STATE_FILE  # noqa: E402
from services.mt5.mt5_xau_m15_paper_test_supervisor import (  # noqa: E402
    repair_orphan_state,
    run_xau_m15_paper_test_supervisor,
)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.repair_orphan_state:
        result = repair_orphan_state(
            state_file=args.state_file,
            results_file=args.results_file,
            confirm_paper_only_repair=args.confirm_paper_only_repair,
        )
    else:
        dry_run = bool(args.dry_run) if args.dry_run is not None else not bool(args.paper_only_confirmed)
        asset_configs = _load_asset_configs(args)
        result = run_xau_m15_paper_test_supervisor(
            base_url=args.base_url,
            symbol=args.symbol,
            broker_symbol=args.broker_symbol,
            timeframe=args.timeframe,
            allowed_symbols=args.allow_symbol,
            asset_configs=asset_configs,
            target_trades=args.target_trades,
            max_cycles=args.max_cycles,
            interval_seconds=args.interval_seconds,
            dry_run=dry_run,
            paper_only_confirmed=args.paper_only_confirmed,
            once=args.once,
            exit_policy=args.exit_policy,
            time_stop_bars=args.time_stop_bars,
            max_hold_minutes=args.max_hold_minutes,
            min_r_to_arm_trailing=args.min_r_to_arm_trailing,
            giveback_r=args.giveback_r,
            fast_loss_cut_r=args.fast_loss_cut_r,
            strict_paper_probe=args.strict_paper_probe,
            explain_gates=args.explain_gates,
            wait_for_signal=args.wait_for_signal,
            max_wait_minutes=args.max_wait_minutes,
            preflight_only=args.preflight_only,
            state_file=args.state_file,
            results_file=args.results_file,
            timeout_seconds=args.timeout_seconds,
        )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True, default=str))
    else:
        print(_human_summary(result))
    return 0


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the XAUUSD M15 paper-only test supervisor. No broker, no real trading.")
    parser.add_argument("--base-url", default="")
    parser.add_argument("--symbol", default="XAUUSD")
    parser.add_argument("--broker-symbol", default="XAUUSD.b")
    parser.add_argument("--timeframe", default="M15")
    parser.add_argument("--allow-symbol", action="append", default=None, help="Explicit paper-test symbol allowlist shorthand. Repeatable. No default; omitted allowlist fails closed.")
    parser.add_argument("--asset-config-json", default="", help="Explicit JSON object/list with enabled paper-only asset config(s).")
    parser.add_argument("--asset-config-file", default="", help="Path to explicit JSON object/list with enabled paper-only asset config(s).")
    parser.add_argument("--target-trades", type=int, default=3)
    parser.add_argument("--max-cycles", type=int, default=120)
    parser.add_argument("--interval-seconds", type=float, default=30.0)
    parser.add_argument("--dry-run", action="store_true", default=None)
    parser.add_argument("--paper-only-confirmed", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--exit-policy", choices=["default", "fast_observation"], default="fast_observation")
    parser.add_argument("--time-stop-bars", type=int, default=1)
    parser.add_argument("--max-hold-minutes", type=float, default=None)
    parser.add_argument("--min-r-to-arm-trailing", type=float, default=0.15)
    parser.add_argument("--giveback-r", type=float, default=0.10)
    parser.add_argument("--fast-loss-cut-r", type=float, default=-0.25)
    parser.add_argument("--strict-paper-probe", action="store_true")
    parser.add_argument("--explain-gates", action="store_true")
    parser.add_argument("--wait-for-signal", action="store_true")
    parser.add_argument("--max-wait-minutes", type=float, default=None)
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_FILE))
    parser.add_argument("--results-file", default=str(DEFAULT_RESULTS_FILE))
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--repair-orphan-state", action="store_true")
    parser.add_argument("--confirm-paper-only-repair", action="store_true")
    parser.add_argument("--json", action="store_true")
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
    if result.get("preflight_only"):
        return "\n".join(
            [
                "MT5 XAUUSD M15 Paper Test Supervisor Preflight Only",
                f"symbol={result.get('symbol')}",
                f"broker_symbol={result.get('broker_symbol')}",
                f"timeframe={result.get('timeframe')}",
                f"status={result.get('status')}",
                f"decision={result.get('decision')}",
                f"blockers={result.get('blockers')}",
                f"next_safe_action={result.get('next_safe_action')}",
                f"db_available={result.get('db_available')}",
                f"db_degraded={result.get('db_degraded')}",
                f"tables_ready={result.get('tables_ready')}",
                f"queue_depth={result.get('queue_depth')}",
                f"readiness_state={result.get('readiness_state')}",
                f"runtime_context_recent={result.get('runtime_context_recent')}",
                f"capital_state={result.get('capital_state')}",
                f"capital_allows_observation={result.get('capital_allows_observation')}",
                f"risk_state={result.get('risk_state')}",
                f"risk_allows_observation={result.get('risk_allows_observation')}",
                f"open_count={result.get('open_count')}",
                f"merged_open_count={result.get('merged_open_count')}",
                f"closed_count={result.get('closed_count')}",
                f"broker_touched={result.get('broker_touched')}",
                f"order_executed={result.get('order_executed')}",
                f"order_policy={result.get('order_policy')}",
            ]
        )
    preflight = result.get("preflight") if isinstance(result.get("preflight"), dict) else {}
    db = preflight.get("db_state") if isinstance(preflight.get("db_state"), dict) else {}
    open_payload = preflight.get("open_payload") if isinstance(preflight.get("open_payload"), dict) else {}
    batch = result.get("batch") if isinstance(result.get("batch"), dict) else {}
    return "\n".join(
        [
            "MT5 XAUUSD M15 Paper Test Supervisor",
            f"symbol={result.get('symbol')}",
            f"broker_symbol={result.get('broker_symbol')}",
            f"timeframe={result.get('timeframe')}",
            f"status={result.get('status')}",
            f"supervisor_state={result.get('supervisor_state')}",
            f"stop_reason={result.get('stop_reason')}",
            f"current_phase={result.get('current_phase')}",
            f"readiness_state={result.get('readiness_state')}",
            f"gate_summary={_compact_json(result.get('gate_summary') if isinstance(result.get('gate_summary'), dict) else {})}",
            f"next_action={result.get('next_action')}",
            f"failed_gate_names={result.get('failed_gate_names')}",
            f"risk_governor_reason={result.get('risk_governor_reason')}",
            f"recent_edge_negative={result.get('recent_edge_negative')}",
            f"entry_allowed_for_paper_test={result.get('entry_allowed_for_paper_test')}",
            f"entry_block_type={result.get('entry_block_type')}",
            f"db_available={db.get('db_available')}",
            f"db_degraded={db.get('db_degraded')}",
            f"tables_ready={db.get('tables_ready')}",
            f"queue_depth={db.get('queue_depth')}",
            f"open_source={open_payload.get('open_source')}",
            f"merged_open_count={open_payload.get('merged_open_count', open_payload.get('open_count'))}",
            f"batch_runner_state={batch.get('runner_state')}",
            f"session_id={result.get('session_id')}",
            f"session_started_at={result.get('session_started_at')}",
            f"target_scope={result.get('target_scope')}",
            f"session_trades_opened={result.get('session_trades_opened')}",
            f"session_trades_closed={result.get('session_trades_closed')}",
            f"valid_trades_closed={result.get('valid_trades_closed')}",
            f"invalid_samples={result.get('invalid_samples')}",
            f"historical_closed_count={result.get('historical_closed_count')}",
            f"current_shadow_id={result.get('current_shadow_id')}",
            f"current_shadow_source={result.get('current_shadow_source')}",
            f"open_count={result.get('open_count')}",
            f"cycles_completed={result.get('cycles_completed')}",
            f"win_rate={result.get('win_rate')}",
            f"expectancy={result.get('expectancy')}",
            f"profit_factor={result.get('profit_factor')}",
            f"last_closed_trade={_compact_json(result.get('last_closed_trade') if isinstance(result.get('last_closed_trade'), dict) else {})}",
            f"paper_shadow_created={result.get('paper_shadow_created')}",
            f"paper_close_applied={result.get('paper_close_applied')}",
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
