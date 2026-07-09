from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_xau_m15_paper_test_supervisor import run_xau_m15_paper_test_supervisor  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    symbol = str(args.symbol or "BTCUSD").upper().strip()
    timeframe = str(args.timeframe or "M15").upper().strip()
    dry_run = bool(args.dry_run) if args.dry_run is not None else not bool(args.paper_only_confirmed)
    state_file = args.state_file or f"data/research_outputs/{symbol.lower()}_m15_paper_batch_state.json"
    results_file = args.results_file or f"data/research_outputs/{symbol.lower()}_m15_paper_batch_results.json"
    asset_configs = _load_asset_configs(args)
    result = run_xau_m15_paper_test_supervisor(
        base_url=args.base_url,
        symbol=symbol,
        broker_symbol=args.broker_symbol or symbol,
        timeframe=timeframe,
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
        state_file=state_file,
        results_file=results_file,
        timeout_seconds=args.timeout_seconds,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True, default=str))
    else:
        print(_human_summary(result))
    return 0


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run BTCUSD/ETHUSD M15 paper-only supervisor lab. No broker, no real trading.")
    parser.add_argument("--symbol", default="BTCUSD")
    parser.add_argument("--broker-symbol", default="")
    parser.add_argument("--timeframe", choices=["M15"], default="M15", help="Paper-only crypto supervisor timeframe. Only M15 is enabled.")
    parser.add_argument("--allow-symbol", action="append", default=None, help="Explicit paper-test symbol allowlist shorthand. Repeatable. No default; omitted allowlist fails closed.")
    parser.add_argument("--asset-config-json", default="", help="Explicit JSON object/list with enabled paper-only asset config(s).")
    parser.add_argument("--asset-config-file", default="", help="Path to explicit JSON object/list with enabled paper-only asset config(s).")
    parser.add_argument("--base-url", default="")
    parser.add_argument("--target-trades", type=int, default=20)
    parser.add_argument("--max-cycles", type=int, default=200)
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
    parser.add_argument("--state-file", default="")
    parser.add_argument("--results-file", default="")
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
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
    batch = result.get("batch") if isinstance(result.get("batch"), dict) else {}
    stats = batch.get("batch_stats") if isinstance(batch.get("batch_stats"), dict) else {}
    return "\n".join(
        [
            "MT5 Crypto M15 Paper Test Supervisor",
            f"symbol={result.get('symbol')}",
            f"broker_symbol={result.get('broker_symbol')}",
            f"timeframe={result.get('timeframe')}",
            f"supervisor_state={result.get('supervisor_state')}",
            f"stop_reason={result.get('stop_reason')}",
            f"readiness_state={result.get('readiness_state')}",
            f"bars_count={result.get('bars_count')}",
            f"market_active={result.get('market_active')}",
            f"market_active_reason={result.get('market_active_reason')}",
            f"entry_allowed_for_paper_test={result.get('entry_allowed_for_paper_test')}",
            f"max_open_positions_total={result.get('max_open_positions_total')}",
            f"next_action={result.get('next_action')}",
            f"session_id={result.get('session_id')}",
            f"session_trades_opened={result.get('session_trades_opened')}",
            f"valid_trades_closed={result.get('valid_trades_closed')}",
            f"invalid_samples={result.get('invalid_samples')}",
            f"win_rate={result.get('win_rate')}",
            f"profit_factor={result.get('profit_factor')}",
            f"expectancy={result.get('expectancy')}",
            f"avg_r={result.get('avg_r')}",
            f"batch_stats={json.dumps(stats, sort_keys=True, ensure_ascii=True, separators=(',', ':'), default=str)}",
            f"candidate_activated={result.get('candidate_activated')}",
            f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
            f"broker_touched={result.get('broker_touched')}",
            f"order_executed={result.get('order_executed')}",
            f"order_policy={result.get('order_policy')}",
        ]
    )


if __name__ == "__main__":
    raise SystemExit(main())
