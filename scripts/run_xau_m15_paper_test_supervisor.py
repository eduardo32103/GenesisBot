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
        result = run_xau_m15_paper_test_supervisor(
            base_url=args.base_url,
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
    parser.add_argument("--target-trades", type=int, default=3)
    parser.add_argument("--max-cycles", type=int, default=120)
    parser.add_argument("--interval-seconds", type=float, default=30.0)
    parser.add_argument("--dry-run", action="store_true", default=None)
    parser.add_argument("--paper-only-confirmed", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--exit-policy", choices=["default", "fast_observation"], default="fast_observation")
    parser.add_argument("--time-stop-bars", type=int, default=2)
    parser.add_argument("--max-hold-minutes", type=float, default=None)
    parser.add_argument("--min-r-to-arm-trailing", type=float, default=0.15)
    parser.add_argument("--giveback-r", type=float, default=0.10)
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_FILE))
    parser.add_argument("--results-file", default=str(DEFAULT_RESULTS_FILE))
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--repair-orphan-state", action="store_true")
    parser.add_argument("--confirm-paper-only-repair", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def _human_summary(result: dict[str, Any]) -> str:
    preflight = result.get("preflight") if isinstance(result.get("preflight"), dict) else {}
    db = preflight.get("db_state") if isinstance(preflight.get("db_state"), dict) else {}
    open_payload = preflight.get("open_payload") if isinstance(preflight.get("open_payload"), dict) else {}
    batch = result.get("batch") if isinstance(result.get("batch"), dict) else {}
    return "\n".join(
        [
            "MT5 XAUUSD M15 Paper Test Supervisor",
            f"status={result.get('status')}",
            f"supervisor_state={result.get('supervisor_state')}",
            f"stop_reason={result.get('stop_reason')}",
            f"db_available={db.get('db_available')}",
            f"db_degraded={db.get('db_degraded')}",
            f"tables_ready={db.get('tables_ready')}",
            f"queue_depth={db.get('queue_depth')}",
            f"open_source={open_payload.get('open_source')}",
            f"merged_open_count={open_payload.get('merged_open_count', open_payload.get('open_count'))}",
            f"batch_runner_state={batch.get('runner_state')}",
            f"cycles_completed={result.get('cycles_completed')}",
            f"paper_shadow_created={result.get('paper_shadow_created')}",
            f"paper_close_applied={result.get('paper_close_applied')}",
            f"candidate_activated={result.get('candidate_activated')}",
            f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
            f"broker_touched={result.get('broker_touched')}",
            f"order_executed={result.get('order_executed')}",
            f"order_policy={result.get('order_policy')}",
        ]
    )


if __name__ == "__main__":
    raise SystemExit(main())
