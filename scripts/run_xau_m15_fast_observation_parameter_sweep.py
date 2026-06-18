from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_xau_m15_fast_observation_parameter_sweep import (  # noqa: E402
    DEFAULT_CSV,
    run_xau_m15_fast_observation_parameter_sweep,
)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_xau_m15_fast_observation_parameter_sweep(csv_path=args.csv_path, max_rows=args.max_rows)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True, default=str))
    else:
        print(_summary(result))
    return 0


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="XAUUSD M15 fast observation parameter sweep. Offline/paper-only.")
    parser.add_argument("--csv-path", default=str(DEFAULT_CSV))
    parser.add_argument("--max-rows", type=int, default=20_000)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def _summary(result: dict[str, Any]) -> str:
    recommended = result.get("recommended_live_paper_parameters") if isinstance(result.get("recommended_live_paper_parameters"), dict) else {}
    return "\n".join(
        [
            "MT5 XAUUSD M15 Fast Observation Parameter Sweep",
            f"status={result.get('status')}",
            f"csv_found={result.get('csv_found')}",
            f"rows_loaded={result.get('rows_loaded')}",
            f"evaluations_count={result.get('evaluations_count')}",
            f"recommendation={result.get('recommendation')}",
            f"recommended_time_stop_bars={recommended.get('time_stop_bars')}",
            f"recommended_min_r_to_arm_trailing={recommended.get('min_r_to_arm_trailing')}",
            f"recommended_giveback_r={recommended.get('giveback_r')}",
            f"recommended_fast_loss_cut_r={recommended.get('fast_loss_cut_r')}",
            f"candidate_activated={result.get('candidate_activated')}",
            f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
            f"broker_touched={result.get('broker_touched')}",
            f"order_executed={result.get('order_executed')}",
            f"order_policy={result.get('order_policy')}",
        ]
    )


if __name__ == "__main__":
    raise SystemExit(main())
