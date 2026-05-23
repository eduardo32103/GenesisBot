from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_eth_m30_paper_forward_analytics import (
    DEFAULT_MONITOR_CSV,
    DEFAULT_MONITOR_JSON,
    DEFAULT_OUTPUT_DIR,
    run_eth_m30_paper_forward_analytics,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze ETHUSD/M30 paper-forward observation logs.")
    parser.add_argument("--csv-path", default=str(DEFAULT_MONITOR_CSV), help="Monitor CSV path.")
    parser.add_argument("--json-path", default=str(DEFAULT_MONITOR_JSON), help="Monitor JSON path. Used first when present.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    json_path = Path(args.json_path) if str(args.json_path or "").strip() else None
    result = run_eth_m30_paper_forward_analytics(
        csv_path=args.csv_path,
        json_path=json_path,
        output_dir=args.output_dir,
    )
    bottleneck = result.get("score_component_bottleneck") or {}
    print(f"status={result['status']}")
    print(f"samples_total={result['samples_total']}")
    print(f"runtime_snapshot_complete_pct={result['runtime_snapshot_complete_pct']}")
    print(f"bar_context_pct={result['bar_context_pct']}")
    print(f"active_true_count={result['active_true_count']}")
    print(f"applies_to_paper_shadow_count={result['applies_to_paper_shadow_count']}")
    print(f"decision_counts={result['decision_counts']}")
    print(f"top_decision_reasons={result['top_decision_reasons']}")
    print(f"near_miss_counts={result['near_miss_counts']}")
    print(f"score_gap_distribution={result['score_gap_distribution']}")
    print(f"momentum_gate_report={result['momentum_gate_report']}")
    print(f"top_momentum_near_misses={result['top_momentum_near_misses']}")
    print(f"score_component_bottleneck={bottleneck.get('dominant_component')}")
    print(f"bottleneck_component_ranking={result['bottleneck_component_ranking']}")
    print(f"bottleneck_reason_ranking={result['bottleneck_reason_ranking']}")
    print(f"max_open_trades_diagnostic={result['max_open_trades_diagnostic']}")
    print(f"shadow_occupancy_inconsistency={result['shadow_occupancy_inconsistency']}")
    print(f"human_bottleneck_explanation={result['human_bottleneck_explanation']}")
    print(f"top_near_miss_timestamps={result['top_near_miss_timestamps']}")
    print(f"recommendation_actions={result['recommendation_actions']}")
    print(f"summary={result['output_paths']['summary']}")
    print("broker_touched=false")
    print("order_executed=false")
    print("order_policy=journal_only_no_broker")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
