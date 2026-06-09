from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_paper_forward_candidate_rotation import run_paper_forward_candidate_rotation


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    search_root = Path(args.results_dir) if args.results_dir else ROOT / "data" / "backtests" / "multisymbol"
    result = run_paper_forward_candidate_rotation(
        result_paths=[Path(path) for path in args.result_path],
        search_root=search_root,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok") else 1


def _human_summary(result: dict[str, Any]) -> str:
    lines = [
        "MT5 paper-forward candidate rotation",
        f"recommendation={result.get('recommendation')}",
        f"candidate_activated={result.get('candidate_activated')}",
        f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
        f"useful_rows={result.get('useful_rows')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
        "",
        "Loaded sources:",
    ]
    loaded_sources = result.get("loaded_sources") or []
    if loaded_sources:
        lines.extend(f"- {source}" for source in loaded_sources)
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "Missing sources:",
        ]
    )
    missing_sources = result.get("missing_sources") or []
    if missing_sources:
        lines.extend(f"- {source}" for source in missing_sources)
    else:
        lines.append("- none")
    skipped_sources = result.get("skipped_sources") or []
    if skipped_sources:
        lines.extend(["", "Skipped sources:"])
        for item in skipped_sources:
            lines.append(f"- {item.get('path')} reason={item.get('reason')}")
    lines.extend(
        [
            "",
            "Ranking:",
        ]
    )
    header = "symbol | timeframe | profile | family | recent_closed | total_closed | recent_pf | total_pf | expectancy | mc_pf | spread_x2_pf | degraded | status | action"
    lines.append(header)
    lines.append("-" * len(header))
    for row in result.get("ranking") or []:
        lines.append(
            " | ".join(
                [
                    str(row.get("symbol") or ""),
                    str(row.get("timeframe") or ""),
                    str(row.get("profile") or ""),
                    str(row.get("family") or ""),
                    str(row.get("recent_closed") or 0),
                    str(row.get("total_closed") or 0),
                    str(row.get("recent_pf") or 0.0),
                    str(row.get("total_pf") or 0.0),
                    str(row.get("expectancy") or 0.0),
                    str(row.get("monte_carlo_stressed_pf") or 0.0),
                    str(row.get("spread_x2_pf") or 0.0),
                    str(row.get("degraded_by_registry") or False),
                    str(row.get("candidate_status") or ""),
                    str(row.get("recommended_next_action") or ""),
                ]
            )
        )
    excluded = result.get("excluded_by_degradation_registry") or []
    lines.append("")
    lines.append("Excluded by degradation registry:")
    if excluded:
        for row in excluded:
            lines.append(f"- {row.get('symbol')} {row.get('timeframe')} {row.get('profile')} reason={row.get('degradation_reason')}")
    else:
        lines.append("- none")
    recommended = result.get("recommended_candidate")
    lines.append("")
    lines.append(
        "recommended_candidate="
        + (
            f"{recommended.get('symbol')} {recommended.get('timeframe')} {recommended.get('profile')}"
            if isinstance(recommended, dict)
            else "none"
        )
    )
    return "\n".join(lines)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rank paper-forward candidates without activating them.")
    parser.add_argument("--results-dir", default="", help="Optional directory containing small existing result CSV/JSON files.")
    parser.add_argument("--result-path", action="append", default=[], help="Optional existing result CSV/JSON path. Can be repeated.")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
