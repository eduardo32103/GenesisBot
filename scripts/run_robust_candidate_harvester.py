from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_robust_candidate_harvester import (  # noqa: E402
    DEFAULT_PROCESSED_SOURCES,
    run_robust_candidate_harvester,
)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    paths = _paths(args.processed_source_paths)
    result = run_robust_candidate_harvester(
        processed_source_paths=paths,
        load_persistent=not args.no_persistent,
        max_candidates=args.max_candidates,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True, default=str))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok") else 1


def _human_summary(result: dict[str, Any]) -> str:
    lines = [
        "MT5 Robust Candidate Harvester",
        f"status={result.get('status')}",
        f"mode={result.get('mode')}",
        f"loaded_sources={_comma(result.get('loaded_sources') or [])}",
        f"missing_sources={_comma(result.get('missing_sources') or [])}",
        f"skipped_sources={len(result.get('skipped_sources') or [])}",
        f"persistent_memory_source={result.get('persistent_memory_source')}",
        f"persistent_memory_db_degraded={result.get('persistent_memory_db_degraded')}",
        f"raw_rows={result.get('raw_rows')}",
        f"useful_rows={result.get('useful_rows')}",
        f"recommendation={result.get('recommendation')}",
        f"recommended_next_research_phase={result.get('recommended_next_research_phase')}",
        f"candidate_activated={result.get('candidate_activated')}",
        f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
        f"paper_rotation_applied={result.get('paper_rotation_applied')}",
        f"applies_to_real_trading={result.get('applies_to_real_trading')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
        "",
        "Top candidates:",
    ]
    lines.extend(_row_lines(result.get("top_candidates") or []))
    lines.extend(["", "Rejected candidates:"])
    lines.extend(_row_lines(result.get("rejected_candidates") or []))
    skipped = result.get("skipped_sources") or []
    if skipped:
        lines.extend(["", "Skipped sources:"])
        lines.extend(f"- {item}" for item in skipped)
    return "\n".join(lines)


def _row_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    lines: list[str] = []
    for row in rows[:30]:
        lines.append(
            "- "
            f"{row.get('symbol')} {row.get('timeframe')} {row.get('profile')} "
            f"family={row.get('family')} recent={row.get('recent_closed')} total={row.get('total_closed')} "
            f"recent_pf={row.get('recent_pf')} total_pf={row.get('total_pf')} exp={row.get('expectancy')} "
            f"mc_pf={row.get('monte_carlo_stressed_pf')} remove_best_5={row.get('remove_best_5_pf')} "
            f"status={row.get('candidate_status')} rejections={','.join(row.get('rejection_reasons') or []) or 'none'}"
        )
    return lines


def _paths(value: str) -> list[str | Path] | None:
    if value:
        return [item.strip() for item in value.split(",") if item.strip()]
    return [ROOT / path.relative_to(ROOT) if path.is_absolute() and str(path).startswith(str(ROOT)) else path for path in DEFAULT_PROCESSED_SOURCES]


def _comma(values: list[Any]) -> str:
    return ",".join(str(value) for value in values) if values else "none"


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Search processed sources for robust paper-only candidates.")
    parser.add_argument("--processed-source-paths", default="", help="Comma-separated processed CSV/JSON sources.")
    parser.add_argument("--max-candidates", type=int, default=10)
    parser.add_argument("--no-persistent", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
