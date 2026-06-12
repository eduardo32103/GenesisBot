from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_fast_edge_factory import run_fast_edge_factory  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_fast_edge_factory(
        run_fast_scans=args.run_fast_scans,
        deep_validate_candidate=args.deep_validate_candidate,
        max_evaluations=args.max_evaluations,
        processed_source_paths=_paths(args.processed_source_paths),
        load_persistent=not args.no_persistent,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True, default=str))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok") else 1


def _human_summary(result: dict[str, Any]) -> str:
    db_state = result.get("db_state") if isinstance(result.get("db_state"), dict) else {}
    recommended = result.get("recommended_next_candidate") if isinstance(result.get("recommended_next_candidate"), dict) else None
    lines = [
        "MT5 Fast Edge Factory",
        f"factory_state={result.get('factory_state')}",
        f"mode={result.get('mode')}",
        f"db_state={_compact_json(db_state)}",
        f"lessons_loaded={result.get('lessons_loaded')}",
        f"rejected_families_loaded={result.get('rejected_families_loaded')}",
        f"degraded_profiles_loaded={result.get('degraded_profiles_loaded')}",
        f"scans_run={_comma(result.get('scans_run') or [])}",
        f"heavy_backtests_run={result.get('heavy_backtests_run')}",
        f"offline_backtests_run={result.get('offline_backtests_run')}",
        f"max_evaluations={result.get('max_evaluations')}",
        f"evaluations_count={result.get('evaluations_count')}",
        f"unique_evaluations_count={result.get('unique_evaluations_count')}",
        f"max_evaluations_respected={result.get('max_evaluations_respected')}",
        f"candidates_found={result.get('candidates_found')}",
        "recommended_next_candidate="
        + (
            f"{recommended.get('symbol')} {recommended.get('timeframe')} {recommended.get('profile')} "
            f"status={recommended.get('candidate_status')}"
            if recommended
            else "none"
        ),
        f"recommended_next_script={result.get('recommended_next_script')}",
        f"recommended_next_command={result.get('recommended_next_command')}",
        f"recommended_next_research_phase={result.get('recommended_next_research_phase')}",
        f"recommendation={result.get('recommendation')}",
        f"candidate_activated={result.get('candidate_activated')}",
        f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
        f"paper_rotation_applied={result.get('paper_rotation_applied')}",
        f"applies_to_real_trading={result.get('applies_to_real_trading')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
        "",
        "Factory lines:",
    ]
    lines.extend(_factory_lines(result.get("factory_lines") or []))
    lines.extend(["", "Rejected summary:"])
    lines.extend(_rejected_summary_lines(result.get("rejected_summary") or []))
    lines.extend(["", "Top rejected:"])
    lines.extend(_candidate_lines(result.get("top_rejected") or []))
    lines.extend(["", "Top candidates:"])
    lines.extend(_candidate_lines(result.get("top_candidates") or []))
    lines.extend(["", "Deep validation candidates:"])
    lines.extend(_candidate_lines(result.get("deep_validation_candidates") or []))
    lines.extend(["", "Avoided families:"])
    lines.extend(_avoid_lines(result.get("avoided_families") or []))
    return "\n".join(lines)


def _factory_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    return [f"- {row.get('family_name')} priority={row.get('priority_score')}" for row in rows]


def _rejected_summary_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    lines: list[str] = []
    for row in rows:
        reasons = "; ".join(
            f"{item.get('reason')}={item.get('count')}"
            for item in row.get("top_rejection_reasons") or []
        )
        lines.append(f"- {row.get('family')} rejected_count={row.get('rejected_count')} reasons={reasons or 'none'}")
    return lines


def _candidate_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    lines: list[str] = []
    for row in rows[:10]:
        lines.append(
            "- "
            + f"{row.get('symbol')} {row.get('timeframe')} {row.get('profile')} "
            + f"family={row.get('family')} recent={row.get('recent_closed')} total={row.get('total_closed')} "
            + f"recent_pf={row.get('recent_pf')} total_pf={row.get('total_pf')} "
            + f"mc_pf={row.get('monte_carlo_stressed_pf')} remove_best_5={row.get('remove_best_5_pf')} "
            + f"status={row.get('candidate_status')} rejections={','.join(row.get('rejection_reasons') or []) or 'none'}"
        )
    return lines


def _avoid_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    return [
        f"- {row.get('label')} source={row.get('source')} reason={row.get('reason')}"
        for row in rows[:30]
    ]


def _paths(value: str) -> list[str] | None:
    if not value:
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


def _compact_json(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, default=str)


def _comma(values: list[Any]) -> str:
    return ",".join(str(value) for value in values) if values else "none"


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a fast paper-only MT5 edge research batch.")
    parser.add_argument("--run-fast-scans", action="store_true", help="Run fast processed-source scans.")
    parser.add_argument("--max-evaluations", type=int, default=300)
    parser.add_argument("--deep-validate-candidate", default="", help="Validate one explicit candidate id only.")
    parser.add_argument("--processed-source-paths", default="", help="Comma-separated processed CSV/JSON sources.")
    parser.add_argument("--no-persistent", action="store_true", help="Do not read Persistent Intelligence recent lessons.")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
