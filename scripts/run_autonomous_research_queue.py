from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_autonomous_research_queue import run_autonomous_research_queue  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_autonomous_research_queue(
        run_fast_scans=args.run_fast_scans,
        run_deep_validation=args.run_deep_validation,
        candidate=args.candidate,
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
    top = result.get("top_candidate") if isinstance(result.get("top_candidate"), dict) else None
    lines = [
        "MT5 Autonomous Research Queue",
        f"research_queue_state={result.get('research_queue_state')}",
        f"mode={result.get('mode')}",
        f"db_state={_compact_json(db_state)}",
        f"lessons_loaded={result.get('lessons_loaded')}",
        f"rejected_families_loaded={result.get('rejected_families_loaded')}",
        f"degraded_profiles_loaded={result.get('degraded_profiles_loaded')}",
        f"scans_run={_comma(result.get('scans_run') or [])}",
        f"heavy_backtests_run={result.get('heavy_backtests_run')}",
        f"offline_backtests_run={result.get('offline_backtests_run')}",
        f"max_evaluations={result.get('max_evaluations')}",
        f"candidate_evaluations_considered={result.get('candidate_evaluations_considered')}",
        f"max_evaluations_respected={result.get('max_evaluations_respected')}",
        f"candidates_found={result.get('candidates_found')}",
        "top_candidate="
        + (
            f"{top.get('symbol')} {top.get('timeframe')} {top.get('profile')} "
            f"status={top.get('candidate_status')}"
            if top
            else "none"
        ),
        f"recommendation={result.get('recommendation')}",
        f"recommended_next_research_phase={result.get('recommended_next_research_phase')}",
        f"recommended_next_script={result.get('recommended_next_script')}",
        f"candidate_activated={result.get('candidate_activated')}",
        f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
        f"paper_rotation_applied={result.get('paper_rotation_applied')}",
        f"applies_to_real_trading={result.get('applies_to_real_trading')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
        "",
        "Avoided families:",
    ]
    lines.extend(_avoid_lines(result.get("avoided_families") or []))
    lines.extend(["", "Next hypotheses:"])
    lines.extend(_hypothesis_lines(result.get("next_hypotheses") or []))
    lines.extend(["", "Evaluated candidates:"])
    lines.extend(_candidate_lines(result.get("evaluated_candidates") or []))
    return "\n".join(lines)


def _avoid_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    return [
        f"- {row.get('label')} source={row.get('source')} reason={row.get('reason')}"
        for row in rows[:40]
    ]


def _hypothesis_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    return [
        "- "
        + f"{index + 1}. {row.get('family_name')} priority={row.get('priority_score')} "
        + f"action={row.get('recommended_next_action')} script={row.get('recommended_next_script')}"
        for index, row in enumerate(rows[:12])
    ]


def _candidate_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    lines: list[str] = []
    for row in rows[:30]:
        lines.append(
            "- "
            + f"{row.get('symbol')} {row.get('timeframe')} {row.get('profile')} "
            + f"recent={row.get('recent_closed')} total={row.get('total_closed')} "
            + f"recent_pf={row.get('recent_pf')} total_pf={row.get('total_pf')} "
            + f"mc_pf={row.get('monte_carlo_stressed_pf')} remove_best_5={row.get('remove_best_5_pf')} "
            + f"status={row.get('candidate_status')} rejections={','.join(row.get('rejection_reasons') or []) or 'none'}"
        )
    return lines


def _paths(value: str) -> list[str] | None:
    if not value:
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


def _comma(values: list[Any]) -> str:
    return ",".join(str(value) for value in values) if values else "none"


def _compact_json(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, default=str)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plan the next MT5 paper-only research queue step.")
    parser.add_argument("--run-fast-scans", action="store_true", help="Run fast processed-source scans only.")
    parser.add_argument("--run-deep-validation", action="store_true", help="Run one explicit candidate validator when available.")
    parser.add_argument("--candidate", default="", help="Candidate id for --run-deep-validation.")
    parser.add_argument("--max-evaluations", type=int, default=100)
    parser.add_argument("--processed-source-paths", default="", help="Comma-separated processed CSV/JSON sources.")
    parser.add_argument("--no-persistent", action="store_true", help="Do not read Persistent Intelligence recent lessons.")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
