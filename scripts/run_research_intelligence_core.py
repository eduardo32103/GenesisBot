from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_research_intelligence_core import run_research_intelligence_core


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    search_root = Path(args.results_dir) if args.results_dir else ROOT / "data" / "backtests" / "multisymbol"
    result = run_research_intelligence_core(
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
        "Genesis Research Intelligence Core",
        f"mode={result.get('mode')}",
        f"recommendation={result.get('recommendation')}",
        f"recommended_next_research_phase={result.get('recommended_next_research_phase')}",
        f"offline_backtests_run={result.get('offline_backtests_run')}",
        f"candidate_activated={result.get('candidate_activated')}",
        f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
        f"applies_to_real_trading={result.get('applies_to_real_trading')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
    ]
    lines.extend(["", "Rejected clusters:"])
    lines.extend(_cluster_lines(result.get("rejected_clusters") or []))
    lines.extend(["", "Failure patterns:"])
    lines.extend(_pattern_lines(result.get("failure_patterns") or []))
    lines.extend(["", "Avoid next:"])
    lines.extend(_avoid_lines(result.get("avoid_next") or []))
    lines.extend(["", "Unresolved opportunities:"])
    lines.extend(_opportunity_lines(result.get("unresolved_opportunities") or []))
    lines.extend(["", "Research gaps:"])
    lines.extend(_gap_lines(result.get("research_gaps") or []))
    lines.extend(["", "Next hypotheses:"])
    lines.extend(_hypothesis_lines(result.get("next_hypotheses") or []))
    lines.extend(["", "Priority queue:"])
    lines.extend(_queue_lines(result.get("priority_queue") or []))
    return "\n".join(lines)


def _cluster_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    return [
        "- "
        + f"{row.get('symbol')} {row.get('timeframe')} {row.get('profile_or_pattern')} "
        + f"source={row.get('source')} reason={row.get('rejection_reason')} "
        + f"categories={','.join(row.get('failure_categories') or [])}"
        for row in rows
    ]


def _pattern_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    return [
        f"- {row.get('category')} count={row.get('count')} examples={'; '.join((row.get('examples') or [])[:3])}"
        for row in rows
    ]


def _avoid_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    return [f"- {row.get('scope')} action={row.get('recommended_action')} reason={row.get('reason')}" for row in rows]


def _opportunity_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    return [
        "- "
        + f"{row.get('source')} {row.get('symbol')} {row.get('timeframe')} {row.get('profile')} "
        + f"status={row.get('candidate_status')} action={row.get('recommended_action')}"
        for row in rows[:12]
    ]


def _gap_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    return [f"- {row.get('gap')} reason={row.get('reason')} next={row.get('next_step')}" for row in rows]


def _hypothesis_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    return [
        "- "
        + f"{row.get('family_name')} priority={row.get('priority_score')} "
        + f"symbols={','.join(row.get('symbols_to_test') or [])} "
        + f"timeframes={','.join(row.get('timeframes_to_test') or [])} "
        + f"heavy_backtest_required={row.get('heavy_backtest_required')}"
        for row in rows
    ]


def _queue_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    return [
        "- "
        + f"rank={row.get('rank')} family={row.get('family_name')} priority={row.get('priority_score')} "
        + f"action={row.get('recommended_next_action')} max_offline={row.get('max_offline_evaluations_suggested')}"
        for row in rows
    ]


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a fast MT5 research intelligence plan without activation.")
    parser.add_argument("--results-dir", default="", help="Optional directory containing small processed result CSV/JSON files.")
    parser.add_argument("--result-path", action="append", default=[], help="Optional processed result CSV/JSON path. Can be repeated.")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
