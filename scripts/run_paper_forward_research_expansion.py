from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_paper_forward_research_expansion import run_paper_forward_research_expansion


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    search_root = Path(args.results_dir) if args.results_dir else ROOT / "data" / "backtests" / "multisymbol"
    result = run_paper_forward_research_expansion(
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
        "MT5 paper-forward research expansion",
        f"recommendation={result.get('recommendation')}",
        f"candidate_activated={result.get('candidate_activated')}",
        f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
        f"applies_to_real_trading={result.get('applies_to_real_trading')}",
        f"useful_rows={result.get('useful_rows')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
        "",
        "Loaded sources:",
    ]
    loaded_sources = result.get("loaded_sources") or []
    lines.extend(f"- {source}" for source in loaded_sources) if loaded_sources else lines.append("- none")

    lines.extend(["", "Missing sources:"])
    missing_sources = result.get("missing_sources") or []
    lines.extend(f"- {source}" for source in missing_sources) if missing_sources else lines.append("- none")

    skipped_sources = result.get("skipped_sources") or []
    if skipped_sources:
        lines.extend(["", "Skipped sources:"])
        for item in skipped_sources:
            lines.append(f"- {item.get('path')} reason={item.get('reason')}")

    lines.extend(["", "Main ranking:"])
    lines.extend(_ranking_lines(result.get("ranking") or [], limit=25))

    lines.extend(["", "Near misses:"])
    near_misses = result.get("near_misses") or []
    if near_misses:
        for row in near_misses[:12]:
            lines.append(
                f"- {row.get('symbol')} {row.get('timeframe')} {row.get('profile')} "
                f"failed={','.join(row.get('failed_gates') or [])} "
                f"hardening={row.get('hardening_recommendation')}"
            )
    else:
        lines.append("- none")

    lines.extend(["", "Excluded by registry or sibling risk:"])
    excluded = result.get("excluded_by_registry_or_sibling_risk") or []
    if excluded:
        for row in excluded:
            lines.append(
                f"- {row.get('symbol')} {row.get('timeframe')} {row.get('profile')} "
                f"status={row.get('candidate_status')} sibling_of={row.get('sibling_of_degraded_profile') or ''}"
            )
    else:
        lines.append("- none")

    lines.extend(["", "Top hardening families:"])
    hardening = result.get("top_3_hardening_families") or []
    if hardening:
        for row in hardening:
            lines.append(
                f"- {row.get('symbol')} {row.get('timeframe')} {row.get('family')} "
                f"profile={row.get('representative_profile')} failed={','.join(row.get('failed_gates') or [])}"
            )
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


def _ranking_lines(rows: list[dict[str, Any]], *, limit: int) -> list[str]:
    header = (
        "symbol | timeframe | profile | family | recent_closed | total_closed | recent_pf | total_pf | expectancy | "
        "mc_pf | spread_x2_pf | remove_best_5_pf | fragile | single_trade | degraded | sibling_risk | status | action"
    )
    lines = [header, "-" * len(header)]
    for row in rows[:limit]:
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
                    "" if row.get("remove_best_5_pf") is None else str(row.get("remove_best_5_pf")),
                    str(row.get("fragile_regime_dependency") or False),
                    str(row.get("single_trade_dependency") or False),
                    str(row.get("degraded_by_registry") or False),
                    str(row.get("sibling_risk") or False),
                    str(row.get("candidate_status") or ""),
                    str(row.get("recommended_next_action") or ""),
                ]
            )
        )
    return lines


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Expand paper-forward research without activating candidates.")
    parser.add_argument("--results-dir", default="", help="Optional directory containing small processed result CSV/JSON files.")
    parser.add_argument("--result-path", action="append", default=[], help="Optional processed result CSV/JSON path. Can be repeated.")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
