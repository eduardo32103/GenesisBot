from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_volatility_compression_breakout_feature_scan import (  # noqa: E402
    run_volatility_compression_breakout_feature_scan,
)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_volatility_compression_breakout_feature_scan(
        csv_dirs=_paths(args.csv_dirs),
        symbols=args.symbols,
        timeframes=args.timeframes,
        max_rows_per_file=args.max_rows_per_file,
        max_evaluations=args.max_evaluations,
        load_persistent=not args.no_persistent,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True, default=str))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok") else 1


def _human_summary(result: dict[str, Any]) -> str:
    candidate = result.get("recommended_next_candidate") if isinstance(result.get("recommended_next_candidate"), dict) else None
    lines = [
        "MT5 Volatility Compression Breakout Feature Scan",
        f"status={result.get('status')}",
        f"mode={result.get('mode')}",
        f"scanned_csvs={_comma(result.get('scanned_csvs') or [])}",
        f"missing_csvs={_comma(result.get('missing_csvs') or [])}",
        f"lessons_loaded={result.get('lessons_loaded')}",
        f"rejected_families_loaded={result.get('rejected_families_loaded')}",
        f"degraded_profiles_loaded={result.get('degraded_profiles_loaded')}",
        f"evaluations_count={result.get('evaluations_count')}",
        f"recommendation={result.get('recommendation')}",
        f"recommended_next_candidate={_candidate_label(candidate)}",
        f"recommended_next_script={result.get('recommended_next_script')}",
        f"recommended_next_research_phase={result.get('recommended_next_research_phase')}",
        f"candidate_activated={result.get('candidate_activated')}",
        f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
        f"paper_rotation_applied={result.get('paper_rotation_applied')}",
        f"applies_to_real_trading={result.get('applies_to_real_trading')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
        "",
        "Top feature edges:",
    ]
    lines.extend(_row_lines(result.get("top_feature_edges") or []))
    lines.extend(["", "Near misses:"])
    lines.extend(_row_lines(result.get("near_misses") or []))
    lines.extend(["", "Deep validation candidates:"])
    lines.extend(_row_lines(result.get("deep_validation_candidates") or []))
    lines.extend(["", "Rejected summary:"])
    lines.extend(_summary_lines(result.get("rejected_summary") or []))
    lines.extend(["", "Top rejected:"])
    lines.extend(_row_lines(result.get("top_rejected") or []))
    return "\n".join(lines)


def _row_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    lines: list[str] = []
    for row in rows[:10]:
        lines.append(
            "- "
            + f"{row.get('symbol')} {row.get('timeframe')} {row.get('profile')} "
            + f"closed={row.get('total_closed')}/{row.get('recent_closed')} "
            + f"pf={row.get('total_pf')}/{row.get('recent_pf')} "
            + f"exp={row.get('expectancy')}/{row.get('recent_expectancy')} "
            + f"spread_x2={row.get('spread_x2_pf')} remove_best_5={row.get('remove_best_5_pf')} "
            + f"status={row.get('candidate_status')} rejections={','.join(row.get('rejection_reasons') or []) or 'none'}"
        )
    return lines


def _summary_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    output: list[str] = []
    for row in rows[:20]:
        reasons = "; ".join(
            f"{item.get('reason')}={item.get('count')}"
            for item in row.get("top_rejection_reasons") or []
        )
        output.append(f"- {row.get('family')} rejected_count={row.get('rejected_count')} reasons={reasons or 'none'}")
    return output


def _candidate_label(candidate: dict[str, Any] | None) -> str:
    if not candidate:
        return "none"
    return f"{candidate.get('symbol')} {candidate.get('timeframe')} {candidate.get('profile')}"


def _paths(value: str) -> list[str] | None:
    if not value:
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


def _comma(values: list[Any]) -> str:
    return ",".join(str(value) for value in values) if values else "none"


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan local OHLC for volatility compression breakout edges.")
    parser.add_argument("--csv-dirs", default="", help="Comma-separated OHLC CSV directories.")
    parser.add_argument("--symbols", default="", help="Comma-separated symbols.")
    parser.add_argument("--timeframes", default="", help="Comma-separated timeframes.")
    parser.add_argument("--max-rows-per-file", type=int, default=5000)
    parser.add_argument("--max-evaluations", type=int, default=240)
    parser.add_argument("--no-persistent", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
