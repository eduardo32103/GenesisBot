from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_multi_timeframe_trend_pullback_feature_scan import run_multi_timeframe_trend_pullback_feature_scan


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_multi_timeframe_trend_pullback_feature_scan(
        csv_dir=Path(args.csv_dir) if args.csv_dir else ROOT / "data" / "backtests" / "multisymbol",
        symbols=args.symbols,
        timeframes=args.timeframes,
        max_rows_per_file=args.max_rows_per_file,
        max_evaluations=args.max_evaluations,
        run_deep_scan=args.run_deep_scan,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok") else 1


def _human_summary(result: dict[str, Any]) -> str:
    lines = [
        "MT5 multi-timeframe trend pullback feature scan",
        f"mode={result.get('mode')}",
        f"run_deep_scan={result.get('run_deep_scan')}",
        f"max_rows_per_file={result.get('max_rows_per_file')}",
        f"max_evaluations={result.get('max_evaluations')}",
        f"scanned_symbols={','.join(result.get('scanned_symbols') or []) or 'none'}",
        f"scanned_timeframes={','.join(result.get('scanned_timeframes') or []) or 'none'}",
        f"evaluations_count={result.get('evaluations_count')}",
        f"recommendation={result.get('recommendation')}",
        f"recommended_next_research_phase={result.get('recommended_next_research_phase')}",
        f"proxy_only={result.get('proxy_only')}",
        f"requires_real_hardening={result.get('requires_real_hardening')}",
        f"hardening_required_before_candidate={result.get('hardening_required_before_candidate')}",
        f"cannot_be_paper_forward_candidate={result.get('cannot_be_paper_forward_candidate')}",
        f"proxy_reliability_warning={result.get('proxy_reliability_warning')}",
        f"candidate_activated={result.get('candidate_activated')}",
        f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
        f"applies_to_real_trading={result.get('applies_to_real_trading')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
        "",
        "Top feature edges:",
    ]
    lines.extend(_edge_lines(result.get("top_feature_edges") or []))
    lines.extend(["", "Near misses:"])
    lines.extend(_edge_lines(result.get("near_misses") or []))
    lines.extend(["", "Rejected by registry:"])
    lines.extend(_rejected_lines(result.get("rejected_by_registry") or []))
    lines.extend(["", "Data quality issues:"])
    lines.extend(_quality_lines(result.get("data_quality_issues") or []))
    return "\n".join(lines)


def _edge_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    return [
        "- "
        + f"{row.get('symbol')} {row.get('timeframe')}/{row.get('higher_timeframe')} {row.get('profile')} "
        + f"signals={row.get('signal_count')} recent={row.get('recent_signal_count')} "
        + f"pf={row.get('profit_factor_proxy')} exp={row.get('expectancy_proxy')} "
        + f"recent_exp={row.get('recent_expectancy_proxy')} status={row.get('scan_status')} "
        + f"proxy_only={row.get('proxy_only')} requires_real_hardening={row.get('requires_real_hardening')} "
        + f"cannot_be_paper_forward_candidate={row.get('cannot_be_paper_forward_candidate')} "
        + f"proxy_warning={row.get('proxy_reliability_warning') or ''} "
        + f"reasons={','.join(row.get('rejection_reasons') or []) or 'none'}"
        for row in rows[:12]
    ]


def _rejected_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    return [
        "- "
        + f"{row.get('symbol')} {row.get('timeframe')}/{row.get('higher_timeframe')} {row.get('profile')} "
        + f"status={row.get('scan_status') or row.get('candidate_status')} "
        + f"reason={row.get('research_rejection_reason') or row.get('degradation_reason') or row.get('rejection_reason') or row.get('sibling_risk_reason')} "
        + f"proxy_warning={row.get('proxy_reliability_warning') or ''}"
        for row in rows[:20]
    ]


def _quality_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    return [
        "- "
        + f"{row.get('symbol')} {row.get('timeframe')}/{row.get('higher_timeframe')} {row.get('profile')} "
        + f"data_quality={row.get('data_quality')} reasons={','.join(row.get('rejection_reasons') or [])}"
        for row in rows[:20]
    ]


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fast offline multi-timeframe trend pullback feature scan.")
    parser.add_argument("--csv-dir", default="", help="Directory containing local OHLC CSVs.")
    parser.add_argument("--max-rows-per-file", type=int, default=2500)
    parser.add_argument("--symbols", default="", help="Comma-separated symbols. Defaults to the research universe.")
    parser.add_argument("--timeframes", default="", help="Comma-separated operating timeframes. Defaults to M15,M30,H1.")
    parser.add_argument("--max-evaluations", type=int, default=120)
    parser.add_argument("--run-deep-scan", action="store_true", help="Allow a larger offline scan using the configured row/evaluation limits.")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
