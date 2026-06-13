from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_xau_m15_volatility_compression_deep_validation import (  # noqa: E402
    run_xau_m15_volatility_compression_deep_validation,
)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_xau_m15_volatility_compression_deep_validation(
        csv_paths=_paths(args.csv_paths),
        max_bars=args.max_bars,
        monte_carlo_simulations=args.monte_carlo_simulations,
        load_persistent=args.load_persistent,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True, default=str))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok") else 1


def _human_summary(result: dict[str, Any]) -> str:
    best = result.get("best_variant") if isinstance(result.get("best_variant"), dict) else None
    lines = [
        "MT5 XAUUSD M15 Volatility Compression Deep Validation",
        f"status={result.get('status')}",
        f"candidate_profile={result.get('candidate_profile')}",
        f"source_csvs_used={_comma(result.get('source_csvs_used') or [])}",
        f"missing_csvs={_comma(result.get('missing_csvs') or [])}",
        f"variants_tested={_comma(result.get('variants_tested') or [])}",
        f"best_variant={_variant_label(best)}",
        f"monte_carlo_stressed_pf={result.get('monte_carlo_stressed_pf')}",
        f"spread_x2_pf={result.get('spread_x2_pf')}",
        f"remove_best_5_pf={result.get('remove_best_5_pf')}",
        f"recommendation={result.get('recommendation')}",
        f"paper_observation_ready={result.get('paper_observation_ready')}",
        f"requires_human_approval={result.get('requires_human_approval')}",
        f"candidate_activated={result.get('candidate_activated')}",
        f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
        f"applies_to_real_trading={result.get('applies_to_real_trading')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
        "",
        "Metrics by window:",
    ]
    lines.extend(_window_lines(result.get("metrics_by_window") or []))
    lines.extend(["", "Gates:"])
    lines.extend(_gate_lines(result.get("gates") or {}))
    lines.extend(["", f"rejection_reasons={_comma(result.get('rejection_reasons') or [])}", "", "Variant ranking:"])
    lines.extend(_variant_lines(result.get("variant_results") or []))
    lines.extend(["", "Compact persistence payload:"])
    lines.append(json.dumps(result.get("compact_persistence_payload") or {}, indent=2, sort_keys=True, ensure_ascii=True))
    return "\n".join(lines)


def _window_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    output: list[str] = []
    for row in rows:
        output.append(
            "- "
            + f"{row.get('window')} closed={row.get('closed')} "
            + f"win_rate={row.get('win_rate')} pf={row.get('profit_factor')} "
            + f"expectancy={row.get('expectancy')} max_dd={row.get('max_drawdown')} "
            + f"loss_streak={row.get('consecutive_losses')} pass={row.get('passes_recent_gate')}"
        )
    return output


def _gate_lines(gates: dict[str, dict[str, Any]]) -> list[str]:
    if not gates:
        return ["- none"]
    return [
        f"- {name}: passed={gate.get('passed')} actual={gate.get('actual')} required={gate.get('required')}"
        for name, gate in gates.items()
    ]


def _variant_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    output: list[str] = []
    for row in rows:
        output.append(
            "- "
            + f"{row.get('mode')} closed={row.get('total_closed')}/{row.get('recent_closed')} "
            + f"pf={row.get('total_pf')}/{row.get('recent_pf')} "
            + f"mc_pf={row.get('monte_carlo_stressed_pf')} "
            + f"spread_x2={row.get('spread_x2_pf')} remove_best_5={row.get('remove_best_5_pf')} "
            + f"status={row.get('candidate_status')} rejections={_comma(row.get('rejection_reasons') or [])}"
        )
    return output


def _variant_label(row: dict[str, Any] | None) -> str:
    if not row:
        return "none"
    return (
        f"{row.get('symbol')} {row.get('timeframe')} {row.get('profile')} "
        f"closed={row.get('total_closed')}/{row.get('recent_closed')} "
        f"pf={row.get('total_pf')}/{row.get('recent_pf')}"
    )


def _paths(value: str) -> list[str] | None:
    if not value:
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


def _comma(values: list[Any]) -> str:
    return ",".join(str(value) for value in values) if values else "none"


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deep-validate XAUUSD M15 volatility compression breakout locally.")
    parser.add_argument("--csv-paths", default="", help="Comma-separated local OHLC CSV paths.")
    parser.add_argument("--max-bars", type=int, default=20000)
    parser.add_argument("--monte-carlo-simulations", type=int, default=250)
    parser.add_argument("--load-persistent", action="store_true", help="Read compact Persistent Intelligence context if available.")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
