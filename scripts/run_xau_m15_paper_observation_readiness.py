from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_xau_m15_paper_observation_readiness import (  # noqa: E402
    run_xau_m15_paper_observation_readiness,
)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_xau_m15_paper_observation_readiness()
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True, default=str))
    else:
        print(_human_summary(result))
    return 0


def _human_summary(result: dict[str, Any]) -> str:
    lines = [
        "MT5 XAUUSD M15 Paper Observation Readiness",
        f"status={result.get('status')}",
        f"candidate_found={result.get('candidate_found')}",
        f"candidate_status={result.get('candidate_status')}",
        f"symbol={result.get('symbol')}",
        f"broker_symbol={result.get('broker_symbol')}",
        f"timeframe={result.get('timeframe')}",
        f"db_state={_json_line(result.get('db_state') or {})}",
        f"runtime_context_available={result.get('runtime_context_available')}",
        f"runtime_context_recent={result.get('runtime_context_recent')}",
        f"runtime_snapshot_complete={result.get('runtime_snapshot_complete')}",
        f"runtime_snapshot_context={result.get('runtime_snapshot_context')}",
        f"runtime_snapshot_source={result.get('runtime_snapshot_source')}",
        f"symbol_alias_used={result.get('symbol_alias_used')}",
        f"latest_tick_at={result.get('latest_tick_at')}",
        f"latest_bars_at={result.get('latest_bars_at')}",
        f"bars_available={result.get('bars_available')}",
        f"bars_count={result.get('bars_count')}",
        f"m15_bars_status={result.get('m15_bars_status')}",
        f"tick_available={result.get('tick_available')}",
        f"tick_merged_into_bar_context={result.get('tick_merged_into_bar_context')}",
        f"spread_available={result.get('spread_available')}",
        f"capital_state={result.get('capital_state')}",
        f"capital_allows_observation={result.get('capital_allows_observation')}",
        f"adaptive_state={result.get('adaptive_state')}",
        f"adaptive_allows_observation={result.get('adaptive_allows_observation')}",
        f"risk_state={result.get('risk_state')}",
        f"risk_allows_observation={result.get('risk_allows_observation')}",
        f"open_shadow_count={result.get('open_shadow_count')}",
        f"readiness_state={result.get('readiness_state')}",
        f"recommendation={result.get('recommendation')}",
        f"failed_gates={_comma(result.get('failed_gates') or [])}",
        f"candidate_activated={result.get('candidate_activated')}",
        f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
        "",
        "Gates:",
    ]
    lines.extend(_gate_lines(result.get("gates") or {}))
    return "\n".join(lines)


def _gate_lines(gates: dict[str, dict[str, Any]]) -> list[str]:
    if not gates:
        return ["- none"]
    return [
        f"- {name}: passed={gate.get('passed')} actual={gate.get('actual')} required={gate.get('required')}"
        for name, gate in gates.items()
    ]


def _json_line(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":"))


def _comma(values: list[Any]) -> str:
    return ",".join(str(value) for value in values) if values else "none"


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check XAUUSD M15 paper observation readiness.")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
