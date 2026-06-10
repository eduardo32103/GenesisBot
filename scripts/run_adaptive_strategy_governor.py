from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_adaptive_strategy_governor import run_adaptive_strategy_governor


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_adaptive_strategy_governor(
        load_shadow_snapshot=not args.no_shadow_snapshot,
        load_rotation=not args.no_rotation,
        load_intelligence=not args.no_intelligence,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok") else 1


def _human_summary(result: dict[str, Any]) -> str:
    lines = [
        "MT5 Adaptive Strategy Governor",
        f"mode={result.get('mode')}",
        f"global_state={result.get('global_state')}",
        f"decision={result.get('decision')}",
        f"reason={result.get('reason')}",
        f"recommended_next_action={result.get('recommended_next_action')}",
        f"candidate_activated={result.get('candidate_activated')}",
        f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
        "",
        "Active profiles:",
    ]
    lines.extend(_profile_lines(result.get("active_profiles") or []))
    lines.extend(["", "Paused profiles:"])
    lines.extend(_profile_lines(result.get("paused_profiles") or []))
    lines.extend(["", "Degraded profiles:"])
    lines.extend(_profile_lines(result.get("degraded_profiles") or []))
    lines.extend(["", "Circuit breakers:"])
    active_breakers = [row for row in result.get("circuit_breakers") or [] if row.get("active")]
    if active_breakers:
        for row in active_breakers:
            lines.append(
                f"- {row.get('name')} critical={row.get('critical')} reason={row.get('reason')} detail={row.get('detail')}"
            )
    else:
        lines.append("- none")
    lines.extend(["", "Rotation candidates:"])
    lines.extend(_candidate_lines(result.get("rotation_candidates") or []))
    lines.extend(["", "Rejected candidates:"])
    lines.extend(_candidate_lines(result.get("rejected_candidates") or []))
    return "\n".join(lines)


def _profile_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    return [
        (
            f"- {row.get('symbol')} {row.get('timeframe')} {row.get('profile')} "
            f"state={row.get('active_state')} trades={row.get('trades_forward')} "
            f"pf={row.get('profit_factor')} expectancy={row.get('expectancy')} "
            f"health={row.get('health_status')} action={row.get('recommended_action')}"
        )
        for row in rows
    ]


def _candidate_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    return [
        (
            f"- {row.get('symbol')} {row.get('timeframe')} {row.get('profile')} "
            f"status={row.get('candidate_status')} action={row.get('recommended_next_action')} "
            f"degraded={row.get('degraded_by_registry')} rejected={row.get('rejected_by_research_registry')} "
            f"sibling_risk={row.get('sibling_risk')}"
        )
        for row in rows
    ]


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the paper-only adaptive strategy governor.")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--no-shadow-snapshot", action="store_true", help="Do not read local shadow-trade memory.")
    parser.add_argument("--no-rotation", action="store_true", help="Do not run paper-forward candidate rotation.")
    parser.add_argument("--no-intelligence", action="store_true", help="Do not run research intelligence core.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
