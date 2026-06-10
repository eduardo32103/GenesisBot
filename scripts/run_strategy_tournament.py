from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_strategy_tournament import run_strategy_tournament


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_strategy_tournament(
        load_shadow_snapshot=not args.no_shadow_snapshot,
        load_persistent=not args.no_persistent,
        load_rotation=not args.no_rotation,
        persist_events=not args.no_persist_events,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok") else 1


def _human_summary(result: dict[str, Any]) -> str:
    lines = [
        "MT5 Strategy Tournament",
        f"mode={result.get('mode')}",
        f"decision={result.get('decision')}",
        f"reason={result.get('reason')}",
        f"recommended_action={result.get('recommended_action')}",
        f"candidate_activated={result.get('candidate_activated')}",
        f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
        "",
        "Top candidate:",
    ]
    top = result.get("top_candidate")
    lines.append(_profile_line(top) if isinstance(top, dict) and top else "- none")
    lines.extend(["", "Ranked profiles:"])
    ranked = result.get("ranked_profiles") or []
    if ranked:
        for row in ranked[:20]:
            lines.append(_profile_line(row))
    else:
        lines.append("- none")
    lines.extend(["", "Paused profiles:"])
    lines.extend(_profile_lines(result.get("paused_profiles") or []))
    lines.extend(["", "Degraded profiles:"])
    lines.extend(_profile_lines(result.get("degraded_profiles") or []))
    lines.extend(["", "Rejected profiles:"])
    lines.extend(_profile_lines(result.get("rejected_profiles") or []))
    lines.extend(["", "Switching rules:"])
    lines.extend(f"- {rule}" for rule in result.get("switching_rules") or [])
    return "\n".join(lines)


def _profile_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    return [_profile_line(row) for row in rows]


def _profile_line(row: dict[str, Any]) -> str:
    return (
        f"- rank={row.get('rank')} {row.get('symbol')} {row.get('timeframe')} {row.get('profile')} "
        f"trades={row.get('trades_forward')} win_rate={row.get('win_rate')} pf={row.get('profit_factor')} "
        f"expectancy={row.get('expectancy')} drawdown={row.get('max_drawdown')} "
        f"losses={row.get('consecutive_losses')} score={row.get('tournament_score')} "
        f"action={row.get('recommended_action')} degraded={row.get('degraded_by_registry')} "
        f"rejected={row.get('rejected_by_research_registry')} sibling_risk={row.get('sibling_risk')}"
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the paper-only MT5 strategy tournament.")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--no-shadow-snapshot", action="store_true", help="Do not read local shadow-trade memory.")
    parser.add_argument("--no-persistent", action="store_true", help="Do not read Persistent Intelligence.")
    parser.add_argument("--no-rotation", action="store_true", help="Do not run candidate rotation.")
    parser.add_argument("--no-persist-events", action="store_true", help="Do not write tournament diagnostic events.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
