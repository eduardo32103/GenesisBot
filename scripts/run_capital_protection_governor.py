from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_capital_protection_governor import run_capital_protection_governor


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_capital_protection_governor(
        load_shadow_snapshot=not args.no_shadow_snapshot,
        load_persistent=not args.no_persistent,
        persist_events=not args.no_persist_events,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok") else 1


def _human_summary(result: dict[str, Any]) -> str:
    lines = [
        "MT5 Capital Protection Governor",
        f"mode={result.get('mode')}",
        f"decision={result.get('decision')}",
        f"reason={result.get('reason')}",
        f"capital_state={result.get('capital_state')}",
        f"safe_to_trade={result.get('safe_to_trade')}",
        f"daily_loss_pct={result.get('daily_loss_pct')}",
        f"weekly_loss_pct={result.get('weekly_loss_pct')}",
        f"current_drawdown_pct={result.get('current_drawdown_pct')}",
        f"max_drawdown_pct={result.get('max_drawdown_pct')}",
        f"open_shadow_exposure={result.get('open_shadow_exposure')}",
        f"open_profile_count={result.get('open_profile_count')}",
        f"consecutive_losses_global={result.get('consecutive_losses_global')}",
        f"risk_budget_remaining={json.dumps(result.get('risk_budget_remaining') or {}, sort_keys=True)}",
        f"recommended_action={result.get('recommended_action')}",
        f"candidate_activated={result.get('candidate_activated')}",
        f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
        "",
        "Circuit breakers:",
    ]
    active = [row for row in result.get("circuit_breakers") or [] if isinstance(row, dict) and row.get("active")]
    rows = active if active else [row for row in result.get("circuit_breakers") or [] if isinstance(row, dict)]
    if rows:
        for row in rows:
            lines.append(
                f"- {row.get('name')} active={row.get('active')} critical={row.get('critical')} "
                f"reason={row.get('reason')} detail={row.get('detail')}"
            )
    else:
        lines.append("- none")
    return "\n".join(lines)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the paper-only MT5 capital protection governor.")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--no-shadow-snapshot", action="store_true", help="Do not read local shadow-trade memory.")
    parser.add_argument("--no-persistent", action="store_true", help="Do not read Persistent Intelligence.")
    parser.add_argument("--no-persist-events", action="store_true", help="Do not write governor diagnostic events.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
