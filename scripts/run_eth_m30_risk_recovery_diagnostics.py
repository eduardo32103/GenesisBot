from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_risk_recovery import mt5_risk_recovery_status


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = mt5_risk_recovery_status(symbol=args.symbol, timeframe=args.timeframe)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok") else 1


def _human_summary(result: dict[str, Any]) -> str:
    blockers = result.get("blocker_source") if isinstance(result.get("blocker_source"), dict) else {}
    details = result.get("blocker_source_details") if isinstance(result.get("blocker_source_details"), dict) else {}
    metrics = result.get("current_metrics") if isinstance(result.get("current_metrics"), dict) else {}
    requirements = result.get("recovery_requirements") if isinstance(result.get("recovery_requirements"), dict) else {}
    explicit_sources = (requirements.get("clear_explicit_negative_edge_flag") or {}).get("sources") if isinstance(requirements.get("clear_explicit_negative_edge_flag"), dict) else []
    cooldown = requirements.get("cooldown_if_any") if isinstance(requirements.get("cooldown_if_any"), dict) else {}
    lines = [
        f"ETHUSD M30 RiskGovernor recovery diagnostics",
        f"symbol={result.get('symbol')} timeframe={result.get('timeframe')}",
        f"risk_governor_allowed={result.get('risk_governor_allowed')} reason={result.get('risk_governor_reason')} state={result.get('risk_state')}",
        f"recovery_status={result.get('recovery_status')}",
        "",
        "Why blocked:",
        _why_blocked(result, blockers),
        "",
        "Exact recent_edge_negative source:",
        f"- latest_performance_summary.negative_recent_edge={blockers.get('latest_performance_summary.negative_recent_edge')} source={details.get('performance_summary_source')}",
        f"- latest_adaptive_state.negative_edge={blockers.get('latest_adaptive_state.negative_edge')} source={details.get('adaptive_state_source')}",
        f"- computed_recent_pf_rule={blockers.get('computed_recent_pf_rule')} (recent_closed={metrics.get('recent_closed')}, PF={metrics.get('recent_profit_factor')}, expectancy={metrics.get('recent_expectancy')})",
        f"- active explicit sources={explicit_sources}",
        "",
        "Recovery:",
        f"- can_recover_automatically={not bool(result.get('indefinite_block_risk'))}",
        f"- cooldown={cooldown.get('value', 'none_detected_in_risk_governor')}",
        f"- indefinite_block_risk={result.get('indefinite_block_risk')}",
        f"- recovery_requirements={json.dumps(requirements, sort_keys=True, ensure_ascii=True)}",
        f"- recommended_action={result.get('recommended_action')}",
        "",
        "Safety:",
        f"- broker_touched={result.get('broker_touched')}",
        f"- order_executed={result.get('order_executed')}",
        f"- order_policy={result.get('order_policy')}",
        f"- applies_to_real_trading={result.get('applies_to_real_trading')}",
        f"- automatic_promotion={result.get('automatic_promotion')}",
    ]
    return "\n".join(lines)


def _why_blocked(result: dict[str, Any], blockers: dict[str, Any]) -> str:
    reason = str(result.get("risk_governor_reason") or "")
    if bool(result.get("risk_governor_allowed")):
        return "- RiskGovernor currently passes; keep observation only."
    if reason == "recent_edge_negative":
        if blockers.get("latest_performance_summary.negative_recent_edge") or blockers.get("latest_adaptive_state.negative_edge"):
            return "- Blocked because an explicit recent_edge_negative source is still active."
        if blockers.get("computed_recent_pf_rule"):
            return "- Blocked because recent_closed >= 10, recent PF < 1.0, and recent expectancy <= 0."
        return "- Blocked by RiskGovernor recent_edge_negative."
    return f"- Blocked by RiskGovernor reason={reason or 'unknown'}."


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only ETHUSD/M30 RiskGovernor recovery diagnostics.")
    parser.add_argument("--symbol", default="ETHUSD")
    parser.add_argument("--timeframe", default="M30")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
