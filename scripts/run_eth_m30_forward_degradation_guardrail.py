from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_eth_m30_forward_degradation import eth_m30_forward_degradation_status
from services.mt5.mt5_risk_recovery import mt5_risk_recovery_status


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    guardrail = eth_m30_forward_degradation_status(symbol=args.symbol, timeframe=args.timeframe)
    recovery = mt5_risk_recovery_status(symbol=args.symbol, timeframe=args.timeframe)
    result = {**guardrail, "risk_governor_reason": recovery.get("risk_governor_reason") or guardrail.get("risk_governor_reason") or ""}
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok") else 1


def _human_summary(result: dict[str, Any]) -> str:
    lines = [
        "ETHUSD M30 paper-forward early degradation guardrail",
        f"symbol={result.get('symbol')} timeframe={result.get('timeframe')} profile={result.get('profile')}",
        f"current_status={result.get('current_status')}",
        f"new_status={result.get('new_status')}",
        f"open_shadow_count={result.get('open_shadow_count')}",
        f"trades_forward={result.get('trades_forward')}",
        f"wins={result.get('wins')}",
        f"losses={result.get('losses')}",
        f"win_rate={result.get('win_rate')}",
        f"profit_factor={result.get('profit_factor')}",
        f"expectancy={result.get('expectancy')}",
        f"risk_governor_reason={result.get('risk_governor_reason')}",
        f"recommendation={result.get('recommendation')}",
        f"degradation_reason={result.get('degradation_reason')}",
        f"whether_degradation_is_safe={result.get('whether_degradation_is_safe')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
        f"applies_to_real_trading={result.get('applies_to_real_trading')}",
        f"automatic_promotion={result.get('automatic_promotion')}",
        f"promoted_profile_mutated={result.get('promoted_profile_mutated')}",
    ]
    return "\n".join(lines)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only ETHUSD/M30 early forward degradation guardrail.")
    parser.add_argument("--symbol", default="ETHUSD")
    parser.add_argument("--timeframe", default="M30")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
