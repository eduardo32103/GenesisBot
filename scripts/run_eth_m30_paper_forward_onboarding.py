from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.mt5.mt5_eth_m30_paper_forward_candidate import eth_m30_forward_profile_state  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect ETHUSD M30 paper-forward candidate onboarding state.")
    parser.add_argument("--symbol", default="ETHUSD")
    parser.add_argument("--timeframe", default="M30")
    parser.add_argument("--json", action="store_true", help="Print full JSON payload.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    state = eth_m30_forward_profile_state(symbol=args.symbol, timeframe=args.timeframe)
    if args.json:
        print(json.dumps(state, indent=2, sort_keys=True))
    else:
        print(f"symbol={state.get('symbol')}")
        print(f"timeframe={state.get('timeframe')}")
        print(f"profile={state.get('profile')}")
        print(f"status={state.get('status')}")
        print(f"active={state.get('active')}")
        print(f"applies_to_paper_shadow={state.get('applies_to_paper_shadow')}")
        print(f"applies_to_real_trading={state.get('applies_to_real_trading')}")
        print(f"reason={state.get('reason')}")
        print(f"recent_pf={(state.get('metadata') or {}).get('recent_pf')}")
        print(f"total_pf={(state.get('metadata') or {}).get('total_pf')}")
        print(f"capital_preservation_passed={(state.get('metadata') or {}).get('capital_preservation_passed')}")
        print(f"automatic_promotion={state.get('automatic_promotion')}")
        print(f"promoted_profile_mutated={state.get('promoted_profile_mutated')}")
        print(f"forward_state_mutated={state.get('forward_state_mutated')}")
        print(f"broker_touched={state.get('broker_touched')}")
        print(f"order_executed={state.get('order_executed')}")
        print(f"order_policy={state.get('order_policy')}")
    return 0 if state.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
