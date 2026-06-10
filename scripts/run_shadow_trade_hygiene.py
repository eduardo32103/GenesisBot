from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_shadow_trade_hygiene import run_shadow_trade_hygiene


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_shadow_trade_hygiene(
        max_open_shadow_trades=args.max_open_shadow_trades,
        max_profile_open_shadows=args.max_profile_open_shadows,
        stale_hours=args.stale_hours,
        load_shadow_snapshot=not args.no_shadow_snapshot,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok") else 1


def _human_summary(result: dict[str, Any]) -> str:
    lines = [
        "MT5 Shadow Trade Hygiene",
        f"mode={result.get('mode')}",
        f"open_shadow_trades={result.get('open_shadow_trades')}",
        f"safe_to_open_new_shadow={result.get('safe_to_open_new_shadow')}",
        f"recommended_cleanup_action={result.get('recommended_cleanup_action')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
        "",
        "Stale shadow trades:",
    ]
    lines.extend(_item_lines(result.get("stale_shadow_trades") or [], "shadow_trade_id"))
    lines.extend(["", "Duplicate shadow clusters:"])
    lines.extend(_item_lines(result.get("duplicate_shadow_clusters") or [], "shadow_trade_ids"))
    lines.extend(["", "Profiles with too many open shadows:"])
    lines.extend(_item_lines(result.get("profiles_with_too_many_open_shadows") or [], "shadow_trade_ids"))
    return "\n".join(lines)


def _item_lines(rows: list[dict[str, Any]], id_key: str) -> list[str]:
    if not rows:
        return ["- none"]
    lines: list[str] = []
    for row in rows:
        lines.append(
            f"- {row.get('symbol')} {row.get('timeframe')} {row.get('profile')} "
            f"open_count={row.get('open_count', '')} {id_key}={row.get(id_key)}"
        )
    return lines


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Report shadow-trade hygiene without closing or deleting trades.")
    parser.add_argument("--max-open-shadow-trades", type=int, default=3)
    parser.add_argument("--max-profile-open-shadows", type=int, default=1)
    parser.add_argument("--stale-hours", type=float, default=12.0)
    parser.add_argument("--no-shadow-snapshot", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
