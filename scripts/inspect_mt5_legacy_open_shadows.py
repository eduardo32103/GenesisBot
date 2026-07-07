from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_legacy_shadow_inspector import inspect_legacy_open_shadows  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = inspect_legacy_open_shadows(
        limit=args.limit,
        status=args.status,
        require_live_db=args.require_live_db,
        redact_ids=args.redact_ids,
        include_sensitive_ids=args.include_sensitive_ids,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok") else 1


def _human_summary(result: dict[str, Any]) -> str:
    lines = [
        "MT5 Legacy Open Shadow Inspector",
        f"status={result.get('status')}",
        f"source_name={result.get('source_name')}",
        f"backend_type={result.get('backend_type')}",
        f"live_db_required={result.get('live_db_required')}",
        f"live_db_detected={result.get('live_db_detected')}",
        f"source_matches_capital_protection={result.get('source_matches_capital_protection')}",
        f"source_fingerprint={result.get('source_fingerprint')}",
        f"limit_used={result.get('limit_used')}",
        f"effective_fetch_limit={result.get('effective_fetch_limit')}",
        f"status_filter={result.get('status_filter')}",
        f"open_shadow_trades_count={result.get('open_shadow_trades_count')}",
        f"symbols_included={result.get('symbols_included')}",
        f"oldest_open_at={result.get('oldest_open_at')}",
        f"newest_open_at={result.get('newest_open_at')}",
        f"recommendation={result.get('recommendation')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
        "",
        "Records sample:",
    ]
    sample = result.get("records_sample") if isinstance(result.get("records_sample"), list) else []
    if not sample:
        lines.append("- none")
    for row in sample[:20]:
        if not isinstance(row, dict):
            continue
        lines.append(
            f"- {row.get('shadow_trade_id')} {row.get('symbol')} {row.get('timeframe')} "
            f"{row.get('profile')} status={row.get('status')} close_record={row.get('has_matching_close_record')}"
        )
    return "\n".join(lines)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only inspection of legacy MT5 open shadows used by Capital Protection.")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--status", default="open")
    parser.add_argument("--require-live-db", action="store_true")
    parser.add_argument("--redact-ids", action="store_true", default=True)
    parser.add_argument("--include-sensitive-ids", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
