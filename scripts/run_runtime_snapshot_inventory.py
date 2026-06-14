from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_runtime_snapshot import runtime_snapshot_inventory  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = _fetch_remote(args) if args.base_url else runtime_snapshot_inventory(
        lookup_symbols=[args.symbol, args.broker_symbol],
        lookup_timeframe=args.timeframe,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True, default=str))
    else:
        print(_human_summary(result))
    return 0


def _human_summary(result: dict[str, Any]) -> str:
    return "\n".join(
        [
            "MT5 Runtime Snapshot Inventory",
            f"status={result.get('status')}",
            f"source={result.get('inventory_source') or 'local_process_memory'}",
            f"snapshot_count={result.get('snapshot_count')}",
            f"snapshot_keys={_json_line(result.get('snapshot_keys') or [])}",
            f"symbols_seen={_json_line(result.get('symbols_seen') or [])}",
            f"timeframes_seen_by_symbol={_json_line(result.get('timeframes_seen_by_symbol') or {})}",
            f"latest_tick_by_symbol={_json_line(result.get('latest_tick_by_symbol') or {})}",
            f"bars_count_by_symbol_timeframe={_json_line(result.get('bars_count_by_symbol_timeframe') or {})}",
            f"latest_bars_at_by_symbol_timeframe={_json_line(result.get('latest_bars_at_by_symbol_timeframe') or {})}",
            f"alias_map_used={_json_line(result.get('alias_map_used') or {})}",
            f"lookup_timeframe={result.get('lookup_timeframe')}",
            f"XAUUSD lookup result={_json_line(result.get('xauusd_lookup_result') or {})}",
            f"XAUUSD.b lookup result={_json_line(result.get('xauusd_b_lookup_result') or {})}",
            f"candidate_activated={result.get('candidate_activated')}",
            f"paper_shadow_created={result.get('paper_shadow_created')}",
            f"broker_touched={result.get('broker_touched')}",
            f"order_executed={result.get('order_executed')}",
            f"order_policy={result.get('order_policy')}",
        ]
    )


def _json_line(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":"))


def _fetch_remote(args: argparse.Namespace) -> dict[str, Any]:
    base = str(args.base_url or "").rstrip("/")
    query = urlencode({"symbol": args.symbol, "broker_symbol": args.broker_symbol, "timeframe": args.timeframe})
    url = f"{base}/api/genesis/mt5/runtime-snapshot/inventory?{query}"
    with urlopen(url, timeout=max(1.0, float(args.timeout_seconds or 10.0))) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if isinstance(payload, dict):
        payload["inventory_source"] = "remote_runtime_endpoint"
        return payload
    return {
        "ok": False,
        "status": "runtime_snapshot_inventory_invalid_remote_payload",
        "inventory_source": "remote_runtime_endpoint",
        "candidate_activated": False,
        "paper_shadow_created": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print MT5 runtime snapshot inventory without mutating runtime state.")
    parser.add_argument("--symbol", default="XAUUSD")
    parser.add_argument("--broker-symbol", default="XAUUSD.b")
    parser.add_argument("--timeframe", default="M15")
    parser.add_argument("--base-url", default="", help="Optional live Genesis base URL to inspect the web process memory.")
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
