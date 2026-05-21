from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.mt5.mt5_symbol_cost_model import ALIAS_PATTERNS, discover_alias  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="List and resolve MT5 available symbols. Read-only: no orders, no credentials.")
    parser.add_argument("--symbols", default="BTCUSD,ETHUSD,XAUUSD,NAS100,US500,EURUSD,GBPUSD")
    parser.add_argument("--output-json", default="")
    parser.add_argument("--output-csv", default="")
    args = parser.parse_args(argv)

    try:
        import MetaTrader5 as mt5  # type: ignore
    except ImportError:
        print("ERROR: Python package 'MetaTrader5' is not installed. Install it with: pip install MetaTrader5", file=sys.stderr)
        return 2

    print("Genesis MT5 symbol discovery: read-only symbols_get. No orders. No credentials.")
    if not mt5.initialize():
        code, message = mt5.last_error()
        print(f"ERROR: Could not connect to MetaTrader 5 terminal. MT5 last_error={code} {message}", file=sys.stderr)
        return 3

    try:
        symbols = mt5.symbols_get()
        if symbols is None:
            code, message = mt5.last_error()
            print(f"ERROR: mt5.symbols_get failed. MT5 last_error={code} {message}", file=sys.stderr)
            return 4
        available = [str(getattr(symbol, "name", "") or "") for symbol in symbols if getattr(symbol, "name", "")]
        requested = [part.strip().upper() for part in str(args.symbols or "").split(",") if part.strip()]
        rows = []
        for item in requested:
            match = discover_alias(item, available)
            rows.append(
                {
                    **match,
                    "patterns_checked": ",".join(ALIAS_PATTERNS.get(item, [item])),
                    "available_count": len(available),
                    "broker_touched": False,
                    "order_executed": False,
                    "order_policy": "journal_only_no_broker",
                }
            )
        _print_rows(rows)
        if args.output_json:
            Path(args.output_json).parent.mkdir(parents=True, exist_ok=True)
            Path(args.output_json).write_text(json.dumps({"ok": True, "rows": rows}, indent=2, ensure_ascii=True), encoding="utf-8")
        if args.output_csv:
            _write_csv(rows, Path(args.output_csv))
        return 0
    finally:
        mt5.shutdown()


def _print_rows(rows: list[dict[str, Any]]) -> None:
    if not rows:
        print("No requested symbols.")
        return
    headers = ["requested_symbol", "resolved_symbol", "status", "matched_alias"]
    widths = {header: len(header) for header in headers}
    for row in rows:
        for header in headers:
            widths[header] = max(widths[header], len(str(row.get(header, ""))))
    print(" | ".join(header.ljust(widths[header]) for header in headers))
    print("-+-".join("-" * widths[header] for header in headers))
    for row in rows:
        print(" | ".join(str(row.get(header, "")).ljust(widths[header]) for header in headers))


def _write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = [
        "requested_symbol",
        "resolved_symbol",
        "status",
        "matched_alias",
        "patterns_checked",
        "available_count",
        "broker_touched",
        "order_executed",
        "order_policy",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    raise SystemExit(main())
