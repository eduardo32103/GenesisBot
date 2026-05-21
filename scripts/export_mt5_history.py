from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


TIMEFRAMES: dict[str, str] = {
    "M1": "TIMEFRAME_M1",
    "M5": "TIMEFRAME_M5",
    "M15": "TIMEFRAME_M15",
    "M30": "TIMEFRAME_M30",
    "H1": "TIMEFRAME_H1",
    "H4": "TIMEFRAME_H4",
    "D1": "TIMEFRAME_D1",
}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export MT5 historical rates to CSV for Genesis paper backtests. Read-only: no orders, no credentials."
    )
    parser.add_argument("--symbol", default="BTCUSD", help="MT5 symbol to export, default BTCUSD.")
    parser.add_argument("--timeframe", default="H1", help="Timeframe: M1, M5, M15, M30, H1, H4, D1. Default H1.")
    parser.add_argument("--bars", type=int, default=3000, help="Number of historical bars to request. Default 3000.")
    parser.add_argument("--output", default="data/backtests/BTCUSD_H1.csv", help="Output CSV path.")
    args = parser.parse_args()

    symbol = str(args.symbol or "BTCUSD").strip()
    if not symbol:
        symbol = "BTCUSD"
    timeframe_name = str(args.timeframe or "H1").upper().strip()
    bars = max(1, int(args.bars or 3000))
    output = Path(args.output)

    try:
        import MetaTrader5 as mt5  # type: ignore
    except ImportError:
        print(
            "ERROR: Python package 'MetaTrader5' is not installed. Install it with: pip install MetaTrader5",
            file=sys.stderr,
        )
        return 2

    timeframe_attr = TIMEFRAMES.get(timeframe_name)
    if not timeframe_attr or not hasattr(mt5, timeframe_attr):
        print(f"ERROR: Unsupported timeframe '{timeframe_name}'. Use one of: {', '.join(TIMEFRAMES)}", file=sys.stderr)
        return 2
    timeframe = getattr(mt5, timeframe_attr)

    print("Genesis MT5 history export: read-only rates request. No orders. No credentials.")
    if not mt5.initialize():
        code, message = mt5.last_error()
        print(
            "ERROR: Could not connect to MetaTrader 5 terminal. "
            "Open MT5 locally, log into your demo account in the terminal, then run again. "
            f"MT5 last_error={code} {message}",
            file=sys.stderr,
        )
        return 3

    try:
        selected = mt5.symbol_select(symbol, True)
        if not selected:
            code, message = mt5.last_error()
            print(f"ERROR: Could not select symbol '{symbol}'. MT5 last_error={code} {message}", file=sys.stderr)
            return 4

        rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, bars)
        if rates is None or len(rates) == 0:
            code, message = mt5.last_error()
            print(
                f"ERROR: No historical rates returned for {symbol} {timeframe_name}. "
                f"Check Market Watch/history download. MT5 last_error={code} {message}",
                file=sys.stderr,
            )
            return 5

        rows = [_rate_to_row(rate) for rate in rates]
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=["time", "open", "high", "low", "close", "volume"])
            writer.writeheader()
            writer.writerows(rows)

        print(f"OK: exported {len(rows)} bars for {symbol} {timeframe_name} to {output}")
        return 0
    finally:
        mt5.shutdown()


def _rate_to_row(rate: Any) -> dict[str, Any]:
    timestamp = int(_field(rate, "time") or 0)
    volume = _field(rate, "tick_volume")
    if volume is None:
        volume = _field(rate, "real_volume") or 0
    return {
        "time": datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat(),
        "open": _field(rate, "open"),
        "high": _field(rate, "high"),
        "low": _field(rate, "low"),
        "close": _field(rate, "close"),
        "volume": volume,
    }


def _field(rate: Any, name: str) -> Any:
    try:
        return rate[name]
    except Exception:
        return getattr(rate, name, None)


if __name__ == "__main__":
    raise SystemExit(main())
