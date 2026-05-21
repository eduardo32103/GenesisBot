import argparse
import json
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-path", required=True)
    parser.add_argument("--symbol", default="BTCUSD")
    parser.add_argument("--timeframe", default="M30")
    parser.add_argument("--profile", default="quality_loose")
    parser.add_argument("--base", default="https://genesisbot-production.up.railway.app")
    parser.add_argument("--max-bars", type=int, default=500)
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()

    path = Path(args.csv_path)
    if not path.exists():
        print(f"CSV not found: {path}")
        sys.exit(1)

    csv_text = path.read_text(encoding="utf-8-sig", errors="replace")
    lines = csv_text.count("\n") + 1
    url = f"{args.base}/api/genesis/mt5/forward-replay/run"

    body = {
        "symbol": args.symbol,
        "timeframe": args.timeframe,
        "profile": args.profile,
        "csv_text": csv_text,
        "initial_balance": 100000,
        "spread_points": 30,
        "slippage_points": 5,
        "commission": 0,
        "max_bars": args.max_bars,
        "checkpoints": [10, 25, 50, 100],
        "persist": False,
    }

    data = json.dumps(body).encode("utf-8")

    print("Genesis forward replay Python runner")
    print(f"endpoint: {url}")
    print(f"csv_path: {path}")
    print(f"csv_size_bytes: {path.stat().st_size}")
    print(f"csv_lines: {lines}")
    print(f"max_bars: {args.max_bars}")
    print(f"timeout: {args.timeout}")
    print("posting...")

    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )

    started = time.time()
    try:
        with urllib.request.urlopen(req, timeout=args.timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        print(f"HTTP ERROR: {e.code}")
        print(e.read().decode("utf-8", errors="replace"))
        sys.exit(1)
    except Exception as e:
        print(f"REQUEST FAILED: {type(e).__name__}: {e}")
        sys.exit(1)

    print(f"done_ms: {round((time.time() - started) * 1000, 2)}")

    try:
        result = json.loads(raw)
    except Exception:
        print(raw)
        return

    summary_keys = [
        "status", "symbol", "timeframe", "profile", "bars_loaded",
        "total_trades", "closed", "wins", "losses", "win_rate",
        "profit_factor", "expectancy", "max_drawdown", "degraded",
        "degradation_reason", "broker_touched", "order_executed",
        "order_policy"
    ]

    print("\n===== SUMMARY =====")
    for key in summary_keys:
        if key in result:
            print(f"{key}: {result.get(key)}")

    print("\n===== CHECKPOINTS =====")
    checkpoints = result.get("checkpoints", [])
    if isinstance(checkpoints, list):
        for cp in checkpoints:
            print(cp)
    else:
        print(checkpoints)

    print("\n===== RAW JSON SAVED =====")
    out = Path("data/backtests/forward_replay_result.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(out)

if __name__ == "__main__":
    main()
