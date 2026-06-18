from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_xau_m15_runtime_open_shadow_backfill import run_xau_m15_runtime_open_shadow_backfill


DEFAULT_SNAPSHOT = Path("data/research_outputs/xau_m15_runtime_open_shadow_snapshot_for_backfill.json")


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill one XAUUSD M15 runtime-open paper shadow into Persistent Intelligence.")
    parser.add_argument("--base-url", default="", help="Optional live Genesis base URL. If set, POSTs the snapshot to the live process.")
    parser.add_argument("--snapshot-file", default=str(DEFAULT_SNAPSHOT))
    parser.add_argument("--confirm-paper-only-backfill", action="store_true")
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    snapshot_path = Path(args.snapshot_file)
    snapshot = _load_snapshot_file(snapshot_path)
    if args.base_url:
        result = _post_live_backfill(
            args.base_url,
            snapshot=snapshot,
            confirm=bool(args.confirm_paper_only_backfill),
            timeout_seconds=args.timeout_seconds,
        )
    else:
        result = run_xau_m15_runtime_open_shadow_backfill(
            snapshot=snapshot,
            snapshot_file=snapshot_path,
            confirm_paper_only_backfill=bool(args.confirm_paper_only_backfill),
        )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print("MT5 XAUUSD M15 Runtime Open Shadow Backfill")
        for key in (
            "status",
            "payload_valid",
            "applied",
            "shadow_trade_id",
            "existing_shadow_found",
            "rows_written",
            "rows_updated",
            "persistent_open_ready",
            "duplicate_prevented",
            "reason",
            "broker_touched",
            "order_executed",
            "order_policy",
        ):
            print(f"{key}={result.get(key)}")
    return 0 if result.get("ok") else 1


def _post_live_backfill(base_url: str, *, snapshot: dict, confirm: bool, timeout_seconds: float) -> dict:
    url = f"{str(base_url).rstrip('/')}/api/genesis/mt5/shadow-trades/runtime-open/backfill"
    payload = {
        "confirm_paper_only_backfill": bool(confirm),
        "snapshot": snapshot,
    }
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=utf-8", "Accept": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=max(1.0, float(timeout_seconds or 20.0))) as response:
        data = json.loads(response.read().decode("utf-8"))
    return data if isinstance(data, dict) else {"ok": False, "reason": "invalid_http_payload"}


def _load_snapshot_file(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    return data if isinstance(data, dict) else {}


if __name__ == "__main__":
    raise SystemExit(main())
