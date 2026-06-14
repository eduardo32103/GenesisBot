from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_xau_m15_paper_observation_readiness import (  # noqa: E402
    run_xau_m15_paper_observation_cycle,
)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = _fetch_remote(args) if args.base_url else run_xau_m15_paper_observation_cycle(paper_shadow_once=args.paper_shadow_once)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True, default=str))
    else:
        print(_human_summary(result))
    return 0


def _human_summary(result: dict[str, Any]) -> str:
    readiness = result.get("readiness") if isinstance(result.get("readiness"), dict) else {}
    return "\n".join(
        [
            "MT5 XAUUSD M15 Paper Observation Cycle",
            f"status={result.get('status')}",
            f"mode={result.get('mode')}",
            f"symbol={result.get('symbol')}",
            f"broker_symbol={result.get('broker_symbol')}",
            f"timeframe={result.get('timeframe')}",
            f"candidate_profile={result.get('candidate_profile')}",
            f"paper_shadow_once_requested={result.get('paper_shadow_once_requested')}",
            f"readiness_state={result.get('readiness_state')}",
            f"recommendation={result.get('recommendation')}",
            f"reason={result.get('reason')}",
            f"hypothetical_signal={_json_line(result.get('hypothetical_signal') or {})}",
            f"runtime_context_available={readiness.get('runtime_context_available')}",
            f"runtime_snapshot_context={readiness.get('runtime_snapshot_context')}",
            f"symbol_alias_used={readiness.get('symbol_alias_used')}",
            f"latest_tick_at={readiness.get('latest_tick_at')}",
            f"latest_bars_at={readiness.get('latest_bars_at')}",
            f"bars_count={readiness.get('bars_count')}",
            f"paper_shadow_created={result.get('paper_shadow_created')}",
            f"shadow_trade_id={result.get('shadow_trade_id')}",
            f"candidate_activated={result.get('candidate_activated')}",
            f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
            f"broker_touched={result.get('broker_touched')}",
            f"order_executed={result.get('order_executed')}",
            f"order_policy={result.get('order_policy')}",
        ]
    )


def _json_line(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":"))


def _fetch_remote(args: argparse.Namespace) -> dict[str, Any]:
    base = str(args.base_url or "").rstrip("/")
    url = f"{base}/api/genesis/mt5/xau-m15/paper-observation/cycle"
    with urlopen(url, timeout=max(1.0, float(args.timeout_seconds or 10.0))) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if isinstance(payload, dict):
        payload["cycle_source"] = "remote_live_http_process"
        return payload
    return {
        "ok": False,
        "status": "xau_m15_cycle_invalid_remote_payload",
        "mode": "dry_run",
        "cycle_source": "remote_live_http_process",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "paper_shadow_created": False,
        "shadow_trade_id": "",
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dry-run one XAUUSD M15 paper observation cycle.")
    parser.add_argument("--paper-shadow-once", action="store_true", help="Future explicit mode; currently blocked pending human approval.")
    parser.add_argument("--base-url", default="", help="Optional live Genesis base URL to inspect the web process memory.")
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
