from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_xau_m15_paper_shadow_monitor import run_xau_m15_paper_shadow_monitor  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = _fetch_remote(args) if args.base_url else run_xau_m15_paper_shadow_monitor(apply_paper_close=args.apply_paper_close)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True, default=str))
    else:
        print(_human_summary(result))
    return 0


def _human_summary(result: dict[str, Any]) -> str:
    return "\n".join(
        [
            "MT5 XAUUSD M15 Paper Shadow Monitor",
            f"status={result.get('status')}",
            f"monitor_state={result.get('monitor_state')}",
            f"symbol={result.get('symbol')}",
            f"broker_symbol={result.get('broker_symbol')}",
            f"timeframe={result.get('timeframe')}",
            f"candidate_profile={result.get('candidate_profile')}",
            f"shadow_source={result.get('shadow_source')}",
            f"open_shadow_count={result.get('open_shadow_count')}",
            f"shadow_trade_id={result.get('shadow_trade_id')}",
            f"side={result.get('side')}",
            f"entry_price={result.get('entry_price')}",
            f"current_price={result.get('current_price')}",
            f"stop_loss={result.get('stop_loss')}",
            f"take_profit={result.get('take_profit')}",
            f"unrealized_pnl={result.get('unrealized_pnl')}",
            f"unrealized_pnl_pct={result.get('unrealized_pnl_pct')}",
            f"r_multiple={result.get('r_multiple')}",
            f"age_minutes={result.get('age_minutes')}",
            f"bars_since_entry={result.get('bars_since_entry')}",
            f"exit_signal={result.get('exit_signal')}",
            f"exit_reason={result.get('exit_reason')}",
            f"safety_exit_triggered={result.get('safety_exit_triggered')}",
            f"safety_exit_category={result.get('safety_exit_category')}",
            f"safety_exit_reason_detail={result.get('safety_exit_reason_detail')}",
            f"risk_block_type={result.get('risk_block_type')}",
            f"risk_block_applies_to_current_shadow={result.get('risk_block_applies_to_current_shadow')}",
            f"max_open_trades_limit={result.get('max_open_trades_limit')}",
            f"should_close_paper={result.get('should_close_paper')}",
            f"should_watch_only={result.get('should_watch_only')}",
            f"close_decision_reason={result.get('close_decision_reason')}",
            f"paper_close_applied={result.get('paper_close_applied')}",
            f"shadow_status_after={result.get('shadow_status_after')}",
            f"candidate_activated={result.get('candidate_activated')}",
            f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
            f"broker_touched={result.get('broker_touched')}",
            f"order_executed={result.get('order_executed')}",
            f"order_policy={result.get('order_policy')}",
        ]
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor one XAUUSD M15 paper shadow. Dry-run by default.")
    parser.add_argument("--apply-paper-close", action="store_true", help="Apply a paper-only close if the existing shadow has a valid exit signal.")
    parser.add_argument("--base-url", default="", help="Optional live Genesis base URL to monitor the web process runtime.")
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def _fetch_remote(args: argparse.Namespace) -> dict[str, Any]:
    base = str(args.base_url or "").rstrip("/")
    url = f"{base}/api/genesis/mt5/xau-m15/paper-shadow/monitor"
    if args.apply_paper_close:
        body = json.dumps({"apply_paper_close": True}).encode("utf-8")
        request = Request(
            url,
            data=body,
            headers={"Content-Type": "application/json; charset=utf-8", "Accept": "application/json"},
            method="POST",
        )
    else:
        request = Request(url, headers={"Accept": "application/json"}, method="GET")
    with urlopen(request, timeout=max(1.0, float(args.timeout_seconds or 10.0))) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if isinstance(payload, dict):
        payload["monitor_source"] = "remote_live_http_process"
        return payload
    return {
        "ok": False,
        "status": "xau_m15_paper_shadow_monitor_invalid_remote_payload",
        "monitor_state": "remote_error",
        "open_shadow_count": 0,
        "paper_close_applied": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


if __name__ == "__main__":
    raise SystemExit(main())
