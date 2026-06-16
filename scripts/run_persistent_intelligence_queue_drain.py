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

from services.mt5.mt5_persistent_intelligence_store import persistent_intelligence_queue_drain  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.base_url:
        result = _post_live_queue_drain(
            args.base_url,
            max_items=args.max_items,
            keep_failed_noncritical=args.keep_failed_noncritical,
            timeout_seconds=args.timeout_seconds,
        )
    else:
        result = persistent_intelligence_queue_drain(
            max_items=args.max_items,
            drop_failed_noncritical=not args.keep_failed_noncritical,
        )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True, default=str))
    else:
        print(_human_summary(result))
    return 0


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Drain queued Persistent Intelligence writes safely. No broker, no trading.")
    parser.add_argument("--base-url", default="", help="Optional live Genesis base URL. Drains the web-process queue through HTTP.")
    parser.add_argument("--max-items", type=int, default=50)
    parser.add_argument("--keep-failed-noncritical", action="store_true", help="Keep failed noncritical queued writes instead of dropping them.")
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def _post_live_queue_drain(
    base_url: str,
    *,
    max_items: int,
    keep_failed_noncritical: bool,
    timeout_seconds: float,
) -> dict[str, Any]:
    target = base_url.rstrip("/") + "/api/genesis/mt5/persistent-intelligence/queue-drain"
    payload = {
        "confirm_queue_drain": True,
        "max_items": int(max_items or 50),
        "keep_failed_noncritical": bool(keep_failed_noncritical),
    }
    request = Request(
        target,
        data=json.dumps(payload, ensure_ascii=True).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=float(timeout_seconds or 15.0)) as response:
        body = response.read()
    try:
        result = json.loads(body.decode("utf-8")) if body else {}
    except json.JSONDecodeError:
        result = {"ok": False, "status": "persistent_intelligence_queue_drain_http_parse_failed"}
    result.setdefault("queue_drain_source", "live_http_web_process")
    return result


def _human_summary(result: dict[str, Any]) -> str:
    drain = result.get("drain") if isinstance(result.get("drain"), dict) else {}
    health = result.get("healthcheck") if isinstance(result.get("healthcheck"), dict) else {}
    return "\n".join(
        [
            "MT5 Persistent Intelligence Queue Drain",
            f"status={result.get('status')}",
            f"provider={result.get('provider')}",
            f"drain_attempted={result.get('drain_attempted')}",
            f"queue_depth_before={result.get('queue_depth_before', drain.get('before_queue_depth', ((result.get('before') if isinstance(result.get('before'), dict) else {}) or {}).get('queue_depth')))}",
            f"queue_depth_after={result.get('queue_depth_after', drain.get('after_queue_depth', result.get('queue_depth')))}",
            f"queued_writes_before={result.get('queued_writes_before')}",
            f"queued_writes_after={result.get('queued_writes_after')}",
            f"attempted={drain.get('attempted')}",
            f"succeeded={drain.get('succeeded')}",
            f"failed={drain.get('failed')}",
            f"dropped_noncritical_writes_this_drain={result.get('dropped_noncritical_writes_this_drain', drain.get('dropped_noncritical_writes'))}",
            f"critical_writes_retained={result.get('critical_writes_retained', drain.get('critical_writes_retained'))}",
            f"queue_depth={result.get('queue_depth')}",
            f"queued_writes={result.get('queued_writes')}",
            f"queued_writes_total={result.get('queued_writes_total')}",
            f"failed_writes={result.get('failed_writes')}",
            f"dropped_noncritical_writes={result.get('dropped_noncritical_writes')}",
            f"last_queue_drain_attempt_at={result.get('last_queue_drain_attempt_at')}",
            f"queue_drain_succeeded={result.get('queue_drain_succeeded')}",
            f"queue_drain_failed_count={result.get('queue_drain_failed_count')}",
            f"db_available={health.get('db_available')}",
            f"db_degraded={health.get('db_degraded')}",
            f"tables_ready={health.get('tables_ready')}",
            f"recommendation={health.get('recommendation')}",
            f"decision={result.get('decision')}",
            f"reason={result.get('reason')}",
            f"candidate_activated={result.get('candidate_activated')}",
            f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
            f"broker_touched={result.get('broker_touched')}",
            f"order_executed={result.get('order_executed')}",
            f"order_policy={result.get('order_policy')}",
        ]
    )


if __name__ == "__main__":
    raise SystemExit(main())
