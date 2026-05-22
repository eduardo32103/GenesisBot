from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen


DEFAULT_BASE_URL = "https://genesisbot-production.up.railway.app"
DEFAULT_OUTPUT_DIR = Path("data") / "backtests" / "multisymbol"
DEFAULT_ENDPOINT_TIMEOUT = 20

FetchFn = Callable[[str, int], dict[str, Any]]
SleepFn = Callable[[float], None]


def run_eth_m30_paper_forward_monitor(
    *,
    base_url: str = DEFAULT_BASE_URL,
    symbol: str = "ETHUSD",
    timeframe: str = "M30",
    interval_sec: float = 300.0,
    iterations: int = 12,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    timeout_sec: int = DEFAULT_ENDPOINT_TIMEOUT,
    fetcher: FetchFn | None = None,
    sleep_fn: SleepFn | None = None,
) -> dict[str, Any]:
    clean_symbol = _symbol(symbol or "ETHUSD")
    clean_timeframe = _timeframe(timeframe or "M30")
    safe_iterations = max(1, int(iterations or 1))
    safe_interval = max(0.0, float(interval_sec or 0.0))
    fetch = fetcher or fetch_json
    sleeper = sleep_fn or time.sleep
    snapshots: list[dict[str, Any]] = []

    for index in range(safe_iterations):
        snapshots.append(
            collect_eth_m30_paper_forward_snapshot(
                base_url=base_url,
                symbol=clean_symbol,
                timeframe=clean_timeframe,
                timeout_sec=timeout_sec,
                fetcher=fetch,
            )
        )
        if index < safe_iterations - 1 and safe_interval > 0:
            sleeper(safe_interval)

    summary = summarize_monitor_snapshots(snapshots, symbol=clean_symbol, timeframe=clean_timeframe)
    result = {
        "ok": True,
        "status": "eth_m30_paper_forward_monitor_completed",
        "base_url": _base_url(base_url),
        "symbol": clean_symbol,
        "timeframe": clean_timeframe,
        "iterations": safe_iterations,
        "interval_sec": safe_interval,
        "snapshots": snapshots,
        "summary": summary,
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        "applies_to_real_trading": False,
        **_safety(),
        "created_at": _now(),
    }
    paths = write_eth_m30_paper_forward_monitor_outputs(result, output_dir)
    result["output_paths"] = {name: str(path) for name, path in paths.items()}
    return result


def collect_eth_m30_paper_forward_snapshot(
    *,
    base_url: str = DEFAULT_BASE_URL,
    symbol: str = "ETHUSD",
    timeframe: str = "M30",
    timeout_sec: int = DEFAULT_ENDPOINT_TIMEOUT,
    fetcher: FetchFn | None = None,
) -> dict[str, Any]:
    clean_symbol = _symbol(symbol or "ETHUSD")
    clean_timeframe = _timeframe(timeframe or "M30")
    fetch = fetcher or fetch_json
    timestamp = _now()
    payloads: dict[str, dict[str, Any]] = {}
    errors: dict[str, str] = {}
    endpoints = _endpoints(base_url, clean_symbol, clean_timeframe)

    for name, url in endpoints.items():
        try:
            payloads[name] = fetch(url, timeout_sec)
        except Exception as exc:
            payloads[name] = {}
            errors[name] = _safe_error(exc)

    forward = payloads.get("forward_profile_state") or {}
    risk = payloads.get("risk_state") or {}
    decision = payloads.get("decision") or {}
    open_trades = payloads.get("shadow_trades_open") or {}
    health = payloads.get("health") or {}
    broker_touched = any(bool(item.get("broker_touched")) for item in payloads.values() if isinstance(item, dict))
    order_executed = any(bool(item.get("order_executed")) for item in payloads.values() if isinstance(item, dict))
    order_policy = _first_policy(payloads) or "journal_only_no_broker"

    return {
        "timestamp": timestamp,
        "symbol": clean_symbol,
        "timeframe": clean_timeframe,
        "ok": not errors and not broker_touched and not order_executed and order_policy == "journal_only_no_broker",
        "health_status": health.get("status") or "",
        "forward_status": forward.get("status") or "",
        "forward_profile": forward.get("profile") or "",
        "active": bool(forward.get("active")),
        "applies_to_paper_shadow": bool(forward.get("applies_to_paper_shadow")),
        "applies_to_real_trading": bool(forward.get("applies_to_real_trading")),
        "risk_state": risk.get("risk_state") or "",
        "risk_allowed": bool(risk.get("allowed")),
        "risk_reason": risk.get("reason") or "",
        "risk_governor_allowed": bool(decision.get("risk_governor_allowed", risk.get("allowed", False))),
        "risk_governor_reason": decision.get("risk_governor_reason") or risk.get("reason") or "",
        "decision": decision.get("decision") or "",
        "decision_reason": decision.get("reason") or "",
        "paper_forward_candidate_profile": decision.get("paper_forward_candidate_profile") or forward.get("profile") or "",
        "paper_forward_candidate_active": bool(decision.get("paper_forward_candidate_active") or forward.get("active")),
        "open_shadow_count": int(_number(open_trades.get("open_count")) or 0),
        "blocking_shadow_trade_id": decision.get("blocking_shadow_trade_id") or forward.get("blocking_shadow_trade_id") or "",
        "paper_exploration_created": bool(decision.get("paper_exploration_created")),
        "paper_exploration_reason": decision.get("paper_exploration_reason") or "",
        "endpoint_failures": len(errors),
        "endpoint_errors": errors,
        "degraded": bool(errors) or broker_touched or order_executed or order_policy != "journal_only_no_broker",
        "degradation_reason": _degradation_reason(errors, broker_touched, order_executed, order_policy),
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        "broker_touched": broker_touched,
        "order_executed": order_executed,
        "order_policy": order_policy,
        "raw": payloads,
    }


def summarize_monitor_snapshots(snapshots: list[dict[str, Any]], *, symbol: str = "ETHUSD", timeframe: str = "M30") -> dict[str, Any]:
    clean = [item for item in snapshots if isinstance(item, dict)]
    total = len(clean)
    active_count = sum(1 for item in clean if item.get("active"))
    no_snapshot_blocks = sum(1 for item in clean if item.get("decision_reason") == "no_runtime_snapshot_for_requested_timeframe")
    risk_blocks = sum(
        1
        for item in clean
        if str(item.get("decision_reason") or "").startswith("risk_governor_block")
        or (item.get("risk_governor_allowed") is False and item.get("risk_governor_reason") not in {"", "risk_governor_pass"})
    )
    endpoint_failures = sum(int(_number(item.get("endpoint_failures")) or 0) for item in clean)
    broker_attempts = sum(1 for item in clean if item.get("broker_touched"))
    orders = sum(1 for item in clean if item.get("order_executed"))
    max_open = max((int(_number(item.get("open_shadow_count")) or 0) for item in clean), default=0)
    paper_creates = sum(1 for item in clean if item.get("paper_exploration_created"))
    decision_counts = _counts(item.get("decision") or "UNKNOWN" for item in clean)
    reason_counts = _counts(item.get("decision_reason") or "unknown" for item in clean)
    status = _monitor_status(endpoint_failures, total, broker_attempts, orders, max_open)
    return {
        "symbol": _symbol(symbol),
        "timeframe": _timeframe(timeframe),
        "samples": total,
        "active_true_count": active_count,
        "decision_counts": decision_counts,
        "risk_governor_block_count": risk_blocks,
        "no_runtime_snapshot_count": no_snapshot_blocks,
        "paper_shadow_open_attempts_observed": paper_creates,
        "max_open_shadow_count": max_open,
        "shadow_open_count_latest": int(_number((clean[-1] if clean else {}).get("open_shadow_count")) or 0),
        "endpoint_failure_count": endpoint_failures,
        "broker_touched_count": broker_attempts,
        "order_executed_count": orders,
        "status": status,
        "recommendation": "observation_only" if status != "healthy_observation" else "continue_paper_forward_observation",
        "should_degrade_to_observation_only": status in {"degraded_api_failures", "unsafe_broker_or_order_detected", "open_trade_limit_violation"},
        "reason_counts": reason_counts,
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        "applies_to_real_trading": False,
        **_safety(),
        "updated_at": _now(),
    }


def write_eth_m30_paper_forward_monitor_outputs(result: dict[str, Any], output_dir: str | Path = DEFAULT_OUTPUT_DIR) -> dict[str, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    csv_path = root / "eth_m30_paper_forward_monitor_log.csv"
    json_path = root / "eth_m30_paper_forward_monitor_log.json"
    summary_path = root / "eth_m30_paper_forward_monitor_summary.md"
    rows = result.get("snapshots") if isinstance(result.get("snapshots"), list) else []
    headers = [
        "timestamp",
        "symbol",
        "timeframe",
        "forward_status",
        "forward_profile",
        "active",
        "risk_state",
        "risk_allowed",
        "risk_reason",
        "risk_governor_allowed",
        "risk_governor_reason",
        "decision",
        "decision_reason",
        "paper_forward_candidate_profile",
        "open_shadow_count",
        "broker_touched",
        "order_executed",
        "order_policy",
        "endpoint_failures",
        "degraded",
        "degradation_reason",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True), encoding="utf-8")
    summary_path.write_text(eth_m30_paper_forward_monitor_summary_markdown(result), encoding="utf-8")
    return {"csv": csv_path, "json": json_path, "summary": summary_path}


def eth_m30_paper_forward_monitor_summary_markdown(result: dict[str, Any]) -> str:
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    lines = [
        "# ETHUSD M30 Paper-Forward Monitor Summary",
        "",
        "Paper-only observation monitor for ETHUSD/M30. It only reads Genesis MT5 endpoints and writes local observation logs.",
        "",
        f"Samples: `{summary.get('samples', 0)}`.",
        f"Active true count: `{summary.get('active_true_count', 0)}`.",
        f"Decision counts: `{summary.get('decision_counts', {})}`.",
        f"RiskGovernor blocks: `{summary.get('risk_governor_block_count', 0)}`.",
        f"No runtime snapshot blocks: `{summary.get('no_runtime_snapshot_count', 0)}`.",
        f"Paper shadow open attempts observed: `{summary.get('paper_shadow_open_attempts_observed', 0)}`.",
        f"Latest open shadow count: `{summary.get('shadow_open_count_latest', 0)}`.",
        f"Endpoint failures: `{summary.get('endpoint_failure_count', 0)}`.",
        f"Monitor status: `{summary.get('status', '')}`.",
        f"Recommendation: `{summary.get('recommendation', '')}`.",
        "",
        "## Safety",
        f"- broker_touched=`{summary.get('broker_touched', False)}`",
        f"- order_executed=`{summary.get('order_executed', False)}`",
        f"- order_policy=`{summary.get('order_policy', 'journal_only_no_broker')}`",
        "- No real trading.",
        "- No order_send.",
        "- No promoted-profile real mutation.",
        "- No automatic promotion.",
    ]
    return "\n".join(lines) + "\n"


def fetch_json(url: str, timeout_sec: int = DEFAULT_ENDPOINT_TIMEOUT) -> dict[str, Any]:
    with urlopen(url, timeout=max(1, int(timeout_sec or DEFAULT_ENDPOINT_TIMEOUT))) as response:
        body = response.read().decode("utf-8")
    data = json.loads(body)
    return data if isinstance(data, dict) else {}


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_eth_m30_paper_forward_monitor(
        base_url=args.base_url,
        symbol=args.symbol,
        timeframe=args.timeframe,
        interval_sec=args.interval_sec,
        iterations=args.iterations,
        output_dir=args.output_dir,
        timeout_sec=args.timeout_sec,
    )
    print_monitor_result(result)
    return 0 if result.get("ok") else 1


def print_monitor_result(result: dict[str, Any]) -> None:
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    print(f"status={result.get('status')}")
    print(f"samples={summary.get('samples')}")
    print(f"active_true_count={summary.get('active_true_count')}")
    print(f"decision_counts={summary.get('decision_counts')}")
    print(f"risk_governor_block_count={summary.get('risk_governor_block_count')}")
    print(f"no_runtime_snapshot_count={summary.get('no_runtime_snapshot_count')}")
    print(f"endpoint_failure_count={summary.get('endpoint_failure_count')}")
    print(f"monitor_status={summary.get('status')}")
    print(f"recommendation={summary.get('recommendation')}")
    print(f"broker_touched={result.get('broker_touched')}")
    print(f"order_executed={result.get('order_executed')}")
    print(f"order_policy={result.get('order_policy')}")
    paths = result.get("output_paths") if isinstance(result.get("output_paths"), dict) else {}
    for name in ["csv", "json", "summary"]:
        if paths.get(name):
            print(f"{name}_path={paths[name]}")


def _endpoints(base_url: str, symbol: str, timeframe: str) -> dict[str, str]:
    root = _base_url(base_url)
    symbol_q = urlencode({"symbol": symbol})
    timeframe_q = urlencode({"symbol": symbol, "timeframe": timeframe})
    return {
        "health": f"{root}/api/genesis/mt5/health",
        "forward_profile_state": f"{root}/api/genesis/mt5/forward-profile-state?{timeframe_q}",
        "risk_state": f"{root}/api/genesis/mt5/risk-state?{timeframe_q}",
        "decision": f"{root}/api/genesis/mt5/decision?{timeframe_q}",
        "shadow_trades_open": f"{root}/api/genesis/mt5/shadow-trades/open?{symbol_q}",
    }


def _monitor_status(endpoint_failures: int, total: int, broker_attempts: int, orders: int, max_open: int) -> str:
    if broker_attempts or orders:
        return "unsafe_broker_or_order_detected"
    if max_open > 1:
        return "open_trade_limit_violation"
    if total and endpoint_failures >= max(3, total * 2):
        return "degraded_api_failures"
    if endpoint_failures:
        return "partial_endpoint_failures"
    return "healthy_observation"


def _degradation_reason(errors: dict[str, str], broker_touched: bool, order_executed: bool, order_policy: str) -> str:
    if broker_touched:
        return "broker_touched_detected"
    if order_executed:
        return "order_executed_detected"
    if order_policy != "journal_only_no_broker":
        return "order_policy_not_paper_only"
    if errors:
        return "endpoint_failure"
    return ""


def _first_policy(payloads: dict[str, dict[str, Any]]) -> str:
    for payload in payloads.values():
        policy = str((payload or {}).get("order_policy") or "").strip()
        if policy:
            return policy
    return ""


def _safe_error(exc: Exception) -> str:
    if isinstance(exc, HTTPError):
        return f"http_error:{exc.code}"
    if isinstance(exc, URLError):
        return f"url_error:{exc.reason}"
    return f"{type(exc).__name__}:{exc}"


def _counts(values: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        key = str(value or "unknown")
        counts[key] = counts.get(key, 0) + 1
    return counts


def _base_url(value: str) -> str:
    return str(value or DEFAULT_BASE_URL).strip().rstrip("/")


def _symbol(value: object) -> str:
    return str(value or "").upper().strip()


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _number(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ETHUSD M30 paper-forward observation monitor.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--symbol", default="ETHUSD")
    parser.add_argument("--timeframe", default="M30")
    parser.add_argument("--interval-sec", type=float, default=300.0)
    parser.add_argument("--iterations", type=int, default=12)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--timeout-sec", type=int, default=DEFAULT_ENDPOINT_TIMEOUT)
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
