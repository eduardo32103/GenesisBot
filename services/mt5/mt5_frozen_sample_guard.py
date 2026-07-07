from __future__ import annotations

from typing import Any


FROZEN_SAMPLE_GUARD_VERSION = "2026-07-05.frozen_sample_guard.v1"
TIME_STOP_REASONS = {"time_stop", "paper_timebox_exit"}


def evaluate_frozen_sample(trade: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(trade or {})
    symbol = str(payload.get("symbol") or "").upper().strip().replace(".B", "")
    entry = _number(payload.get("entry_price"))
    exit_price = _number(payload.get("exit_price") or payload.get("last_price"))
    pnl = _number(payload.get("pnl")) or 0.0
    exit_reason = str(payload.get("exit_reason") or "").strip()
    same_price = entry is not None and exit_price is not None and abs(float(entry) - float(exit_price)) <= 1e-12
    flat_pnl = abs(float(pnl)) <= 1e-12
    no_movement = bool(
        payload.get("market_inactive_or_frozen")
        or payload.get("no_price_movement")
        or payload.get("market_active") is False
        or str(payload.get("market_active") or "").casefold() == "false"
    )
    legacy_flat_xau_time_stop = (
        symbol == "XAUUSD"
        and same_price
        and flat_pnl
        and exit_reason in TIME_STOP_REASONS
        and not str(payload.get("price_source") or "").strip()
        and payload.get("market_active") is not True
    )
    frozen = same_price and flat_pnl and exit_reason in TIME_STOP_REASONS and (no_movement or legacy_flat_xau_time_stop)
    return {
        "ok": True,
        "guard_version": FROZEN_SAMPLE_GUARD_VERSION,
        "frozen_sample": bool(frozen),
        "sample_valid": not frozen,
        "invalid_reason": "market_inactive_or_frozen" if frozen else "",
        "metric_exclusion_reason": "excluded_from_winrate_frozen_market" if frozen else "",
        "use_for_winrate": not frozen,
        "use_for_optimization": not frozen,
        "use_for_calibration": not frozen,
        "strategy_promotion_eligible": not frozen,
        "candidate_promotion_eligible": not frozen,
        "entry_equals_exit": bool(same_price),
        "flat_pnl": bool(flat_pnl),
        "time_stop_exit": exit_reason in TIME_STOP_REASONS,
        "movement_sufficient": not no_movement,
        **_safety(),
    }


def _number(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
