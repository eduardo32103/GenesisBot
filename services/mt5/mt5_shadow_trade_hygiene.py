from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any


DEFAULT_MAX_OPEN_SHADOW_TRADES = 3
DEFAULT_MAX_PROFILE_OPEN_SHADOWS = 1
DEFAULT_STALE_HOURS = 12.0


def run_shadow_trade_hygiene(
    *,
    open_trades: list[dict[str, Any]] | None = None,
    max_open_shadow_trades: int = DEFAULT_MAX_OPEN_SHADOW_TRADES,
    max_profile_open_shadows: int = DEFAULT_MAX_PROFILE_OPEN_SHADOWS,
    stale_hours: float = DEFAULT_STALE_HOURS,
    load_shadow_snapshot: bool = True,
) -> dict[str, Any]:
    trades = list(open_trades if open_trades is not None else _load_open_trades(load_shadow_snapshot))
    duplicate_clusters = _duplicate_clusters(trades)
    profile_clusters = _profiles_with_too_many_open_shadows(trades, max_profile_open_shadows)
    stale = _stale_trades(trades, stale_hours)
    safe_to_open = len(trades) <= int(max_open_shadow_trades)
    recommended_cleanup_action = _recommended_cleanup_action(
        open_count=len(trades),
        safe_to_open=safe_to_open,
        stale=stale,
        duplicate_clusters=duplicate_clusters,
        profile_clusters=profile_clusters,
    )
    return {
        "ok": True,
        "status": "shadow_trade_hygiene_ready",
        "mode": "report_only_no_auto_close",
        "open_shadow_trades": len(trades),
        "stale_shadow_trades": stale,
        "duplicate_shadow_clusters": duplicate_clusters,
        "profiles_with_too_many_open_shadows": profile_clusters,
        "safe_to_open_new_shadow": safe_to_open,
        "recommended_cleanup_action": recommended_cleanup_action,
        "shadow_trades_mutated": False,
        "closed_shadow_trades": False,
        "deleted_shadow_trades": False,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        **_safety(),
    }


def _load_open_trades(load_shadow_snapshot: bool) -> list[dict[str, Any]]:
    if not load_shadow_snapshot:
        return []
    try:
        from services.mt5.mt5_shadow_trading import MT5ShadowTrading

        snapshot = MT5ShadowTrading().snapshot(limit=500)
        return [row for row in snapshot.get("open_trades") or [] if isinstance(row, dict)]
    except Exception:
        return []


def _duplicate_clusters(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        key = (
            _symbol(trade.get("symbol")),
            _timeframe(trade.get("timeframe")),
            str(trade.get("strategy_profile") or trade.get("profile") or trade.get("family") or "").strip(),
            str(trade.get("side") or trade.get("action") or "").lower().strip(),
        )
        grouped[key].append(trade)
    clusters: list[dict[str, Any]] = []
    for (symbol, timeframe, profile, side), rows in grouped.items():
        if len(rows) <= 1:
            continue
        clusters.append(
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "profile": profile,
                "side": side,
                "open_count": len(rows),
                "shadow_trade_ids": _trade_ids(rows),
                **_safety(),
            }
        )
    clusters.sort(key=lambda row: (-int(row["open_count"]), row["symbol"], row["profile"]))
    return clusters


def _profiles_with_too_many_open_shadows(trades: list[dict[str, Any]], max_profile_open_shadows: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        key = (
            _symbol(trade.get("symbol")),
            _timeframe(trade.get("timeframe")),
            str(trade.get("strategy_profile") or trade.get("profile") or trade.get("family") or "").strip(),
        )
        grouped[key].append(trade)
    rows: list[dict[str, Any]] = []
    for (symbol, timeframe, profile), items in grouped.items():
        if len(items) <= int(max_profile_open_shadows):
            continue
        rows.append(
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "profile": profile,
                "open_count": len(items),
                "max_profile_open_shadows": int(max_profile_open_shadows),
                "shadow_trade_ids": _trade_ids(items),
                **_safety(),
            }
        )
    rows.sort(key=lambda row: (-int(row["open_count"]), row["symbol"], row["profile"]))
    return rows


def _stale_trades(trades: list[dict[str, Any]], stale_hours: float) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    stale: list[dict[str, Any]] = []
    for trade in trades:
        opened = _parse_time(trade.get("opened_at") or trade.get("created_at") or trade.get("timestamp"))
        if opened is None:
            continue
        age_hours = (now - opened).total_seconds() / 3600.0
        if age_hours < float(stale_hours):
            continue
        stale.append(
            {
                "symbol": _symbol(trade.get("symbol")),
                "timeframe": _timeframe(trade.get("timeframe")),
                "profile": str(trade.get("strategy_profile") or trade.get("profile") or trade.get("family") or "").strip(),
                "shadow_trade_id": str(trade.get("shadow_trade_id") or trade.get("trade_id") or ""),
                "opened_at": str(trade.get("opened_at") or ""),
                "age_hours": round(age_hours, 4),
                **_safety(),
            }
        )
    stale.sort(key=lambda row: -float(row["age_hours"]))
    return stale


def _recommended_cleanup_action(
    *,
    open_count: int,
    safe_to_open: bool,
    stale: list[dict[str, Any]],
    duplicate_clusters: list[dict[str, Any]],
    profile_clusters: list[dict[str, Any]],
) -> str:
    if not safe_to_open:
        return "review_open_shadow_over_limit_before_new_entries"
    if duplicate_clusters:
        return "review_duplicate_shadow_clusters"
    if profile_clusters:
        return "review_profiles_with_too_many_open_shadows"
    if stale:
        return "review_stale_shadow_trades"
    if open_count == 0:
        return "no_open_shadow_cleanup_needed"
    return "monitor_open_shadow_trades"


def _trade_ids(trades: list[dict[str, Any]]) -> list[str]:
    return [str(row.get("shadow_trade_id") or row.get("trade_id") or "") for row in trades if str(row.get("shadow_trade_id") or row.get("trade_id") or "").strip()]


def _parse_time(value: object) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _symbol(value: object) -> str:
    return str(value or "").upper().strip().replace(".B", "")


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
