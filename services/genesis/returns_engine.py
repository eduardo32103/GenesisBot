from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

_RANGES = ("1D", "1W", "1M", "1Y", "5Y", "MAX")


def calculate_return_detail(
    points: list[dict[str, Any]],
    range_name: str,
    *,
    source: str = "FMP",
    used_live_quote_as_last: bool = False,
    confidence: str | None = None,
) -> dict[str, Any]:
    clean = [_shape_point(point) for point in points]
    clean = [point for point in clean if point is not None]
    if len(clean) < 2:
        return {
            "range": range_name,
            "first_date": "",
            "last_date": "",
            "first_close": None,
            "last_close": None,
            "return_pct": None,
            "source": source,
            "points_used": len(clean),
            "used_live_quote_as_last": used_live_quote_as_last,
            "confidence": confidence or "low",
        }
    first = clean[0]
    last = clean[-1]
    first_close = first["close"]
    last_close = last["close"]
    return_pct = ((last_close - first_close) / first_close) * 100 if first_close else None
    return {
        "range": range_name,
        "first_date": first["date"],
        "last_date": last["date"],
        "first_close": round(first_close, 6),
        "last_close": round(last_close, 6),
        "return_pct": round(return_pct, 4) if return_pct is not None else None,
        "source": source,
        "points_used": len(clean),
        "used_live_quote_as_last": used_live_quote_as_last,
        "confidence": confidence or "high",
    }


def calculate_returns(
    eod_points: list[dict[str, Any]],
    intraday_points: list[dict[str, Any]] | None = None,
    *,
    source: str = "FMP",
    current_price: object = None,
    previous_close: object = None,
    current_date: str = "live",
) -> dict[str, dict[str, Any]]:
    eod = [_shape_point(point) for point in eod_points]
    eod = [point for point in eod if point is not None]
    intraday = [_shape_point(point) for point in (intraday_points or [])]
    intraday = [point for point in intraday if point is not None]
    one_day_points, one_day_used_live, one_day_confidence = _one_day_points(
        eod,
        intraday,
        current_price=current_price,
        previous_close=previous_close,
        current_date=current_date,
    )
    return {
        "1D": calculate_return_detail(
            one_day_points,
            "1D",
            source=source,
            used_live_quote_as_last=one_day_used_live,
            confidence=one_day_confidence,
        ),
        "1W": calculate_return_detail(slice_points_for_range(eod, "1W"), "1W", source=source),
        "1M": calculate_return_detail(slice_points_for_range(eod, "1M"), "1M", source=source),
        "1Y": calculate_return_detail(slice_points_for_range(eod, "1Y"), "1Y", source=source),
        "5Y": calculate_return_detail(slice_points_for_range(eod, "5Y"), "5Y", source=source),
        "MAX": calculate_return_detail(eod, "MAX", source=source),
    }


def flatten_return_details(details: dict[str, dict[str, Any]]) -> dict[str, float | None]:
    return {range_name: details.get(range_name, {}).get("return_pct") for range_name in _RANGES}


def slice_points_for_range(points: list[dict[str, Any]], range_name: str) -> list[dict[str, Any]]:
    if not points:
        return []
    normalized = str(range_name or "").upper()
    if normalized == "MAX":
        return points
    if normalized == "1W":
        return _slice_since(points, days=7, fallback_count=7)
    if normalized == "1M":
        return _slice_since(points, days=31, fallback_count=23)
    if normalized == "1Y":
        return _slice_since(points, days=366, fallback_count=260)
    if normalized == "5Y":
        return _slice_since(points, days=366 * 5, fallback_count=1260)
    return points


def _slice_since(points: list[dict[str, Any]], *, days: int, fallback_count: int) -> list[dict[str, Any]]:
    last_date = _parse_date(points[-1].get("date") or points[-1].get("time"))
    if last_date is None:
        return points[-fallback_count:]
    cutoff = last_date - timedelta(days=days)
    sliced = [point for point in points if (_parse_date(point.get("date") or point.get("time")) or last_date) >= cutoff]
    return sliced if len(sliced) >= 2 else points[-fallback_count:]


def _shape_point(point: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(point, dict):
        return None
    close = _num(point.get("close"))
    date = str(point.get("date") or point.get("time") or "").strip()
    if close is None or close <= 0 or not date:
        return None
    return {"date": date, "time": date, "close": close}


def _one_day_points(
    eod: list[dict[str, Any]],
    intraday: list[dict[str, Any]],
    *,
    current_price: object = None,
    previous_close: object = None,
    current_date: str = "live",
) -> tuple[list[dict[str, Any]], bool, str]:
    if len(intraday) >= 2:
        return intraday, False, "high"
    current = _num(current_price)
    previous = _num(previous_close)
    if current is not None and current > 0 and previous is not None and previous > 0:
        previous_date = eod[-1]["date"] if eod else "previous_close"
        return [
            {"date": previous_date, "close": previous},
            {"date": str(current_date or "live"), "close": current},
        ], True, "high"
    if len(eod) >= 2:
        return eod[-2:], False, "medium"
    return eod[-2:], False, "low"


def _num(value: object) -> float | None:
    try:
        if value in (None, "", "None"):
            return None
        numeric = float(value)
        return numeric
    except Exception:
        return None


def _parse_date(value: object):
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw[:10])
    except Exception:
        return None
