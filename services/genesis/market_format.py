from __future__ import annotations

import math


def number_or_none(value: object) -> float | None:
    try:
        if value in (None, "", "None"):
            return None
        numeric = float(value)
        return numeric if math.isfinite(numeric) else None
    except Exception:
        return None


def market_tone(value: object) -> str:
    numeric = number_or_none(value)
    if numeric is None or numeric == 0:
        return "neutral"
    return "positive" if numeric > 0 else "negative"


def market_class(value: object) -> str:
    tone = market_tone(value)
    if tone == "positive":
        return "up"
    if tone == "negative":
        return "down"
    return "flat"


def format_signed_money(value: object, empty: str = "Sin dato") -> str:
    numeric = number_or_none(value)
    if numeric is None:
        return empty
    sign = "+" if numeric > 0 else "-" if numeric < 0 else ""
    return f"{sign}${abs(numeric):,.2f}"


def format_signed_percent(value: object, empty: str = "Sin dato") -> str:
    numeric = number_or_none(value)
    if numeric is None:
        return empty
    sign = "+" if numeric > 0 else "-" if numeric < 0 else ""
    return f"{sign}{abs(numeric):.2f}%"


def format_market_number(value: object, *, currency: str = "USD", asset_type: str = "equity", empty: str = "Sin precio") -> str:
    numeric = number_or_none(value)
    if numeric is None:
        return empty
    prefix = "$" if currency.upper() in {"USD", "USDT", ""} else f"{currency.upper()} "
    if asset_type == "crypto" and abs(numeric) >= 1000:
        return f"{prefix}{numeric:,.2f}"
    if asset_type == "crypto" and abs(numeric) < 1:
        return f"{prefix}{numeric:,.6f}".rstrip("0").rstrip(".")
    return f"{prefix}{numeric:,.2f}"

