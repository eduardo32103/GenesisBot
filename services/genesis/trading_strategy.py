from __future__ import annotations

from typing import Any


def _num(value: Any) -> float | None:
    try:
        if value in (None, "", "None"):
            return None
        return float(value)
    except Exception:
        return None


def _money_short(value: float | None) -> str:
    if value is None:
        return "monto pendiente"
    sign = "-" if value < 0 else ""
    abs_value = abs(value)
    if abs_value >= 1_000_000_000:
        return f"{sign}${abs_value / 1_000_000_000:.1f}B"
    if abs_value >= 1_000_000:
        return f"{sign}${abs_value / 1_000_000:.1f}M"
    if abs_value >= 1_000:
        return f"{sign}${abs_value / 1_000:.0f}K"
    return f"{sign}${abs_value:.0f}"


def _fmt_price(value: float | None) -> str:
    return "precio pendiente" if value is None else f"${value:,.2f}"


def build_signal_strategy(ticker: str, fields: dict[str, Any]) -> dict[str, Any]:
    """Create a no-broker validation plan for Genesis alerts.

    This is deliberately a validation framework, not order routing. It helps Genesis
    rank opportunity/risk signals without issuing real trades.
    """
    price = _num(fields.get("price"))
    change_pct = _num(fields.get("change_pct"))
    volume = _num(fields.get("volume"))
    avg_volume = _num(fields.get("avg_volume"))
    relative_volume = _num(fields.get("relative_volume"))
    if relative_volume is None and volume is not None and avg_volume and avg_volume > 0:
        relative_volume = volume / avg_volume
    dollar_volume = _num(fields.get("dollar_volume"))
    if dollar_volume is None and price is not None and volume is not None:
        dollar_volume = price * volume
    support = _num(fields.get("support"))
    resistance = _num(fields.get("resistance"))

    score = 45.0
    if change_pct is not None:
        score += min(18.0, abs(change_pct) * 4.0)
        if change_pct > 0:
            score += 5.0
    if relative_volume is not None:
        score += min(18.0, max(0.0, relative_volume - 1.0) * 14.0)
    elif dollar_volume is not None and dollar_volume >= 1_000_000_000:
        score += 8.0
    if price is not None and support is not None and resistance is not None and resistance > support:
        position = (price - support) / (resistance - support)
        if 0.45 <= position <= 0.88:
            score += 8.0
        elif position > 0.92:
            score -= 4.0
    score = max(0.0, min(100.0, score))

    if change_pct is not None and change_pct > 0.35:
        bias = "bullish"
    elif change_pct is not None and change_pct < -0.35:
        bias = "bearish"
    else:
        bias = "neutral"

    if score >= 72:
        grade = "A"
        label = "Oportunidad fuerte en validación"
    elif score >= 60:
        grade = "B"
        label = "Oportunidad en vigilancia"
    elif score >= 48:
        grade = "C"
        label = "Señal temprana"
    else:
        grade = "D"
        label = "Solo radar"

    validation = []
    if resistance is not None:
        validation.append(f"cierre arriba de {_fmt_price(resistance)}")
    else:
        validation.append("ruptura de rango confirmado")
    if relative_volume is not None:
        validation.append(f"volumen relativo mayor a {max(1.2, min(2.0, relative_volume)):.1f}x")
    else:
        validation.append("volumen acompanando el movimiento")
    validation.append("noticia o catalizador alineado")

    invalidation = (
        f"pérdida de {_fmt_price(support)}"
        if support is not None
        else "pérdida de mínimo intradía o volumen seco"
    )
    if bias == "bearish":
        entry_condition = "solo vigilar rebote; no perseguir caída sin recuperación de nivel"
        invalidation = f"recuperación sostenida arriba de {_fmt_price(resistance)}" if resistance is not None else "recuperación de estructura"
    else:
        entry_condition = "esperar ruptura + retesteo con volumen antes de considerar paper"

    return {
        "name": "Genesis 10% mensual - validación por precio, volumen y catalizador",
        "grade": grade,
        "score": round(score, 1),
        "label": label,
        "bias": bias,
        "entry_condition": entry_condition,
        "invalidation": invalidation,
        "validation": validation[:3],
        "risk_note": "Solo paper/radar: no broker, no orden real, no compra automática.",
        "flow_context": _money_short(dollar_volume),
        "summary": (
            f"{ticker}: {label.lower()} ({grade}, {score:.0f}/100). "
            f"Validar con {', '.join(validation[:2])}; invalidar con {invalidation}."
        ),
    }
