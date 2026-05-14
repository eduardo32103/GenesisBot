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


def _fmt_volume(value: float | None) -> str:
    if value is None:
        return "volumen pendiente"
    sign = "-" if value < 0 else ""
    abs_value = abs(value)
    if abs_value >= 1_000_000_000:
        return f"{sign}{abs_value / 1_000_000_000:.1f}B"
    if abs_value >= 1_000_000:
        return f"{sign}{abs_value / 1_000_000:.1f}M"
    if abs_value >= 1_000:
        return f"{sign}{abs_value / 1_000:.0f}K"
    return f"{sign}{abs_value:.0f}"


def _fmt_ratio(value: float | None) -> str:
    return "relativo pendiente" if value is None else f"{value:.1f}x"


def _coalesce_num(fields: dict[str, Any], *names: str) -> float | None:
    for name in names:
        value = _num(fields.get(name))
        if value is not None:
            return value
    return None


def _indicator(name: str, status: str, detail: str, value: float | None = None) -> dict[str, Any]:
    item: dict[str, Any] = {"name": name, "status": status, "detail": detail}
    if value is not None:
        item["value"] = round(value, 4)
    return item


def _above(value: float | None, pct: float = 0.003) -> float | None:
    return None if value is None else value * (1 + pct)


def _below(value: float | None, pct: float = 0.003) -> float | None:
    return None if value is None else value * (1 - pct)


def _is_crypto_ticker(ticker: str) -> bool:
    symbol = str(ticker or "").strip().upper()
    return symbol.endswith("-USD") or symbol in {"BTC", "ETH", "SOL", "DOGE", "XRP"}


def _safe_dollar_volume(ticker: str, price: float | None, volume: float | None, direct: float | None) -> float | None:
    limit = 1_000_000_000_000
    if direct is not None:
        if 0 < direct <= limit:
            return direct
        if _is_crypto_ticker(ticker) and volume is not None and 0 < volume <= limit:
            return volume
        return None
    if price is None or volume is None:
        return None
    computed = price * volume
    if _is_crypto_ticker(ticker) and computed > limit:
        return volume if 0 < volume <= limit else None
    return computed if 0 < computed <= limit else None


def build_signal_strategy(ticker: str, fields: dict[str, Any]) -> dict[str, Any]:
    """Create a no-broker validation plan for Genesis alerts.

    The output is deliberately concrete: entry, invalidation, required
    volume and the indicator evidence behind the decision.
    """
    symbol = str(ticker or "MERCADO").strip().upper()
    price = _coalesce_num(fields, "price", "current_price", "last_price")
    change_pct = _coalesce_num(fields, "change_pct", "daily_change_pct", "changesPercentage")
    volume = _coalesce_num(fields, "volume", "vol")
    avg_volume = _coalesce_num(fields, "avg_volume", "avgVolume", "average_volume")
    relative_volume = _coalesce_num(fields, "relative_volume", "relativeVolume")
    if relative_volume is None and volume is not None and avg_volume and avg_volume > 0:
        relative_volume = volume / avg_volume
    dollar_volume = _safe_dollar_volume(symbol, price, volume, _coalesce_num(fields, "dollar_volume", "dollarVolume"))
    support = _coalesce_num(fields, "support", "day_low", "dayLow", "low")
    resistance = _coalesce_num(fields, "resistance", "day_high", "dayHigh", "high")
    rsi = _coalesce_num(fields, "rsi", "RSI")
    macd = _coalesce_num(fields, "macd", "MACD")
    ema20 = _coalesce_num(fields, "ema20", "ema_20", "EMA20")
    ema50 = _coalesce_num(fields, "ema50", "ema_50", "EMA50")
    ema200 = _coalesce_num(fields, "ema200", "ema_200", "EMA200")
    fib_618 = _coalesce_num(fields, "fib_618", "fibonacci_618", "fib0618")
    volatility = _coalesce_num(fields, "volatility")

    required_relative_volume = max(1.2, min(2.0, relative_volume or 1.2))
    required_volume = None
    if avg_volume and avg_volume > 0:
        required_volume = avg_volume * required_relative_volume
    elif volume and volume > 0:
        required_volume = volume * 1.10

    bullish_votes = 0
    bearish_votes = 0
    indicator_stack: list[dict[str, Any]] = []

    if price is not None and support is not None and resistance is not None and resistance > support:
        position = (price - support) / (resistance - support)
        if position >= 0.82:
            bullish_votes += 1
            indicator_stack.append(_indicator("Precio", "bullish", f"Cerca de ruptura: soporte {_fmt_price(support)} / resistencia {_fmt_price(resistance)}", position))
        elif position <= 0.25:
            bearish_votes += 1
            indicator_stack.append(_indicator("Precio", "bearish", f"Cerca de soporte: {_fmt_price(support)}; riesgo si lo pierde", position))
        else:
            indicator_stack.append(_indicator("Precio", "neutral", f"Entre soporte {_fmt_price(support)} y resistencia {_fmt_price(resistance)}", position))
    elif price is not None:
        indicator_stack.append(_indicator("Precio", "neutral", f"Precio vivo {_fmt_price(price)}; falta rango completo"))
    else:
        indicator_stack.append(_indicator("Precio", "pending", "Sin precio vivo confirmado"))

    if relative_volume is not None:
        if relative_volume >= 1.5:
            bullish_votes += 1
            indicator_stack.append(_indicator("Volumen", "bullish", f"Volumen relativo {_fmt_ratio(relative_volume)}; pide minimo {_fmt_volume(required_volume)}", relative_volume))
        elif relative_volume < 0.8:
            bearish_votes += 1
            indicator_stack.append(_indicator("Volumen", "bearish", f"Volumen seco {_fmt_ratio(relative_volume)}; no confirma ruptura", relative_volume))
        else:
            indicator_stack.append(_indicator("Volumen", "neutral", f"Volumen relativo {_fmt_ratio(relative_volume)}; minimo deseado {_fmt_volume(required_volume)}", relative_volume))
    elif volume is not None:
        indicator_stack.append(_indicator("Volumen", "neutral", f"Volumen {_fmt_volume(volume)}; falta promedio para medir fuerza"))
    else:
        indicator_stack.append(_indicator("Volumen", "pending", "Sin volumen confirmado"))

    if rsi is not None:
        if rsi >= 72:
            bearish_votes += 1
            indicator_stack.append(_indicator("RSI", "bearish", f"RSI {rsi:.1f}: extendido, no perseguir sin pullback", rsi))
        elif rsi <= 35:
            bullish_votes += 1
            indicator_stack.append(_indicator("RSI", "bullish", f"RSI {rsi:.1f}: sobreventa vigilable si rebota", rsi))
        elif 45 <= rsi <= 65:
            bullish_votes += 1
            indicator_stack.append(_indicator("RSI", "bullish", f"RSI {rsi:.1f}: zona saludable", rsi))
        else:
            indicator_stack.append(_indicator("RSI", "neutral", f"RSI {rsi:.1f}: sin extremo claro", rsi))
    else:
        indicator_stack.append(_indicator("RSI", "pending", "RSI pendiente"))

    if macd is not None:
        if macd > 0:
            bullish_votes += 1
            indicator_stack.append(_indicator("MACD", "bullish", f"MACD positivo {macd:.2f}", macd))
        elif macd < 0:
            bearish_votes += 1
            indicator_stack.append(_indicator("MACD", "bearish", f"MACD negativo {macd:.2f}", macd))
        else:
            indicator_stack.append(_indicator("MACD", "neutral", "MACD plano", macd))
    else:
        indicator_stack.append(_indicator("MACD", "pending", "MACD pendiente"))

    ema_checks = [(20, ema20), (50, ema50), (200, ema200)]
    ema_above = 0
    ema_below = 0
    for _, ema in ema_checks:
        if price is None or ema is None:
            continue
        if price >= ema:
            ema_above += 1
        else:
            ema_below += 1
    if ema_above or ema_below:
        if ema_above >= 2:
            bullish_votes += 1
            status = "bullish"
            detail = f"Precio arriba de {ema_above}/3 EMAs clave"
        elif ema_below >= 2:
            bearish_votes += 1
            status = "bearish"
            detail = f"Precio debajo de {ema_below}/3 EMAs clave"
        else:
            status = "neutral"
            detail = "EMAs mixtas"
        indicator_stack.append(_indicator("EMAs", status, detail))
    else:
        indicator_stack.append(_indicator("EMAs", "pending", "EMAs pendientes"))

    if price is not None and fib_618 is not None:
        if price >= fib_618:
            bullish_votes += 1
            indicator_stack.append(_indicator("Fib 0.618", "bullish", f"Precio arriba de {_fmt_price(fib_618)}", fib_618))
        else:
            bearish_votes += 1
            indicator_stack.append(_indicator("Fib 0.618", "bearish", f"Precio debajo de {_fmt_price(fib_618)}", fib_618))
    else:
        indicator_stack.append(_indicator("Fib 0.618", "pending", "Fibonacci pendiente"))

    score = 42.0
    if change_pct is not None:
        score += min(14.0, abs(change_pct) * 2.8)
        score += 4.0 if change_pct > 0 else -2.0 if change_pct < 0 else 0.0
    if relative_volume is not None:
        score += min(16.0, max(0.0, relative_volume - 1.0) * 12.0)
    elif dollar_volume is not None and dollar_volume >= 1_000_000_000:
        score += 5.0
    score += bullish_votes * 5.0
    score -= bearish_votes * 3.0
    if volatility is not None and volatility > 6:
        score -= 4.0
    score = max(0.0, min(100.0, score))

    if bearish_votes > bullish_votes + 1 or (change_pct is not None and change_pct < -0.75 and bearish_votes >= bullish_votes):
        bias = "bearish"
    elif bullish_votes >= bearish_votes + 1 or (change_pct is not None and change_pct > 0.75 and bullish_votes >= bearish_votes):
        bias = "bullish"
    else:
        bias = "neutral"

    if score >= 76:
        grade = "A"
        label = "Oportunidad fuerte en validacion"
    elif score >= 64:
        grade = "B"
        label = "Setup en validacion"
    elif score >= 52:
        grade = "C"
        label = "Senal temprana"
    else:
        grade = "D"
        label = "Solo radar"

    bullish_entry_level = _above(resistance) if resistance is not None else _above(price, 0.012)
    bullish_invalidation = _below(support) if support is not None else _below(price, 0.025)
    defensive_reclaim = _above(resistance) if resistance is not None else _above(price, 0.018)
    defensive_break = _below(support) if support is not None else _below(price, 0.018)

    validation: list[str] = []
    if bullish_entry_level is not None:
        validation.append(f"cierre arriba de {_fmt_price(bullish_entry_level)}")
    else:
        validation.append("precio vivo confirmado")
    if required_volume is not None:
        validation.append(f"volumen minimo {_fmt_volume(required_volume)}")
    else:
        validation.append("volumen relativo minimo 1.2x")
    validation.append("RSI/MACD sin divergencia fuerte")

    if bias == "bearish":
        entry_condition = (
            f"no comprar mientras no recupere {_fmt_price(defensive_reclaim)}; "
            f"reducir riesgo si pierde {_fmt_price(defensive_break)} con volumen"
        )
        invalidation = f"la tesis bajista se invalida si recupera {_fmt_price(defensive_reclaim)} con volumen >= {_fmt_volume(required_volume)}"
        entry_level = defensive_reclaim
        invalidation_level = defensive_break
    else:
        entry_condition = (
            f"cierre arriba de {_fmt_price(bullish_entry_level)} con volumen >= {_fmt_volume(required_volume)} "
            "y RSI/MACD acompanando"
        )
        invalidation = f"se cancela si pierde {_fmt_price(bullish_invalidation)} o rompe con volumen seco"
        entry_level = bullish_entry_level
        invalidation_level = bullish_invalidation

    if price is None:
        decision = "wait"
        decision_label = "esperar datos"
        decision_reason = "sin precio confirmado Genesis no valida entrada ni salida"
    elif bias == "bearish" and score >= 54:
        decision = "reduce_or_sell_risk"
        decision_label = "vender / reducir riesgo"
        decision_reason = "la lectura multi-indicador prioriza defensa antes que entrada"
    elif bias == "bullish" and score >= 76:
        decision = "buy_cautiously"
        decision_label = "comprar con cautela"
        decision_reason = "precio, volumen e indicadores tienen validacion suficiente para radar paper"
    elif score >= 60:
        decision = "watch_confirmation"
        decision_label = "vigilar confirmacion"
        decision_reason = "hay senal, pero falta confirmacion limpia de nivel o volumen"
    elif score >= 48:
        decision = "wait_for_setup"
        decision_label = "esperar ruptura"
        decision_reason = "senal temprana; actuar ahora seria prematuro"
    else:
        decision = "wait"
        decision_label = "esperar"
        decision_reason = "la evidencia no alcanza para elevar la alerta"

    if decision == "buy_cautiously":
        summary = (
            f"{symbol}: comprar con cautela solo si cierra arriba de {_fmt_price(entry_level)} "
            f"con volumen >= {_fmt_volume(required_volume)}. Invalida debajo de {_fmt_price(invalidation_level)}."
        )
    elif decision == "reduce_or_sell_risk":
        summary = (
            f"{symbol}: no comprar ahora; reducir riesgo si pierde {_fmt_price(invalidation_level)}. "
            f"Para volver a interesar debe recuperar {_fmt_price(entry_level)} con volumen."
        )
    elif decision == "watch_confirmation":
        summary = (
            f"{symbol}: vigilar. Entrada solo arriba de {_fmt_price(entry_level)} con volumen >= {_fmt_volume(required_volume)}; "
            f"se invalida bajo {_fmt_price(invalidation_level)}."
        )
    else:
        summary = (
            f"{symbol}: esperar. Necesita {validation[0]} y {validation[1]}; "
            f"riesgo si pierde {_fmt_price(invalidation_level)}."
        )

    return {
        "name": "Genesis multi-indicador - validación de precio, volumen, tendencia y riesgo",
        "grade": grade,
        "score": round(score, 1),
        "label": label,
        "bias": bias,
        "decision": decision,
        "decision_label_es": decision_label,
        "decision_reason_es": decision_reason,
        "entry_condition": entry_condition,
        "invalidation": invalidation,
        "entry_level": entry_level,
        "confirmation_price": entry_level,
        "invalidation_level": invalidation_level,
        "required_volume": required_volume,
        "required_relative_volume": required_relative_volume,
        "validation": validation[:3],
        "indicator_stack": indicator_stack,
        "risk_note": "Solo paper/radar: no broker, no orden real, no compra automatica.",
        "flow_context": _money_short(dollar_volume),
        "summary": summary,
        "thesis_es": (
            f"Lectura {grade}: {decision_label}. "
            "Usa precio, volumen, RSI, MACD, EMAs, Fibonacci y soporte/resistencia."
        ),
    }
