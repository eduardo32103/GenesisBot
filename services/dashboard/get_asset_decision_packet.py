from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from app.settings import load_settings
from integrations.fmp.client import FmpClient
from services.dashboard.get_macro_activity_snapshot import get_macro_activity_snapshot
from services.dashboard.get_money_flow_jarvis_answer import get_money_flow_jarvis_answer
from services.dashboard.get_radar_snapshot import get_radar_snapshot

_LOGGER = logging.getLogger("genesis.dashboard.asset_packet")


def _normalize_ticker(value: Any) -> str:
    return str(value or "").strip().upper()


def _safe_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except Exception:
        return None
    return numeric


def _compact_text(value: Any, limit: int = 220) -> str:
    return " ".join(str(value or "").split())[:limit]


def _humanize_panel_text(value: Any) -> str:
    text = _compact_text(value)
    replacements = {
        "insufficient_confirmation": "confirmacion insuficiente",
        "portfolio_fallback": "datos locales",
        "contingency": "datos de respaldo",
        "unavailable": "sin datos disponibles",
        "degraded": "datos parciales",
        "runtime": "sistema local",
        "endpoint": "consulta",
        "snapshot": "lectura guardada",
    }
    for raw, human in replacements.items():
        text = text.replace(raw, human).replace(raw.upper(), human)
    return text


def _fmp_live_ready(settings: Any) -> bool:
    return bool(getattr(settings, "fmp_api_key", "") and getattr(settings, "fmp_live_enabled", False))


def _find_radar_item(ticker: str) -> dict[str, Any]:
    snapshot = get_radar_snapshot()
    for item in snapshot.get("items") or []:
        if isinstance(item, dict) and _normalize_ticker(item.get("ticker")) == ticker:
            return item
    return {}


def _load_fmp_context(ticker: str, settings: Any) -> dict[str, Any]:
    if not _fmp_live_ready(settings):
        return {"live_ready": False, "quote": {}, "profile": {}, "news": [], "history": []}

    client = FmpClient(getattr(settings, "fmp_api_key", ""), logger=_LOGGER)
    quote: dict[str, Any] = {}
    profile: dict[str, Any] = {}
    news: list[dict[str, Any]] = []
    history: list[dict[str, Any]] = []

    try:
        quote = client.get_quote(ticker) or {}
    except Exception:
        quote = {}
    try:
        profile = client.get_profile(ticker) or {}
    except Exception:
        profile = {}
    try:
        news = client.get_stock_news(ticker, limit=3) or []
    except Exception:
        news = []
    try:
        history = client.get_historical_eod(ticker, limit=30) or []
    except Exception:
        history = []

    return {"live_ready": True, "quote": quote, "profile": profile, "news": news, "history": history}


def _history_close(row: dict[str, Any]) -> float | None:
    for key in ("close", "adjClose", "price"):
        value = _safe_float(row.get(key))
        if value is not None and value > 0:
            return value
    return None


def _trend_from_history(history: list[dict[str, Any]], percent_change: float | None) -> tuple[str, str]:
    closes = [_history_close(row) for row in history if isinstance(row, dict)]
    closes = [value for value in closes if value is not None]
    if len(closes) >= 2:
        latest = closes[0]
        older = closes[-1]
        if older and older > 0:
            change = ((latest - older) / older) * 100
            if change >= 3:
                return "positiva", f"Tendencia positiva en el historico corto: {change:.1f}%."
            if change <= -3:
                return "negativa", f"Tendencia negativa en el historico corto: {change:.1f}%."
            return "lateral", f"Tendencia lateral en el historico corto: {change:.1f}%."
    if percent_change is not None:
        if percent_change >= 2:
            return "positiva", f"Movimiento reciente positivo: {percent_change:.1f}%."
        if percent_change <= -2:
            return "negativa", f"Movimiento reciente negativo: {percent_change:.1f}%."
        return "lateral", f"Movimiento reciente acotado: {percent_change:.1f}%."
    return "no concluyente", "Sin historico suficiente para leer tendencia."


def _money_flow_context(ticker: str) -> dict[str, Any]:
    try:
        payload = get_money_flow_jarvis_answer(f"flujo de capital {ticker}")
    except Exception:
        return {
            "answer": "Sin lectura util de Dinero Grande.",
            "item": {},
            "whale_identified": False,
            "flow_detected": False,
        }

    items = payload.get("items") if isinstance(payload.get("items"), list) else []
    item = items[0] if items and isinstance(items[0], dict) else {}
    return {
        "answer": _humanize_panel_text(payload.get("answer") or "Sin lectura util de Dinero Grande."),
        "item": item,
        "whale_identified": bool(item.get("whale_identified")),
        "flow_detected": bool(item.get("flow_detected") or item.get("attention") == "merece atencion"),
    }


def _macro_context() -> dict[str, Any]:
    try:
        payload = get_macro_activity_snapshot()
    except Exception:
        payload = {}
    macro = payload.get("macro") if isinstance(payload.get("macro"), dict) else {}
    return {
        "available": bool(macro.get("available", False)),
        "summary": _compact_text(macro.get("summary") or macro.get("note") or "Sin contexto macro activo."),
        "bias": _compact_text(macro.get("bias_label") or "sin contexto macro activo", 120),
        "dominant_risk": _compact_text(macro.get("dominant_risk") or "", 160),
        "confidence": int(macro.get("confidence") or 0),
    }


def _news_read(news: list[dict[str, Any]]) -> str:
    clean_titles = [
        _compact_text(item.get("title"), 120)
        for item in news
        if isinstance(item, dict) and _compact_text(item.get("title"), 120)
    ]
    if not clean_titles:
        return "Sin noticias activas en la fuente consultada."
    return "Noticias recientes: " + " | ".join(clean_titles[:2])


def _technical_read(
    price: float | None,
    percent_change: float | None,
    volume: float | None,
    avg_volume: float | None,
    trend_label: str,
) -> str:
    if price is None:
        return "Sin precio actual confirmado; lectura tecnica no concluyente."
    parts = [f"Precio disponible: {price}."]
    if percent_change is not None:
        parts.append(f"Cambio reciente: {percent_change:.2f}%.")
    if volume and avg_volume and avg_volume > 0:
        ratio = volume / avg_volume
        parts.append(f"Volumen relativo aproximado: {ratio:.2f}x.")
    else:
        parts.append("Volumen relativo no confirmado.")
    parts.append(f"Tendencia: {trend_label}.")
    return " ".join(parts)


def _fundamental_read(profile: dict[str, Any], quote: dict[str, Any]) -> str:
    sector = _compact_text(profile.get("sector") or "", 80)
    industry = _compact_text(profile.get("industry") or "", 100)
    pe = _safe_float(quote.get("pe"))
    market_cap = _safe_float(quote.get("marketCap"))
    if not profile and pe is None and market_cap is None:
        return "Sin perfil fundamental suficiente."
    parts = []
    if sector or industry:
        parts.append(f"Perfil: {sector or 'sector sin dato'} / {industry or 'industria sin dato'}.")
    if pe and pe > 0:
        parts.append(f"PER aproximado disponible: {pe:.1f}.")
    if market_cap and market_cap > 0:
        parts.append("Capitalizacion disponible en datos directos.")
    return " ".join(parts) or "Perfil disponible, pero faltan metricas para valorar con fuerza."


def _make_decision(
    *,
    price: float | None,
    percent_change: float | None,
    trend_label: str,
    volume: float | None,
    avg_volume: float | None,
    news_count: int,
    flow_detected: bool,
    macro_available: bool,
    macro_risk: str,
) -> tuple[str, str]:
    if price is None:
        return "No concluyente", "no concluyente"
    if percent_change is not None and percent_change <= -5:
        return "Evitar por ahora", "media"
    if macro_risk and any(token in macro_risk.lower() for token in ("alto", "riesgo", "crisis", "conflicto")) and percent_change is not None and percent_change < 0:
        return "Evitar por ahora", "media"

    volume_confirmed = bool(volume and avg_volume and avg_volume > 0 and volume / avg_volume >= 1.2)
    positive_trend = trend_label == "positiva"
    positive_move = percent_change is not None and percent_change > 0

    if positive_trend and volume_confirmed and news_count > 0 and flow_detected and macro_available:
        return "Comprar con cautela", "media"
    if (positive_trend or positive_move or flow_detected) and not (volume_confirmed and macro_available):
        return "Esperar confirmacion", "media"
    if positive_trend or positive_move or flow_detected or news_count > 0:
        return "Vigilar", "media"
    return "Vigilar", "baja"


def _scenario_text(ticker: str, decision: str, supports: list[str], risks: list[str]) -> dict[str, str]:
    support = supports[0] if supports else "aparece nueva confirmacion de precio, volumen o flujo"
    risk = risks[0] if risks else "faltan confirmaciones"
    return {
        "alcista": f"{ticker} mejora si {support.lower()} y el volumen confirma continuidad.",
        "neutral": f"{ticker} se mantiene en espera si el precio no confirma direccion y siguen faltando datos.",
        "bajista": f"{ticker} se deteriora si {risk.lower()} o el precio pierde soporte con volumen.",
    }


def get_asset_decision_packet(ticker: str) -> dict[str, Any]:
    normalized_ticker = _normalize_ticker(ticker)
    settings = load_settings()
    radar_item = _find_radar_item(normalized_ticker)
    fmp = _load_fmp_context(normalized_ticker, settings) if normalized_ticker else {"quote": {}, "profile": {}, "news": [], "history": [], "live_ready": False}
    quote = fmp.get("quote") if isinstance(fmp.get("quote"), dict) else {}
    profile = fmp.get("profile") if isinstance(fmp.get("profile"), dict) else {}
    news = fmp.get("news") if isinstance(fmp.get("news"), list) else []
    history = fmp.get("history") if isinstance(fmp.get("history"), list) else []

    price = _safe_float(quote.get("price"))
    if price is None:
        price = _safe_float(radar_item.get("reference_price"))
    change = _safe_float(quote.get("change"))
    percent_change = _safe_float(quote.get("changesPercentage"))
    volume = _safe_float(quote.get("volume") or quote.get("vol"))
    avg_volume = _safe_float(quote.get("avgVolume"))
    trend_label, trend_summary = _trend_from_history(history, percent_change)
    money_flow = _money_flow_context(normalized_ticker) if normalized_ticker else {"answer": "Sin ticker.", "flow_detected": False, "whale_identified": False, "item": {}}
    macro = _macro_context()

    supports: list[str] = []
    risks: list[str] = []
    missing: list[str] = []

    if price is not None:
        supports.append("Precio actual confirmado por datos directos." if quote else "Referencia de precio disponible.")
    else:
        missing.append("precio actual")
    if trend_label == "positiva":
        supports.append(trend_summary)
    elif trend_label == "negativa":
        risks.append(trend_summary)
    else:
        missing.append("tendencia confirmada")
    if volume and avg_volume and avg_volume > 0:
        supports.append("Volumen relativo disponible.")
    else:
        missing.append("volumen relativo")
    if news:
        supports.append("Noticias recientes disponibles para revisar impacto.")
    else:
        missing.append("noticias activas")
    if money_flow.get("flow_detected"):
        supports.append("Dinero Grande muestra flujo a vigilar.")
    else:
        missing.append("flujo confirmado")
    if macro["available"]:
        supports.append("Contexto macro activo disponible.")
    else:
        missing.append("contexto macro/noticias")
    if not money_flow.get("whale_identified"):
        missing.append("ballena identificada")
    if percent_change is not None and percent_change <= -2:
        risks.append(f"Presion negativa reciente: {percent_change:.2f}%.")
    if not macro["available"]:
        risks.append("Sin contexto macro/noticias activo para explicar catalizadores externos.")
    if not news:
        risks.append("Sin noticias activas confirmadas en esta lectura.")

    decision_label, confidence = _make_decision(
        price=price,
        percent_change=percent_change,
        trend_label=trend_label,
        volume=volume,
        avg_volume=avg_volume,
        news_count=len(news),
        flow_detected=bool(money_flow.get("flow_detected")),
        macro_available=bool(macro["available"]),
        macro_risk=macro["dominant_risk"],
    )

    if decision_label == "Comprar con cautela":
        next_step = "Validar volumen, noticia/catalizador y riesgo antes de aumentar exposicion."
    elif decision_label == "Esperar confirmacion":
        next_step = "Esperar confirmacion de volumen, noticias o flujo antes de operar."
    elif decision_label == "Evitar por ahora":
        next_step = "Evitar actuar hasta que el precio estabilice y baje el riesgo."
    elif decision_label == "Vigilar":
        next_step = "Vigilar continuidad del precio y buscar confirmacion de volumen o flujo."
    else:
        next_step = "No operar con fuerza hasta completar evidencia basica."

    company_name = _compact_text(
        profile.get("companyName") or profile.get("companyNameUSD") or profile.get("name") or quote.get("name") or normalized_ticker,
        120,
    )
    whale_read = (
        f"Ballena identificada: {_compact_text((money_flow.get('item') or {}).get('whale_entity'), 120)}."
        if money_flow.get("whale_identified")
        else "Sin ballena identificada con la fuente activa."
    )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "ticker": normalized_ticker,
        "company_name": company_name,
        "price": price,
        "change": change,
        "percent_change": percent_change,
        "volume": volume,
        "sector": _compact_text(profile.get("sector"), 100),
        "industry": _compact_text(profile.get("industry"), 120),
        "trend_summary": trend_summary,
        "technical_read": _technical_read(price, percent_change, volume, avg_volume, trend_label),
        "fundamental_read": _fundamental_read(profile, quote),
        "news_read": _news_read(news),
        "money_flow_read": money_flow["answer"],
        "whale_read": whale_read,
        "macro_read": macro["summary"] if macro["available"] else "Sin contexto macro/noticias activo.",
        "risks": risks[:5] or ["Riesgo no concluyente: faltan confirmaciones independientes."],
        "supports": supports[:5] or ["Solo hay evidencia limitada; no alcanza para elevar la lectura."],
        "missing_evidence": sorted(set(missing)),
        "confidence": confidence,
        "decision_label": decision_label,
        "next_step": next_step,
        "scenarios": _scenario_text(normalized_ticker, decision_label, supports, risks),
        "source_status": {
            "fmp_live_ready": bool(fmp.get("live_ready")),
            "quote_available": bool(quote),
            "profile_available": bool(profile),
            "news_available": bool(news),
            "history_available": bool(history),
            "money_flow_available": bool(money_flow.get("flow_detected") or money_flow.get("whale_identified")),
            "macro_available": bool(macro["available"]),
            "whale_identified": bool(money_flow.get("whale_identified")),
        },
    }
