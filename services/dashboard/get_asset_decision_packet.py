from __future__ import annotations

import logging
import re
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
    text = _compact_text(value, 360)
    replacements = (
        (r"Money Flow ready causalidad probabilidad probability disabled", "Dinero Grande sin confirmacion suficiente"),
        (r"causalidad probabilidad", "causalidad probable"),
        (r"probability disabled", "sin confirmacion suficiente"),
        (r"probability ready", "probabilidad disponible"),
        (r"detection ready", "deteccion disponible"),
        (r"insufficient_confirmation", "No concluyente"),
        (r"portfolio_fallback", "datos locales"),
        (r"contingency", "datos de respaldo"),
        (r"unavailable", "sin datos disponibles"),
        (r"degraded", "datos parciales"),
        (r"runtime", "sistema local"),
        (r"endpoint", "consulta"),
        (r"snapshot", "lectura guardada"),
        (r"Money Flow", "Dinero Grande"),
    )
    for raw, human in replacements:
        pattern = raw
        if raw.replace("_", "").replace(" ", "").isalnum():
            pattern = rf"\b{raw}\b"
        text = re.sub(pattern, human, text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text.replace("_", " ")).strip()[:360]


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


def _history_volume(row: dict[str, Any]) -> float | None:
    value = _safe_float(row.get("volume"))
    if value is not None and value > 0:
        return value
    return None


def _history_metrics(history: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [row for row in history if isinstance(row, dict)]
    closes = [_history_close(row) for row in rows]
    closes = [value for value in closes if value is not None]
    volumes = [_history_volume(row) for row in rows]
    volumes = [value for value in volumes if value is not None]
    if not closes:
        return {
            "count": 0,
            "max_30d": None,
            "min_30d": None,
            "support": None,
            "resistance": None,
            "avg_volume": None,
            "trend_change_pct": None,
        }
    trend_change_pct = None
    if len(closes) >= 2 and closes[-1] > 0:
        trend_change_pct = ((closes[0] - closes[-1]) / closes[-1]) * 100
    return {
        "count": len(closes),
        "max_30d": max(closes),
        "min_30d": min(closes),
        "support": min(closes),
        "resistance": max(closes),
        "avg_volume": (sum(volumes) / len(volumes)) if volumes else None,
        "trend_change_pct": trend_change_pct,
    }


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
        return "Sin noticias relevantes disponibles en esta lectura."
    return (
        "Noticias relevantes: "
        + " | ".join(clean_titles[:3])
        + ". Impacto probable: pueden mover expectativa, pero debe confirmarse en precio y volumen."
    )


def _technical_read(
    price: float | None,
    percent_change: float | None,
    volume: float | None,
    avg_volume: float | None,
    trend_label: str,
    support: float | None = None,
    resistance: float | None = None,
) -> str:
    if price is None:
        return "Sin precio actual confirmado; lectura tecnica no concluyente."
    parts = [f"Precio actual: {price}."]
    if percent_change is not None:
        parts.append(f"Cambio diario: {percent_change:.2f}%.")
    parts.append(f"Tendencia: {trend_label}.")
    if support is not None and resistance is not None:
        parts.append(f"Zona simple 30 dias: soporte {support:.2f}, resistencia {resistance:.2f}.")
    if volume and avg_volume and avg_volume > 0:
        ratio = volume / avg_volume
        parts.append(f"Volumen relativo aproximado: {ratio:.2f}x.")
    else:
        parts.append("Volumen relativo no confirmado.")
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
    history_count: int,
    profile_available: bool,
    news_count: int,
    flow_detected: bool,
    macro_available: bool,
    macro_risk: str,
) -> tuple[str, str, str, str, str]:
    if price is None:
        return (
            "No concluyente",
            "no concluyente",
            "falta precio actual confirmado.",
            "Confirmar precio directo e historico reciente.",
            "La lectura no se sostiene sin precio actual.",
        )
    if percent_change is not None and percent_change <= -5:
        return (
            "Evitar por ahora",
            "media",
            "hay deterioro fuerte en el movimiento diario.",
            "Esperar estabilizacion y recuperacion con volumen.",
            "Seguiria invalidada si el precio mantiene presion negativa.",
        )
    if macro_risk and any(token in macro_risk.lower() for token in ("alto", "riesgo", "crisis", "conflicto")) and percent_change is not None and percent_change < 0:
        return (
            "Evitar por ahora",
            "media",
            "riesgo macro activo con precio bajo presion.",
            "Esperar que baje el riesgo externo y que el precio recupere.",
            "Se invalida si el catalizador externo empeora o el precio pierde soporte.",
        )
    if trend_label == "negativa":
        return (
            "Evitar por ahora",
            "media",
            "la tendencia reciente esta negativa.",
            "Esperar recuperacion de tendencia y volumen comprador.",
            "Se mantiene invalidada si el precio sigue bajo presion o pierde soporte.",
        )

    volume_confirmed = bool(volume and avg_volume and avg_volume > 0 and volume / avg_volume >= 1.2)
    positive_trend = trend_label == "positiva"
    positive_move = percent_change is not None and percent_change > 0
    history_available = history_count >= 2

    if positive_trend and volume_confirmed and news_count > 0 and flow_detected and macro_available:
        return (
            "Comprar con cautela",
            "media",
            "precio, tendencia, volumen y contexto de apoyo estan alineados.",
            "Validar entrada contra soporte y no perseguir vela extendida.",
            "Se invalida si pierde soporte o el volumen comprador desaparece.",
        )
    if positive_trend and (volume_confirmed or news_count > 0 or flow_detected):
        return (
            "Esperar confirmacion",
            "media",
            "la tendencia ayuda, pero faltan confirmaciones completas.",
            "Confirmar volumen, catalizador o flujo antes de operar fuerte.",
            "Se invalida si rompe soporte o el cambio diario gira negativo con volumen.",
        )
    if positive_trend or positive_move or flow_detected or news_count > 0:
        return (
            "Vigilar",
            "media" if history_available or news_count > 0 else "baja",
            "hay datos utiles, pero todavia no alcanzan para una lectura fuerte.",
            "Buscar continuidad de precio y confirmacion de volumen.",
            "Se invalida si el precio pierde soporte o no aparece catalizador.",
        )
    if price is not None and (profile_available or history_available):
        return (
            "Vigilar",
            "baja",
            "hay precio y contexto basico, pero falta catalizador operativo.",
            "Confirmar tendencia, volumen y noticias antes de actuar.",
            "Se invalida si el precio se deteriora o falta liquidez confirmada.",
        )
    return (
        "No concluyente",
        "no concluyente",
        "solo hay evidencia minima y no alcanza para decidir.",
        "Confirmar precio, historico, volumen y catalizadores.",
        "La lectura queda invalida mientras falten datos basicos.",
    )


def _scenario_text(
    ticker: str,
    decision: str,
    supports: list[str],
    risks: list[str],
    support_level: float | None = None,
    resistance_level: float | None = None,
) -> dict[str, str]:
    support = supports[0] if supports else "aparece nueva confirmacion de precio, volumen o flujo"
    risk = risks[0] if risks else "faltan confirmaciones"
    if support_level is not None and resistance_level is not None:
        return {
            "alcista": f"{ticker} mejora si rompe o sostiene resistencia cerca de {resistance_level:.2f} con volumen.",
            "neutral": f"{ticker} queda en espera si se mantiene entre soporte {support_level:.2f} y resistencia {resistance_level:.2f}.",
            "bajista": f"{ticker} se deteriora si pierde soporte cerca de {support_level:.2f} con presion vendedora.",
        }
    return {
        "alcista": f"{ticker} mejora si {support.lower()} y el volumen confirma continuidad.",
        "neutral": f"{ticker} se mantiene en espera si el precio no confirma direccion y siguen faltando datos.",
        "bajista": f"{ticker} se deteriora si {risk.lower()} o el precio pierde soporte con volumen.",
    }


def _recent_news_items(news: list[dict[str, Any]]) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for item in news[:3]:
        if not isinstance(item, dict):
            continue
        title = _compact_text(item.get("title"), 140)
        if not title:
            continue
        items.append(
            {
                "title": title,
                "source": _compact_text(item.get("site") or item.get("source") or "Fuente de mercado", 80),
                "published_at": _compact_text(item.get("publishedDate") or item.get("published_at") or item.get("date"), 40),
                "impact": "Puede actuar como catalizador, pero debe confirmarse en precio y volumen.",
            }
        )
    return items


def _score_context(
    *,
    price: float | None,
    history_count: int,
    relative_volume: float | None,
    profile_available: bool,
    news_count: int,
    flow_detected: bool,
    whale_identified: bool,
    macro_available: bool,
    trend_label: str,
    percent_change: float | None,
) -> tuple[int, int]:
    evidence_score = 0
    evidence_score += 2 if price is not None else 0
    evidence_score += 2 if history_count >= 2 else 0
    evidence_score += 1 if relative_volume is not None else 0
    evidence_score += 1 if profile_available else 0
    evidence_score += 1 if news_count > 0 else 0
    evidence_score += 1 if flow_detected else 0
    evidence_score += 1 if whale_identified else 0
    evidence_score += 1 if macro_available else 0

    risk_score = 0
    risk_score += 2 if trend_label == "negativa" else 0
    risk_score += 2 if percent_change is not None and percent_change <= -3 else 0
    risk_score += 1 if price is None else 0
    risk_score += 1 if history_count < 2 else 0
    risk_score += 1 if relative_volume is None else 0
    risk_score += 1 if not macro_available else 0
    return min(evidence_score, 10), min(risk_score, 10)


def _genesis_action(verdict: str) -> str:
    if verdict == "Comprar con cautela":
        return "Genesis ahora buscaria entrada pequena y disciplinada solo si confirma volumen y respeta soporte."
    if verdict == "Esperar confirmacion":
        return "Genesis ahora no entraria todavia; esperaria confirmacion de volumen o catalizador."
    if verdict == "Vigilar":
        return "Genesis ahora la pondria en vigilancia, sin forzar operacion mediocre."
    if verdict == "Evitar por ahora":
        return "Genesis ahora evitaria entrar hasta que el deterioro se estabilice."
    return "Genesis ahora no operaria: falta evidencia basica."


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
    history_metrics = _history_metrics(history)
    avg_volume = _safe_float(quote.get("avgVolume")) or history_metrics.get("avg_volume")
    relative_volume = (volume / avg_volume) if volume and avg_volume and avg_volume > 0 else None
    trend_label, trend_summary = _trend_from_history(history, percent_change)
    money_flow = _money_flow_context(normalized_ticker) if normalized_ticker else {"answer": "Sin ticker.", "flow_detected": False, "whale_identified": False, "item": {}}
    macro = _macro_context()
    profile_available = bool(profile)
    history_count = int(history_metrics.get("count") or 0)

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
    if history_count < 2:
        missing.append("historico 30 dias")
    if relative_volume is not None:
        supports.append(f"Volumen relativo confirmado: {relative_volume:.2f}x.")
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

    decision_label, confidence, decision_reason, improve_condition, invalidation_condition = _make_decision(
        price=price,
        percent_change=percent_change,
        trend_label=trend_label,
        volume=volume,
        avg_volume=avg_volume,
        history_count=history_count,
        profile_available=profile_available,
        news_count=len(news),
        flow_detected=bool(money_flow.get("flow_detected")),
        macro_available=bool(macro["available"]),
        macro_risk=macro["dominant_risk"],
    )

    if decision_label == "Comprar con cautela":
        next_step = f"{improve_condition} Usar tamano prudente y stop operativo."
    elif decision_label == "Esperar confirmacion":
        next_step = improve_condition
    elif decision_label == "Evitar por ahora":
        next_step = improve_condition
    elif decision_label == "Vigilar":
        next_step = improve_condition
    else:
        next_step = improve_condition
    action_plan = f"{next_step} {_genesis_action(decision_label)}"

    evidence_score, risk_score = _score_context(
        price=price,
        history_count=history_count,
        relative_volume=relative_volume,
        profile_available=profile_available,
        news_count=len(news),
        flow_detected=bool(money_flow.get("flow_detected")),
        whale_identified=bool(money_flow.get("whale_identified")),
        macro_available=bool(macro["available"]),
        trend_label=trend_label,
        percent_change=percent_change,
    )

    company_name = _compact_text(
        profile.get("companyName") or profile.get("companyNameUSD") or profile.get("name") or quote.get("name") or normalized_ticker,
        120,
    )
    whale_read = (
        f"Ballena identificada: {_compact_text((money_flow.get('item') or {}).get('whale_entity'), 120)}."
        if money_flow.get("whale_identified")
        else "Sin ballena identificada con la fuente activa."
    )

    recent_news = _recent_news_items(news)
    money_flow_context = {
        "summary": money_flow["answer"] if money_flow.get("flow_detected") else "Sin senal confiable de Dinero Grande.",
        "flow_detected": bool(money_flow.get("flow_detected")),
        "source": "lectura guardada de flujo",
    }
    whale_context = {
        "identified": bool(money_flow.get("whale_identified")),
        "summary": whale_read,
        "entity": _compact_text((money_flow.get("item") or {}).get("whale_entity"), 120) if money_flow.get("whale_identified") else "",
        "amount": _compact_text((money_flow.get("item") or {}).get("movement_value"), 80),
        "date": _compact_text((money_flow.get("item") or {}).get("timestamp"), 80),
        "confidence": _compact_text((money_flow.get("item") or {}).get("confidence") or "no concluyente", 80),
    }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "ticker": normalized_ticker,
        "company_name": company_name,
        "price": price,
        "change": change,
        "percent_change": percent_change,
        "daily_change": change,
        "daily_change_pct": percent_change,
        "volume": volume,
        "sector": _compact_text(profile.get("sector"), 100),
        "industry": _compact_text(profile.get("industry"), 120),
        "trend_30d": trend_label,
        "trend_summary": trend_summary,
        "technical_read": _technical_read(
            price,
            percent_change,
            volume,
            avg_volume,
            trend_label,
            _safe_float(history_metrics.get("support")),
            _safe_float(history_metrics.get("resistance")),
        ),
        "fundamental_read": _fundamental_read(profile, quote),
        "news_read": _news_read(news),
        "recent_news": recent_news,
        "money_flow_read": money_flow["answer"],
        "money_flow": money_flow_context,
        "whale_read": whale_read,
        "whale_context": whale_context,
        "macro_read": macro["summary"] if macro["available"] else "Sin contexto macro/noticias activo.",
        "macro_context": {
            "available": bool(macro["available"]),
            "summary": macro["summary"] if macro["available"] else "Sin catalizador macro/noticias confirmado en esta lectura.",
            "bias": macro["bias"],
            "dominant_risk": macro["dominant_risk"],
            "confidence": macro["confidence"],
        },
        "support": history_metrics.get("support"),
        "resistance": history_metrics.get("resistance"),
        "support_level": history_metrics.get("support"),
        "resistance_level": history_metrics.get("resistance"),
        "max_30d": history_metrics.get("max_30d"),
        "min_30d": history_metrics.get("min_30d"),
        "avg_volume": avg_volume,
        "relative_volume": relative_volume,
        "history_points": history_metrics.get("count"),
        "evidence_score": evidence_score,
        "risk_score": risk_score,
        "decision_reason": decision_reason,
        "improve_condition": improve_condition,
        "invalidation_condition": invalidation_condition,
        "verdict": decision_label,
        "verdict_reason": decision_reason,
        "action_plan": action_plan,
        "invalidation": invalidation_condition,
        "risks": risks[:5] or ["Riesgo no concluyente: faltan confirmaciones independientes."],
        "supports": supports[:5] or ["Solo hay evidencia limitada; no alcanza para elevar la lectura."],
        "missing_evidence": sorted(set(missing)),
        "confidence": confidence,
        "decision_label": decision_label,
        "next_step": action_plan,
        "scenarios": _scenario_text(
            normalized_ticker,
            decision_label,
            supports,
            risks,
            _safe_float(history_metrics.get("support")),
            _safe_float(history_metrics.get("resistance")),
        ),
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
