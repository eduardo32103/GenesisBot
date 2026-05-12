from __future__ import annotations

from typing import Any

from services.genesis.market_format import format_market_number, format_signed_money, format_signed_percent, number_or_none


class ResponseComposer:
    def general(self) -> str:
        return (
            "Estoy activo. Puedo leer mercado, cartera, noticias, alertas, ballenas, clima o una gráfica; "
            "cuando falte una fuente te lo digo claro y cuando haya datos los convierto en lectura útil."
        )

    def greeting(self) -> str:
        return (
            "Hola, estoy aquí. Tengo el radar financiero listo: precios, noticias, alertas y flujo institucional "
            "separados de lo no confirmado. Dime un activo o una pregunta y lo convierto en lectura Genesis."
        )

    def no_confirmed_price(self, ticker: str) -> str:
        return f"{ticker}: no tengo precio confirmado para ese activo."

    def compact(self, parts: list[Any]) -> str:
        return " ".join(str(part).strip() for part in parts if str(part or "").strip())

    def asset_analysis(
        self,
        ticker: str,
        *,
        quote: dict[str, Any],
        technical: dict[str, Any] | None = None,
        alerts: list[dict[str, Any]] | None = None,
        whales: dict[str, Any] | None = None,
        news: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        price = quote.get("current_price")
        pct = number_or_none(quote.get("daily_change_pct"))
        bias = _bias_from_pct(pct)
        indicators = (technical or {}).get("indicators") if isinstance((technical or {}).get("indicators"), dict) else {}
        levels = {
            "support": indicators.get("support"),
            "resistance": indicators.get("resistance"),
            "golden_pocket": indicators.get("golden_pocket"),
        }
        alert_items = alerts or []
        whale_events = (whales or {}).get("events") if isinstance(whales, dict) else []
        title = f"{ticker}: lectura Genesis"
        thesis = _asset_thesis(ticker, quote, indicators, alert_items)
        return {
            "kind": "asset_analysis",
            "ticker": ticker,
            "title": title,
            "thesis": thesis,
            "bias": bias,
            "confidence": _confidence(quote, technical, alert_items),
            "price": {
                "current_price": price,
                "formatted_price": quote.get("formatted_price") or format_market_number(price, currency=quote.get("currency") or "USD"),
                "previous_close": quote.get("previous_close"),
                "daily_change": quote.get("daily_change"),
                "daily_change_pct": quote.get("daily_change_pct"),
                "source": quote.get("source"),
                "is_live": quote.get("is_live"),
                "sanity": quote.get("sanity"),
            },
            "chart": (technical or {}).get("chart") or {},
            "indicators": {
                "rsi": indicators.get("rsi"),
                "macd": indicators.get("macd"),
                "volume": indicators.get("volume"),
                "avg_volume_20": indicators.get("avg_volume_20"),
                "relative_volume": indicators.get("relative_volume"),
                "trend": indicators.get("trend"),
                "momentum": indicators.get("momentum"),
                "risk": indicators.get("risk"),
                "sma": indicators.get("sma"),
                "ema": indicators.get("ema"),
                "fibonacci": indicators.get("fibonacci"),
                "golden_pocket": indicators.get("golden_pocket"),
                "volatility": indicators.get("volatility"),
                "support": indicators.get("support"),
                "resistance": indicators.get("resistance"),
            },
            "levels": levels,
            "scenario": _scenario(bias, indicators),
            "news_impact": {
                "items": news or [],
                "summary": "Sin noticia confirmada en la fuente activa." if not news else f"{len(news)} noticias confirmadas para contexto.",
            },
            "whale_activity": {
                "events": whale_events or [],
                "summary": (whales or {}).get("summary") if isinstance(whales, dict) else {},
            },
            "alerts": alert_items,
            "sections": [
                {"title": "Lectura rapida", "bullets": [thesis]},
                {"title": "Catalizadores", "bullets": _catalysts(quote, indicators, news or [], whale_events or [])},
                {"title": "Riesgos", "bullets": _risks(quote, indicators, alert_items)},
                {"title": "Que vigilar", "bullets": _watch_items(ticker, indicators, levels)},
            ],
        }

    def market_briefing(self, overview: dict[str, Any]) -> dict[str, Any]:
        return {
            "kind": "market_briefing",
            "tone": overview.get("tone") or "sin confirmacion suficiente",
            "summary": overview.get("summary") or overview.get("answer") or "",
            "movers": overview.get("movers") or [],
            "risks": overview.get("risks") or [],
            "alerts": overview.get("alerts") or [],
            "whales": overview.get("whales") or [],
            "news": overview.get("news") or [],
            "watch": overview.get("watch") or [],
            "source_status": overview.get("source_status") or {},
            "sections": [
                {"title": "Lectura rapida", "bullets": [overview.get("summary") or overview.get("answer") or "Sin lectura confirmada."]},
                {"title": "Riesgos", "bullets": [item.get("text") or str(item) for item in overview.get("risks", [])[:4]]},
                {"title": "Que vigilar", "bullets": [item.get("reason") or item.get("ticker") or str(item) for item in overview.get("watch", [])[:4]]},
            ],
        }

    def news_brief(self, overview: dict[str, Any]) -> dict[str, Any]:
        news = overview.get("news") if isinstance(overview.get("news"), list) else []
        important = [item for item in news if item.get("is_important")][:5] or news[:3]
        latest = sorted(news, key=lambda item: str(item.get("published_at") or item.get("date") or ""), reverse=True)[:8]
        summary = overview.get("summary") or overview.get("answer") or "Genesis revisa titulares confirmados sin inventar catalizadores."
        return {
            "kind": "news_brief",
            "tone": overview.get("tone") or "neutral",
            "summary": summary,
            "important_news": important,
            "latest_news": latest,
            "news": news,
            "alerts": overview.get("alerts") or [],
            "whales": overview.get("whales") or [],
            "watch": overview.get("watch") or [],
            "source_status": overview.get("source_status") or {},
            "sections": [
                {"title": "Lectura rapida", "bullets": [summary]},
                {"title": "Importantes", "bullets": _news_titles(important, 4)},
                {"title": "Ultimas", "bullets": _news_titles(latest, 4)},
                {"title": "Que vigilar", "bullets": [item.get("reason") or item.get("ticker") or str(item) for item in overview.get("watch", [])[:4]] or ["Confirmar impacto en precio, volumen y activos propios."]},
            ],
        }

    def alerts_digest(self, payload: dict[str, Any]) -> dict[str, Any]:
        alerts = payload.get("items") if isinstance(payload.get("items"), list) else []
        summary_map = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        summary = payload.get("answer") or summary_map.get("engine_summary") or ""
        return {
            "kind": "alerts_digest",
            "title": "Alertas Genesis",
            "summary": summary or "Genesis vigila precio, volumen, soporte/resistencia y contexto sin inventar señales.",
            "alerts": alerts[:8],
            "metrics": {
                "total": len(alerts),
                "high": len([item for item in alerts if str(item.get("severity")).lower() == "high"]),
                "technical": len([item for item in alerts if str(item.get("source")).lower() == "technical"]),
            },
            "sections": [
                {"title": "Lectura rapida", "bullets": [_alert_line(item) for item in alerts[:3]] or ["Sin alerta fuerte confirmada."]},
                {"title": "Que vigilar", "bullets": [str(item.get("what_to_watch") or item.get("summary") or "").strip() for item in alerts[:4] if str(item.get("what_to_watch") or item.get("summary") or "").strip()] or ["Confirmar precio, volumen relativo y niveles."]},
            ],
        }

    def whale_flow(self, payload: dict[str, Any]) -> dict[str, Any]:
        events = payload.get("events") if isinstance(payload.get("events"), list) else []
        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        confirmed = [item for item in events if item.get("event_type") == "whale_confirmed" and item.get("entity_name")]
        estimated = [item for item in events if item not in confirmed]
        return {
            "kind": "whale_flow",
            "title": "Ballenas / Smart money",
            "summary": payload.get("answer") or _whale_summary_line(summary, confirmed, estimated),
            "events": events[:8],
            "confirmed": confirmed[:5],
            "estimated": estimated[:5],
            "metrics": {
                "confirmed_value": summary.get("confirmed_value"),
                "watched_volume": summary.get("watched_volume") or summary.get("total_estimated_value"),
                "confirmed_count": len(confirmed),
                "estimated_count": len(estimated),
                "confidence": summary.get("confidence") or "low",
            },
            "sections": [
                {"title": "Lectura rápida", "bullets": [_whale_event_line(item) for item in events[:3]] or ["No hay ballena confirmada con entidad y monto; Genesis sigue el volumen vigilado y lo separa de una compra real."]},
                {"title": "Qué NO significa", "bullets": ["El dollar volume técnico no es monto confirmado de ballena.", "Sin entidad confirmada no se afirma compra directa."]},
                {"title": "Qué vigilar", "bullets": ["Volumen relativo, ruptura o rechazo de niveles, reacción de precio y noticias relacionadas."]},
            ],
        }

    def performance_review(self, report: dict[str, Any]) -> dict[str, Any]:
        metrics = report.get("metrics") if isinstance(report.get("metrics"), dict) else {}
        today = report.get("today") if isinstance(report.get("today"), dict) else {}
        recent = report.get("recent") if isinstance(report.get("recent"), list) else []
        learning = report.get("learning") if isinstance(report.get("learning"), list) else []
        accuracy = metrics.get("accuracy")
        accuracy_text = f"{accuracy}%" if accuracy is not None else "pendiente"
        return {
            "kind": "performance_review",
            "title": "Precision Genesis",
            "summary": report.get("answer") or "Genesis mide aciertos, fallos y tesis abiertas desde su memoria.",
            "metrics": {
                "hits": metrics.get("hits") or 0,
                "misses": metrics.get("misses") or 0,
                "watching": metrics.get("watching") or 0,
                "accuracy": accuracy,
                "missing_price": metrics.get("missing_price") or 0,
                "priced_decisions": metrics.get("priced_decisions") or 0,
            },
            "today": {
                "hits": today.get("hits") or 0,
                "misses": today.get("misses") or 0,
                "watching": today.get("watching") or 0,
            },
            "recent": recent[:6],
            "best": report.get("best"),
            "worst": report.get("worst"),
            "sections": [
                {"title": "Hoy", "bullets": [f"{today.get('hits') or 0} aciertos, {today.get('misses') or 0} fallos, {today.get('watching') or 0} en vigilancia."]},
                {"title": "Precision", "bullets": [f"Precision medida: {accuracy_text}.", f"Decisiones con precio: {metrics.get('priced_decisions') or 0}."]},
                {"title": "Aprendizaje", "bullets": learning[:4] or ["Aun falta historial; Genesis seguira midiendo outcomes."]},
            ],
        }

    def general_answer(self, answer: str) -> dict[str, Any]:
        return {"kind": "general_answer", "answer": answer, "sections": [{"title": "Respuesta", "bullets": [answer]}]}


def _bias_from_pct(value: float | None) -> str:
    if value is None:
        return "neutral"
    if value > 0.35:
        return "bullish"
    if value < -0.35:
        return "bearish"
    return "neutral"


def _confidence(quote: dict[str, Any], technical: dict[str, Any] | None, alerts: list[dict[str, Any]]) -> float:
    score = 0.35
    if quote.get("current_price") and quote.get("sanity", {}).get("ok", True):
        score += 0.3
    if technical and technical.get("ok"):
        score += 0.2
    if alerts:
        score += 0.1
    return round(min(score, 0.9), 2)


def _asset_thesis(ticker: str, quote: dict[str, Any], indicators: dict[str, Any], alerts: list[dict[str, Any]]) -> str:
    if not quote.get("current_price"):
        return f"{ticker} no tiene precio confirmado; Genesis evita lectura operativa hasta reconfirmar la fuente."
    pct = format_signed_percent(quote.get("daily_change_pct"))
    trend = indicators.get("trend") or "tendencia sin confirmar"
    alert_text = f" Hay {len(alerts)} alertas con evidencia." if alerts else ""
    return f"{ticker} cotiza en {quote.get('formatted_price') or format_market_number(quote.get('current_price'))} ({pct}). La lectura tecnica marca {trend}.{alert_text}"


def _scenario(bias: str, indicators: dict[str, Any]) -> dict[str, Any]:
    support = indicators.get("support")
    resistance = indicators.get("resistance")
    if bias == "bullish":
        return {"probable": "continuacion si respeta soporte y confirma volumen", "invalidacion": f"perder soporte {support}" if support else "perder soporte inmediato"}
    if bias == "bearish":
        return {"probable": "presion mientras no recupere resistencia", "invalidacion": f"recuperar resistencia {resistance}" if resistance else "recuperar resistencia inmediata"}
    return {"probable": "rango o espera de confirmacion", "invalidacion": "ruptura con volumen contra el sesgo"}


def _catalysts(quote: dict[str, Any], indicators: dict[str, Any], news: list[dict[str, Any]], whales: list[dict[str, Any]]) -> list[str]:
    items = []
    pct = number_or_none(quote.get("daily_change_pct"))
    if pct is not None:
        items.append(f"Movimiento diario {format_signed_money(quote.get('daily_change'))} / {format_signed_percent(pct)} confirmado.")
    if indicators.get("momentum"):
        items.append(f"Momentum: {indicators.get('momentum')}.")
    if news:
        items.append(f"{len(news)} noticias confirmadas aportan contexto.")
    if whales:
        items.append(f"{len(whales)} eventos institucionales confirmados.")
    return items or ["Sin catalizador confirmado; priorizar precio, volumen y niveles."]


def _risks(quote: dict[str, Any], indicators: dict[str, Any], alerts: list[dict[str, Any]]) -> list[str]:
    items = []
    if indicators.get("risk"):
        items.append(f"Riesgo tecnico: {indicators.get('risk')}.")
    if any(str(item.get("severity")) == "high" for item in alerts):
        items.append("Hay alertas de severidad alta que elevan el riesgo de volatilidad.")
    if quote.get("sanity", {}).get("suspicious"):
        items.append("Precio sospechoso bloqueado por la capa PriceTruth.")
    return items or ["Riesgo principal: operar sin confirmacion de volumen y nivel."]


def _watch_items(ticker: str, indicators: dict[str, Any], levels: dict[str, Any]) -> list[str]:
    items = []
    if levels.get("support"):
        items.append(f"{ticker}: soporte {levels['support']}.")
    if levels.get("resistance"):
        items.append(f"{ticker}: resistencia {levels['resistance']}.")
    if indicators.get("rsi") is not None:
        items.append(f"RSI {indicators.get('rsi')}.")
    if indicators.get("macd"):
        items.append("MACD y volumen para confirmar direccion.")
    return items or ["Precio confirmado, volumen relativo y cierre de vela."]


def _news_titles(items: list[dict[str, Any]], limit: int) -> list[str]:
    titles = []
    for item in items[:limit]:
        title = str(item.get("title") or item.get("headline") or "").strip()
        source = str(item.get("source") or "").strip()
        if title and source:
            titles.append(f"{title} ({source})")
        elif title:
            titles.append(title)
    return titles or ["Sin titular confirmado en la fuente activa."]


def _alert_line(item: dict[str, Any]) -> str:
    ticker = str(item.get("ticker") or "Mercado")
    title = str(item.get("title") or item.get("summary") or "Alerta")
    pct = format_signed_percent(item.get("change_pct")) if item.get("change_pct") is not None else "sin cambio directo"
    rel = item.get("relative_volume")
    rel_text = f", vol. rel {rel:.1f}x" if isinstance(rel, (int, float)) else ""
    return f"{ticker}: {title} ({pct}{rel_text})."


def _whale_event_line(item: dict[str, Any]) -> str:
    ticker = str(item.get("ticker") or "Mercado")
    event_type = str(item.get("event_type") or "")
    if event_type == "whale_confirmed":
        amount = format_market_number(item.get("amount_usd"), currency="USD") if item.get("amount_usd") is not None else "monto no confirmado"
        return f"{ticker}: ballena confirmada por {item.get('entity_name') or 'fuente activa'}, {amount}."
    volume_value = item.get("monitored_dollar_volume") if item.get("monitored_dollar_volume") is not None else item.get("dollar_volume")
    volume = format_market_number(volume_value, currency="USD") if volume_value is not None else "sin volumen confirmado"
    direction = item.get("estimated_flow_direction") or item.get("direction_estimate") or "neutral"
    return f"{ticker}: smart money estimado; {volume} vigilado, dirección {direction}; no es compra confirmada."


def _whale_summary_line(summary: dict[str, Any], confirmed: list[dict[str, Any]], estimated: list[dict[str, Any]]) -> str:
    if confirmed:
        return f"{len(confirmed)} ballenas confirmadas y {len(estimated)} flujos estimados en vigilancia."
    if estimated:
        return f"No hay ballenas confirmadas; Genesis vigila {len(estimated)} flujos estimados por volumen, precio y dirección."
    return "No hay ballena confirmada con entidad y monto; Genesis mantiene vigilancia de volumen y precio sin inventar comprador."


def get_response_composer() -> ResponseComposer:
    return ResponseComposer()
