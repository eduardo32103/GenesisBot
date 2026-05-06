from __future__ import annotations

from typing import Any

from services.genesis.market_format import format_market_number, format_signed_money, format_signed_percent, number_or_none


class ResponseComposer:
    def general(self) -> str:
        return "Te sigo. Puedo revisar mercado, cartera, seguimiento, ballenas, alertas, clima o una grafica. Si falta una fuente, te lo digo sin inventar datos."

    def greeting(self) -> str:
        return "Hola. Que quieres revisar hoy?"

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
                "trend": indicators.get("trend"),
                "momentum": indicators.get("momentum"),
                "risk": indicators.get("risk"),
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


def get_response_composer() -> ResponseComposer:
    return ResponseComposer()
