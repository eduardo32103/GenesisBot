from __future__ import annotations

from typing import Any

from services.genesis.market_format import format_market_number, format_signed_money, format_signed_percent, number_or_none
from services.genesis.price_agent import get_price_agent
from services.genesis.ticker_parser import normalize_ticker

_MARKET_TICKERS = ("SPY", "QQQ", "DIA", "BTC-USD", "BZ=F", "IAU", "SLV")


class MarketOverviewAgent:
    def overview(self, question: str = "") -> dict:
        price_agent = get_price_agent()
        quotes = [price_agent.quote(ticker) for ticker in _MARKET_TICKERS]
        confirmed = [quote for quote in quotes if quote.get("current_price")]
        radar = _safe_radar_snapshot()
        alerts = _safe_alerts_snapshot()
        watchlist = _watchlist_items(radar)
        paper = _paper_items(radar)
        movers = _watchlist_movers(watchlist)
        alert_items = _alert_items(alerts)

        unavailable_note = "" if confirmed else "Falta precio confirmado en indices principales; mantengo lectura conservadora."
        answer = _compose_market_briefing(
            confirmed=confirmed,
            movers=movers,
            paper=paper,
            alerts=alert_items,
            unavailable_note=unavailable_note,
            question=question,
        )
        return {
            "intent": "market_overview",
            "answer": answer,
            "quotes": quotes,
            "watchlist_movers": movers,
            "paper_positions": paper[:8],
            "alerts": alert_items[:8],
            "source_policy": "Precios y cambios confirmados por FMP o snapshot validado; Genesis no inventa datos.",
        }


def _compose_market_briefing(
    *,
    confirmed: list[dict[str, Any]],
    movers: dict[str, list[dict[str, Any]]],
    paper: list[dict[str, Any]],
    alerts: list[dict[str, Any]],
    unavailable_note: str,
    question: str,
) -> str:
    tone = _market_tone(confirmed)
    up = _quote_line([quote for quote in confirmed if (number_or_none(quote.get("daily_change_pct")) or 0) > 0][:4])
    down = _quote_line([quote for quote in confirmed if (number_or_none(quote.get("daily_change_pct")) or 0) < 0][:4])
    watch_up = _item_line(movers.get("up", [])[:3])
    watch_down = _item_line(movers.get("down", [])[:3])
    paper_line = _paper_line(paper)
    alert_line = _alert_line(alerts)
    missing = f"\n\nFuente: {unavailable_note}" if unavailable_note else ""
    date_context = " para la ventana consultada" if "viernes" in str(question or "").casefold() or "ayer" in str(question or "").casefold() else ""
    return (
        f"1. Lectura rapida\n"
        f"El mercado luce {tone}{date_context}. No trato esto como senal operativa sin confirmacion de volumen y riesgo.\n\n"
        f"2. Lo que sube / baja\n"
        f"Sube: {up or watch_up or 'sin liderazgo confirmado'}.\n"
        f"Baja: {down or watch_down or 'sin presion confirmada'}.\n\n"
        f"3. Riesgos\n"
        f"Riesgo principal: cambios de amplitud, volatilidad y falta de confirmacion macro/noticias si la fuente no trae contexto externo.\n\n"
        f"4. Ballenas / flujo\n"
        f"Sin ballena institucional confirmada con la fuente activa.\n\n"
        f"5. Alertas relevantes\n"
        f"{alert_line or 'Sin alertas activas de alta prioridad en la lectura disponible.'}\n\n"
        f"6. Que vigilar\n"
        f"Vigilar liderazgo en SPY/QQQ, BTC, Brent y los mayores movimientos de tu watchlist. {paper_line}\n\n"
        f"7. Siguiente paso\n"
        f"Si quieres precision operativa, dime el activo y temporalidad: Genesis abre precio confirmado, velas y retornos 1D/1W/1M/1Y/5Y/MAX."
        f"{missing}"
    )


def _market_tone(quotes: list[dict[str, Any]]) -> str:
    values = [number_or_none(quote.get("daily_change_pct")) for quote in quotes]
    values = [value for value in values if value is not None]
    if not values:
        return "sin confirmacion suficiente"
    average = sum(values) / len(values)
    if average > 0.25:
        return "constructivo"
    if average < -0.25:
        return "presionado"
    return "mixto"


def _quote_line(quotes: list[dict[str, Any]]) -> str:
    parts = []
    for quote in quotes:
        ticker = quote.get("ticker") or "Activo"
        price = format_market_number(quote.get("current_price"), currency=quote.get("currency") or "USD")
        pct = format_signed_percent(quote.get("daily_change_pct"))
        parts.append(f"{ticker} {price} ({pct})")
    return ", ".join(parts)


def _item_line(items: list[dict[str, Any]]) -> str:
    parts = []
    for item in items:
        ticker = normalize_ticker(item.get("ticker") or item.get("symbol"))
        pct = format_signed_percent(item.get("daily_change_pct") or item.get("changesPercentage"))
        if ticker:
            parts.append(f"{ticker} {pct}")
    return ", ".join(parts)


def _paper_line(items: list[dict[str, Any]]) -> str:
    if not items:
        return "Cartera paper sin posiciones confirmadas."
    total = sum((number_or_none(item.get("market_value") or item.get("current_value")) or 0) for item in items)
    leaders = ", ".join(normalize_ticker(item.get("ticker") or item.get("symbol")) for item in items[:3])
    value = format_market_number(total) if total > 0 else "valor sin confirmar"
    return f"Cartera paper: {leaders or 'posiciones'} con {value} estimado."


def _alert_line(items: list[dict[str, Any]]) -> str:
    if not items:
        return ""
    first = items[0]
    ticker = normalize_ticker(first.get("ticker") or first.get("symbol")) or "mercado"
    title = str(first.get("title") or first.get("event") or first.get("summary") or "alerta activa").strip()
    return f"Alertas: {ticker} - {title[:120]}."


def _watchlist_movers(items: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    with_pct = [item for item in items if number_or_none(item.get("daily_change_pct") or item.get("changesPercentage")) is not None]
    ranked = sorted(with_pct, key=lambda item: number_or_none(item.get("daily_change_pct") or item.get("changesPercentage")) or 0, reverse=True)
    return {
        "up": [item for item in ranked if (number_or_none(item.get("daily_change_pct") or item.get("changesPercentage")) or 0) > 0],
        "down": [item for item in reversed(ranked) if (number_or_none(item.get("daily_change_pct") or item.get("changesPercentage")) or 0) < 0],
    }


def _watchlist_items(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    return [item for item in _snapshot_items(snapshot) if item.get("watchlist")]


def _paper_items(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        item
        for item in _snapshot_items(snapshot)
        if str(item.get("mode") or item.get("position_mode") or "").lower() == "paper" or (number_or_none(item.get("units")) or 0) > 0
    ]


def _snapshot_items(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(snapshot, dict):
        return []
    for key in ("items", "positions"):
        if isinstance(snapshot.get(key), list):
            return [item for item in snapshot[key] if isinstance(item, dict)]
    portfolio = snapshot.get("portfolio") if isinstance(snapshot.get("portfolio"), dict) else {}
    for key in ("items", "positions"):
        if isinstance(portfolio.get(key), list):
            return [item for item in portfolio[key] if isinstance(item, dict)]
    return []


def _alert_items(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(snapshot, dict):
        return []
    for key in ("items", "recent_alerts"):
        if isinstance(snapshot.get(key), list):
            return [item for item in snapshot[key] if isinstance(item, dict)]
    return []


def _safe_radar_snapshot() -> dict[str, Any]:
    try:
        from services.dashboard.get_radar_snapshot import get_radar_snapshot

        payload = get_radar_snapshot()
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _safe_alerts_snapshot() -> dict[str, Any]:
    try:
        from services.dashboard.get_alerts_snapshot import get_alerts_snapshot

        payload = get_alerts_snapshot()
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def get_market_overview_agent() -> MarketOverviewAgent:
    return MarketOverviewAgent()
