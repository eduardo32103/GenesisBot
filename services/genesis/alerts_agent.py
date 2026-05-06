from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.genesis.market_format import format_signed_money, format_signed_percent, number_or_none
from services.genesis.ticker_parser import normalize_ticker


class AlertsAgent:
    def summary(self) -> dict:
        try:
            from services.dashboard.get_alerts_snapshot import get_alerts_snapshot

            payload = get_alerts_snapshot()
        except Exception:
            payload = {"items": []}
        items = _extract_alert_items(payload)
        if not items:
            items = _derived_market_alerts()
        answer = _answer(items)
        return {
            "intent": "alerts",
            "answer": answer,
            "items": items[:20],
        }


def get_alerts_agent() -> AlertsAgent:
    return AlertsAgent()


def _extract_alert_items(payload: dict[str, Any] | Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    for key in ("items", "recent_alerts"):
        if isinstance(payload.get(key), list):
            return [item for item in payload[key] if isinstance(item, dict)]
    return []


def _derived_market_alerts() -> list[dict[str, Any]]:
    try:
        from services.dashboard.get_radar_snapshot import get_radar_snapshot

        snapshot = get_radar_snapshot()
    except Exception:
        snapshot = {}
    rows = _snapshot_rows(snapshot)
    alerts: list[dict[str, Any]] = []
    for row in rows:
        ticker = normalize_ticker(row.get("ticker") or row.get("symbol"))
        if not ticker:
            continue
        change_pct = number_or_none(row.get("daily_change_pct") or row.get("changesPercentage") or row.get("change_pct"))
        change_usd = number_or_none(row.get("daily_change") or row.get("change"))
        volume = number_or_none(row.get("volume"))
        avg_volume = number_or_none(row.get("avg_volume") or row.get("avgVolume"))
        if change_pct is not None and abs(change_pct) >= 3:
            direction = "alza fuerte" if change_pct > 0 else "baja fuerte"
            alerts.append(
                {
                    "ticker": ticker,
                    "type": "cambio_fuerte_diario",
                    "severity": "alta" if abs(change_pct) >= 5 else "media",
                    "title": f"{ticker}: {direction}",
                    "summary": f"Movimiento diario {format_signed_money(change_usd)} / {format_signed_percent(change_pct)}. Confirmar volumen antes de actuar.",
                    "impact": "positivo" if change_pct > 0 else "negativo",
                    "watch": "Confirmar continuidad, volumen y nivel tecnico cercano.",
                    "source": "portfolio_snapshot",
                    "confidence": "media",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            )
        if volume is not None and avg_volume and avg_volume > 0 and volume / avg_volume >= 1.8:
            alerts.append(
                {
                    "ticker": ticker,
                    "type": "volumen_anormal",
                    "severity": "media",
                    "title": f"{ticker}: volumen anormal",
                    "summary": f"Volumen relativo {volume / avg_volume:.2f}x contra promedio. Revisar si hay ruptura o noticia.",
                    "impact": "por confirmar",
                    "watch": "Precio contra soporte/resistencia y noticia asociada.",
                    "source": "portfolio_snapshot",
                    "confidence": "media",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            )
    return alerts[:20]


def _snapshot_rows(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
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


def _answer(items: list[dict[str, Any]]) -> str:
    if not items:
        return "Sin alertas activas. Genesis mantiene el feed limpio hasta que exista movimiento, volumen o evento confirmado."
    first = items[0]
    ticker = normalize_ticker(first.get("ticker") or first.get("symbol")) or "mercado"
    title = str(first.get("title") or first.get("event") or first.get("type") or "alerta activa").strip()
    return f"{len(items)} alertas activas. Principal: {ticker} - {title}. Genesis no eleva alertas sin evidencia de precio, volumen o evento."
