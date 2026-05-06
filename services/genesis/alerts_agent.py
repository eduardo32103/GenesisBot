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
            return [_normalize_alert(item) for item in payload[key] if isinstance(item, dict)]
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
            direction = "bullish" if change_pct > 0 else "bearish"
            direction_text = "alza fuerte" if change_pct > 0 else "baja fuerte"
            severity = "high" if abs(change_pct) >= 5 else "medium"
            created_at = datetime.now(timezone.utc).isoformat()
            alerts.append(
                {
                    "id": f"technical:{ticker}:daily_move:{created_at}",
                    "ticker": ticker,
                    "type": "cambio_fuerte_diario",
                    "direction": direction,
                    "severity": severity,
                    "title": f"{ticker}: {direction_text}",
                    "summary": f"Movimiento diario {format_signed_money(change_usd)} / {format_signed_percent(change_pct)}. Confirmar volumen antes de actuar.",
                    "impact": "positivo" if change_pct > 0 else "negativo",
                    "impact_text": "Impulso alcista de corto plazo" if change_pct > 0 else "Presion bajista de corto plazo",
                    "watch": "Confirmar continuidad, volumen y nivel tecnico cercano.",
                    "genesis_reading": "Movimiento real detectado en precio. No es recomendacion operativa sin confirmacion de volumen y nivel tecnico.",
                    "evidence": {
                        "daily_change": change_usd,
                        "daily_change_pct": change_pct,
                    },
                    "source": "technical",
                    "confidence": "medium",
                    "created_at": created_at,
                    "related_price": number_or_none(row.get("current_price") or row.get("price")),
                    "mini_series": _mini_series(row),
                }
            )
        if volume is not None and avg_volume and avg_volume > 0 and volume / avg_volume >= 1.8:
            created_at = datetime.now(timezone.utc).isoformat()
            alerts.append(
                {
                    "id": f"technical:{ticker}:volume:{created_at}",
                    "ticker": ticker,
                    "type": "volumen_anormal",
                    "direction": "neutral",
                    "severity": "medium",
                    "title": f"{ticker}: volumen anormal",
                    "summary": f"Volumen relativo {volume / avg_volume:.2f}x contra promedio. Revisar si hay ruptura o noticia.",
                    "impact": "por confirmar",
                    "impact_text": "Actividad inusual que exige confirmar direccion del precio.",
                    "watch": "Precio contra soporte/resistencia y noticia asociada.",
                    "genesis_reading": "Hay evidencia de volumen relativo alto; falta validar si el flujo confirma ruptura o distribucion.",
                    "evidence": {
                        "volume": volume,
                        "avg_volume": avg_volume,
                        "relative_volume": round(volume / avg_volume, 4),
                    },
                    "source": "technical",
                    "confidence": "medium",
                    "created_at": created_at,
                    "related_price": number_or_none(row.get("current_price") or row.get("price")),
                    "mini_series": _mini_series(row),
                }
            )
    return alerts[:20]


def _normalize_alert(item: dict[str, Any]) -> dict[str, Any]:
    ticker = normalize_ticker(item.get("ticker") or item.get("symbol"))
    alert_type = str(item.get("type") or item.get("event") or "alerta").strip() or "alerta"
    created_at = str(item.get("created_at") or item.get("timestamp") or datetime.now(timezone.utc).isoformat())
    direction = str(item.get("direction") or "").lower()
    if direction not in {"bullish", "bearish", "neutral"}:
        impact = str(item.get("impact") or item.get("impacto") or "").lower()
        direction = "bullish" if "posit" in impact or "alc" in impact else "bearish" if "neg" in impact or "baj" in impact else "neutral"
    severity = str(item.get("severity") or "").lower()
    severity_map = {"alta": "high", "media": "medium", "baja": "low"}
    severity = severity_map.get(severity, severity if severity in {"low", "medium", "high"} else "medium")
    return {
        **item,
        "id": str(item.get("id") or f"{item.get('source') or 'alert'}:{ticker}:{alert_type}:{created_at}"),
        "ticker": ticker,
        "type": alert_type,
        "direction": direction,
        "severity": severity,
        "confidence": item.get("confidence") or "medium",
        "title": str(item.get("title") or f"{ticker}: {alert_type}").strip(),
        "impact_text": str(item.get("impact_text") or item.get("summary") or item.get("impact") or "Evento en vigilancia.").strip(),
        "genesis_reading": str(item.get("genesis_reading") or item.get("summary") or item.get("watch") or "").strip(),
        "evidence": item.get("evidence") if isinstance(item.get("evidence"), dict) else {},
        "created_at": created_at,
        "source": str(item.get("source") or "snapshot").strip(),
        "related_price": number_or_none(item.get("related_price") or item.get("current_price") or item.get("price")),
        "mini_series": item.get("mini_series") if isinstance(item.get("mini_series"), list) else _mini_series(item),
    }


def _mini_series(row: dict[str, Any]) -> list[float]:
    series = row.get("mini_series") or row.get("sparkline") or row.get("prices")
    if isinstance(series, list):
        clean = [number_or_none(item) for item in series]
        return [float(item) for item in clean if item is not None][-12:]
    previous = number_or_none(row.get("previous_close") or row.get("previousClose"))
    current = number_or_none(row.get("current_price") or row.get("price"))
    if previous is not None and current is not None:
        return [previous, current]
    return []


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
