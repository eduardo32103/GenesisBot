from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.settings import load_settings
from services.dashboard.get_operational_health import _connect_database, _safe_iso

_DEFAULT_ALERT_WINDOW_DAYS = 45
_DEFAULT_RECENT_LIMIT = 6

_ALERT_TYPE_LABELS = {
    "geo_macro": "Geo / Macro",
    "sentinel_news": "Sentinela",
    "protection": "Proteccion",
    "divergence": "Divergencia",
    "market": "Market",
}


def _empty_snapshot(note: str) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "window_days": _DEFAULT_ALERT_WINDOW_DAYS,
            "total_recent": 0,
            "active_alerts": 0,
            "validated_alerts": 0,
            "avg_score": None,
            "win_rate": None,
            "pass_rate": None,
            "engine_summary": note,
            "data_origin": "unavailable",
            "last_update": "",
        },
        "items": [],
        "recent_alerts": [],
    }


def _normalize_label(alert_type: str) -> str:
    raw = str(alert_type or "").strip().lower()
    return _ALERT_TYPE_LABELS.get(raw, raw.replace("_", " ").title() or "Alerta")


def _build_engine_summary(total_recent: int, validated_alerts: int, avg_score: float | None, win_rate: float | None, pass_rate: float | None) -> str:
    if total_recent <= 0:
        return "Todavia no hay alertas recientes registradas."
    if validated_alerts <= 0:
        if pass_rate is not None:
            return f"Hay alertas recientes y filtro activo ({pass_rate:.1f}% de paso), pero aun sin validaciones suficientes."
        return "Hay alertas recientes, pero todavia no existen validaciones suficientes para puntuar el motor."
    if avg_score is None or win_rate is None:
        return "Existen validaciones recientes, pero el score agregado aun no esta disponible de forma consistente."
    if avg_score >= 1.0 and win_rate >= 55:
        return f"El motor viene leyendo bien en la ventana reciente (score {avg_score:+.2f}, acierto {win_rate:.1f}%)."
    if avg_score <= -0.35 or win_rate < 45:
        return f"El motor esta en zona fragil en la ventana reciente (score {avg_score:+.2f}, acierto {win_rate:.1f}%)."
    return f"El motor esta mixto pero utilizable (score {avg_score:+.2f}, acierto {win_rate:.1f}%)."


def _format_alert_state(event_status: str, validation_outcome: str) -> str:
    outcome = str(validation_outcome or "").strip().lower()
    if outcome.startswith("ganadora_fuerte"):
        return "Validada fuerte"
    if outcome.startswith("ganadora"):
        return "Validada positiva"
    if outcome.startswith("fallida_fuerte"):
        return "Validada negativa fuerte"
    if outcome.startswith("fallida"):
        return "Validada negativa"
    if outcome.startswith("mixta"):
        return "Validada mixta"

    status = str(event_status or "tracking").strip().lower()
    if status == "completed":
        return "Completada"
    return "Seguimiento"


def _fetch_alerts_snapshot(database_url: str, *, window_days: int = _DEFAULT_ALERT_WINDOW_DAYS, recent_limit: int = _DEFAULT_RECENT_LIMIT) -> dict[str, Any]:
    conn = None
    cutoff = (datetime.now(timezone.utc) - timedelta(days=max(7, int(window_days)))).isoformat()
    try:
        conn = _connect_database(database_url)
        if not conn:
            return _empty_snapshot("No pude conectarme a la base de datos para leer alertas.")

        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT COUNT(*) AS total_recent,
                   SUM(CASE WHEN status = 'completed' THEN 0 ELSE 1 END) AS active_alerts,
                   MAX(created_at) AS last_created_at
            FROM alert_events
            WHERE created_at >= %s
            """,
            (cutoff,),
        )
        totals_row = cursor.fetchone() or (0, 0, "")
        total_recent = int(totals_row[0] or 0)
        active_alerts = int(totals_row[1] or 0)
        last_created_at = _safe_iso(totals_row[2])

        cursor.execute(
            """
            SELECT COUNT(DISTINCT e.alert_id) AS validated_alerts,
                   AVG(v.score_value) AS avg_score,
                   SUM(CASE WHEN LOWER(COALESCE(v.outcome_label, '')) LIKE 'ganadora%%' THEN 1 ELSE 0 END) AS wins,
                   COUNT(*) AS validation_count,
                   MAX(v.evaluated_at) AS last_evaluated_at
            FROM alert_validations v
            JOIN alert_events e ON e.alert_id = v.alert_id
            WHERE e.created_at >= %s AND v.evaluated_at IS NOT NULL
            """,
            (cutoff,),
        )
        validation_row = cursor.fetchone() or (0, None, 0, 0, "")
        validated_alerts = int(validation_row[0] or 0)
        avg_score = float(validation_row[1]) if validation_row[1] is not None else None
        wins = int(validation_row[2] or 0)
        validation_count = int(validation_row[3] or 0)
        win_rate = round((wins / validation_count * 100.0), 2) if validation_count > 0 else None
        last_evaluated_at = _safe_iso(validation_row[4])

        cursor.execute(
            """
            SELECT COUNT(*) AS audit_count,
                   SUM(CASE WHEN was_allowed = 1 THEN 1 ELSE 0 END) AS allowed_count
            FROM alert_policy_audit
            WHERE created_at >= %s
            """,
            (cutoff,),
        )
        policy_row = cursor.fetchone() or (0, 0)
        audit_count = int(policy_row[0] or 0)
        allowed_count = int(policy_row[1] or 0)
        pass_rate = round((allowed_count / audit_count * 100.0), 2) if audit_count > 0 else None

        cursor.execute(
            """
            SELECT alert_id, alert_type, ticker, title, summary, source, signal_strength, status, created_at
            FROM alert_events
            WHERE created_at >= %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (cutoff, int(recent_limit)),
        )
        recent_rows = cursor.fetchall() or []
        alert_ids = [str(row[0]) for row in recent_rows if row and row[0]]

        latest_validation_map: dict[str, dict[str, Any]] = {}
        if alert_ids:
            placeholders = ", ".join(["%s"] * len(alert_ids))
            cursor.execute(
                f"""
                SELECT DISTINCT ON (alert_id)
                       alert_id, horizon_key, evaluated_at, score_value, signed_return_pct, outcome_label
                FROM alert_validations
                WHERE alert_id IN ({placeholders}) AND evaluated_at IS NOT NULL
                ORDER BY alert_id, evaluated_at DESC
                """,
                tuple(alert_ids),
            )
            for row in cursor.fetchall() or []:
                latest_validation_map[str(row[0])] = {
                    "horizon_key": str(row[1] or "").upper(),
                    "evaluated_at": _safe_iso(row[2]),
                    "score_value": float(row[3]) if row[3] is not None else None,
                    "signed_return_pct": float(row[4]) if row[4] is not None else None,
                    "outcome_label": str(row[5] or "").strip(),
                }

        conn.commit()

        recent_alerts: list[dict[str, Any]] = []
        for row in recent_rows:
            alert_id = str(row[0] or "")
            latest_validation = latest_validation_map.get(alert_id, {})
            recent_alerts.append(
                {
                    "alert_id": alert_id,
                    "alert_type": str(row[1] or "").strip(),
                    "alert_type_label": _normalize_label(row[1]),
                    "ticker": str(row[2] or "").strip().upper(),
                    "title": str(row[3] or "").strip(),
                    "summary": str(row[4] or "").strip(),
                    "source": str(row[5] or "").strip() or "runtime",
                    "signal_strength": float(row[6] or 0.0),
                    "status": str(row[7] or "tracking").strip().lower() or "tracking",
                    "created_at": _safe_iso(row[8]),
                    "state_label": _format_alert_state(row[7], latest_validation.get("outcome_label", "")),
                    "latest_validation": latest_validation,
                }
            )

        last_update = max([value for value in [last_created_at, last_evaluated_at] if value], default="")

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": {
                "window_days": max(7, int(window_days)),
                "total_recent": total_recent,
                "active_alerts": active_alerts,
                "validated_alerts": validated_alerts,
                "avg_score": round(avg_score, 3) if avg_score is not None else None,
                "win_rate": win_rate,
                "pass_rate": pass_rate,
                "engine_summary": _build_engine_summary(total_recent, validated_alerts, avg_score, win_rate, pass_rate),
                "data_origin": "database",
                "last_update": last_update,
            },
            "items": recent_alerts,
            "recent_alerts": recent_alerts,
        }
    except Exception as exc:
        return _empty_snapshot(f"No pude construir el snapshot de alertas: {exc}")
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def get_alerts_snapshot() -> dict[str, Any]:
    settings = load_settings()
    snapshot = _fetch_alerts_snapshot(settings.database_url)
    if isinstance(snapshot.get("items"), list) and snapshot.get("items"):
        snapshot["items"] = _enrich_alert_items(snapshot["items"])
        snapshot["recent_alerts"] = snapshot["items"]
        return snapshot
    derived = _derived_technical_alerts()
    if derived:
        items = [*derived, *[item for item in snapshot.get("items", []) if isinstance(item, dict)]]
        snapshot["items"] = items[:12]
        snapshot["recent_alerts"] = snapshot["items"]
        summary = snapshot.get("summary") if isinstance(snapshot.get("summary"), dict) else {}
        summary["total_recent"] = max(int(summary.get("total_recent") or 0), len(snapshot["items"]))
        summary["active_alerts"] = max(int(summary.get("active_alerts") or 0), len(derived))
        if not summary.get("engine_summary") or "Todavia no hay" in str(summary.get("engine_summary")):
            summary["engine_summary"] = "Genesis genero alertas tecnicas con precio, cambio, volumen y rango disponible."
        snapshot["summary"] = summary
    return snapshot


def _derived_technical_alerts() -> list[dict[str, Any]]:
    try:
        from services.dashboard.get_radar_snapshot import get_radar_snapshot

        radar = get_radar_snapshot()
    except Exception:
        return []
    rows = radar.get("items") if isinstance(radar, dict) and isinstance(radar.get("items"), list) else []
    alerts: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc).isoformat()
    for row in rows[:25]:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or row.get("symbol") or "").upper()
        if not ticker:
            continue
        price = _price_num(row.get("current_price"), row.get("reference_price"), row.get("price"))
        pct = _num(row.get("daily_change_pct") or row.get("changesPercentage"))
        volume = _num(row.get("volume"))
        avg_volume = _num(row.get("avg_volume") or row.get("average_volume") or row.get("avgVolume"))
        day_high = _num(row.get("day_high") or row.get("dayHigh"))
        day_low = _num(row.get("day_low") or row.get("dayLow"))
        context = {
            "price": price,
            "change": _num(row.get("daily_change") or row.get("change")),
            "change_pct": pct,
            "volume": volume,
            "avg_volume": avg_volume,
            "relative_volume": (volume / avg_volume if volume and avg_volume else _num(row.get("relative_volume") or row.get("relativeVolume"))),
            "day_high": day_high,
            "day_low": day_low,
            "support": _num(row.get("support") or row.get("support_level")),
            "resistance": _num(row.get("resistance") or row.get("resistance_level")),
        }
        if pct is not None and abs(pct) >= 3:
            direction = "alza" if pct > 0 else "baja"
            alerts.append(_technical_alert(ticker, f"Movimiento fuerte de {direction}", f"{ticker} se mueve {pct:+.2f}% en la sesion.", "price_change", pct, now, context))
        if volume and avg_volume and avg_volume > 0 and volume / avg_volume >= 1.8:
            rel = volume / avg_volume
            context["relative_volume"] = rel
            alerts.append(_technical_alert(ticker, "Volumen inusual", f"{ticker} opera {rel:.1f}x su volumen promedio disponible.", "unusual_volume", rel, now, context))
        if price and day_high and day_low and day_high > day_low:
            position = (price - day_low) / (day_high - day_low)
            if position >= 0.92:
                alerts.append(_technical_alert(ticker, "Precio cerca de resistencia diaria", f"{ticker} esta cerca del maximo del dia; vigilar ruptura con volumen.", "range_breakout", position, now, context))
            elif position <= 0.08:
                alerts.append(_technical_alert(ticker, "Precio cerca de soporte diario", f"{ticker} esta cerca del minimo del dia; vigilar defensa del soporte.", "range_support", position, now, context))
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for alert in alerts:
        key = f"{alert['ticker']}:{alert['alert_type']}"
        if key in seen:
            continue
        seen.add(key)
        unique.append(alert)
    if not unique:
        for row in sorted(rows, key=lambda item: abs(_num(item.get("daily_change_pct") or item.get("changesPercentage")) or 0), reverse=True)[:6]:
            if not isinstance(row, dict):
                continue
            ticker = str(row.get("ticker") or row.get("symbol") or "").upper()
            price = _price_num(row.get("current_price"), row.get("reference_price"), row.get("price"))
            if not ticker or price is None:
                continue
            pct = _num(row.get("daily_change_pct") or row.get("changesPercentage")) or 0.0
            volume = _num(row.get("volume"))
            avg_volume = _num(row.get("avg_volume") or row.get("average_volume") or row.get("avgVolume"))
            day_high = _num(row.get("day_high") or row.get("dayHigh"))
            day_low = _num(row.get("day_low") or row.get("dayLow"))
            context = {
                "price": price,
                "change": _num(row.get("daily_change") or row.get("change")),
                "change_pct": pct,
                "volume": volume,
                "avg_volume": avg_volume,
                "relative_volume": (volume / avg_volume if volume and avg_volume else _num(row.get("relative_volume") or row.get("relativeVolume"))),
                "day_high": day_high,
                "day_low": day_low,
                "support": _num(row.get("support") or row.get("support_level")),
                "resistance": _num(row.get("resistance") or row.get("resistance_level")),
            }
            unique.append(
                _technical_alert(
                    ticker,
                    "Vigilancia tecnica",
                    f"{ticker} no dispara alerta fuerte, pero tiene precio confirmado y contexto de volumen/rango para seguimiento.",
                    "technical_watch",
                    pct,
                    now,
                    context,
                )
            )
    return unique[:8]


def _technical_alert(ticker: str, title: str, summary: str, alert_type: str, strength: float, now: str, context: dict[str, Any]) -> dict[str, Any]:
    severity = "high" if abs(strength) >= 5 else "medium"
    impact = "bullish" if strength > 0 and alert_type != "range_support" else "bearish" if strength < 0 else "neutral"
    price = context.get("price")
    volume = context.get("volume")
    dollar_volume = price * volume if price is not None and volume is not None else None
    day_low = context.get("day_low")
    day_high = context.get("day_high")
    support = context.get("support") or day_low
    resistance = context.get("resistance") or day_high
    trend = _trend_from_change(context.get("change_pct"))
    momentum = _momentum_from_fields(context.get("change_pct"), context.get("relative_volume"))
    return {
        "alert_id": f"technical:{ticker}:{alert_type}",
        "id": f"technical:{ticker}:{alert_type}",
        "alert_type": alert_type,
        "alert_type_label": "Tecnica",
        "ticker": ticker,
        "title": title,
        "summary": summary,
        "source": "technical",
        "signal_strength": round(float(strength or 0), 4),
        "status": "tracking",
        "created_at": now,
        "timestamp": now,
        "severity": severity,
        "impact": impact,
        "confidence": "medium",
        "direction": "inflow" if impact == "bullish" else "outflow" if impact == "bearish" else "neutral",
        "price": price,
        "change": context.get("change"),
        "change_pct": context.get("change_pct"),
        "volume": volume,
        "avg_volume": context.get("avg_volume"),
        "relative_volume": context.get("relative_volume"),
        "dollar_volume": dollar_volume,
        "support": support,
        "resistance": resistance,
        "trend": trend,
        "momentum": momentum,
        "risk": _risk_from_alert(impact, support, resistance),
        "what_it_means": _what_alert_means(ticker, impact, context.get("relative_volume")),
        "what_to_watch": _what_alert_watch(ticker, support, resistance),
        "affected_portfolio_assets": [ticker],
        "day_range": {"low": day_low, "high": day_high},
        "mini_series": [context.get("change_pct") or 0, strength, context.get("relative_volume") or 0],
        "state_label": "Vigilancia",
        "evidence": {"derived_from": "radar_snapshot", "source": "technical", **{key: value for key, value in context.items() if value is not None}},
        "genesis_reading": f"{title}: no es orden; sirve para decidir si esperar confirmacion o reducir riesgo.",
    }


def _enrich_alert_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    radar_by_ticker = _radar_by_ticker()
    enriched: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        ticker = str(item.get("ticker") or "").strip().upper()
        market = radar_by_ticker.get(ticker, {}) if ticker else {}
        price = _first_num(item.get("price"), market.get("current_price"), market.get("price"))
        change = _first_num(item.get("change"), item.get("daily_change"), market.get("daily_change"), market.get("change"))
        change_pct = _first_num(item.get("change_pct"), item.get("daily_change_pct"), market.get("daily_change_pct"), market.get("changesPercentage"))
        volume = _first_num(item.get("volume"), market.get("volume"))
        avg_volume = _first_num(item.get("avg_volume"), item.get("avgVolume"), market.get("avg_volume"), market.get("avgVolume"), market.get("average_volume"))
        relative_volume = _first_num(item.get("relative_volume"), item.get("relativeVolume"), market.get("relative_volume"), market.get("relativeVolume"))
        if relative_volume is None and volume is not None and avg_volume:
            relative_volume = volume / avg_volume
        dollar_volume = _first_num(item.get("dollar_volume"), item.get("dollarVolume"))
        if dollar_volume is None and price is not None and volume is not None:
            dollar_volume = price * volume
        support = _first_num(item.get("support"), market.get("support"), market.get("support_level"), market.get("day_low"), market.get("dayLow"))
        resistance = _first_num(item.get("resistance"), market.get("resistance"), market.get("resistance_level"), market.get("day_high"), market.get("dayHigh"))
        impact = str(item.get("impact") or "").strip()
        if not impact:
            if change_pct is not None and change_pct > 0:
                impact = "bullish"
            elif change_pct is not None and change_pct < 0:
                impact = "bearish"
            else:
                impact = "neutral"
        alert_id = str(item.get("alert_id") or item.get("id") or f"alert:{ticker or 'macro'}:{item.get('alert_type') or item.get('title') or item.get('created_at') or len(enriched)}")
        trend = item.get("trend") or _trend_from_change(change_pct)
        momentum = item.get("momentum") or _momentum_from_fields(change_pct, relative_volume)
        risk = item.get("risk") or _risk_from_alert(impact, support, resistance)
        enriched.append(
            {
                **item,
                "id": alert_id,
                "alert_id": alert_id,
                "asset_name": str(market.get("name") or market.get("asset_name") or ticker or "Mercado"),
                "description": item.get("description") or item.get("summary") or item.get("title") or "",
                "severity": item.get("severity") or ("high" if change_pct is not None and abs(change_pct) >= 3 else "medium"),
                "impact": impact,
                "direction": item.get("direction") or ("bullish" if impact == "bullish" else "bearish" if impact == "bearish" else "neutral"),
                "confidence": item.get("confidence") or ("medium" if price is not None or item.get("source") else "low"),
                "price": price,
                "change": change,
                "change_pct": change_pct,
                "volume": volume,
                "avg_volume": avg_volume,
                "relative_volume": relative_volume,
                "dollar_volume": dollar_volume,
                "support": support,
                "resistance": resistance,
                "trend": trend,
                "momentum": momentum,
                "risk": risk,
                "mini_series": item.get("mini_series") or [value for value in (change_pct, relative_volume, item.get("signal_strength")) if value is not None],
                "evidence": item.get("evidence") or {"source": item.get("source") or "database", "market_fields_enriched": bool(market)},
                "genesis_reading": item.get("genesis_reading")
                or _alert_reading(ticker, impact, item.get("title") or item.get("summary") or ""),
                "what_it_means": item.get("what_it_means") or _what_alert_means(ticker or "Mercado", impact, relative_volume),
                "what_to_watch": item.get("what_to_watch") or _what_alert_watch(ticker or "Mercado", support, resistance),
                "affected_portfolio_assets": item.get("affected_portfolio_assets") or ([ticker] if ticker else []),
            }
        )
    return enriched


def _radar_by_ticker() -> dict[str, dict[str, Any]]:
    try:
        from services.dashboard.get_radar_snapshot import get_radar_snapshot

        snapshot = get_radar_snapshot()
    except Exception:
        return {}
    output: dict[str, dict[str, Any]] = {}
    for row in snapshot.get("items") or []:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or row.get("symbol") or "").strip().upper()
        if ticker:
            output[ticker] = row
    return output


def _first_num(*values: object) -> float | None:
    for value in values:
        numeric = _num(value)
        if numeric is not None:
            return numeric
    return None


def _price_num(*values: object) -> float | None:
    fallback_zero: float | None = None
    for value in values:
        numeric = _num(value)
        if numeric is None:
            continue
        if numeric > 0:
            return numeric
        if fallback_zero is None:
            fallback_zero = numeric
    return fallback_zero


def _alert_reading(ticker: str, impact: str, title: object) -> str:
    scope = ticker or "esta alerta macro"
    label = str(title or "evento").strip()
    if impact == "bullish":
        return f"{scope}: {label}. Puede ser oportunidad solo si precio y volumen confirman continuidad."
    if impact == "bearish":
        return f"{scope}: {label}. Riesgo en vigilancia; revisar soporte, volumen y exposicion en cartera."
    return f"{scope}: {label}. Lectura neutral; vigilar si cambia precio, volumen o contexto macro."


def _trend_from_change(change_pct: object) -> str:
    value = _num(change_pct)
    if value is None:
        return "sin tendencia confirmada"
    if value > 1.5:
        return "alcista intradia"
    if value < -1.5:
        return "bajista intradia"
    return "lateral / confirmacion pendiente"


def _momentum_from_fields(change_pct: object, relative_volume: object) -> str:
    pct = _num(change_pct) or 0.0
    rel = _num(relative_volume) or 0.0
    if abs(pct) >= 3 and rel >= 1.8:
        return "alto: precio y volumen empujan juntos"
    if abs(pct) >= 2 or rel >= 1.5:
        return "medio: senal visible, falta continuidad"
    return "moderado: vigilancia sin ruptura fuerte"


def _risk_from_alert(impact: str, support: object, resistance: object) -> str:
    if impact == "bearish":
        return "riesgo de perdida de soporte" if support is not None else "riesgo de presion bajista sin nivel confirmado"
    if impact == "bullish":
        return "riesgo de rechazo en resistencia" if resistance is not None else "riesgo de falso rompimiento sin volumen"
    return "riesgo principal: falta de confirmacion"


def _what_alert_means(ticker: str, impact: str, relative_volume: object) -> str:
    rel = _num(relative_volume)
    volume_part = f" con volumen relativo {rel:.1f}x" if rel is not None else ""
    if impact == "bullish":
        return f"{ticker} muestra sesgo positivo{volume_part}; Genesis lo trata como posible oportunidad si respeta nivel."
    if impact == "bearish":
        return f"{ticker} muestra presion{volume_part}; Genesis lo trata como riesgo activo para cartera/watchlist."
    return f"{ticker} esta en vigilancia{volume_part}; aun no hay direccion suficiente para elevar conviccion."


def _what_alert_watch(ticker: str, support: object, resistance: object) -> str:
    pieces = []
    if support is not None:
        pieces.append(f"soporte {support}")
    if resistance is not None:
        pieces.append(f"resistencia {resistance}")
    levels = ", ".join(pieces) if pieces else "precio, volumen y cierre de vela"
    return f"Vigilar {levels} en {ticker}; confirmar con noticia o volumen antes de actuar."


def _num(value: object) -> float | None:
    try:
        if value in (None, "", "None"):
            return None
        return float(value)
    except Exception:
        return None
