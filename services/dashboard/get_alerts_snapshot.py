from __future__ import annotations

import time
import copy
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from app.settings import load_settings
from services.dashboard.get_operational_health import _connect_database, _safe_iso
from services.genesis.trading_strategy import build_signal_strategy

_DEFAULT_ALERT_WINDOW_DAYS = 45
_DEFAULT_RECENT_LIMIT = 6
_OPPORTUNITY_TTL_SECONDS = 55
_ALERT_DB_TTL_SECONDS = 45
_ALERT_DB_TIMEOUT_SECONDS = 1.5
_DERIVED_ALERT_TIMEOUT_SECONDS = 1.8
_OPPORTUNITY_UNIVERSE = ("NVDA", "MSFT", "AAPL", "META", "AMZN", "TSLA", "NFLX", "AMD", "AVGO", "SPY", "QQQ", "BTC-USD")
_OPPORTUNITY_CACHE: dict[str, Any] = {"expires_at": 0.0, "items": []}
_ALERT_DB_CACHE: dict[str, Any] = {"expires_at": 0.0, "snapshot": None}
_MAX_MONITORED_DOLLAR_VOLUME = 1_000_000_000_000

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


def _is_crypto_ticker(ticker: str) -> bool:
    symbol = str(ticker or "").strip().upper()
    return symbol.endswith("-USD") or symbol in {"BTC", "ETH", "SOL", "DOGE", "XRP"}


def _safe_alert_dollar_volume(
    ticker: str,
    price: float | None,
    volume: float | None,
    direct_value: object = None,
) -> float | None:
    direct = _num(direct_value)
    if direct is not None:
        if 0 < direct <= _MAX_MONITORED_DOLLAR_VOLUME:
            return direct
        if _is_crypto_ticker(ticker) and volume is not None and 0 < volume <= _MAX_MONITORED_DOLLAR_VOLUME:
            return volume
        return None
    if price is None or volume is None:
        return None
    computed = price * volume
    if _is_crypto_ticker(ticker) and computed > _MAX_MONITORED_DOLLAR_VOLUME:
        return volume if 0 < volume <= _MAX_MONITORED_DOLLAR_VOLUME else None
    if 0 < computed <= _MAX_MONITORED_DOLLAR_VOLUME:
        return computed
    return None


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


def _fetch_alerts_snapshot_fast(database_url: str) -> dict[str, Any]:
    if not database_url:
        return _fetch_alerts_snapshot(database_url)
    now_ts = time.time()
    cached = _ALERT_DB_CACHE.get("snapshot")
    if cached and _ALERT_DB_CACHE.get("expires_at", 0.0) > now_ts:
        return copy.deepcopy(cached)

    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(_fetch_alerts_snapshot, database_url)
    try:
        snapshot = future.result(timeout=_ALERT_DB_TIMEOUT_SECONDS)
        _ALERT_DB_CACHE["expires_at"] = now_ts + _ALERT_DB_TTL_SECONDS
        _ALERT_DB_CACHE["snapshot"] = copy.deepcopy(snapshot)
        return snapshot
    except TimeoutError:
        if cached:
            return copy.deepcopy(cached)
        return _empty_snapshot("DB lenta; Genesis usa FMP/radar tecnico confirmado sin bloquear Alertas.")
    except Exception as exc:
        if cached:
            return copy.deepcopy(cached)
        return _empty_snapshot(f"No pude leer alertas DB rapido: {exc}")
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def get_alerts_snapshot() -> dict[str, Any]:
    settings = load_settings()
    snapshot = _fetch_alerts_snapshot_fast(settings.database_url)
    opportunity_items = _market_opportunity_alerts()
    if isinstance(snapshot.get("items"), list) and snapshot.get("items"):
        snapshot["items"] = _merge_alert_rows(_enrich_alert_items(snapshot["items"]), opportunity_items)
        snapshot["recent_alerts"] = snapshot["items"]
        snapshot["opportunities"] = [item for item in snapshot["items"] if item.get("is_opportunity")]
        return snapshot
    derived = _derived_technical_alerts_fast()
    if derived:
        items = _enrich_alert_items([*derived, *opportunity_items, *[item for item in snapshot.get("items", []) if isinstance(item, dict)]])
        snapshot["items"] = items[:12]
        snapshot["recent_alerts"] = snapshot["items"]
        snapshot["opportunities"] = [item for item in snapshot["items"] if item.get("is_opportunity")]
        summary = snapshot.get("summary") if isinstance(snapshot.get("summary"), dict) else {}
        summary["total_recent"] = max(int(summary.get("total_recent") or 0), len(snapshot["items"]))
        summary["active_alerts"] = max(int(summary.get("active_alerts") or 0), len(derived))
        if not summary.get("engine_summary") or "Todavia no hay" in str(summary.get("engine_summary")):
            summary["engine_summary"] = "Genesis generó alertas técnicas con precio, cambio, volumen y rango disponible."
        snapshot["summary"] = summary
    elif opportunity_items:
        snapshot["items"] = _enrich_alert_items(opportunity_items)[:8]
        snapshot["recent_alerts"] = snapshot["items"]
        snapshot["opportunities"] = [item for item in snapshot["items"] if item.get("is_opportunity")]
        summary = snapshot.get("summary") if isinstance(snapshot.get("summary"), dict) else {}
        summary["total_recent"] = len(snapshot["items"])
        summary["active_alerts"] = len(snapshot["items"])
        summary["engine_summary"] = "Genesis encontró oportunidades externas importantes con FMP; ninguna es orden real."
        snapshot["summary"] = summary
    return snapshot


def _merge_alert_rows(primary: list[dict[str, Any]], opportunities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = {str(item.get("id") or item.get("alert_id") or "") for item in primary}
    merged: list[dict[str, Any]] = []
    for item in opportunities:
        key = str(item.get("id") or item.get("alert_id") or "")
        if key and key in seen:
            continue
        merged.append(item)
        if key:
            seen.add(key)
    merged.extend(primary)
    return merged[:14]


def _market_opportunity_alerts() -> list[dict[str, Any]]:
    settings = load_settings()
    if not (getattr(settings, "fmp_api_key", "") and getattr(settings, "fmp_live_enabled", False)):
        _OPPORTUNITY_CACHE["expires_at"] = 0.0
        _OPPORTUNITY_CACHE["items"] = []
        return []
    now_ts = time.time()
    if _OPPORTUNITY_CACHE["expires_at"] > now_ts:
        return [dict(item) for item in _OPPORTUNITY_CACHE.get("items", [])]
    bulk_quotes = _fetch_opportunity_quotes_bulk(settings.fmp_api_key)
    if not bulk_quotes:
        _OPPORTUNITY_CACHE["expires_at"] = now_ts + min(_OPPORTUNITY_TTL_SECONDS, 20)
        _OPPORTUNITY_CACHE["items"] = []
        return []

    now = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    for ticker in _OPPORTUNITY_UNIVERSE:
        try:
            quote = bulk_quotes.get(ticker)
        except Exception:
            continue
        if not isinstance(quote, dict):
            continue
        price = _price_num(quote.get("current_price"), quote.get("price"))
        volume = _num(quote.get("volume"))
        change_pct = _num(quote.get("daily_change_pct") or quote.get("changesPercentage")) or 0.0
        if price is None:
            continue
        dollar_volume = _safe_alert_dollar_volume(
            ticker,
            price,
            volume,
            quote.get("dollar_volume") or quote.get("dollarVolume") or quote.get("quoteVolume") or quote.get("quote_volume"),
        )
        if not _is_important_opportunity(change_pct, dollar_volume, volume):
            continue
        direction = "bullish" if change_pct > 0 else "bearish" if change_pct < 0 else "neutral"
        context = {
            "price": price,
            "change": _num(quote.get("daily_change") or quote.get("change")),
            "change_pct": change_pct,
            "volume": volume,
            "avg_volume": _num(quote.get("avg_volume") or quote.get("avgVolume")),
            "relative_volume": _num(quote.get("relative_volume") or quote.get("relativeVolume")),
            "dollar_volume": dollar_volume,
            "support": _num(quote.get("day_low") or quote.get("dayLow")),
            "resistance": _num(quote.get("day_high") or quote.get("dayHigh")),
        }
        strategy = build_signal_strategy(ticker, context)
        flow = _format_money_short(dollar_volume) if dollar_volume is not None else "volumen pendiente"
        rows.append(
            {
                "alert_id": f"opportunity:{ticker}",
                "id": f"opportunity:{ticker}",
                "alert_type": "opportunity_scan",
                "alert_type_label": "Oportunidad",
                "ticker": ticker,
                "asset_name": str(quote.get("name") or ticker),
                "title": f"{ticker}: oportunidad externa detectada",
                "title_es": f"{ticker}: oportunidad externa detectada",
                "summary": f"{ticker}: {change_pct:+.2f}% con {flow} de flujo observado. {strategy['summary']}",
                "summary_es": f"{ticker}: {change_pct:+.2f}% con {flow} de flujo observado. {strategy['summary']}",
                "source": "fmp_opportunity_scan",
                "signal_strength": strategy["score"],
                "status": "opportunity",
                "is_opportunity": True,
                "created_at": now,
                "timestamp": now,
                "severity": "high" if strategy["score"] >= 72 else "medium",
                "impact": direction,
                "confidence": "medium" if strategy["score"] >= 60 else "low",
                "direction": direction,
                **context,
                "trend": _trend_from_change(change_pct),
                "momentum": _momentum_from_fields(change_pct, context.get("relative_volume")),
                "risk": strategy["invalidation"],
                "strategy": strategy,
                "decision": strategy.get("decision"),
                "decision_label_es": strategy.get("decision_label_es"),
                "decision_reason_es": strategy.get("decision_reason_es"),
                "action_verdict": strategy.get("decision_label_es"),
                "what_it_means": strategy["summary"],
                "what_happened_es": f"Genesis detectó un activo importante fuera de tu cartera/watchlist con precio y flujo FMP activos.",
                "why_it_matters_es": "Puede convertirse en oportunidad solo si valida precio, volumen y catalizador; no es compra real.",
                "what_to_watch": "; ".join(strategy["validation"]),
                "what_to_watch_es": "; ".join(strategy["validation"]),
                "affected_portfolio_assets": [],
                "affected_watchlist_assets": [],
                "day_range": {"low": context.get("support"), "high": context.get("resistance")},
                "mini_series": [change_pct, context.get("relative_volume") or 0, strategy["score"]],
                "genesis_reading": strategy["summary"],
                "genesis_reading_es": strategy["summary"],
                "evidence": {"derived_from": "fmp_opportunity_scan", "source": "FMP", **{key: value for key, value in context.items() if value is not None}},
            }
        )
    rows.sort(key=lambda item: (float(item.get("signal_strength") or 0), abs(float(item.get("dollar_volume") or 0))), reverse=True)
    output = rows[:5]
    _OPPORTUNITY_CACHE["expires_at"] = now_ts + _OPPORTUNITY_TTL_SECONDS
    _OPPORTUNITY_CACHE["items"] = output
    return [dict(item) for item in output]


def _fetch_opportunity_quotes_bulk(api_key: str) -> dict[str, dict[str, Any]]:
    if not api_key:
        return {}
    symbol_map: dict[str, str] = {}
    symbols: list[str] = []
    for ticker in _OPPORTUNITY_UNIVERSE:
        symbol = ticker.replace("-USD", "USD") if ticker.endswith("-USD") else ticker
        symbols.append(symbol)
        symbol_map[symbol.upper()] = ticker
    try:
        response = requests.get(
            "https://financialmodelingprep.com/stable/quote",
            params={"symbol": ",".join(symbols), "apikey": api_key},
            timeout=2,
        )
        if response.status_code != 200:
            return {}
        payload = response.json()
    except Exception:
        return {}
    rows = payload if isinstance(payload, list) else [payload] if isinstance(payload, dict) else []
    quotes: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw_symbol = str(row.get("symbol") or "").strip().upper()
        ticker = symbol_map.get(raw_symbol)
        if not ticker:
            continue
        price = _price_num(row.get("price"))
        if price is None:
            continue
        quotes[ticker] = {
            "price": price,
            "current_price": price,
            "volume": _num(row.get("volume")),
            "avgVolume": _num(row.get("avgVolume")),
            "change": _num(row.get("change")),
            "changesPercentage": _num(row.get("changesPercentage")),
            "name": row.get("name") or row.get("companyName") or ticker,
            "dayHigh": _num(row.get("dayHigh") or row.get("high")),
            "dayLow": _num(row.get("dayLow") or row.get("low")),
        }
    return quotes


def _is_important_opportunity(change_pct: float, dollar_volume: float | None, volume: float | None) -> bool:
    if dollar_volume is not None and dollar_volume >= 1_000_000_000:
        return True
    if abs(change_pct) >= 0.8 and volume is not None and volume >= 2_000_000:
        return True
    if abs(change_pct) >= 1.5:
        return True
    return False


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
            dollar_volume = _safe_alert_dollar_volume(
                ticker,
                price,
                volume,
                row.get("dollar_volume") or row.get("dollarVolume") or row.get("quoteVolume") or row.get("quote_volume"),
            )
            title = _fallback_watch_title(ticker, pct, context.get("relative_volume"), day_low, day_high, price, volume=volume, dollar_volume=dollar_volume)
            summary = _fallback_watch_summary(
                ticker,
                pct,
                context.get("relative_volume"),
                support=context.get("support") or day_low,
                resistance=context.get("resistance") or day_high,
                volume=volume,
                dollar_volume=dollar_volume,
            )
            unique.append(
                _technical_alert(
                    ticker,
                    title,
                    summary,
                    "technical_watch",
                    pct,
                    now,
                    context,
                )
            )
    return unique[:8]


def _derived_technical_alerts_fast() -> list[dict[str, Any]]:
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(_derived_technical_alerts)
    try:
        return future.result(timeout=_DERIVED_ALERT_TIMEOUT_SECONDS)
    except TimeoutError:
        return []
    except Exception:
        return []
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def _technical_alert(ticker: str, title: str, summary: str, alert_type: str, strength: float, now: str, context: dict[str, Any]) -> dict[str, Any]:
    severity = "high" if abs(strength) >= 5 else "medium"
    impact = "bullish" if strength > 0 and alert_type != "range_support" else "bearish" if strength < 0 else "neutral"
    price = context.get("price")
    volume = context.get("volume")
    dollar_volume = _safe_alert_dollar_volume(ticker, price, volume, context.get("dollar_volume"))
    day_low = context.get("day_low")
    day_high = context.get("day_high")
    support = context.get("support") or day_low
    resistance = context.get("resistance") or day_high
    trend = _trend_from_change(context.get("change_pct"))
    momentum = _momentum_from_fields(context.get("change_pct"), context.get("relative_volume"))
    strategy = build_signal_strategy(
        ticker,
        {
            "price": price,
            "change_pct": context.get("change_pct"),
            "volume": volume,
            "avg_volume": context.get("avg_volume"),
            "relative_volume": context.get("relative_volume"),
            "dollar_volume": dollar_volume,
            "support": support,
            "resistance": resistance,
        },
    )
    return {
        "alert_id": f"technical:{ticker}:{alert_type}",
        "id": f"technical:{ticker}:{alert_type}",
        "alert_type": alert_type,
        "alert_type_label": "Tecnica",
        "ticker": ticker,
        "asset_name": ticker,
        "title": title,
        "title_es": title,
        "summary": summary,
        "summary_es": summary,
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
        "strategy": strategy,
        "decision": strategy.get("decision"),
        "decision_label_es": strategy.get("decision_label_es"),
        "decision_reason_es": strategy.get("decision_reason_es"),
        "action_verdict": strategy.get("decision_label_es"),
        "what_it_means": _what_alert_means(ticker, impact, context.get("relative_volume")),
        "what_happened_es": summary,
        "why_it_matters_es": _what_alert_means(ticker, impact, context.get("relative_volume")),
        "what_to_watch": _what_alert_watch(ticker, support, resistance),
        "what_to_watch_es": _what_alert_watch(ticker, support, resistance),
        "affected_portfolio_assets": [ticker],
        "affected_watchlist_assets": [ticker],
        "day_range": {"low": day_low, "high": day_high},
        "mini_series": [context.get("change_pct") or 0, strength, context.get("relative_volume") or 0],
        "state_label": "Vigilancia",
        "evidence": {"derived_from": "radar_snapshot", "source": "technical", **{key: value for key, value in context.items() if value is not None}},
        "genesis_reading": f"{title}: no es orden; {strategy['summary']}",
        "genesis_reading_es": f"{title}: no es orden; {strategy['summary']}",
    }


def _fallback_watch_title(
    ticker: str,
    pct: float,
    relative_volume: float | None,
    day_low: float | None,
    day_high: float | None,
    price: float | None,
    *,
    volume: float | None = None,
    dollar_volume: float | None = None,
) -> str:
    if relative_volume is not None and relative_volume >= 1.3:
        return f"{ticker}: volumen relativo en vigilancia"
    if dollar_volume is not None and dollar_volume >= 100_000_000:
        return f"{ticker}: {_format_money_short(dollar_volume)} negociados en sesion"
    if volume is not None and volume >= 1_000_000:
        return f"{ticker}: volumen visible en mercado"
    if pct >= 0.5:
        return f"{ticker}: sesgo positivo moderado"
    if pct <= -0.5:
        return f"{ticker}: presion moderada"
    if price is not None and day_high is not None and day_low is not None and day_high > day_low:
        position = (price - day_low) / (day_high - day_low)
        if position >= 0.75:
            return f"{ticker}: cerca de zona alta diaria"
        if position <= 0.25:
            return f"{ticker}: cerca de zona baja diaria"
    return f"{ticker}: precio confirmado en radar"


def _fallback_watch_summary(
    ticker: str,
    pct: float,
    relative_volume: float | None,
    *,
    support: float | None,
    resistance: float | None,
    volume: float | None = None,
    dollar_volume: float | None = None,
) -> str:
    volume_part = f" volumen relativo {relative_volume:.1f}x," if relative_volume is not None else ""
    if not volume_part and dollar_volume is not None:
        volume_part = f" volumen negociado {_format_money_short(dollar_volume)},"
    elif not volume_part and volume is not None:
        volume_part = f" volumen {volume:,.0f},"
    level_bits = []
    if support is not None:
        level_bits.append(f"soporte {support}")
    if resistance is not None:
        level_bits.append(f"resistencia {resistance}")
    levels = ", ".join(level_bits) if level_bits else "niveles no confirmados"
    return f"{ticker}: movimiento {pct:+.2f}%,{volume_part} zona {levels}. Lectura: vigilar ruptura o rechazo con volumen antes de actuar."


def _format_money_short(value: float | None) -> str:
    if value is None:
        return "monto pendiente"
    abs_value = abs(float(value))
    sign = "-" if value < 0 else ""
    if abs_value >= 1_000_000_000:
        return f"{sign}${abs_value / 1_000_000_000:.1f}B"
    if abs_value >= 1_000_000:
        return f"{sign}${abs_value / 1_000_000:.1f}M"
    if abs_value >= 1_000:
        return f"{sign}${abs_value / 1_000:.0f}K"
    return f"{sign}${abs_value:.0f}"


def _enrich_alert_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    radar_by_ticker = _radar_by_ticker()
    enriched: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        ticker = str(item.get("ticker") or "").strip().upper()
        market = radar_by_ticker.get(ticker, {}) if ticker else {}
        price = _price_num(item.get("price"), market.get("current_price"), market.get("price"), market.get("reference_price"))
        change = _first_num(item.get("change"), item.get("daily_change"), market.get("daily_change"), market.get("change"))
        change_pct = _first_num(item.get("change_pct"), item.get("daily_change_pct"), market.get("daily_change_pct"), market.get("changesPercentage"))
        volume = _first_num(item.get("volume"), market.get("volume"))
        avg_volume = _first_num(item.get("avg_volume"), item.get("avgVolume"), market.get("avg_volume"), market.get("avgVolume"), market.get("average_volume"))
        relative_volume = _first_num(item.get("relative_volume"), item.get("relativeVolume"), market.get("relative_volume"), market.get("relativeVolume"))
        if relative_volume is None and volume is not None and avg_volume:
            relative_volume = volume / avg_volume
        dollar_volume = _safe_alert_dollar_volume(
            ticker,
            price,
            volume,
            _first_num(item.get("dollar_volume"), item.get("dollarVolume"), item.get("quoteVolume"), item.get("quote_volume")),
        )
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
        strategy = item.get("strategy") if isinstance(item.get("strategy"), dict) else build_signal_strategy(
            ticker or "MERCADO",
            {
                "price": price,
                "change_pct": change_pct,
                "volume": volume,
                "avg_volume": avg_volume,
                "relative_volume": relative_volume,
                "dollar_volume": dollar_volume,
                "support": support,
                "resistance": resistance,
            },
        )
        title_es = item.get("title_es") or item.get("title") or "Alerta Genesis"
        summary_es = item.get("summary_es") or item.get("summary") or item.get("description") or ""
        if ticker and ("precio confirmado" in str(title_es).casefold() or not summary_es):
            title_es = _enriched_watch_title(ticker, change_pct, relative_volume, dollar_volume, volume)
            summary_es = _enriched_watch_summary(ticker, change_pct, dollar_volume, volume, support, resistance)
        enriched.append(
            {
                **item,
                "id": alert_id,
                "alert_id": alert_id,
                "asset_name": str(market.get("name") or market.get("asset_name") or ticker or "Mercado"),
                "title_es": title_es,
                "summary_es": summary_es,
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
                "strategy": strategy,
                "decision": item.get("decision") or strategy.get("decision"),
                "decision_label_es": item.get("decision_label_es") or strategy.get("decision_label_es"),
                "decision_reason_es": item.get("decision_reason_es") or strategy.get("decision_reason_es"),
                "action_verdict": item.get("action_verdict") or strategy.get("decision_label_es"),
                "mini_series": item.get("mini_series") or [value for value in (change_pct, relative_volume, item.get("signal_strength")) if value is not None],
                "evidence": item.get("evidence") or {"source": item.get("source") or "database", "market_fields_enriched": bool(market)},
                "genesis_reading": item.get("genesis_reading")
                or _alert_reading(ticker, impact, item.get("title") or item.get("summary") or ""),
                "genesis_reading_es": item.get("genesis_reading_es")
                or item.get("genesis_reading")
                or _alert_reading(ticker, impact, item.get("title") or item.get("summary") or ""),
                "what_it_means": item.get("what_it_means") or _what_alert_means(ticker or "Mercado", impact, relative_volume),
                "what_happened_es": item.get("what_happened_es") or item.get("summary") or item.get("description") or item.get("title") or "",
                "why_it_matters_es": item.get("why_it_matters_es") or _what_alert_means(ticker or "Mercado", impact, relative_volume),
                "what_to_watch": item.get("what_to_watch") or _what_alert_watch(ticker or "Mercado", support, resistance),
                "what_to_watch_es": item.get("what_to_watch_es") or item.get("what_to_watch") or _what_alert_watch(ticker or "Mercado", support, resistance),
                "affected_portfolio_assets": item.get("affected_portfolio_assets") or ([ticker] if ticker else []),
                "affected_watchlist_assets": item.get("affected_watchlist_assets") or ([ticker] if ticker else []),
            }
        )
    return enriched


def _enriched_watch_title(
    ticker: str,
    change_pct: float | None,
    relative_volume: float | None,
    dollar_volume: float | None,
    volume: float | None,
) -> str:
    if relative_volume is not None and relative_volume >= 1.3:
        return f"{ticker}: volumen relativo {relative_volume:.1f}x"
    if dollar_volume is not None and dollar_volume >= 100_000_000:
        return f"{ticker}: {_format_money_short(dollar_volume)} negociados"
    if volume is not None and volume >= 1_000_000:
        return f"{ticker}: {volume:,.0f} unidades negociadas"
    pct = change_pct or 0.0
    if pct > 0:
        return f"{ticker}: sesgo positivo en vigilancia"
    if pct < 0:
        return f"{ticker}: presion bajista en vigilancia"
    return f"{ticker}: rango lateral en vigilancia"


def _enriched_watch_summary(
    ticker: str,
    change_pct: float | None,
    dollar_volume: float | None,
    volume: float | None,
    support: float | None,
    resistance: float | None,
) -> str:
    flow = _format_money_short(dollar_volume) if dollar_volume is not None else (f"{volume:,.0f} unidades" if volume is not None else "volumen pendiente")
    pct = change_pct or 0.0
    levels = []
    if support is not None:
        levels.append(f"soporte {support}")
    if resistance is not None:
        levels.append(f"resistencia {resistance}")
    level_text = ", ".join(levels) if levels else "niveles por confirmar"
    return f"{ticker}: {pct:+.2f}% con {flow} de flujo observado; {level_text}. Genesis vigila ruptura/rechazo antes de actuar."


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
    for value in values:
        numeric = _num(value)
        if numeric is not None and numeric > 0:
            return numeric
    return None


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
        return "medio: señal visible, falta continuidad"
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
