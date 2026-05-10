from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from functools import partial
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from api.routes.dashboard import (
    add_dashboard_portfolio_ticker,
    get_dashboard_asset_chart,
    get_dashboard_alert_drilldown,
    get_dashboard_alerts,
    get_dashboard_executive_queue,
    get_dashboard_fmp_dependencies,
    get_dashboard_genesis,
    get_dashboard_health,
    get_dashboard_macro_activity,
    get_dashboard_money_flow_causal,
    get_dashboard_money_flow_detection,
    get_dashboard_money_flow_jarvis,
    get_dashboard_money_flow_model,
    get_dashboard_news,
    get_dashboard_reliability,
    get_dashboard_radar_drilldown,
    get_dashboard_radar,
    get_dashboard_source_health,
    get_dashboard_whales,
    remove_dashboard_portfolio_purchase,
    remove_dashboard_portfolio_ticker,
    search_dashboard_market_ticker,
    simulate_dashboard_portfolio_purchase,
)
from services.dashboard.get_genesis_answer import get_genesis_fallback_answer
from services.genesis.chart_image_analysis import analyze_chart_image
from services.genesis.intelligence_core import ask_genesis
from services.genesis.memory_store import MemoryStore
from services.genesis.trading_strategy import build_signal_strategy

_ROOT_DIR = Path(__file__).resolve().parents[1]
_DASHBOARD_DIR = _ROOT_DIR / "app" / "dashboard"
_PRODUCTION_API_ORIGIN = os.getenv(
    "GENESIS_PRODUCTION_API_ORIGIN",
    "https://genesisbot-production.up.railway.app",
).rstrip("/")
_PROXY_GET_PATHS = {
    "/api/dashboard/alerts",
    "/api/dashboard/alerts/drilldown",
    "/api/dashboard/asset/chart",
    "/api/dashboard/chart",
    "/api/dashboard/fmp",
    "/api/dashboard/genesis",
    "/api/dashboard/macro-activity",
    "/api/dashboard/market/search",
    "/api/dashboard/news",
    "/api/dashboard/money-flow/causal",
    "/api/dashboard/money-flow/detection",
    "/api/dashboard/money-flow/jarvis",
    "/api/dashboard/money-flow/model",
    "/api/dashboard/portfolio",
    "/api/dashboard/portfolio/drilldown",
    "/api/dashboard/radar",
    "/api/dashboard/radar/drilldown",
    "/api/dashboard/source-health",
    "/api/dashboard/whales",
    "/api/genesis/briefing",
    "/api/genesis/memory/recent",
}
_PROXY_GET_PREFIXES = ("/api/genesis/memory/ticker/",)
_PROXY_OPPORTUNITY_TICKERS = ("NVDA", "MSFT", "NFLX", "META", "TSLA", "SPY", "QQQ", "BTC-USD")
_YAHOO_CHART_CACHE: dict[str, tuple[float, dict]] = {}
_YAHOO_TIMEFRAMES = {
    "1D": ("1d", "5m"),
    "1W": ("5d", "15m"),
    "1M": ("1mo", "1d"),
    "1Y": ("1y", "1d"),
    "5Y": ("5y", "1wk"),
    "MAX": ("max", "1mo"),
}
_PROXY_POST_PATHS = {
    "/api/genesis/analyze-image",
    "/api/genesis/ask",
    "/api/genesis/memory/event",
}


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _running_on_hosted_runtime() -> bool:
    return any(
        os.getenv(name)
        for name in (
            "RAILWAY_ENVIRONMENT",
            "RAILWAY_PROJECT_ID",
            "RAILWAY_SERVICE_ID",
            "RAILWAY_PUBLIC_DOMAIN",
            "RAILWAY_PRIVATE_DOMAIN",
        )
    )


def _local_live_sources_missing() -> bool:
    if _running_on_hosted_runtime():
        return False
    if _truthy(os.getenv("GENESIS_DISABLE_PROD_PROXY")):
        return False
    fmp_ready = bool(os.getenv("FMP_API_KEY", "").strip()) and _truthy(os.getenv("FMP_LIVE_ENABLED"))
    llm_needed = _truthy(os.getenv("GENESIS_LLM_ENABLED"))
    llm_ready = bool(os.getenv("OPENAI_API_KEY", "").strip())
    return not fmp_ready or (llm_needed and not llm_ready)


def _is_proxy_path(path: str, method: str) -> bool:
    if method == "GET":
        return path in _PROXY_GET_PATHS or any(path.startswith(prefix) for prefix in _PROXY_GET_PREFIXES)
    if method == "POST":
        return path in _PROXY_POST_PATHS
    return False


def create_app() -> dict[str, str]:
    return {
        "dashboard": "shell_ready",
        "ui_root": "/",
        "health_endpoint": "/api/dashboard/health",
        "reliability_endpoint": "/api/dashboard/reliability",
        "executive_queue_endpoint": "/api/dashboard/executive-queue",
        "genesis_endpoint": "/api/dashboard/genesis?q={question}&context={context}&ticker={ticker}&panel_context={json}",
        "genesis_ask_endpoint": "/api/genesis/ask",
        "genesis_image_analysis_endpoint": "/api/genesis/analyze-image",
        "genesis_memory_recent_endpoint": "/api/genesis/memory/recent",
        "genesis_memory_ticker_endpoint": "/api/genesis/memory/ticker/{ticker}",
        "genesis_memory_event_endpoint": "/api/genesis/memory/event",
        "genesis_briefing_endpoint": "/api/genesis/briefing",
        "dashboard_chart_endpoint": "/api/dashboard/chart?ticker={symbol}&range={range}",
        "money_flow_model_endpoint": "/api/dashboard/money-flow/model",
        "money_flow_detection_endpoint": "/api/dashboard/money-flow/detection",
        "money_flow_causal_endpoint": "/api/dashboard/money-flow/causal",
        "money_flow_jarvis_endpoint": "/api/dashboard/money-flow/jarvis?q={question}",
        "radar_endpoint": "/api/dashboard/radar",
        "radar_drilldown_endpoint": "/api/dashboard/radar/drilldown?ticker={symbol}",
        "portfolio_endpoint": "/api/dashboard/portfolio",
        "portfolio_drilldown_endpoint": "/api/dashboard/portfolio/drilldown?ticker={symbol}",
        "asset_chart_endpoint": "/api/dashboard/asset/chart?ticker={symbol}&range={range}",
        "market_search_endpoint": "/api/dashboard/market/search?q={symbol}",
        "portfolio_add_endpoint": "/api/dashboard/portfolio/watchlist/add",
        "portfolio_remove_endpoint": "/api/dashboard/portfolio/watchlist/remove",
        "portfolio_paper_endpoint": "/api/dashboard/portfolio/paper-buy",
        "portfolio_paper_remove_endpoint": "/api/dashboard/portfolio/paper-remove",
        "alerts_endpoint": "/api/dashboard/alerts",
        "alerts_drilldown_endpoint": "/api/dashboard/alerts/drilldown?alert_id={id}",
        "news_endpoint": "/api/dashboard/news",
        "whales_endpoint": "/api/dashboard/whales",
        "fmp_endpoint": "/api/dashboard/fmp",
        "source_health_endpoint": "/api/dashboard/source-health",
        "macro_activity_endpoint": "/api/dashboard/macro-activity",
    }


def _safe_num(value: object) -> float | None:
    try:
        if value in (None, "", "None"):
            return None
        return float(value)
    except Exception:
        return None


def _first_safe_num(*values: object) -> float | None:
    for value in values:
        number = _safe_num(value)
        if number is not None:
            return number
    return None


def _normalize_quote_change_fields(row: dict) -> dict:
    if not isinstance(row, dict):
        return row
    price = _first_safe_num(row.get("current_price"), row.get("price"))
    previous_close = _safe_num(row.get("previous_close"))
    daily_change = _first_safe_num(row.get("daily_change"), row.get("change"))
    daily_change_pct = _first_safe_num(row.get("daily_change_pct"), row.get("change_pct"), row.get("percent_change"))
    if daily_change is None and price is not None and previous_close:
        daily_change = price - previous_close
    if (daily_change_pct is None or abs(daily_change_pct) < 0.005) and daily_change is not None and previous_close:
        daily_change_pct = (daily_change / previous_close) * 100
    if daily_change is not None:
        row["daily_change"] = daily_change
        row["change"] = daily_change
    if daily_change_pct is not None:
        row["daily_change_pct"] = daily_change_pct
        row["change_pct"] = daily_change_pct
        row["percent_change"] = daily_change_pct
    return row


def _normalize_market_payload(payload: dict) -> dict:
    rows = payload.get("results") if isinstance(payload, dict) else None
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, dict):
                _normalize_quote_change_fields(row)
    return payload


def _money_short(value: object) -> str:
    number = _safe_num(value)
    if number is None:
        return "volumen pendiente"
    sign = "-" if number < 0 else ""
    absolute = abs(number)
    if absolute >= 1_000_000_000:
        return f"{sign}${absolute / 1_000_000_000:.1f}B"
    if absolute >= 1_000_000:
        return f"{sign}${absolute / 1_000_000:.1f}M"
    if absolute >= 1_000:
        return f"{sign}${absolute / 1_000:.0f}K"
    return f"{sign}${absolute:.0f}"


def _is_crypto_ticker(ticker: str) -> bool:
    symbol = str(ticker or "").upper()
    return symbol.endswith("-USD") or symbol in {"BTC", "ETH", "SOL", "DOGE", "XRP"}


def _safe_monitored_dollar_volume_for_proxy(
    ticker: str,
    price: object,
    volume: object,
    direct_value: object = None,
) -> tuple[float | None, str, bool]:
    direct = _safe_num(direct_value)
    vol = _safe_num(volume)
    if direct is not None:
        if 0 < direct <= 1_000_000_000_000:
            return direct, "reported_dollar_volume", False
        if _is_crypto_ticker(ticker) and vol is not None and 0 < vol <= 1_000_000_000_000:
            return vol, "crypto_quote_volume", True
        return None, "blocked_absurd", True
    px = _safe_num(price)
    if px is None or vol is None:
        return None, "missing", False
    computed = px * vol
    if _is_crypto_ticker(ticker) and computed > 1_000_000_000_000:
        if 0 < vol <= 1_000_000_000_000:
            return vol, "crypto_quote_volume", True
        return None, "blocked_absurd", True
    if 0 < computed <= 1_000_000_000_000:
        return computed, "price_times_volume", False
    return None, "blocked_absurd", True


def _copy_strategy_decision(row: dict, strategy: dict | None = None) -> None:
    if not isinstance(row, dict):
        return
    strategy = strategy if isinstance(strategy, dict) else row.get("strategy")
    if not isinstance(strategy, dict):
        return
    row.setdefault("decision", strategy.get("decision"))
    row.setdefault("decision_label_es", strategy.get("decision_label_es"))
    row.setdefault("decision_reason_es", strategy.get("decision_reason_es"))
    row.setdefault("action_verdict", strategy.get("decision_label_es"))


def _massage_proxy_payload(path: str, data: bytes, body: dict | None = None) -> bytes:
    try:
        payload = json.loads(data.decode("utf-8"))
    except Exception:
        return data
    if not isinstance(payload, dict):
        return data
    if path == "/api/dashboard/market/search":
        _normalize_market_payload(payload)
    elif path == "/api/dashboard/news":
        _massage_news_payload(payload)
    elif path == "/api/dashboard/alerts":
        _massage_alerts_payload(payload)
    elif path in {"/api/dashboard/whales", "/api/dashboard/money-flow/causal", "/api/dashboard/money-flow/detection", "/api/dashboard/money-flow/jarvis"}:
        _massage_whales_payload(payload)
    elif path == "/api/genesis/ask":
        payload = _correct_genesis_proxy_payload(payload, body or {})
        payload = _enrich_genesis_asset_quote(payload)
        payload = _enrich_genesis_trade_decision(payload, _genesis_message_from_body(body or {}))
        payload = _enrich_genesis_whale_payload(payload)
    elif path == "/api/genesis/analyze-image":
        payload = _massage_image_analysis_payload(payload, body or {})
    return json.dumps(payload).encode("utf-8")


def _massage_image_analysis_payload(payload: dict, body: dict) -> dict:
    if not isinstance(payload, dict):
        return payload
    message = _genesis_message_from_body(body)
    tickers = payload.get("tickers") if isinstance(payload.get("tickers"), list) else []
    if not tickers and message:
        try:
            from services.genesis.ticker_parser import extract_tickers_from_prompt

            tickers = extract_tickers_from_prompt(message)
        except Exception:
            tickers = []
    answer = str(payload.get("assistant_narrative") or payload.get("answer") or "").strip()
    payload["intent"] = payload.get("intent") or "image_chart_analysis"
    payload["response_type"] = payload.get("response_type") or "chart_analysis"
    payload["tickers"] = tickers
    if "structured" not in payload or not isinstance(payload.get("structured"), dict):
        sentences = [part.strip(" -") for part in answer.replace("\n", ". ").split(".") if part.strip()]
        payload["structured"] = {
            "kind": "chart_image_analysis",
            "title": "Analisis visual de grafica",
            "ticker": tickers[0] if tickers else "",
            "status": payload.get("status") or "vision_proxy",
            "confidence": 0.42 if "no respond" in answer.lower() else 0.72,
            "summary": sentences[0] if sentences else "Genesis recibio la imagen para lectura visual.",
            "sections": [
                {"title": "Lectura rapida", "bullets": sentences[:2]},
                {"title": "Que vigilar", "bullets": sentences[2:5]},
            ],
        }
    if "vision_policy" not in payload:
        payload["vision_policy"] = "La imagen se interpreta visualmente; precios y retornos se reconfirman con FMP."
    return payload


def _genesis_message_from_body(body: dict | None) -> str:
    if not isinstance(body, dict):
        return ""
    return str(body.get("message") or body.get("question") or "").strip()


def _normalize_analyze_image_body(body: dict | None) -> dict:
    if not isinstance(body, dict):
        body = {}
    normalized = dict(body)
    message = str(
        normalized.get("message")
        or normalized.get("question")
        or normalized.get("prompt")
        or normalized.get("text")
        or ""
    ).strip()
    if not message:
        message = "Analiza esta grafica financiera: tendencia, niveles, volumen, riesgo y que vigilar."
    normalized["message"] = message
    normalized["question"] = message

    image = normalized.get("image") if isinstance(normalized.get("image"), dict) else {}
    image = dict(image)
    data_url = str(
        image.get("data_url")
        or image.get("dataUrl")
        or normalized.get("image_data")
        or normalized.get("imageData")
        or normalized.get("data_url")
        or normalized.get("dataUrl")
        or normalized.get("image_url")
        or normalized.get("imageUrl")
        or ""
    ).strip()
    raw_base64 = str(normalized.get("image_base64") or normalized.get("base64") or "").strip()
    mime_type = str(image.get("type") or normalized.get("mime_type") or normalized.get("mime") or "image/png").strip() or "image/png"
    if raw_base64 and not data_url:
        data_url = f"data:{mime_type};base64,{raw_base64}"
    if data_url.startswith("data:") and ";base64," in data_url:
        prefix, raw_base64 = data_url.split(";base64,", 1)
        detected_mime = prefix.replace("data:", "", 1).strip()
        if detected_mime:
            mime_type = detected_mime
    image["data_url"] = data_url
    image["type"] = mime_type
    normalized["image"] = image
    normalized["image_data"] = data_url
    normalized["image_base64"] = raw_base64
    normalized["mime_type"] = mime_type
    return normalized


def _fold_prompt(value: object) -> str:
    normalized = unicodedata.normalize("NFD", str(value or "").casefold())
    folded = "".join(char for char in normalized if unicodedata.category(char) != "Mn")
    return "".join(char if char.isalnum() else " " for char in folded)


def _is_casual_genesis_prompt(message: str) -> bool:
    text = f" {_fold_prompt(message)} "
    return any(
        token in text
        for token in (
            " como estas ",
            " como vas ",
            " que tal ",
            " todo bien ",
            " estas listo ",
            " estas activa ",
            " estas funcionando ",
            " buenas tardes ",
            " buenas noches ",
            " mi novia ",
            " mi novio ",
            " mi esposa ",
            " mi esposo ",
            " mi pareja ",
            " enojada ",
            " enojado ",
            " molesta ",
            " molesto ",
            " necesito consejo ",
            " problema personal ",
        )
    )


def _is_personal_genesis_prompt(message: str) -> bool:
    text = f" {_fold_prompt(message)} "
    return any(
        token in text
        for token in (
            " mi novia ",
            " mi novio ",
            " mi esposa ",
            " mi esposo ",
            " mi pareja ",
            " enojada ",
            " enojado ",
            " molesta ",
            " molesto ",
            " necesito consejo ",
            " problema personal ",
        )
    )


def _is_market_genesis_prompt(message: str) -> bool:
    text = f" {_fold_prompt(message)} "
    if " mercado libre " in text:
        return False
    if " mercado " in text and not any(token in text for token in (" seguimiento ", " cartera ", " watchlist ", " portfolio ", " paper ")):
        return True
    return any(
        token in text
        for token in (
            " como esta el mercado ",
            " como va el mercado ",
            " mercado el dia de hoy ",
            " mercado hoy ",
            " que esta pasando hoy ",
            " viernes pasado ",
        )
    )


def _is_news_genesis_prompt(message: str) -> bool:
    text = f" {_fold_prompt(message)} "
    return any(
        token in text
        for token in (
            " noticia ",
            " noticias ",
            " titulares ",
            " catalizador ",
            " catalizadores ",
            " que esta pasando en noticias ",
            " que paso en noticias ",
            " noticias importantes ",
            " ultimas noticias ",
        )
    )


def _is_memory_genesis_prompt(message: str) -> bool:
    text = f" {_fold_prompt(message)} "
    return any(
        token in text
        for token in (
            " que aprendiste ",
            " aprendiste de ",
            " que recuerdas ",
            " que hicimos ",
            " historial de ",
            " memoria de ",
            " mis consultas recientes ",
            " alertas funcionaron ",
            " noticias movieron ",
        )
    )


def _is_whale_genesis_prompt(message: str) -> bool:
    text = f" {_fold_prompt(message)} "
    return any(
        token in text
        for token in (
            " ballena ",
            " ballenas ",
            " ballnea ",
            " ballneas ",
            " ballnes ",
            " smart money ",
            " dinero grande ",
            " flujo institucional ",
            " dinero institucional ",
            " manos fuertes ",
        )
    )


def _prompt_tickers(message: str, context: object | None = None) -> list[str]:
    try:
        from services.genesis.ticker_parser import extract_tickers_from_prompt, normalize_ticker

        tickers: list[str] = []
        for raw in extract_tickers_from_prompt(message, context=context):
            ticker = normalize_ticker(raw)
            if ticker and ticker not in tickers:
                tickers.append(ticker)
        return tickers
    except Exception:
        logging.getLogger("genesis.dashboard").warning("Ticker extraction failed for Genesis prompt", exc_info=True)
        return []


def _is_asset_genesis_prompt(message: str, context: object | None = None) -> bool:
    if not str(message or "").strip():
        return False
    if (
        _is_casual_genesis_prompt(message)
        or _is_news_genesis_prompt(message)
        or _is_whale_genesis_prompt(message)
        or _is_memory_genesis_prompt(message)
    ):
        return False
    tickers = _prompt_tickers(message, context=context)
    if not tickers:
        return False
    text = f" {_fold_prompt(message)} "
    asset_intent_tokens = (
        " analiza ",
        " analizar ",
        " opinion ",
        " opinas ",
        " comprar ",
        " compro ",
        " vender ",
        " vendo ",
        " precio ",
        " grafica ",
        " grafico ",
        " chart ",
        " soporte ",
        " resistencia ",
        " rsi ",
        " macd ",
        " ema ",
        " que pasa con ",
        " que esta pasando con ",
        " deberia ",
        " conviene ",
    )
    return any(token in text for token in asset_intent_tokens) or len(tickers) == 1


def _is_trade_decision_prompt(message: str) -> bool:
    text = f" {_fold_prompt(message)} "
    decision_tokens = (
        " deberia ",
        " deberia comprar ",
        " deberia vender ",
        " conviene ",
        " buena idea ",
        " comprar ",
        " compro ",
        " entrada ",
        " entrar ",
        " vender ",
        " vendo ",
        " mantener ",
        " aguantar ",
        " salirme ",
        " operar ",
    )
    return any(token in text for token in decision_tokens)


def _local_asset_genesis_payload(body: dict, message: str) -> dict:
    panel_context = body.get("panel_context") if isinstance(body.get("panel_context"), dict) else None
    tickers = _prompt_tickers(message, context=panel_context)
    ticker = tickers[0] if tickers else str(body.get("ticker") or "")
    result = ask_genesis(
        message,
        context=str(body.get("context") or "general"),
        ticker=ticker,
        panel_context=panel_context,
        conversation_id=str(body.get("conversation_id") or "default"),
    )
    result = _enrich_genesis_asset_quote(result)
    result = _enrich_genesis_trade_decision(result, message)
    result = _enrich_genesis_whale_payload(result)
    return result


def _production_get_json(path: str, *, timeout: float = 6) -> dict:
    try:
        target = f"{_PRODUCTION_API_ORIGIN}{path}"
        request = Request(target, headers={"Accept": "application/json", "User-Agent": "GenesisLocalProxy/1.0"}, method="GET")
        with urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _yahoo_symbol(ticker: str) -> str:
    symbol = str(ticker or "").strip().upper()
    if symbol == "BTC":
        return "BTC-USD"
    return symbol


def _yahoo_fetch_chart(ticker: str, timeframe: str = "1D") -> dict:
    symbol = _yahoo_symbol(ticker)
    normalized_timeframe = str(timeframe or "1D").strip().upper()
    yahoo_range, interval = _YAHOO_TIMEFRAMES.get(normalized_timeframe, _YAHOO_TIMEFRAMES["1Y"])
    cache_key = f"{symbol}:{normalized_timeframe}:{interval}"
    ttl = 20 if normalized_timeframe == "1D" else 180
    cached = _YAHOO_CHART_CACHE.get(cache_key)
    now = time.time()
    if cached and now - cached[0] <= ttl:
        return cached[1]
    try:
        target = f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(symbol)}?range={quote(yahoo_range)}&interval={quote(interval)}"
        request = Request(target, headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0 Genesis/1.0"}, method="GET")
        with urlopen(request, timeout=8) as response:
            data = json.loads(response.read().decode("utf-8"))
        result = ((data.get("chart") or {}).get("result") or [{}])[0]
        if not isinstance(result, dict):
            result = {}
        _YAHOO_CHART_CACHE[cache_key] = (now, result)
        return result
    except Exception:
        logging.getLogger("genesis.dashboard").warning("Yahoo chart fallback unavailable for %s", symbol, exc_info=True)
        return {}


def _yahoo_shape_points(result: dict) -> list[dict]:
    timestamps = result.get("timestamp") if isinstance(result, dict) else []
    quote_rows = ((result.get("indicators") or {}).get("quote") or [{}]) if isinstance(result, dict) else [{}]
    series = quote_rows[0] if quote_rows and isinstance(quote_rows[0], dict) else {}
    if not isinstance(timestamps, list) or not timestamps:
        return []
    opens = series.get("open") if isinstance(series.get("open"), list) else []
    highs = series.get("high") if isinstance(series.get("high"), list) else []
    lows = series.get("low") if isinstance(series.get("low"), list) else []
    closes = series.get("close") if isinstance(series.get("close"), list) else []
    volumes = series.get("volume") if isinstance(series.get("volume"), list) else []
    points: list[dict] = []
    last_close: float | None = None
    for index, raw_ts in enumerate(timestamps):
        close = _safe_num(closes[index] if index < len(closes) else None)
        if close is None:
            continue
        opened = _safe_num(opens[index] if index < len(opens) else None) or last_close or close
        high = _safe_num(highs[index] if index < len(highs) else None) or max(opened, close)
        low = _safe_num(lows[index] if index < len(lows) else None) or min(opened, close)
        volume = _safe_num(volumes[index] if index < len(volumes) else None)
        try:
            stamp = datetime.fromtimestamp(float(raw_ts), timezone.utc).isoformat()
        except Exception:
            stamp = str(raw_ts)
        points.append(
            {
                "time": stamp,
                "date": stamp[:10],
                "open": opened,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            }
        )
        last_close = close
    return points


def _yahoo_quote_row(ticker: str) -> dict | None:
    result = _yahoo_fetch_chart(ticker, "1D")
    meta = result.get("meta") if isinstance(result, dict) else {}
    if not isinstance(meta, dict):
        meta = {}
    points = _yahoo_shape_points(result)
    last_close = _safe_num(points[-1].get("close")) if points else None
    price = _safe_num(meta.get("regularMarketPrice")) or last_close
    if price is None or price <= 0:
        return None
    previous = _safe_num(meta.get("previousClose") or meta.get("chartPreviousClose"))
    change = price - previous if previous else None
    change_pct = (change / previous * 100) if change is not None and previous else None
    symbol = _yahoo_symbol(ticker)
    return {
        "ticker": symbol,
        "name": meta.get("longName") or meta.get("shortName") or symbol,
        "current_price": price,
        "price": price,
        "daily_change": change,
        "daily_change_pct": change_pct,
        "change": change,
        "change_pct": change_pct,
        "percent_change": change_pct,
        "previous_close": previous,
        "day_high": _safe_num(meta.get("regularMarketDayHigh")) or (max((_safe_num(point.get("high")) or price) for point in points) if points else None),
        "day_low": _safe_num(meta.get("regularMarketDayLow")) or (min((_safe_num(point.get("low")) or price) for point in points) if points else None),
        "volume": _safe_num(meta.get("regularMarketVolume")) or (sum((_safe_num(point.get("volume")) or 0) for point in points) if points else None),
        "quote_timestamp": meta.get("regularMarketTime") or (points[-1].get("time") if points else datetime.now(timezone.utc).isoformat()),
        "source": "yahoo_chart_fallback",
        "source_label": "Yahoo Chart fallback",
        "provider_used": "yahoo_chart_fallback",
    }


def _yahoo_market_search_payload(query: str) -> dict:
    row = _yahoo_quote_row(query)
    if not row:
        return {"ok": False, "status": "not_found", "results": [], "provider_used": "yahoo_chart_fallback"}
    requested = str(query or "").strip().upper()
    if requested:
        row["ticker"] = requested
    return {
        "ok": True,
        "status": "ready",
        "results": [row],
        "provider_used": "yahoo_chart_fallback",
        "cache_hit": False,
    }


def _simple_return(first: object, last: object) -> float | None:
    start = _safe_num(first)
    end = _safe_num(last)
    if start is None or end is None or start == 0:
        return None
    return (end - start) / start * 100


def _yahoo_asset_chart_payload(ticker: str, timeframe: str = "1Y") -> dict:
    normalized_ticker = _yahoo_symbol(ticker)
    normalized_timeframe = str(timeframe or "1Y").strip().upper()
    if normalized_timeframe not in _YAHOO_TIMEFRAMES:
        normalized_timeframe = "1Y"
    result = _yahoo_fetch_chart(normalized_ticker, normalized_timeframe)
    points = _yahoo_shape_points(result)
    quote_row = _yahoo_quote_row(normalized_ticker) or {}
    if len(points) < 2:
        return {
            "ok": False,
            "status": "no_data",
            "ticker": normalized_ticker,
            "range": normalized_timeframe,
            "timeframe": normalized_timeframe,
            "points": [],
            "ohlc": [],
            "message": "No hay datos OHLC suficientes para esta temporalidad.",
            "source": {"provider": "Yahoo Chart fallback", "live_enabled": True, "price_only": False},
        }
    first = points[0]
    last = points[-1]
    summary_change = (_safe_num(last.get("close")) or 0) - (_safe_num(first.get("close")) or 0)
    selected_return = _simple_return(first.get("close"), last.get("close"))
    returns = {"1D": None, "1W": None, "1M": None, "1Y": None, "5Y": None, "MAX": None}
    returns[normalized_timeframe] = selected_return
    try:
        from services.genesis.technical_analysis import compute_technical_indicators

        indicators = compute_technical_indicators(points)
    except Exception:
        indicators = {}
    return {
        "ok": True,
        "status": "ready",
        "ticker": normalized_ticker,
        "selected_range": normalized_timeframe,
        "timeframe": normalized_timeframe,
        "range": normalized_timeframe,
        "name": quote_row.get("name") or normalized_ticker,
        "points": points,
        "ohlc": points,
        "returns": returns,
        "return_details": {},
        "indicators": indicators,
        "summary": {
            "start_price": first.get("close"),
            "end_price": last.get("close"),
            "change": summary_change,
            "change_pct": selected_return,
        },
        "max_history_years": 0.0,
        "history_points": len(points),
        "raw_eod_points": len(points),
        "selected_range_points": len(points),
        "fmp_endpoint_used": "yahoo-chart-fallback",
        "has_full_history": normalized_timeframe in {"5Y", "MAX"},
        "is_max_truncated": normalized_timeframe == "MAX",
        "max_truncated": normalized_timeframe == "MAX",
        "truncation_reason": "fallback_publico_sin_fmp_local",
        "max_history_note": "Grafica servida por fallback publico cuando FMP/Railway no responde localmente.",
        "first_date": first.get("date"),
        "last_date": last.get("date"),
        "first_close": first.get("close"),
        "last_close": last.get("close"),
        "quote": quote_row,
        "source": {
            "provider": "Yahoo Chart fallback",
            "endpoint": "query1.finance.yahoo.com/v8/finance/chart",
            "live_enabled": True,
            "price_only": False,
            "downsampled": False,
            "raw_points": len(points),
            "selected_range_points": len(points),
            "fallback": True,
        },
    }


def _call_json_with_timeout(fn, timeout: float, fallback: dict) -> dict:
    result_queue: queue.Queue = queue.Queue(maxsize=1)

    def _runner() -> None:
        try:
            result_queue.put(fn(), block=False)
        except Exception:
            result_queue.put(fallback, block=False)

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    try:
        result = result_queue.get(timeout=timeout)
    except queue.Empty:
        return fallback
    return result if isinstance(result, dict) else fallback


def _production_whale_snapshot() -> dict:
    paths = {
        "whales": "/api/dashboard/whales",
        "detection": "/api/dashboard/money-flow/detection",
        "causal": "/api/dashboard/money-flow/causal",
    }
    results: dict[str, dict] = {}
    executor = ThreadPoolExecutor(max_workers=3)
    future_map = {executor.submit(_production_get_json, path, timeout=4): key for key, path in paths.items()}
    try:
        for future in as_completed(future_map, timeout=4.5):
            key = future_map[future]
            try:
                value = future.result(timeout=0)
            except Exception:
                value = {}
            if isinstance(value, dict):
                results[key] = value
    except Exception:
        pass
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
    if _whale_payload_rows(results.get("whales") or {}):
        return results["whales"]
    return {"ok": True, **results}


def _fast_whale_snapshot_for_prompt() -> dict:
    local_detection = _call_json_with_timeout(
        get_dashboard_money_flow_detection,
        3.5,
        {"ok": True, "items": [], "detection": {"items": []}, "source_status": {"status": "timeout"}},
    )
    if _whale_payload_rows(local_detection):
        return {"ok": True, "detection": local_detection}
    return _production_whale_snapshot()


def _whale_prompt_fallback_payload(message: str, snapshot: dict | None = None) -> dict:
    snapshot = snapshot if isinstance(snapshot, dict) else {}
    if snapshot:
        _massage_whales_payload(snapshot, hydrate_missing=False)
    rows = _whale_payload_rows(snapshot)
    focus: list[str] = []
    for row in rows[:4]:
        ticker = str(row.get("ticker") or row.get("asset_name") or "").upper()
        monitored = _safe_num(row.get("monitored_dollar_volume") or row.get("dollar_volume"))
        confirmed_amount = _safe_num(row.get("confirmed_amount_usd") or row.get("amount_usd"))
        if ticker and row.get("confirmed") and (row.get("entity_name") or row.get("entity")) and confirmed_amount:
            focus.append(f"{ticker} {_money_short(confirmed_amount)} confirmado")
        elif ticker and monitored:
            focus.append(f"{ticker} {_money_short(monitored)} vigilado")
        elif ticker:
            focus.append(f"{ticker} pendiente de volumen")
    answer = (
        "En claro: no hay ballena confirmada con entidad y monto; Genesis esta viendo "
        f"{', '.join(focus)}. Es actividad relevante para vigilar, no compra confirmada."
        if focus
        else (
            "En claro: esta es una pregunta sobre ballenas y smart money, no un ticker. "
            "Genesis debe separar flujo vigilado de ballena confirmada: solo llamo ballena confirmada "
            "a un evento con entidad, monto y fuente; si falta eso, lo trato como vigilancia de volumen/precio."
        )
    )
    metrics = _whale_metrics_from_rows(rows)
    return {
        "ok": True,
        "status": "genesis_intelligence_ready",
        "intent": "whale_activity",
        "response_type": "whale_flow",
        "answer": answer,
        "tickers": [],
        "kind": "whale_flow",
        "fast_whale_snapshot": True,
        "whales": {
            "answer": answer,
            "items": rows,
            "events": rows,
            "summary": metrics,
        },
        "structured": {
            "kind": "whale_flow",
            "title": "Ballenas / Smart money",
            "summary": answer,
            "events": rows,
            "metrics": metrics,
            "sections": [
                {
                    "title": "Lectura rapida",
                    "bullets": [
                        answer,
                        "Si no hay entidad y monto confirmados, lo presento como flujo vigilado.",
                    ],
                },
                {
                    "title": "Que vigilar",
                    "bullets": ["Volumen relativo, direccion de precio y fuente del flujo."],
                },
            ],
        },
    }


def _whale_payload_row_lists(payload: dict) -> list[list[dict]]:
    if not isinstance(payload, dict):
        return []
    candidates: list[object] = []
    for key in ("events", "items", "estimated", "confirmed", "premium_activity"):
        if isinstance(payload.get(key), list):
            candidates.append(payload.get(key))
    whales = payload.get("whales")
    if isinstance(whales, dict):
        for key in ("events", "items", "estimated", "confirmed"):
            if isinstance(whales.get(key), list):
                candidates.append(whales.get(key))
        snapshot = whales.get("snapshot")
        if isinstance(snapshot, dict):
            for key in ("events", "items", "estimated", "confirmed"):
                if isinstance(snapshot.get(key), list):
                    candidates.append(snapshot.get(key))
    structured = payload.get("structured")
    if isinstance(structured, dict) and isinstance(structured.get("events"), list):
        candidates.append(structured.get("events"))
    for nested_key in ("causal", "detection", "snapshot"):
        nested = payload.get(nested_key)
        if isinstance(nested, dict):
            for key in ("events", "items", "estimated", "confirmed"):
                if isinstance(nested.get(key), list):
                    candidates.append(nested.get(key))
    row_lists: list[list[dict]] = []
    for candidate in candidates:
        rows = [row for row in candidate if isinstance(row, dict)] if isinstance(candidate, list) else []
        if rows:
            row_lists.append(rows)
    return row_lists


def _whale_payload_rows(payload: dict) -> list[dict]:
    seen: set[int] = set()
    merged: list[dict] = []
    for rows in _whale_payload_row_lists(payload):
        for row in rows:
            marker = id(row)
            if marker in seen:
                continue
            seen.add(marker)
            merged.append(row)
    return merged


def _strict_confirmed_whale(row: dict) -> bool:
    if not isinstance(row, dict):
        return False
    amount = _safe_num(row.get("confirmed_amount_usd") or row.get("amount_usd"))
    return bool(
        (row.get("confirmed") or row.get("event_type") == "whale_confirmed")
        and (row.get("entity_name") or row.get("entity"))
        and amount is not None
        and 0 < amount <= 1_000_000_000_000
    )


def _whale_metrics_from_rows(rows: list[dict]) -> dict:
    confirmed_rows = [row for row in rows if _strict_confirmed_whale(row)]
    estimated_rows = [row for row in rows if not _strict_confirmed_whale(row)]
    confirmed_value = sum(
        _safe_num(row.get("confirmed_amount_usd") or row.get("amount_usd")) or 0
        for row in confirmed_rows
    )
    watched_volume = sum(
        _safe_num(row.get("monitored_dollar_volume") or row.get("dollar_volume")) or 0
        for row in estimated_rows
    )
    return {
        "confirmed_count": len(confirmed_rows),
        "estimated_count": len(estimated_rows),
        "confirmed_value": confirmed_value or None,
        "watched_volume": watched_volume or None,
        "monitored_dollar_volume": watched_volume or None,
    }


def _apply_whale_metrics(payload: dict, rows: list[dict]) -> None:
    if not isinstance(payload, dict):
        return
    metrics = _whale_metrics_from_rows(rows)
    payload["summary"] = {**(payload.get("summary") if isinstance(payload.get("summary"), dict) else {}), **metrics}
    payload["metrics"] = {**(payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}), **metrics}
    whales = payload.get("whales")
    if isinstance(whales, dict):
        whales["summary"] = {**(whales.get("summary") if isinstance(whales.get("summary"), dict) else {}), **metrics}
        whales["metrics"] = {**(whales.get("metrics") if isinstance(whales.get("metrics"), dict) else {}), **metrics}
    structured = payload.get("structured")
    if isinstance(structured, dict):
        structured["metrics"] = {**(structured.get("metrics") if isinstance(structured.get("metrics"), dict) else {}), **metrics}


def _enrich_genesis_whale_payload(result: dict) -> dict:
    if not isinstance(result, dict):
        return result
    if not (
        result.get("intent") in {"whale_activity", "money_flow"}
        or result.get("response_type") == "whale_flow"
        or result.get("kind") == "whale_flow"
    ):
        return result

    containers: list[dict] = []
    if isinstance(result.get("events"), list):
        containers.append(result)
    whales = result.setdefault("whales", {})
    if isinstance(whales, dict):
        if isinstance(whales.get("events"), list):
            containers.append(whales)
        snapshot = whales.get("snapshot")
        if isinstance(snapshot, dict) and isinstance(snapshot.get("events"), list):
            containers.append(snapshot)
    structured = result.get("structured")
    if isinstance(structured, dict) and isinstance(structured.get("events"), list):
        containers.append(structured)

    hydrate_missing = not bool(result.get("fast_whale_snapshot"))
    for container in containers:
        _massage_whales_payload(container, hydrate_missing=hydrate_missing)

    rows = _whale_payload_rows(result)
    if rows:
        _massage_whales_payload({"events": rows}, hydrate_missing=hydrate_missing)

    watched_volume = sum(
        _safe_num(row.get("monitored_dollar_volume") or row.get("dollar_volume")) or 0
        for row in rows
    )
    confirmed_value = sum(
        _safe_num(row.get("confirmed_amount_usd") or row.get("amount_usd")) or 0
        for row in rows
        if _strict_confirmed_whale(row)
    )
    confirmed_count = sum(1 for row in rows if _strict_confirmed_whale(row))
    estimated_count = max(0, len(rows) - confirmed_count)
    focus = []
    for row in rows[:3]:
        ticker = str(row.get("ticker") or row.get("asset_name") or "").upper()
        monitored = _safe_num(row.get("monitored_dollar_volume") or row.get("dollar_volume"))
        if ticker and monitored:
            focus.append(f"{ticker} {_money_short(monitored)} vigilados")
        elif ticker:
            focus.append(f"{ticker} pendiente de volumen")
    answer = (
        "En claro: no hay ballena confirmada con entidad y monto; Genesis está viendo "
        f"{', '.join(focus)}. Es actividad relevante para vigilar, no compra confirmada."
        if focus
        else "En claro: no hay ballena confirmada con entidad y monto; Genesis vigila volumen, precio y flujo sin inventar comprador."
    )

    metrics = {
        "confirmed_value": confirmed_value or None,
        "watched_volume": watched_volume or None,
        "confirmed_count": confirmed_count,
        "estimated_count": estimated_count,
        "confidence": "medium" if watched_volume else "low",
    }
    result.update(
        {
            "ok": True,
            "intent": "whale_activity",
            "response_type": "whale_flow",
            "kind": "whale_flow",
            "answer": answer,
            "assistant_narrative": answer,
            "tickers": [],
        }
    )
    whales.update({"answer": answer, "items": rows, "events": rows, "summary": metrics})
    if isinstance(whales.get("snapshot"), dict):
        whales["snapshot"]["events"] = rows
        whales["snapshot"]["metrics"] = metrics
    result["structured"] = {
        "kind": "whale_flow",
        "title": "Ballenas / Smart money",
        "summary": answer,
        "events": rows,
        "metrics": metrics,
        "sections": [
            {
                "title": "Lectura rápida",
                "bullets": [
                    answer,
                    "Separado: volumen vigilado no es monto confirmado de ballena.",
                ],
            },
            {
                "title": "Qué vigilar",
                "bullets": [
                    "Volumen relativo, reacción de precio, ruptura o rechazo de niveles.",
                    "Sube a confirmada solo si aparece entidad, monto y fuente directa.",
                ],
            },
        ],
    }
    return result


def _news_prompt_fallback_payload(message: str) -> dict:
    answer = (
        "En noticias: esta pregunta pide contexto de titulares, no un ticker. "
        "Genesis debe usar FMP/RSS, separar importantes y ultimas, y explicar impacto en tus activos sin inventar precios."
    )
    return {
        "ok": True,
        "status": "genesis_intelligence_ready",
        "intent": "macro_news",
        "response_type": "news_brief",
        "answer": answer,
        "tickers": [],
        "kind": "news_brief",
        "overview": {
            "answer": answer,
            "summary": answer,
            "news": [],
            "source_status": {"fallback": True},
        },
        "structured": {
            "kind": "news_brief",
            "title": "Noticias",
            "summary": answer,
            "important_news": [],
            "latest_news": [],
            "news": [],
            "sections": [
                {"title": "Lectura rapida", "bullets": [answer]},
                {"title": "Que vigilar", "bullets": ["Impacto en precio.", "Volumen posterior al titular.", "Activos afectados de cartera/watchlist."]},
            ],
        },
    }


def _correct_genesis_proxy_payload(payload: dict, body: dict) -> dict:
    message = _genesis_message_from_body(body)
    panel_context = body.get("panel_context") if isinstance(body.get("panel_context"), dict) else None
    if _is_casual_genesis_prompt(message):
        personal = _is_personal_genesis_prompt(message)
        answer = (
            "Te escucho. Esto es una pregunta cotidiana, no un ticker. "
            "Genesis puede responder como asistente general y solo usa datos financieros cuando realmente pides mercado o activos."
        ) if personal else (
            "Estoy activo y listo. Puedo leer mercado, noticias, alertas, ballenas, "
            "cartera o un activo sin convertir una frase normal en ticker."
        )
        return {
            "ok": True,
            "status": "genesis_intelligence_ready",
            "intent": "general" if personal else "greeting",
            "response_type": "general_assistant",
            "answer": answer,
            "tickers": [],
            "kind": "general_assistant",
            "structured": {
                "kind": "general_assistant",
                "title": "Modo humano" if personal else "Genesis",
                "mode": "Vida diaria" if personal else "Asistente completo",
                "summary": answer,
                "confidence": 0.72,
                "sections": [
                    {"title": "Lectura rapida", "bullets": [answer]},
                    {"title": "Siguiente paso", "bullets": ["Cuentame el contexto y te doy una respuesta clara.", "Si es mercado, valido FMP/backend antes de dar cifras."]},
                ],
            },
        }
    if _is_asset_genesis_prompt(message, panel_context):
        if payload.get("intent") in {"ticker_analysis", "technical_indicators", "chart_request"} or payload.get("response_type") in {"asset_analysis", "chart_analysis"}:
            return payload
        try:
            return _local_asset_genesis_payload(body, message)
        except Exception:
            logging.getLogger("genesis.dashboard").warning("Local asset prompt correction failed", exc_info=True)
    if _is_whale_genesis_prompt(message) and not (
        payload.get("intent") in {"whale_activity", "money_flow"} or payload.get("response_type") == "whale_flow"
    ):
        snapshot = _fast_whale_snapshot_for_prompt()
        return _enrich_genesis_whale_payload(_whale_prompt_fallback_payload(message, snapshot))
    if _is_news_genesis_prompt(message):
        if payload.get("intent") == "macro_news" or payload.get("response_type") == "news_brief":
            payload["tickers"] = []
            payload.pop("quote", None)
            payload.pop("chart", None)
            payload.pop("technical", None)
            return payload
        try:
            local = ask_genesis(
                message,
                context=str(body.get("context") or "general"),
                ticker="",
                panel_context=panel_context,
                conversation_id=str(body.get("conversation_id") or "default"),
            )
            if isinstance(local, dict) and local.get("intent") == "macro_news":
                return local
        except Exception:
            logging.getLogger("genesis.dashboard").warning("Local news prompt correction failed", exc_info=True)
        return _news_prompt_fallback_payload(message)
    if _is_market_genesis_prompt(message) and payload.get("intent") in {"ticker_analysis", "technical_indicators", "chart_request"}:
        try:
            local = ask_genesis(
                message,
                context=str(body.get("context") or "general"),
                ticker="",
                panel_context=panel_context,
                conversation_id=str(body.get("conversation_id") or "default"),
            )
            if isinstance(local, dict) and local.get("intent") == "market_overview":
                return local
        except Exception:
            logging.getLogger("genesis.dashboard").warning("Local market prompt correction failed", exc_info=True)
        return {
            "ok": True,
            "status": "genesis_intelligence_ready",
            "intent": "market_overview",
            "response_type": "market_summary",
            "answer": "Lectura de mercado: Genesis revisa índices, BTC, Brent, alertas y noticias; no detectó un ticker específico en tu pregunta.",
            "tickers": [],
            "kind": "market_briefing",
        }
    return payload


def _massage_alerts_payload(payload: dict) -> None:
    existing_tickers: set[str] = set()
    for key in ("items", "recent_alerts"):
        rows = payload.get(key)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            ticker = str(row.get("ticker") or "Mercado").upper()
            if ticker and ticker != "MERCADO":
                existing_tickers.add(ticker)
            price = _safe_num(row.get("price"))
            volume = _safe_num(row.get("volume"))
            dollar_volume, dollar_basis, dollar_suspicious = _safe_monitored_dollar_volume_for_proxy(
                ticker,
                price,
                volume,
                row.get("dollar_volume") or row.get("dollarVolume"),
            )
            if dollar_volume is not None:
                row["dollar_volume"] = dollar_volume
                row["dollarVolume"] = dollar_volume
                row["dollar_volume_basis"] = dollar_basis
            elif dollar_suspicious:
                row["dollar_volume"] = None
                row["dollarVolume"] = None
                row["dollar_volume_basis"] = dollar_basis
                row["amount_suspicious"] = True
            _copy_strategy_decision(row)
            title = str(row.get("title_es") or row.get("title") or "")
            summary = str(row.get("summary_es") or row.get("summary") or "")
            bland = "precio confirmado" in title.casefold() or "genesis lo mantiene" in summary.casefold()
            macro_repeated = bool(ticker != "MERCADO" and any(token in title.casefold() for token in ("petr", "oil", "geopolit", "cnbc daily open")))
            if bland:
                flow = _money_short(dollar_volume) if dollar_volume is not None else (f"{volume:,.0f} unidades" if volume is not None else "volumen pendiente")
                pct = _safe_num(row.get("change_pct"))
                support = row.get("support")
                resistance = row.get("resistance")
                row["title_es"] = f"{ticker}: {flow} negociados"
                row["summary_es"] = (
                    f"{ticker}: {(pct or 0):+.2f}% con {flow} observado; "
                    f"soporte {support or 'pendiente'}, resistencia {resistance or 'pendiente'}. "
                    "Genesis vigila ruptura, rechazo y volumen antes de actuar."
                )
                row["genesis_reading_es"] = (
                    f"{ticker}: alerta tecnica con precio y flujo visibles. No es orden; sirve para priorizar vigilancia."
                )
            elif macro_repeated:
                flow = _money_short(dollar_volume) if dollar_volume is not None else "volumen pendiente"
                row["title_es"] = f"{ticker}: catalizador macro afectando vigilancia"
                row["summary_es"] = (
                    f"{ticker} queda expuesto a este titular macro; precio {price if price is not None else 'no directo'} "
                    f"y flujo {flow}. Genesis revisa si la noticia se confirma en volumen y rango."
                )
                row["genesis_reading_es"] = (
                    f"{ticker}: la noticia no es señal aislada; pesa si mueve precio, volumen o rompe soporte/resistencia."
                )
    current_count = sum(len(payload.get(key) or []) for key in ("items", "recent_alerts") if isinstance(payload.get(key), list))
    opportunities = _proxy_opportunity_rows(existing_tickers) if current_count < 4 else []
    if opportunities:
        payload["opportunities"] = opportunities
        for key in ("items", "recent_alerts"):
            rows = payload.get(key)
            if not isinstance(rows, list):
                rows = []
            seen = {str(row.get("id") or row.get("alert_id") or "") for row in rows if isinstance(row, dict)}
            merged = []
            for row in opportunities:
                row_id = str(row.get("id") or row.get("alert_id") or "")
                if row_id not in seen:
                    merged.append(row)
                    seen.add(row_id)
            payload[key] = [*merged, *rows][:14]
        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        summary["opportunities"] = len(opportunities)
        summary["engine_summary"] = "Genesis agregó oportunidades externas importantes con FMP/proxy; son radar paper, no órdenes reales."
        payload["summary"] = summary


def _proxy_opportunity_rows(existing_tickers: set[str]) -> list[dict]:
    tickers = [ticker for ticker in _PROXY_OPPORTUNITY_TICKERS if ticker not in existing_tickers]
    if not tickers:
        return []

    search_by_ticker: dict[str, dict] = {}
    executor = ThreadPoolExecutor(max_workers=min(4, len(tickers)))
    future_map = {executor.submit(_market_search_for_proxy, ticker): ticker for ticker in tickers}
    try:
        for future in as_completed(future_map, timeout=2):
            ticker = future_map[future]
            try:
                result = future.result(timeout=0)
            except Exception:
                result = {}
            if isinstance(result, dict):
                search_by_ticker[ticker] = result
    except Exception:
        pass
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    rows: list[dict] = []
    for ticker, search in search_by_ticker.items():
        candidates = search.get("results") if isinstance(search, dict) else []
        asset = candidates[0] if isinstance(candidates, list) and candidates else None
        if not isinstance(asset, dict):
            continue
        price = _safe_num(asset.get("current_price") or asset.get("price"))
        volume = _safe_num(asset.get("volume"))
        change_pct = _safe_num(asset.get("daily_change_pct") or asset.get("change_pct")) or 0.0
        if price is None:
            continue
        dollar_volume, dollar_basis, dollar_suspicious = _safe_monitored_dollar_volume_for_proxy(ticker, price, volume)
        if not _proxy_opportunity_is_important(change_pct, dollar_volume, volume):
            continue
        support = _safe_num(asset.get("day_low") or asset.get("dayLow"))
        resistance = _safe_num(asset.get("day_high") or asset.get("dayHigh"))
        strategy = build_signal_strategy(
            ticker,
            {
                "price": price,
                "change_pct": change_pct,
                "volume": volume,
                "dollar_volume": dollar_volume,
                "dollar_volume_basis": dollar_basis,
                "amount_suspicious": dollar_suspicious,
                "support": support,
                "resistance": resistance,
            },
        )
        row_id = f"opportunity:{ticker}"
        flow = _money_short(dollar_volume)
        rows.append(
            {
                "id": row_id,
                "alert_id": row_id,
                "ticker": ticker,
                "asset_name": asset.get("name") or ticker,
                "title_es": f"{ticker}: oportunidad externa detectada",
                "summary_es": f"{ticker}: {change_pct:+.2f}% con {flow} de flujo observado. {strategy['summary']}",
                "alert_type": "opportunity_scan",
                "source": "FMP oportunidad",
                "status": "opportunity",
                "is_opportunity": True,
                "price": price,
                "change": _safe_num(asset.get("daily_change") or asset.get("change")),
                "change_pct": change_pct,
                "volume": volume,
                "dollar_volume": dollar_volume,
                "dollar_volume_basis": dollar_basis,
                "amount_suspicious": dollar_suspicious,
                "support": support,
                "resistance": resistance,
                "impact": "bullish" if change_pct > 0 else "bearish" if change_pct < 0 else "neutral",
                "direction": "bullish" if change_pct > 0 else "bearish" if change_pct < 0 else "neutral",
                "severity": "high" if strategy["score"] >= 72 else "medium",
                "confidence": "medium" if strategy["score"] >= 60 else "low",
                "strategy": strategy,
                "decision": strategy.get("decision"),
                "decision_label_es": strategy.get("decision_label_es"),
                "decision_reason_es": strategy.get("decision_reason_es"),
                "action_verdict": strategy.get("decision_label_es"),
                "genesis_reading_es": strategy["summary"],
                "what_it_means": strategy["summary"],
                "what_to_watch_es": "; ".join(strategy["validation"]),
                "affected_portfolio_assets": [],
                "affected_watchlist_assets": [],
                "mini_series": [change_pct, strategy["score"], volume or 0],
            }
        )
    rows.sort(key=lambda row: (float(row.get("strategy", {}).get("score") or 0), abs(float(row.get("dollar_volume") or 0))), reverse=True)
    return rows[:4]


def _proxy_opportunity_is_important(change_pct: float, dollar_volume: float | None, volume: float | None) -> bool:
    if dollar_volume is not None and dollar_volume >= 1_000_000_000:
        return True
    if volume is not None and volume >= 10_000_000:
        return True
    return abs(change_pct) >= 1.0


def _massage_whales_payload(payload: dict, *, hydrate_missing: bool = True) -> None:
    row_lists = _whale_payload_row_lists(payload)
    if not row_lists:
        return
    quote_cache: dict[str, dict] = {}
    missing_tickers: set[str] = set()
    for events in row_lists:
        for row in events:
            if not isinstance(row, dict):
                continue
            ticker = str(row.get("ticker") or row.get("symbol") or "").upper()
            if ticker and (
                _first_safe_num(row.get("price"), row.get("price_used"), row.get("current_price"), row.get("currentPrice")) is None
                or _first_safe_num(row.get("volume"), row.get("monitored_volume"), row.get("monitoredVolume")) is None
            ):
                missing_tickers.add(ticker)
    if hydrate_missing and missing_tickers:
        executor = None
        try:
            executor = ThreadPoolExecutor(max_workers=min(6, len(missing_tickers)))
            future_map = {executor.submit(_market_search_for_proxy, ticker): ticker for ticker in missing_tickers}
            for future in as_completed(future_map, timeout=2.5):
                ticker = future_map[future]
                try:
                    quote_cache[ticker] = future.result(timeout=0)
                except Exception:
                    quote_cache[ticker] = {}
        except Exception:
            logging.getLogger("genesis.dashboard").warning("Parallel whale quote enrichment failed", exc_info=True)
        finally:
            if executor is not None:
                executor.shutdown(wait=False, cancel_futures=True)
    for events in row_lists:
        for row in events:
            if not isinstance(row, dict):
                continue
            confirmed_amount = _safe_num(row.get("confirmed_amount_usd") or row.get("amount_usd"))
            confirmed = bool(
                (row.get("confirmed") or row.get("event_type") == "whale_confirmed")
                and (row.get("entity_name") or row.get("entity"))
                and confirmed_amount is not None
                and 0 < confirmed_amount <= 1_000_000_000_000
            )
            ticker = str(row.get("ticker") or row.get("symbol") or "").upper()
            if ticker and (
                _first_safe_num(row.get("price"), row.get("price_used"), row.get("current_price"), row.get("currentPrice")) is None
                or _first_safe_num(row.get("volume"), row.get("monitored_volume"), row.get("monitoredVolume")) is None
            ):
                _enrich_whale_event_with_quote(row, ticker, quote_cache)
            price = _first_safe_num(row.get("price"), row.get("price_used"), row.get("current_price"), row.get("currentPrice"))
            volume = _first_safe_num(row.get("volume"), row.get("monitored_volume"), row.get("monitoredVolume"))
            dollar_volume, dollar_basis, dollar_suspicious = _safe_monitored_dollar_volume_for_proxy(
                ticker,
                price,
                volume,
                _first_safe_num(row.get("dollar_volume"), row.get("dollarVolume"), row.get("monitored_dollar_volume"), row.get("monitoredDollarVolume")),
            )
            if price is not None:
                row["price"] = price
                row["price_used"] = price
                row["current_price"] = price
                row["currentPrice"] = price
            if volume is not None:
                row["volume"] = volume
                row["monitored_volume"] = volume
                row["monitoredVolume"] = volume
            if dollar_volume is not None:
                row["dollar_volume"] = dollar_volume
                row["dollarVolume"] = dollar_volume
                row["monitored_dollar_volume"] = dollar_volume
                row["monitoredDollarVolume"] = dollar_volume
                row["monitored_volume_basis"] = dollar_basis
                row["monitored_value_suspicious"] = dollar_suspicious
            elif dollar_suspicious and not confirmed:
                row["amount_suspicious"] = True
                row["monitored_value_suspicious"] = True
                row["monitored_volume_basis"] = dollar_basis
                row["monitored_dollar_volume"] = None
                row["monitoredDollarVolume"] = None
                row["dollar_volume"] = None
                row["dollarVolume"] = None
                row["estimated_flow"] = None
                row["genesis_reading_es"] = (
                    f"{ticker}: volumen bruto demasiado grande para tratarlo como flujo institucional. "
                    "Genesis lo oculta como monto y espera confirmación de fuente."
                )
            if not confirmed:
                row["confirmed"] = False
                if row.get("event_type") == "whale_confirmed":
                    row["event_type"] = "smart_money_estimate"
                row["confirmed_amount_usd"] = None
                row["amount_usd"] = None
                row["amountUsd"] = None
            else:
                row["confirmed"] = True
                row["event_type"] = "whale_confirmed"
                row["confirmed_amount_usd"] = confirmed_amount
        events.sort(key=lambda item: (_safe_num(item.get("monitored_dollar_volume") or item.get("dollar_volume")) or 0), reverse=True)
    all_rows = _whale_payload_rows(payload)
    _apply_whale_metrics(payload, all_rows)


def _enrich_whale_event_with_quote(row: dict, ticker: str, quote_cache: dict[str, dict] | None = None) -> None:
    if quote_cache is not None and ticker in quote_cache:
        search = quote_cache[ticker]
    else:
        search = _market_search_for_proxy(ticker)
        if quote_cache is not None:
            quote_cache[ticker] = search
    rows = search.get("results") if isinstance(search, dict) and isinstance(search.get("results"), list) else []
    asset = next((item for item in rows if str(item.get("ticker") or "").upper() == ticker), rows[0] if rows else None)
    if not isinstance(asset, dict):
        return
    price = _first_safe_num(asset.get("current_price"), asset.get("price"), asset.get("reference_price"))
    volume = _first_safe_num(asset.get("volume"), asset.get("monitored_volume"), asset.get("avg_volume"))
    if price is not None:
        row["price"] = price
        row["price_used"] = price
        row["current_price"] = price
        row["currentPrice"] = price
    if volume is not None:
        row["volume"] = volume
        row["monitored_volume"] = volume
        row["monitoredVolume"] = volume
    if price is not None and volume is not None:
        dollar_volume, dollar_basis, dollar_suspicious = _safe_monitored_dollar_volume_for_proxy(
            ticker,
            price,
            volume,
            asset.get("dollar_volume") or asset.get("dollarVolume") or asset.get("quoteVolume") or asset.get("quote_volume"),
        )
        if dollar_volume is None:
            row["amount_suspicious"] = dollar_suspicious
            row["monitored_volume_basis"] = dollar_basis
            row["genesis_reading_es"] = (
                f"{ticker}: precio y volumen detectados, pero el valor no pasa validación. "
                "Genesis no lo muestra como monto de ballena."
            )
            return
        row["dollar_volume"] = dollar_volume
        row["dollarVolume"] = dollar_volume
        row["monitored_dollar_volume"] = dollar_volume
        row["monitoredDollarVolume"] = dollar_volume
        row["monitored_volume_basis"] = dollar_basis
        row["monitored_value_suspicious"] = dollar_suspicious
        row["source"] = asset.get("source") or row.get("source") or "datos_directos"
        row["confidence"] = row.get("confidence") if row.get("confidence") not in ("", None, "low", "baja") else "medium"
        row["genesis_reading_es"] = (
            f"{ticker}: {_money_short(dollar_volume)} de volumen vigilado con precio {price}. "
            "No es ballena confirmada; es radar de actividad para priorizar seguimiento."
        )


def _enrich_genesis_asset_quote(result: dict) -> dict:
    if not isinstance(result, dict):
        return result
    response_type = str(result.get("response_type") or result.get("kind") or "")
    if response_type not in {"asset_analysis", "chart_analysis"} and result.get("intent") not in {"ticker_analysis", "technical_indicators", "chart_request"}:
        return result

    def attach_chart_payload(payload: dict, ticker_label: str) -> None:
        ticker_key = str(ticker_label or "").strip().upper()
        if not ticker_key:
            return
        structured = payload.get("structured") if isinstance(payload.get("structured"), dict) else None
        current_chart = structured.get("chart") if isinstance(structured, dict) and isinstance(structured.get("chart"), dict) else {}
        current_points = current_chart.get("points") or current_chart.get("ohlc") if isinstance(current_chart, dict) else []
        if isinstance(current_points, list) and len(current_points) >= 2:
            return
        chart_payload = _yahoo_asset_chart_payload(ticker_key, "1Y")
        if not chart_payload.get("ok"):
            chart_payload = _yahoo_asset_chart_payload(ticker_key, "1D")
        points = chart_payload.get("points") if isinstance(chart_payload.get("points"), list) else []
        if not chart_payload.get("ok") or len(points) < 2:
            return
        compact_chart = {
            "ticker": chart_payload.get("ticker") or ticker_key,
            "range": chart_payload.get("selected_range") or chart_payload.get("range") or "1Y",
            "ohlc": points,
            "points": points,
            "summary": chart_payload.get("summary") or {},
            "returns": chart_payload.get("returns") or {},
            "price_only": False,
            "source": chart_payload.get("source") or {},
        }
        payload["technical"] = {
            "ok": True,
            "ticker": chart_payload.get("ticker") or ticker_key,
            "range": compact_chart["range"],
            "indicators": chart_payload.get("indicators") or {},
            "summary": chart_payload.get("summary") or {},
            "returns": chart_payload.get("returns") or {},
            "history_points": chart_payload.get("history_points") or len(points),
            "selected_range_points": chart_payload.get("selected_range_points") or len(points),
            "source": chart_payload.get("source") or {},
            "chart": compact_chart,
        }
        if isinstance(structured, dict):
            indicators = chart_payload.get("indicators") if isinstance(chart_payload.get("indicators"), dict) else {}
            structured["chart"] = compact_chart
            structured["indicators"] = {
                **(structured.get("indicators") if isinstance(structured.get("indicators"), dict) else {}),
                **indicators,
            }
            structured.setdefault("levels", {})
            if isinstance(structured["levels"], dict):
                support = indicators.get("support")
                resistance = indicators.get("resistance")
                if support is not None:
                    structured["levels"]["support"] = support
                if resistance is not None:
                    structured["levels"]["resistance"] = resistance
            old_sections = structured.get("sections") if isinstance(structured.get("sections"), list) else []
            if old_sections:
                structured["sections"] = [
                    {
                        "title": "Lectura rapida",
                        "bullets": [
                            f"{ticker_key} ya tiene precio y grafica activa; Genesis evita operar solo por precio y valida volumen, niveles y noticias.",
                        ],
                    },
                    {
                        "title": "Que vigilar",
                        "bullets": [
                            "Confirmacion de volumen relativo, soporte/resistencia y catalizadores antes de tomar decision.",
                        ],
                    },
                ]

    def apply_confirmed_asset_copy(payload: dict, quote_payload: dict, ticker_label: str) -> None:
        price_label = quote_payload.get("formatted_price") or _money_short(quote_payload.get("current_price") or quote_payload.get("price"))
        source_label = quote_payload.get("source_label") or quote_payload.get("source") or "fuente activa"
        change = _safe_num(quote_payload.get("daily_change"))
        change_pct = _safe_num(quote_payload.get("daily_change_pct"))
        previous_close = _safe_num(quote_payload.get("previous_close"))
        if (change_pct is None or abs(change_pct) < 0.005) and change is not None and previous_close:
            change_pct = (change / previous_close) * 100
            quote_payload["daily_change_pct"] = change_pct
        move_text = ""
        if change is not None:
            move_text = f" cambio {_money_short(change)}"
            if change_pct is not None and abs(change_pct) >= 0.005:
                move_text += f" ({change_pct:+.2f}%)"
        thesis = (
            f"{ticker_label}: precio confirmado por {source_label} en {price_label}{move_text}. "
            "Genesis usa este dato como base; la decisión depende de volumen, niveles, noticias y riesgo."
        )
        payload["ticker"] = ticker_label
        payload["answer"] = thesis
        quote_payload.pop("message", None)
        structured = payload.get("structured")
        if isinstance(structured, dict):
            structured["ticker"] = ticker_label
            old_thesis = str(structured.get("thesis") or "")
            if (
                "no tiene precio confirmado" in old_thesis.casefold()
                or "no tengo precio confirmado" in old_thesis.casefold()
                or _safe_num(structured.get("confidence")) is None
                or (_safe_num(structured.get("confidence")) or 0) < 0.7
            ):
                structured["thesis"] = thesis
                structured["summary"] = thesis
            structured["confidence"] = max(_safe_num(structured.get("confidence")) or 0, 0.82)
            move = _safe_num(quote_payload.get("daily_change_pct") or quote_payload.get("daily_change"))
            structured["verdict"] = "Alcista" if (move or 0) > 0 else "Bajista" if (move or 0) < 0 else "Neutral"
        attach_chart_payload(payload, ticker_label)

    quote = result.get("quote") if isinstance(result.get("quote"), dict) else {}
    existing_price = _safe_num(quote.get("current_price") or quote.get("price"))
    if existing_price is not None:
        quote["current_price"] = existing_price
        quote["price"] = existing_price
        quote["formatted_price"] = quote.get("formatted_price") or _money_short(existing_price)
        quote["source_label"] = quote.get("source_label") or "Fuente activa"
        quote["is_live"] = True
        quote["is_stale"] = False
        quote["sanity"] = {"ok": True, "suspicious": False, "reason": "Precio confirmado por fuente activa."}
        quote.pop("message", None)
        result["quote"] = quote
        if isinstance(result.get("structured"), dict):
            result["structured"].setdefault("price", {})
            if isinstance(result["structured"]["price"], dict):
                result["structured"]["price"].update(
                    {
                        "price": existing_price,
                        "current_price": existing_price,
                        "formatted_price": quote["formatted_price"],
                        "source": quote.get("source"),
                        "is_live": True,
                        "sanity": quote["sanity"],
                    }
                )
            ticker_label = str(quote.get("ticker") or result["structured"].get("ticker") or "").upper()
            apply_confirmed_asset_copy(result, quote, ticker_label)
        else:
            apply_confirmed_asset_copy(result, quote, str(quote.get("ticker") or "").upper())
        return result
    tickers = result.get("tickers") if isinstance(result.get("tickers"), list) else []
    ticker = str(quote.get("ticker") or (tickers[0] if tickers else "") or "").strip().upper()
    if not ticker:
        return result
    search = _market_search_for_proxy(ticker)
    rows = search.get("results") if isinstance(search, dict) and isinstance(search.get("results"), list) else []
    asset = next((item for item in rows if str(item.get("ticker") or "").upper() == ticker), rows[0] if rows else None)
    if not isinstance(asset, dict) or _safe_num(asset.get("current_price")) is None:
        return result
    merged_quote = {
        **quote,
        "ticker": ticker,
        "name": asset.get("name") or quote.get("name") or ticker,
        "current_price": asset.get("current_price"),
        "price": asset.get("current_price"),
        "formatted_price": _money_short(asset.get("current_price")),
        "daily_change": asset.get("daily_change"),
        "daily_change_pct": asset.get("daily_change_pct"),
        "previous_close": asset.get("previous_close"),
        "day_low": asset.get("day_low"),
        "day_high": asset.get("day_high"),
        "volume": asset.get("volume"),
        "source": asset.get("source") or "datos_directos",
        "source_label": "Fuente activa",
        "is_live": True,
        "is_stale": False,
        "sanity": {"ok": True, "suspicious": False, "reason": "Precio confirmado por fuente activa."},
    }
    result["quote"] = merged_quote
    if isinstance(result.get("structured"), dict):
        result["structured"].setdefault("price", {})
        if isinstance(result["structured"]["price"], dict):
            result["structured"]["price"].update(
                {
                    "current_price": merged_quote["current_price"],
                    "price": merged_quote["current_price"],
                    "formatted_price": merged_quote["formatted_price"],
                    "daily_change": merged_quote.get("daily_change"),
                    "daily_change_pct": merged_quote.get("daily_change_pct"),
                    "source": merged_quote.get("source"),
                    "is_live": True,
                    "sanity": merged_quote["sanity"],
                }
            )
    apply_confirmed_asset_copy(result, merged_quote, ticker)
    return result


def _enrich_genesis_trade_decision(result: dict, message: str) -> dict:
    if not isinstance(result, dict) or not _is_trade_decision_prompt(message):
        return result
    response_type = str(result.get("response_type") or result.get("kind") or "")
    if response_type not in {"asset_analysis", "chart_analysis"} and result.get("intent") not in {"ticker_analysis", "technical_indicators", "chart_request"}:
        return result

    quote = result.get("quote") if isinstance(result.get("quote"), dict) else {}
    structured = result.get("structured") if isinstance(result.get("structured"), dict) else {}
    indicators = structured.get("indicators") if isinstance(structured.get("indicators"), dict) else {}
    technical = result.get("technical") if isinstance(result.get("technical"), dict) else {}
    if isinstance(technical.get("indicators"), dict):
        indicators = {**indicators, **technical["indicators"]}
    levels = structured.get("levels") if isinstance(structured.get("levels"), dict) else {}

    ticker = str(
        quote.get("ticker")
        or structured.get("ticker")
        or result.get("ticker")
        or (result.get("tickers")[0] if isinstance(result.get("tickers"), list) and result.get("tickers") else "")
        or ""
    ).strip().upper()
    if not ticker:
        return result

    price = _first_safe_num(quote.get("current_price"), quote.get("price"), structured.get("current_price"))
    change = _first_safe_num(quote.get("daily_change"), quote.get("change"), structured.get("change"))
    change_pct = _first_safe_num(quote.get("daily_change_pct"), quote.get("change_pct"), quote.get("percent_change"), structured.get("change_pct"))
    previous_close = _safe_num(quote.get("previous_close"))
    if (change_pct is None or abs(change_pct) < 0.005) and change is not None and previous_close:
        change_pct = (change / previous_close) * 100
    support = _first_safe_num(levels.get("support"), indicators.get("support"), quote.get("day_low"), quote.get("dayLow"))
    resistance = _first_safe_num(levels.get("resistance"), indicators.get("resistance"), quote.get("day_high"), quote.get("dayHigh"))
    volume = _first_safe_num(quote.get("volume"), indicators.get("volume"))
    relative_volume = _first_safe_num(indicators.get("relative_volume"), indicators.get("relativeVolume"), quote.get("relative_volume"))
    rsi = _safe_num(indicators.get("rsi"))
    confidence = _safe_num(structured.get("confidence")) or (0.82 if price is not None else 0.35)
    source = quote.get("source_label") or quote.get("source") or "FMP / datos directos"
    text = f" {_fold_prompt(message)} "
    asks_sell = any(token in text for token in (" vender ", " vendo ", " salirme ", " reducir "))

    price_label = _money_short(price) if price is not None else "precio pendiente"
    pct_label = f"{change_pct:+.2f}%" if change_pct is not None else "cambio pendiente"
    support_label = _money_short(support) if support is not None else "soporte pendiente"
    resistance_label = _money_short(resistance) if resistance is not None else "resistencia pendiente"
    volume_label = f"{volume:,.0f}" if volume is not None else "volumen pendiente"
    rel_volume_label = f"{relative_volume:.2f}x" if relative_volume is not None else "volumen relativo pendiente"

    action = "wait"
    label = "Esperar confirmacion"
    tone = "neutral"
    reason = "Genesis no tiene suficiente confirmacion de precio, volumen y niveles para subir la conviccion."
    entry = f"Esperar ruptura y cierre arriba de {resistance_label} con volumen sostenido; si no confirma, no perseguir el movimiento."
    invalidation = f"Invalidar la idea si pierde {support_label} o si el volumen se seca despues de tocar resistencia."
    risk = "Riesgo principal: entrar solo por precio sin confirmacion de volumen, noticias y soporte."

    if price is None:
        label = "Esperar datos confirmados"
        reason = f"No hay precio confirmado para {ticker}; Genesis no convierte esto en senal operativa."
        entry = "Reintentar cuando FMP/backend entregue precio, historico y volumen."
        invalidation = "Sin precio confirmado, la lectura queda invalidada para decision."
    elif asks_sell:
        if change_pct is not None and change_pct < 0:
            action = "reduce_risk"
            label = "Reducir riesgo con cautela"
            tone = "bearish"
            reason = f"{ticker} esta presionado ({pct_label}); si ya tienes posicion, Genesis prioriza proteger capital antes que promediar a ciegas."
        else:
            action = "hold_watch"
            label = "Mantener y vigilar"
            reason = f"{ticker} no muestra deterioro suficiente para forzar salida; vigila {support_label} y volumen."
        entry = "Para paper: reducir parcial solo si pierde soporte con volumen o si falla el rebote."
        invalidation = f"La salida pierde fuerza si recupera {resistance_label} con volumen."
    elif change_pct is not None and change_pct < -1.2:
        action = "wait_for_support"
        label = "Esperar soporte"
        tone = "bearish"
        reason = f"{ticker} viene debil ({pct_label}); comprar ahora seria anticiparse sin confirmacion."
        entry = f"Paper solo si respeta {support_label} y rebota con volumen mayor al promedio."
    elif rsi is not None and rsi >= 72:
        action = "wait_for_retest"
        label = "Esperar retest"
        tone = "neutral"
        reason = f"{ticker} puede estar extendido por RSI {rsi:.1f}; Genesis prefiere entrada en retroceso o confirmacion limpia."
        entry = f"Buscar retest cerca de {support_label} o cierre fuerte arriba de {resistance_label} con volumen."
    elif change_pct is not None and change_pct > 0.35 and (relative_volume is None or relative_volume < 1.05):
        action = "watch_confirmation"
        label = "Vigilar confirmacion"
        tone = "bullish"
        reason = f"{ticker} sube ({pct_label}), pero el volumen relativo todavia no confirma fuerza institucional."
    elif change_pct is not None and change_pct >= 0 and confidence >= 0.72:
        action = "buy_cautiously"
        label = "Comprar con cautela"
        tone = "bullish"
        reason = f"{ticker} tiene precio confirmado en {price_label} y sesgo positivo; solo tiene sentido si confirma volumen y respeta niveles."
        entry = f"Paper pequeno si confirma arriba de {resistance_label} o hace retest limpio sin perder {support_label}."
    else:
        reason = f"{ticker} esta en rango ({pct_label}); Genesis espera una senal mas clara antes de actuar."

    decision = {
        "action": action,
        "label_es": label,
        "tone": tone,
        "reason_es": reason,
        "entry_condition_es": entry,
        "invalidation_es": invalidation,
        "risk_es": risk,
        "what_to_watch_es": [
            f"Precio: {price_label} ({pct_label})",
            f"Volumen: {volume_label} / relativo {rel_volume_label}",
            f"Zona: soporte {support_label}, resistencia {resistance_label}",
            "Noticias, alertas y flujo de ballenas antes de subir tamano.",
        ],
        "confidence": min(0.92, max(0.35, confidence)),
        "source": source,
        "not_real_order": True,
    }

    result["decision"] = decision
    if not isinstance(result.get("structured"), dict):
        result["structured"] = {}
    result["structured"]["decision"] = decision
    result["structured"]["scenario"] = {
        **(result["structured"].get("scenario") if isinstance(result["structured"].get("scenario"), dict) else {}),
        "probable": reason,
        "invalidation": invalidation,
    }
    result["structured"]["sections"] = [
        {"title": "Veredicto", "bullets": [f"{label}: {reason}"]},
        {"title": "Entrada condicional", "bullets": [entry]},
        {"title": "Invalidacion", "bullets": [invalidation]},
        {"title": "Que vigilar", "bullets": decision["what_to_watch_es"][:3]},
    ]
    result["answer"] = (
        f"VEREDICTO: {label}. {ticker} cotiza en {price_label} ({pct_label}). {reason}\n\n"
        f"Entrada condicional: {entry}\n\n"
        f"Invalidacion: {invalidation}\n\n"
        f"Riesgo: {risk}\n\n"
        "No es compra real ni orden de broker; es una lectura para validar en paper."
    )
    return result


def _market_search_for_proxy(ticker: str) -> dict:
    normalized_query = str(ticker or "").strip().upper()
    try:
        local = search_dashboard_market_ticker(normalized_query)
        if isinstance(local, dict):
            _normalize_market_payload(local)
        rows = local.get("results") if isinstance(local, dict) else []
        if isinstance(rows, list) and rows and _safe_num(rows[0].get("current_price")) is not None:
            return local
    except Exception:
        pass
    try:
        target = f"{_PRODUCTION_API_ORIGIN}/api/dashboard/market/search?q={quote(normalized_query)}"
        request = Request(target, headers={"Accept": "application/json", "User-Agent": "GenesisLocalProxy/1.0"}, method="GET")
        with urlopen(request, timeout=4) as response:
            payload = json.loads(response.read().decode("utf-8"))
            payload = _normalize_market_payload(payload) if isinstance(payload, dict) else {}
            rows = payload.get("results") if isinstance(payload, dict) else []
            if isinstance(rows, list) and rows:
                return payload
    except Exception:
        pass
    yahoo_payload = _yahoo_market_search_payload(normalized_query)
    yahoo_rows = yahoo_payload.get("results") if isinstance(yahoo_payload, dict) else []
    if isinstance(yahoo_rows, list) and yahoo_rows:
        return yahoo_payload
    if normalized_query.endswith("-USD"):
        base_query = normalized_query.removesuffix("-USD")
        if base_query and base_query != normalized_query:
            payload = _market_search_for_proxy(base_query)
            rows = payload.get("results") if isinstance(payload, dict) else []
            if isinstance(rows, list):
                for row in rows:
                    if isinstance(row, dict):
                        row.setdefault("provider_symbol", row.get("ticker"))
                        row["ticker"] = normalized_query
            return payload
    return {}


def _search_market_with_live_fallback(query: str) -> dict:
    fallback = _normalize_market_payload(search_dashboard_market_ticker(query))
    live = _market_search_for_proxy(query)
    rows = live.get("results") if isinstance(live, dict) else []
    if isinstance(rows, list) and any(_safe_num(row.get("current_price")) is not None for row in rows if isinstance(row, dict)):
        live["provider_used"] = live.get("provider_used") or "railway_fmp_proxy"
        live["cache_hit"] = bool(live.get("cache_hit", False))
        return _normalize_market_payload(live)
    return fallback


def _massage_news_payload(payload: dict) -> None:
    items = _dedupe_news_rows(payload.get("items"))
    if not items:
        fallback_rows = []
        for key in ("important", "latest"):
            rows = payload.get(key)
            if isinstance(rows, list):
                fallback_rows.extend(rows)
        items = _dedupe_news_rows(fallback_rows)
    for row in items:
        ts = _proxy_news_ts(row)
        if ts and not row.get("published_ts"):
            row["published_ts"] = ts
        row["recency_bucket"] = _proxy_news_bucket(row)
        row["is_latest"] = row["recency_bucket"] in {"24h", "7d", "30d"}
    fresh_items = [row for row in items if row.get("is_latest")]
    if fresh_items:
        items = fresh_items
    focus = {_proxy_news_normalize_ticker(ticker) for ticker in payload.get("focus_tickers") or [] if _proxy_news_normalize_ticker(ticker)}
    sorted_items = sorted(items, key=lambda row: (_proxy_news_bucket_rank(row), _proxy_news_ts(row)), reverse=True)
    important = sorted(
        [row for row in sorted_items if _proxy_news_is_important(row)],
        key=lambda row: (_proxy_news_bucket_rank(row), int(_safe_num(row.get("relevance_score")) or 0), _proxy_news_ts(row)),
        reverse=True,
    )[:8]
    latest = sorted(items, key=_proxy_news_ts, reverse=True)[:16]
    mine = sorted([row for row in items if _proxy_news_tickers(row) & focus], key=_proxy_news_ts, reverse=True)[:12]
    global_items = sorted([row for row in items if not (_proxy_news_tickers(row) & focus)], key=_proxy_news_ts, reverse=True)[:12]
    recency_windows = {
        "24h": sum(1 for row in items if _proxy_news_bucket(row) == "24h"),
        "7d": sum(1 for row in items if _proxy_news_bucket(row) in {"24h", "7d"}),
        "30d": sum(1 for row in items if _proxy_news_bucket(row) in {"24h", "7d", "30d"}),
    }
    payload["items"] = items
    payload["important"] = important
    payload["latest"] = latest
    payload["sections"] = {
        "important": important,
        "latest": latest,
        "mine": mine,
        "global": global_items,
    }
    payload["recency_windows"] = recency_windows
    payload["policy"] = "FMP/RSS live separado por filtro; últimas prioriza 24h, luego 7d y máximo 30d. No mezcla alertas ni ballenas como noticias."


def _proxy_news_ts(row: dict) -> int:
    direct = _safe_num(row.get("published_ts") or row.get("publishedTs"))
    if direct and direct > 0:
        return int(direct)
    text = str(row.get("published_at") or row.get("publishedDate") or row.get("date") or row.get("time") or "").strip()
    if not text:
        return 0
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())
    except Exception:
        return 0


def _proxy_news_bucket(row: dict) -> str:
    bucket = str(row.get("recency_bucket") or row.get("recencyBucket") or "").strip().lower()
    if bucket in {"24h", "7d", "30d"}:
        return bucket
    ts = _proxy_news_ts(row)
    if not ts:
        return "unknown"
    age_seconds = max(int(datetime.now(timezone.utc).timestamp()) - ts, 0)
    if age_seconds <= 86_400:
        return "24h"
    if age_seconds <= 7 * 86_400:
        return "7d"
    if age_seconds <= 30 * 86_400:
        return "30d"
    return "old"


def _proxy_news_bucket_rank(row: dict) -> int:
    return {"24h": 3, "7d": 2, "30d": 1, "unknown": 0}.get(_proxy_news_bucket(row), 0)


def _dedupe_news_rows(rows: object) -> list[dict]:
    output: list[dict] = []
    seen: set[str] = set()
    if not isinstance(rows, list):
        return output
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = str(row.get("title_es") or row.get("title") or "").strip()
        if _is_internal_news_title(title):
            continue
        key = str(row.get("id") or row.get("url") or title).strip().casefold()
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(row)
    return output


def _is_internal_news_title(title: str) -> bool:
    text = str(title or "").casefold()
    return any(token in text for token in ("contexto pendiente", "sin contexto", "genesis mantiene vigilancia", "briefing genesis listo"))


def _proxy_news_normalize_ticker(value: object) -> str:
    return str(value or "").strip().upper()


def _proxy_news_tickers(row: dict) -> set[str]:
    values = []
    for key in ("tickers", "assets", "tickers_affected"):
        raw = row.get(key)
        if isinstance(raw, list):
            values.extend(raw)
    return {_proxy_news_normalize_ticker(ticker) for ticker in values if _proxy_news_normalize_ticker(ticker)}


def _proxy_news_is_important(row: dict) -> bool:
    if bool(row.get("is_important")):
        return True
    category = str(row.get("category") or "").strip().casefold()
    impact = str(row.get("impact") or row.get("sentiment") or "").strip().casefold()
    recency = _safe_num(row.get("recency_score")) or 0
    relevance = _safe_num(row.get("relevance_score")) or 0
    if relevance >= 3 and recency >= 1:
        return True
    if category in {"macro", "geopolitics", "commodity", "earnings"} and recency >= 1:
        return True
    return impact in {"bullish", "bearish", "alcista", "bajista"} and recency >= 3


def _quote_rows_from_payload(payload: dict) -> list[dict]:
    rows = payload.get("results") if isinstance(payload, dict) else []
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _live_quote_for_snapshot_ticker(ticker: str) -> dict | None:
    normalized = str(ticker or "").strip().upper()
    if not normalized:
        return None
    rows = _quote_rows_from_payload(_market_search_for_proxy(normalized))
    return next((row for row in rows if str(row.get("ticker") or "").strip().upper() == normalized), rows[0] if rows else None)


def _merge_live_quote_into_snapshot_item(item: dict, quote_row: dict) -> bool:
    _normalize_quote_change_fields(quote_row)
    price = _first_safe_num(quote_row.get("current_price"), quote_row.get("price"))
    if price is None or price <= 0:
        return False
    daily_change = _first_safe_num(quote_row.get("daily_change"), quote_row.get("change"))
    daily_change_pct = _first_safe_num(quote_row.get("daily_change_pct"), quote_row.get("change_pct"), quote_row.get("percent_change"))
    units = _safe_num(item.get("units")) or 0.0
    entry_price = _safe_num(item.get("entry_price")) or _safe_num(item.get("reference_price")) or 0.0
    cost_basis = _safe_num(item.get("cost_basis"))
    if cost_basis is None and units > 0 and entry_price > 0:
        cost_basis = units * entry_price

    item["name"] = quote_row.get("name") or item.get("name") or item.get("ticker")
    item["display_name"] = quote_row.get("display_name") or quote_row.get("name") or item.get("display_name") or item.get("name")
    item["current_price"] = price
    item["reference_price"] = price
    item["daily_change"] = daily_change
    item["daily_change_pct"] = daily_change_pct
    item["change_pct"] = daily_change_pct
    item["percent_change"] = daily_change_pct
    item["previous_close"] = quote_row.get("previous_close")
    item["day_high"] = quote_row.get("day_high")
    item["day_low"] = quote_row.get("day_low")
    item["extended_hours_price"] = quote_row.get("extended_hours_price")
    item["extended_hours_change"] = quote_row.get("extended_hours_change")
    item["extended_hours_change_pct"] = quote_row.get("extended_hours_change_pct")
    item["market_session"] = quote_row.get("market_session") or item.get("market_session") or ""
    item["volume"] = quote_row.get("volume")
    item["quote_timestamp"] = quote_row.get("quote_timestamp") or item.get("quote_timestamp") or item.get("updated_at")
    item["source"] = quote_row.get("source") or "datos_directos"
    item["source_label"] = "FMP / Railway"
    item["source_note"] = "Cotizacion live tomada por proxy seguro; no toca cartera paper."
    item["status"] = "precio_live"

    if units > 0:
        market_value = units * price
        item["market_value"] = market_value
        item["current_value"] = market_value
        if cost_basis is not None:
            item["cost_basis"] = cost_basis
            item["unrealized_pnl"] = market_value - cost_basis
            item["unrealized_pnl_pct"] = ((market_value - cost_basis) / cost_basis * 100) if cost_basis else None
        item["daily_pnl"] = units * daily_change if daily_change is not None else None
    return True


def _recalculate_live_portfolio_summary(snapshot: dict) -> None:
    items = snapshot.get("items") if isinstance(snapshot.get("items"), list) else []
    investment_items = [item for item in items if isinstance(item, dict) and (_safe_num(item.get("units")) or 0) > 0]
    tracked_items = [item for item in items if isinstance(item, dict)]
    total_value = sum((_safe_num(item.get("market_value") or item.get("current_value")) or 0.0) for item in investment_items)
    total_cost = sum((_safe_num(item.get("cost_basis")) or 0.0) for item in investment_items)
    daily_pnl_values = [(_safe_num(item.get("daily_pnl")) or 0.0) for item in investment_items if _safe_num(item.get("daily_pnl")) is not None]
    daily_pnl = sum(daily_pnl_values) if daily_pnl_values else None
    total_unrealized = total_value - total_cost if total_cost or total_value else None
    total_unrealized_pct = (total_unrealized / total_cost * 100) if total_cost and total_unrealized is not None else None

    for item in investment_items:
        market_value = _safe_num(item.get("market_value") or item.get("current_value")) or 0.0
        item["weight_pct"] = (market_value / total_value * 100) if total_value else None

    top = max(investment_items, key=lambda item: _safe_num(item.get("market_value") or item.get("current_value")) or 0.0, default=None)
    summary = snapshot.setdefault("summary", {})
    portfolio = summary.setdefault("portfolio", {})
    patch = {
        "tracked_count": len(tracked_items),
        "investment_count": len(investment_items),
        "reference_count": sum(1 for item in tracked_items if _safe_num(item.get("current_price") or item.get("reference_price")) is not None),
        "unavailable_count": sum(1 for item in tracked_items if _safe_num(item.get("current_price") or item.get("reference_price")) is None),
        "total_value": total_value,
        "total_cost_basis": total_cost,
        "total_unrealized_pnl": total_unrealized,
        "total_unrealized_pnl_pct": total_unrealized_pct,
        "daily_pnl": daily_pnl,
        "daily_pnl_pct": (daily_pnl / total_value * 100) if daily_pnl is not None and total_value else None,
        "number_of_positions": len(investment_items),
        "watchlist_count": sum(1 for item in tracked_items if item.get("watchlist")),
        "top_concentration": {
            "ticker": top.get("ticker"),
            "weight_pct": top.get("weight_pct"),
        } if top else {},
    }
    summary.update(patch)
    portfolio.update(patch)
    summary["data_origin"] = f"{summary.get('data_origin') or 'local'}+live_proxy"
    summary["note"] = "Cartera local preservada; precios enriquecidos desde FMP/Railway cuando local no tiene keys."


def _enrich_portfolio_snapshot_with_live_quotes(snapshot: dict) -> dict:
    items = snapshot.get("items") if isinstance(snapshot.get("items"), list) else []
    touched = 0
    seen: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        ticker = str(item.get("ticker") or "").strip().upper()
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        needs_quote = _safe_num(item.get("current_price")) is None or (_safe_num(item.get("current_price")) or 0) <= 0 or str(item.get("source") or "").lower() in {"contingency", "sin_precio"}
        if not needs_quote:
            continue
        quote_row = _live_quote_for_snapshot_ticker(ticker)
        if quote_row and _merge_live_quote_into_snapshot_item(item, quote_row):
            touched += 1
    if touched:
        snapshot["live_proxy_enriched"] = True
        snapshot["live_proxy_enriched_count"] = touched
        _recalculate_live_portfolio_summary(snapshot)
    return snapshot


class DashboardRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, directory: str | None = None, **kwargs):
        super().__init__(*args, directory=directory or str(_DASHBOARD_DIR), **kwargs)

    def _write_json(self, payload_data: dict, status: HTTPStatus = HTTPStatus.OK) -> None:
        payload = json.dumps(payload_data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _read_json_body(self) -> dict:
        try:
            length = int(self.headers.get("Content-Length") or "0")
        except ValueError:
            length = 0
        if length <= 0:
            return {}
        try:
            raw = self.rfile.read(length).decode("utf-8")
            payload = json.loads(raw)
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _try_proxy_to_production(self, parsed, *, method: str, body: dict | None = None) -> bool:
        if not _local_live_sources_missing() or not _is_proxy_path(parsed.path, method):
            return False
        target = f"{_PRODUCTION_API_ORIGIN}{parsed.path}"
        if parsed.query:
            target = f"{target}?{parsed.query}"
        payload_bytes = b""
        headers = {
            "Accept": "application/json",
            "User-Agent": "GenesisLocalProxy/1.0",
        }
        if method == "POST":
            payload_bytes = json.dumps(body or {}).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"
        request = Request(target, data=payload_bytes if method == "POST" else None, headers=headers, method=method)
        try:
            proxy_timeout = 55 if parsed.path == "/api/genesis/analyze-image" else 6 if parsed.path == "/api/genesis/ask" else 8 if parsed.path == "/api/dashboard/source-health" else 10 if parsed.path == "/api/dashboard/genesis" else 5 if parsed.path in {"/api/dashboard/whales", "/api/dashboard/money-flow/causal", "/api/dashboard/money-flow/detection", "/api/dashboard/money-flow/jarvis"} else 18
            with urlopen(request, timeout=proxy_timeout) as response:
                data = response.read()
                status = int(getattr(response, "status", 200) or 200)
                content_type = response.headers.get("Content-Type", "application/json; charset=utf-8")
                if "json" in content_type:
                    data = _massage_proxy_payload(parsed.path, data, body=body)
        except HTTPError as exc:
            data = exc.read() or json.dumps({"ok": False, "message": "Railway devolvio error seguro."}).encode("utf-8")
            status = int(exc.code or 502)
            content_type = exc.headers.get("Content-Type", "application/json; charset=utf-8")
        except (TimeoutError, URLError, OSError):
            logging.getLogger("genesis.dashboard").warning("Production proxy unavailable for %s %s", method, parsed.path)
            return False
        self.send_response(status)
        self.send_header("Content-Type", content_type if "json" in content_type else "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
        return True

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        body = self._read_json_body()
        if parsed.path == "/api/genesis/analyze-image":
            body = _normalize_analyze_image_body(body)
        message = _genesis_message_from_body(body)
        panel_context = body.get("panel_context") if isinstance(body.get("panel_context"), dict) else None
        if parsed.path == "/api/genesis/ask" and _is_asset_genesis_prompt(message, panel_context):
            result = _local_asset_genesis_payload(body, message)
            self._write_json(result, HTTPStatus.OK if result.get("ok") else HTTPStatus.BAD_REQUEST)
            return
        if parsed.path == "/api/genesis/ask" and (
            _is_casual_genesis_prompt(message)
            or _is_market_genesis_prompt(message)
            or _is_news_genesis_prompt(message)
            or _is_memory_genesis_prompt(message)
            or _is_whale_genesis_prompt(message)
        ):
            if _is_whale_genesis_prompt(message):
                snapshot = _fast_whale_snapshot_for_prompt()
                result = _enrich_genesis_whale_payload(_whale_prompt_fallback_payload(message, snapshot))
                self._write_json(result, HTTPStatus.OK)
                return
            if (
                not _is_casual_genesis_prompt(message)
                and _local_live_sources_missing()
                and self._try_proxy_to_production(parsed, method="POST", body=body)
            ):
                return
            result = ask_genesis(
                message,
                context=str(body.get("context") or "general"),
                ticker="",
                panel_context=panel_context,
                conversation_id=str(body.get("conversation_id") or "default"),
            )
            result = _enrich_genesis_asset_quote(result)
            result = _enrich_genesis_trade_decision(result, message)
            result = _enrich_genesis_whale_payload(result)
            self._write_json(result, HTTPStatus.OK if result.get("ok") else HTTPStatus.BAD_REQUEST)
            return
        if self._try_proxy_to_production(parsed, method="POST", body=body):
            return

        if parsed.path == "/api/genesis/ask":
            result = ask_genesis(
                str(body.get("message") or body.get("question") or ""),
                context=str(body.get("context") or "general"),
                ticker=str(body.get("ticker") or ""),
                panel_context=panel_context,
                conversation_id=str(body.get("conversation_id") or "default"),
            )
            result = _enrich_genesis_asset_quote(result)
            result = _enrich_genesis_whale_payload(result)
            self._write_json(result, HTTPStatus.OK if result.get("ok") else HTTPStatus.BAD_REQUEST)
            return

        if parsed.path == "/api/genesis/analyze-image":
            result = analyze_chart_image(body)
            self._write_json(result, HTTPStatus.OK if result.get("ok") else HTTPStatus.BAD_REQUEST)
            return

        if parsed.path == "/api/genesis/memory/event":
            result = MemoryStore().save_event(
                str(body.get("event_type") or "event"),
                body.get("payload") if isinstance(body.get("payload"), dict) else {},
                source=str(body.get("source") or "api"),
                confidence=body.get("confidence") or "media",
            )
            self._write_json({"ok": True, "event": result})
            return

        if parsed.path in {"/api/dashboard/portfolio/watchlist", "/api/dashboard/portfolio/watchlist/add"}:
            result = add_dashboard_portfolio_ticker(str(body.get("ticker") or ""))
            status = HTTPStatus.OK if result.get("ok") else HTTPStatus.BAD_REQUEST
            self._write_json(result, status)
            return

        if parsed.path in {"/api/dashboard/portfolio/paper", "/api/dashboard/portfolio/paper-buy"}:
            result = simulate_dashboard_portfolio_purchase(
                str(body.get("ticker") or ""),
                units=body.get("units"),
                entry_price=body.get("entry_price"),
            )
            status = HTTPStatus.OK if result.get("ok") else HTTPStatus.BAD_REQUEST
            self._write_json(result, status)
            return

        if parsed.path == "/api/dashboard/portfolio/watchlist/remove":
            result = remove_dashboard_portfolio_ticker(str(body.get("ticker") or ""))
            status = HTTPStatus.OK if result.get("ok") else HTTPStatus.BAD_REQUEST
            self._write_json(result, status)
            return

        if parsed.path == "/api/dashboard/portfolio/paper-remove":
            result = remove_dashboard_portfolio_purchase(str(body.get("ticker") or ""))
            status = HTTPStatus.OK if result.get("ok") else HTTPStatus.BAD_REQUEST
            self._write_json(result, status)
            return

        self._write_json({"ok": False, "message": "Consulta no disponible."}, HTTPStatus.NOT_FOUND)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path in {"/api/dashboard/money-flow/causal", "/api/dashboard/money-flow/jarvis"}:
            if self._try_proxy_to_production(parsed, method="GET"):
                return

        if parsed.path in {"/api/dashboard/radar", "/api/dashboard/portfolio"}:
            payload_data = _enrich_portfolio_snapshot_with_live_quotes(get_dashboard_radar())
            self._write_json(payload_data, HTTPStatus.OK)
            return

        if parsed.path in {"/api/dashboard/asset/chart", "/api/dashboard/chart"} and _local_live_sources_missing():
            query = parse_qs(parsed.query)
            ticker = (query.get("ticker") or query.get("symbol") or [""])[0]
            timeframe = (query.get("range") or query.get("timeframe") or ["1Y"])[0]
            payload_data = _yahoo_asset_chart_payload(ticker, timeframe)
            if payload_data.get("ok"):
                self._write_json(payload_data, HTTPStatus.OK)
                return

        if parsed.path == "/api/dashboard/news":
            if self._try_proxy_to_production(parsed, method="GET"):
                return
            query = parse_qs(parsed.query)
            force_refresh = any(
                str(value).lower() in {"1", "true", "yes", "now"}
                for key in ("refresh", "force", "no_cache")
                for value in query.get(key, [])
            )
            payload = json.dumps(get_dashboard_news(force_refresh=force_refresh)).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path == "/api/dashboard/money-flow/detection":
            payload_data = _call_json_with_timeout(
                get_dashboard_money_flow_detection,
                4,
                {"ok": True, "items": [], "detection": {"items": []}, "source_status": {"status": "timeout", "provider_used": "local_fallback"}},
            )
            _massage_whales_payload(payload_data)
            payload = json.dumps(payload_data).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path == "/api/dashboard/money-flow/causal":
            payload_data = _call_json_with_timeout(
                get_dashboard_money_flow_causal,
                4,
                {"ok": True, "items": [], "causal": {"items": []}, "source_status": {"status": "timeout", "provider_used": "local_fallback"}},
            )
            _massage_whales_payload(payload_data)
            payload = json.dumps(payload_data).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if self._try_proxy_to_production(parsed, method="GET"):
            return

        if parsed.path == "/api/dashboard/health":
            payload = json.dumps(get_dashboard_health()).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path == "/api/dashboard/reliability":
            payload = json.dumps(get_dashboard_reliability()).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path == "/api/dashboard/executive-queue":
            payload = json.dumps(get_dashboard_executive_queue()).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path == "/api/dashboard/genesis":
            query = parse_qs(parsed.query)
            question = (query.get("q") or [""])[0]
            context = (query.get("context") or ["general"])[0]
            ticker = (query.get("ticker") or [""])[0]
            panel_context = (query.get("panel_context") or [""])[0]
            try:
                payload_data = get_dashboard_genesis(question, context=context, ticker=ticker, panel_context=panel_context)
            except Exception:
                logging.getLogger("genesis.dashboard").exception("DASHBOARD GENESIS fallback activated")
                payload_data = get_genesis_fallback_answer(
                    question,
                    context=context,
                    ticker=ticker,
                    panel_context=panel_context,
                    reason="snapshot_failure",
                )
            payload = json.dumps(payload_data).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path == "/api/dashboard/money-flow/model":
            payload = json.dumps(get_dashboard_money_flow_model()).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path == "/api/dashboard/money-flow/detection":
            payload_data = _call_json_with_timeout(
                get_dashboard_money_flow_detection,
                4,
                {"ok": True, "items": [], "detection": {"items": []}, "source_status": {"status": "timeout", "provider_used": "local_fallback"}},
            )
            _massage_whales_payload(payload_data)
            payload = json.dumps(payload_data).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path == "/api/dashboard/money-flow/causal":
            payload_data = _call_json_with_timeout(
                get_dashboard_money_flow_causal,
                4,
                {"ok": True, "items": [], "causal": {"items": []}, "source_status": {"status": "timeout", "provider_used": "local_fallback"}},
            )
            _massage_whales_payload(payload_data)
            payload = json.dumps(payload_data).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path == "/api/dashboard/money-flow/jarvis":
            question = (parse_qs(parsed.query).get("q") or [""])[0]
            payload_data = _call_json_with_timeout(
                lambda: get_dashboard_money_flow_jarvis(question),
                4,
                {"ok": True, "answer": "Genesis no inventa ballenas: fuente lenta, sigo con volumen vigilado disponible.", "items": []},
            )
            _massage_whales_payload(payload_data)
            payload = json.dumps(payload_data).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path in {"/api/dashboard/radar/drilldown", "/api/dashboard/portfolio/drilldown"}:
            ticker = (parse_qs(parsed.query).get("ticker") or [""])[0]
            payload = json.dumps(get_dashboard_radar_drilldown(ticker)).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path in {"/api/dashboard/asset/chart", "/api/dashboard/chart"}:
            query = parse_qs(parsed.query)
            ticker = (query.get("ticker") or [""])[0]
            timeframe = (query.get("range") or query.get("timeframe") or ["1Y"])[0]
            payload = json.dumps(get_dashboard_asset_chart(ticker, timeframe=timeframe)).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path == "/api/genesis/briefing":
            payload = json.dumps(ask_genesis("como va mi cartera", context="portfolio")).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path == "/api/genesis/memory/recent":
            query = parse_qs(parsed.query)
            event_type = (query.get("event_type") or [""])[0] or None
            limit = int((query.get("limit") or ["20"])[0] or 20)
            conversation_id = (query.get("conversation_id") or ["default"])[0] or "default"
            store = MemoryStore()
            payload = json.dumps(
                {
                    "ok": True,
                    "backend": store.backend,
                    "items": store.get_recent_events(limit, event_type),
                    "messages": store.get_recent_messages(conversation_id=conversation_id, limit=limit),
                    "conversations": store.list_conversations(limit),
                    "learned_context": store.get_learned_context(limit),
                    "tracked_entities": store.get_tracked_entities(limit),
                    "recent_topics": store.get_recent_topics(min(limit, 20)),
                    "asset_memory": store.get_asset_memory(limit=limit),
                    "signal_events": store.get_signal_events(limit=limit),
                    "news_events": store.get_news_events(limit=limit),
                    "decision_notes": store.get_decision_notes(limit=limit),
                    "hypothesis_log": store.get_hypotheses(limit=limit),
                    "outcome_tracking": store.get_outcome_tracking(limit=limit),
                    "durable_on_railway": store.backend == "postgres",
                }
            ).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path.startswith("/api/genesis/memory/ticker/"):
            ticker = parsed.path.rsplit("/", 1)[-1]
            store = MemoryStore()
            payload = json.dumps(
                {
                    "ok": True,
                    "backend": store.backend,
                    "ticker": ticker.upper(),
                    "market": store.get_market_memory(ticker),
                    "whales": store.get_whale_memory(ticker),
                    "alerts": store.get_alert_memory(ticker),
                    "learning": store.get_asset_learning_summary(ticker),
                }
            ).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path == "/api/dashboard/market/search":
            query = (parse_qs(parsed.query).get("q") or [""])[0]
            payload = json.dumps(_search_market_with_live_fallback(query)).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path == "/api/dashboard/alerts":
            payload = json.dumps(get_dashboard_alerts()).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path == "/api/dashboard/alerts/drilldown":
            alert_id = (parse_qs(parsed.query).get("alert_id") or [""])[0]
            payload = json.dumps(get_dashboard_alert_drilldown(alert_id)).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path == "/api/dashboard/whales":
            ticker = (parse_qs(parsed.query).get("ticker") or [""])[0]
            whales_payload = get_dashboard_whales(ticker)
            _massage_whales_payload(whales_payload)
            payload = json.dumps(whales_payload).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path == "/api/dashboard/fmp":
            payload = json.dumps(get_dashboard_fmp_dependencies()).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path == "/api/dashboard/source-health":
            payload_data = _call_json_with_timeout(
                get_dashboard_source_health,
                6,
                {
                    "ok": True,
                    "fmp": {"status": "timeout", "last_error_safe": "source health lento; no se exponen secretos"},
                    "rss_news": {"enabled": True, "status": "unknown"},
                    "cache": {"status": "available"},
                },
            )
            payload = json.dumps(payload_data).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path == "/api/dashboard/macro-activity":
            payload = json.dumps(get_dashboard_macro_activity()).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path in {"", "/"}:
            self.path = "/index.html"
        else:
            self.path = parsed.path
        return super().do_GET()

    def log_message(self, format: str, *args) -> None:
        logging.getLogger("genesis.dashboard").info("DASHBOARD HTTP | " + format, *args)


def _resolve_dashboard_host() -> str:
    configured_host = os.getenv("GENESIS_DASHBOARD_HOST", "").strip()
    if configured_host:
        return configured_host
    if os.getenv("PORT"):
        return "0.0.0.0"
    return "127.0.0.1"


def _resolve_dashboard_port() -> int:
    return int(os.getenv("PORT") or os.getenv("GENESIS_DASHBOARD_PORT", "8000"))


def run_dashboard_server(host: str = "127.0.0.1", port: int = 8000) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    handler = partial(DashboardRequestHandler, directory=str(_DASHBOARD_DIR))
    server = ThreadingHTTPServer((host, port), handler)
    logging.getLogger("genesis.dashboard").info("Dashboard shell listening on http://%s:%s", host, port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    run_dashboard_server(host=_resolve_dashboard_host(), port=_resolve_dashboard_port())
