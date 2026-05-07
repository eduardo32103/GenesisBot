from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from app.settings import load_settings
from integrations.fmp.client import FmpClient
from services.genesis.memory_store import MemoryStore
from services.genesis.news_feed import get_news_source_status, get_recent_market_news
from services.genesis.weather_tool import get_weather_answer
from services.portfolio.portfolio_store import PortfolioStore


def get_source_health() -> dict[str, Any]:
    settings = load_settings()
    started = time.monotonic()
    fmp = _fmp_health(settings)
    openai = {
        "key_configured": bool(settings.openai_api_key),
        "llm_enabled": bool(settings.genesis_llm_enabled),
        "model": settings.genesis_llm_model,
        "vision_enabled": bool(settings.genesis_vision_enabled),
    }
    database = _database_health(settings)
    weather = _weather_health()
    rss = _rss_health()
    return {
        "ok": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_ms": int((time.monotonic() - started) * 1000),
        "fmp": fmp,
        "openai": openai,
        "database": database,
        "weather": weather,
        "rss_news": rss,
        "policy": "Diagnostico seguro: no devuelve API keys, tokens ni parametros secretos.",
    }


def _fmp_health(settings: Any) -> dict[str, Any]:
    started = time.monotonic()
    health = {
        "key_configured": bool(getattr(settings, "fmp_api_key", "")),
        "live_enabled": bool(getattr(settings, "fmp_live_enabled", False)),
        "quote_ok": False,
        "quote_sample_count": 0,
        "news_ok": False,
        "news_count": 0,
        "historical_ok": False,
        "historical_points_sample": 0,
        "institutional_ok": False,
        "insider_ok": False,
        "last_error_safe": "",
        "elapsed_ms": 0,
        "source_status": {},
    }
    if not health["key_configured"] or not health["live_enabled"]:
        health["last_error_safe"] = "FMP no esta habilitado en entorno."
        return health
    try:
        client = FmpClient(settings.fmp_api_key)
        quote = client.get_quote("SPY") or {}
        health["quote_ok"] = bool(quote.get("price"))
        health["quote_sample_count"] = 1 if health["quote_ok"] else 0
        history = client.get_historical_eod("SPY", limit=10) or []
        health["historical_ok"] = bool(history)
        health["historical_points_sample"] = len(history)
        news = get_recent_market_news(["SPY", "NVDA", "BTC-USD", "BZ=F"], limit=8, max_age_days=30)
        health["news_ok"] = bool(news)
        health["news_count"] = len(news)
        activity = client.get_smart_money_activity("NVDA", limit=2) or []
        health["institutional_ok"] = bool(activity)
        health["insider_ok"] = any(str(row.get("source") or "").casefold().startswith("insider") for row in activity)
        health["source_status"] = get_news_source_status()
        if not health["quote_ok"]:
            health["last_error_safe"] = client.get_last_error("SPY") or "quote empty"
    except Exception:
        health["last_error_safe"] = "fmp health unavailable"
    health["elapsed_ms"] = int((time.monotonic() - started) * 1000)
    return health


def _database_health(settings: Any) -> dict[str, Any]:
    started = time.monotonic()
    memory_ok = False
    memory_backend = "unavailable"
    try:
        memory = MemoryStore()
        memory_backend = memory.backend
        memory_ok = bool(memory.get_memory_summary())
    except Exception:
        memory_ok = False
    store = PortfolioStore()
    try:
        portfolio_store = store.status()
    finally:
        store.close()
    return {
        "database_url_configured": bool(settings.database_url),
        "memory_ok": memory_ok,
        "memory_backend": memory_backend,
        "portfolio_store": portfolio_store,
        "elapsed_ms": int((time.monotonic() - started) * 1000),
    }


def _weather_health() -> dict[str, Any]:
    started = time.monotonic()
    ok = False
    last_error_safe = ""
    try:
        payload = get_weather_answer("clima en Los Mochis")
        ok = bool(payload.get("ok")) and str(payload.get("source") or "").startswith(("open_meteo", "openweather"))
        if not ok:
            last_error_safe = str(payload.get("source") or "weather empty")
    except Exception:
        last_error_safe = "weather health unavailable"
    return {
        "open_meteo_ok": ok,
        "last_error_safe": last_error_safe,
        "elapsed_ms": int((time.monotonic() - started) * 1000),
    }


def _rss_health() -> dict[str, Any]:
    status = get_news_source_status()
    rss = status.get("rss_news") if isinstance(status, dict) else {}
    return {
        "enabled": True,
        "last_fetch_count": int((rss or {}).get("count") or 0),
        "last_error_safe": str((rss or {}).get("last_error_safe") or ""),
        "elapsed_ms": int((rss or {}).get("elapsed_ms") or 0),
        "cache_hit": bool((rss or {}).get("cache_hit")),
        "status": str((rss or {}).get("status") or "not_checked"),
    }
