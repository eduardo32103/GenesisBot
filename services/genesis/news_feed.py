from __future__ import annotations

import hashlib
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from app.settings import load_settings
from integrations.fmp.client import FmpClient
from services.genesis.ticker_parser import normalize_ticker

_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_NEWS_TTL_SECONDS = 15 * 60
_GLOBAL_TICKERS = ["SPY", "QQQ", "DIA", "NVDA", "AAPL", "MSFT", "TSLA", "META", "BTC-USD", "BZ=F"]


def get_recent_market_news(tickers: list[str] | None = None, *, limit: int = 12, max_age_days: int = 30) -> list[dict[str, Any]]:
    safe_tickers = _safe_tickers(tickers)
    cache_key = f"news:{','.join(safe_tickers)}:{int(limit)}:{int(max_age_days)}"
    now = time.monotonic()
    cached = _CACHE.get(cache_key)
    if cached and now - cached[0] <= _NEWS_TTL_SECONDS:
        return [dict(item, cache_hit=True) for item in cached[1]]

    started = time.monotonic()
    raw_news: list[dict[str, Any]] = []
    try:
        settings = load_settings()
        if getattr(settings, "fmp_api_key", "") and getattr(settings, "fmp_live_enabled", False):
            client = FmpClient(settings.fmp_api_key)
            focus = safe_tickers or _GLOBAL_TICKERS
            raw_news.extend(client.get_market_news(focus, limit=max(int(limit) * 2, 12)) or [])
            for ticker in focus[:8]:
                raw_news.extend(client.get_stock_news(ticker, limit=3) or [])
    except Exception:
        raw_news = raw_news or []

    normalized = _normalize_news(raw_news, safe_tickers, max_age_days=max_age_days)
    if not normalized:
        normalized = [_fallback_news_item(safe_tickers)]
    output = normalized[: max(1, int(limit or 12))]
    elapsed_ms = int((time.monotonic() - started) * 1000)
    for item in output:
        item["elapsed_ms"] = elapsed_ms
        item["cache_hit"] = False
    _CACHE[cache_key] = (now, output)
    return output


def _safe_tickers(tickers: list[str] | None) -> list[str]:
    output: list[str] = []
    for raw in tickers or []:
        ticker = normalize_ticker(raw)
        if ticker and ticker not in output:
            output.append(ticker)
    return output[:12]


def _normalize_news(raw_news: list[dict[str, Any]], focus_tickers: list[str], *, max_age_days: int) -> list[dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(max_age_days or 30)))
    seen_titles: set[str] = set()
    items: list[dict[str, Any]] = []
    focus = set(focus_tickers)
    for raw in raw_news:
        if not isinstance(raw, dict):
            continue
        title = str(raw.get("title") or raw.get("headline") or "").strip()
        if not title:
            continue
        title_key = " ".join(title.casefold().split())
        if title_key in seen_titles:
            continue
        published_at = _published_at(raw)
        if published_at and published_at < cutoff:
            continue
        seen_titles.add(title_key)
        tickers = _article_tickers(raw, focus)
        impact = _impact(title, raw.get("text") or raw.get("summary") or "", raw.get("sentiment") or raw.get("impact"))
        source = str(raw.get("site") or raw.get("publisher") or raw.get("source") or "FMP").strip()
        summary = _summary(raw, title)
        item = {
            "id": _news_id(title, raw.get("url") or raw.get("link") or ""),
            "title": title,
            "summary": summary,
            "source": source,
            "published_at": published_at.isoformat() if published_at else str(raw.get("publishedDate") or raw.get("date") or ""),
            "tickers": tickers,
            "assets": tickers,
            "impact": impact,
            "confidence": _confidence(raw, tickers, published_at),
            "image_url": raw.get("image") or raw.get("image_url") or raw.get("thumbnail") or raw.get("urlToImage") or "",
            "thumbnail": raw.get("image") or raw.get("image_url") or raw.get("thumbnail") or raw.get("urlToImage") or "",
            "url": raw.get("url") or raw.get("link") or "",
            "category": _category(raw, tickers),
            "genesis_takeaway": _takeaway(title, impact, tickers),
            "why_it_matters": _why_it_matters(impact, tickers),
        }
        items.append(item)
    return sorted(items, key=lambda item: (_relevance_score(item, focus), item.get("published_at") or ""), reverse=True)


def _published_at(raw: dict[str, Any]) -> datetime | None:
    value = raw.get("publishedDate") or raw.get("published_at") or raw.get("date") or raw.get("datetime")
    text = str(value or "").strip()
    if not text:
        return None
    if text.isdigit():
        try:
            return datetime.fromtimestamp(float(text), tz=timezone.utc)
        except Exception:
            return None
    for candidate in (text, text.replace("Z", "+00:00")):
        try:
            parsed = datetime.fromisoformat(candidate)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _article_tickers(raw: dict[str, Any], focus: set[str]) -> list[str]:
    values = raw.get("symbols") or raw.get("tickers") or []
    if isinstance(values, str):
        values = [values]
    candidates = [raw.get("symbol"), raw.get("ticker"), *values]
    tickers = []
    for value in candidates:
        ticker = normalize_ticker(value)
        if ticker and ticker not in tickers:
            tickers.append(ticker)
    if not tickers and focus:
        text = f"{raw.get('title') or ''} {raw.get('text') or ''} {raw.get('summary') or ''}".upper()
        for ticker in focus:
            if ticker in text and ticker not in tickers:
                tickers.append(ticker)
    return tickers[:5]


def _impact(title: str, body: str, explicit: object) -> str:
    raw = str(explicit or "").casefold()
    text = f"{title} {body}".casefold()
    if any(token in raw or token in text for token in ("surge", "rally", "rallies", "beats", "upgrade", "alcista", "bull", "sube", "gain", "lifts")):
        return "bullish"
    if any(token in raw or token in text for token in ("falls", "drop", "misses", "downgrade", "bajista", "bear", "cae", "risk", "lawsuit")):
        return "bearish"
    return "neutral"


def _summary(raw: dict[str, Any], title: str) -> str:
    text = str(raw.get("summary") or raw.get("text") or raw.get("content") or raw.get("snippet") or "").strip()
    if not text:
        text = title
    return " ".join(text.split())[:320]


def _category(raw: dict[str, Any], tickers: list[str]) -> str:
    text = f"{raw.get('category') or ''} {raw.get('title') or ''}".casefold()
    if any(token in text for token in ("oil", "crude", "brent", "commodity", "oro", "gold")):
        return "commodity"
    if any(token in text for token in ("bitcoin", "crypto", "btc", "ethereum")):
        return "crypto"
    if any(token in text for token in ("fed", "inflation", "rates", "macro", "gdp", "cpi")):
        return "macro"
    if any(token in text for token in ("war", "tariff", "trump", "china", "geopolit")):
        return "geopolitics"
    if any(token in text for token in ("earnings", "revenue", "guidance")):
        return "earnings"
    return "ticker" if tickers else "macro"


def _confidence(raw: dict[str, Any], tickers: list[str], published_at: datetime | None) -> str:
    score = 0
    if raw.get("url") or raw.get("link"):
        score += 1
    if tickers:
        score += 1
    if published_at:
        score += 1
    return "high" if score >= 3 else "medium" if score >= 2 else "low"


def _takeaway(title: str, impact: str, tickers: list[str]) -> str:
    asset = ", ".join(tickers[:3]) or "mercado"
    if impact == "bullish":
        return f"Puede apoyar a {asset}, pero Genesis exige confirmacion de precio y volumen."
    if impact == "bearish":
        return f"Puede presionar a {asset}; conviene vigilar soporte y reaccion del volumen."
    return f"Contexto relevante para {asset}; Genesis lo trata como catalizador neutral hasta confirmar reaccion."


def _why_it_matters(impact: str, tickers: list[str]) -> str:
    asset = ", ".join(tickers[:3]) or "tus activos"
    if impact == "bullish":
        return f"Puede mejorar apetito por {asset} si el mercado confirma continuidad."
    if impact == "bearish":
        return f"Puede elevar riesgo de volatilidad en {asset}."
    return f"Aporta contexto para decidir si conviene esperar, vigilar o actuar con cautela."


def _relevance_score(item: dict[str, Any], focus: set[str]) -> int:
    tickers = set(item.get("tickers") or [])
    score = 2 if tickers & focus else 0
    if item.get("published_at"):
        score += 1
    if item.get("image_url"):
        score += 1
    return score


def _news_id(title: str, url: object) -> str:
    raw = f"{title}|{url}"
    return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()[:16]


def _fallback_news_item(focus_tickers: list[str]) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    tickers = focus_tickers[:5]
    return {
        "id": "fallback-market-context",
        "title": "Genesis mantiene vigilancia de mercado",
        "summary": "No hay titulares externos recientes confirmados por la fuente activa; Genesis sigue usando precios, alertas, cartera y seguimiento.",
        "source": "Genesis",
        "published_at": now,
        "tickers": tickers,
        "assets": tickers,
        "impact": "neutral",
        "confidence": "low",
        "image_url": "",
        "thumbnail": "",
        "url": "",
        "category": "macro",
        "genesis_takeaway": "Sin noticia externa confirmada; operar solo con precio, volumen y niveles.",
        "why_it_matters": "Evita una pantalla vacia sin inventar titulares ni fuentes.",
        "elapsed_ms": 0,
        "cache_hit": False,
    }
