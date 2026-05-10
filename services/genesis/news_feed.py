from __future__ import annotations

import hashlib
import html
import re
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import quote_plus, urljoin
from xml.etree import ElementTree

import requests

from app.settings import load_settings
from integrations.fmp.client import FmpClient
from services.genesis.ticker_parser import normalize_ticker

_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_OG_CACHE: dict[str, tuple[float, str]] = {}
_SOURCE_STATUS: dict[str, Any] = {}
_NEWS_TTL_SECONDS = 15 * 60
_OG_TTL_SECONDS = 30 * 60
_GLOBAL_TICKERS = ["SPY", "QQQ", "DIA", "NVDA", "AAPL", "MSFT", "TSLA", "META", "BTC-USD", "BZ=F"]
_RSS_TIMEOUT_SECONDS = 1.5
_MAX_OG_IMAGES_PER_BATCH = 8
_CATEGORY_PHOTOS = {
    "market": "https://images.unsplash.com/photo-1611974789855-9c2a0a7236a3?auto=format&fit=crop&w=640&q=80",
    "macro": "https://images.unsplash.com/photo-1526304640581-d334cdbbf45e?auto=format&fit=crop&w=640&q=80",
    "crypto": "https://images.unsplash.com/photo-1518546305927-5a555bb7020d?auto=format&fit=crop&w=640&q=80",
    "commodity": "https://images.unsplash.com/photo-1473341304170-971dccb5ac1e?auto=format&fit=crop&w=640&q=80",
    "geopolitics": "https://images.unsplash.com/photo-1529107386315-e1a2ed48a620?auto=format&fit=crop&w=640&q=80",
    "earnings": "https://images.unsplash.com/photo-1554224155-6726b3ff858f?auto=format&fit=crop&w=640&q=80",
    "tech": "https://images.unsplash.com/photo-1518770660439-4636190af475?auto=format&fit=crop&w=640&q=80",
    "gold": "https://images.unsplash.com/photo-1610375461246-83df859d849d?auto=format&fit=crop&w=640&q=80",
}
_RSS_QUERIES = [
    "market today",
    "stock market today",
    "stock market",
    "S&P 500",
    "Nasdaq",
    "Bitcoin BTC",
    "oil prices",
    "Brent crude oil",
    "gold market",
    "Nvidia",
    "Microsoft",
]


def get_recent_market_news(
    tickers: list[str] | None = None,
    *,
    limit: int = 12,
    max_age_days: int = 30,
    force_refresh: bool = False,
) -> list[dict[str, Any]]:
    safe_tickers = _safe_tickers(tickers)
    cache_key = f"news:{','.join(safe_tickers)}:{int(limit)}:{int(max_age_days)}"
    now = time.monotonic()
    cached = _CACHE.get(cache_key)
    if cached and not force_refresh and now - cached[0] <= _NEWS_TTL_SECONDS:
        _set_source_status("fmp_market_news", "ok", elapsed_ms=0, cache_hit=True, count=len(cached[1]))
        return [dict(item, cache_hit=True) for item in cached[1]]

    started = time.monotonic()
    raw_news: list[dict[str, Any]] = []
    status = "missing_env"
    safe_error = ""
    try:
        settings = load_settings()
        if getattr(settings, "fmp_api_key", "") and getattr(settings, "fmp_live_enabled", False):
            status = "empty"
            client = FmpClient(settings.fmp_api_key)
            focus = safe_tickers or _GLOBAL_TICKERS
            raw_news.extend(client.get_market_news(focus, limit=max(int(limit) * 2, 12)) or [])
            for ticker in focus[:8]:
                raw_news.extend(client.get_stock_news(ticker, limit=3) or [])
            status = "ok" if raw_news else "empty"
    except Exception:
        status = "unavailable"
        safe_error = "news source unavailable"
        raw_news = raw_news or []

    rss_started = time.monotonic()
    rss_items: list[dict[str, Any]] = []
    if len(raw_news) < max(6, int(limit or 12)):
        try:
            focus = safe_tickers or _GLOBAL_TICKERS
            rss_items = _fetch_public_rss_news(focus, limit=max(int(limit or 12) * 2, 12))
            raw_news.extend(rss_items)
            _set_source_status(
                "rss_news",
                "ok" if rss_items else "empty",
                elapsed_ms=int((time.monotonic() - rss_started) * 1000),
                cache_hit=False,
                count=len(rss_items),
            )
        except Exception:
            _set_source_status(
                "rss_news",
                "unavailable",
                elapsed_ms=int((time.monotonic() - rss_started) * 1000),
                cache_hit=False,
                count=0,
                last_error_safe="rss source unavailable",
            )

    normalized = _normalize_news(raw_news, safe_tickers, max_age_days=max_age_days)
    fallback_only = False
    if not normalized:
        normalized = [_fallback_news_item(safe_tickers)]
        fallback_only = True
    output = normalized[: max(1, int(limit or 12))]
    elapsed_ms = int((time.monotonic() - started) * 1000)
    for item in output:
        item["elapsed_ms"] = elapsed_ms
        item["cache_hit"] = False
        item["source_status"] = status
    if not fallback_only:
        _CACHE[cache_key] = (now, output)
    _set_source_status("fmp_market_news", status, elapsed_ms=elapsed_ms, cache_hit=False, count=len(output), last_error_safe=safe_error)
    return output


def get_news_source_status() -> dict[str, Any]:
    return dict(_SOURCE_STATUS)


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
    og_attempts = 0
    for raw in raw_news:
        if not isinstance(raw, dict):
            continue
        title = _strip_html(raw.get("title") or raw.get("headline") or "")
        if not title:
            continue
        if _is_bad_news_title(title):
            continue
        title_key = _dedupe_key(title)
        if title_key in seen_titles:
            continue
        published_at = _published_at(raw)
        if published_at and published_at < cutoff:
            continue
        seen_titles.add(title_key)
        tickers = _article_tickers(raw, focus)
        impact = _impact(title, raw.get("text") or raw.get("summary") or "", raw.get("sentiment") or raw.get("impact"))
        source = _strip_html(raw.get("site") or raw.get("publisher") or raw.get("source") or "FMP")
        summary = _summary(raw, title)
        url = str(raw.get("url") or raw.get("link") or "").strip()
        image_url = str(raw.get("image") or raw.get("image_url") or raw.get("thumbnail") or raw.get("urlToImage") or "").strip()
        if not image_url and url and og_attempts < _MAX_OG_IMAGES_PER_BATCH:
            og_attempts += 1
            image_url = _fetch_og_image(url)
        category = _category(raw, tickers)
        title_es = _spanish_title(title, tickers, category)
        summary_es = _spanish_summary(summary)
        has_source_image = bool(image_url)
        if not image_url:
            image_url = _category_photo_url(title, category, tickers)
        published_ts = _published_ts(published_at)
        age_seconds = _news_age_seconds(published_at)
        item = {
            "id": _news_id(title, source, published_at.isoformat() if published_at else raw.get("publishedDate") or raw.get("date") or "", url),
            "title": title_es,
            "title_es": title_es,
            "original_title": title,
            "summary": summary_es,
            "summary_es": summary_es,
            "original_summary": summary,
            "source": source,
            "published_at": published_at.isoformat() if published_at else str(raw.get("publishedDate") or raw.get("date") or ""),
            "published_ts": published_ts,
            "age_hours": round(age_seconds / 3600, 2) if age_seconds is not None else None,
            "recency_bucket": _recency_bucket(published_at),
            "relative_time": _relative_time(published_at),
            "tickers": tickers,
            "tickers_affected": tickers,
            "asset_names_affected": [_asset_name(ticker) for ticker in tickers],
            "assets": tickers,
            "impact": impact,
            "sentiment": impact,
            "tone": impact,
            "confidence": _confidence(raw, tickers, published_at),
            "image_url": image_url,
            "thumbnail_url": image_url,
            "thumbnail": image_url,
            "url": url,
            "category": category,
            "genesis_takeaway": _takeaway(title_es, impact, tickers),
            "genesis_takeaway_es": _takeaway(title_es, impact, tickers),
            "why_it_matters": _why_it_matters(impact, tickers),
            "why_it_matters_es": _why_it_matters(impact, tickers),
            "what_to_watch_es": _watch_for_news(impact, tickers),
            "watch_points": [_watch_for_news(impact, tickers)],
            "language": "es",
            "image_kind": "real" if has_source_image else "related_photo",
            "provider": raw.get("provider") or raw.get("source_type") or ("fmp" if source == "FMP" else "rss"),
        }
        recency = _recency_score(published_at)
        relevance = _relevance_score(item, focus)
        item["recency_score"] = recency
        item["relevance_score"] = relevance
        item["is_important"] = _is_important(item, relevance, recency)
        item["placeholder_key"] = item["category"]
        item["is_latest"] = recency >= 1
        item["risk"] = _risk_for_impact(impact)
        item["watch"] = _watch_for_news(impact, tickers)
        items.append(item)
    return sorted(
        items,
        key=lambda item: (
            item.get("recency_score") or 0,
            item.get("published_ts") or 0,
            item.get("is_important") is True,
            item.get("relevance_score") or 0,
        ),
        reverse=True,
    )


def _fetch_public_rss_news(focus_tickers: list[str], *, limit: int) -> list[dict[str, Any]]:
    queries = _rss_queries(focus_tickers)
    feeds = _rss_urls(queries, focus_tickers)[:8]
    output: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    try:
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = [executor.submit(_fetch_rss_feed, feed) for feed in feeds]
            for future in as_completed(futures, timeout=3):
                if len(output) >= limit:
                    break
                for item in future.result() or []:
                    url = str(item.get("url") or item.get("link") or "").strip()
                    key = url or _dedupe_key(item.get("title") or "")
                    if key in seen_urls:
                        continue
                    seen_urls.add(key)
                    output.append(item)
                    if len(output) >= limit:
                        break
    except TimeoutError:
        pass
    return output


def _rss_queries(focus_tickers: list[str]) -> list[str]:
    queries: list[str] = []
    for query in _RSS_QUERIES:
        if query not in queries:
            queries.append(query)
    for ticker in focus_tickers[:10]:
        label = {
            "BTC-USD": "Bitcoin BTC",
            "BZ=F": "Brent crude oil",
            "GC=F": "gold futures",
        }.get(ticker, ticker)
        if label not in queries:
            queries.append(label)
    return queries[:16]


def _rss_urls(queries: list[str], focus_tickers: list[str]) -> list[str]:
    urls: list[str] = []
    for query in queries:
        safe = quote_plus(f"{query} finance market when:30d")
        urls.append(f"https://news.google.com/rss/search?q={safe}&hl=en-US&gl=US&ceid=US:en")
    for ticker in focus_tickers[:8]:
        if ticker.endswith("-USD") or ticker in {"BZ=F", "GC=F"}:
            continue
        urls.append(f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={quote_plus(ticker)}&region=US&lang=en-US")
    urls.extend(
        [
            "https://feeds.content.dowjones.io/public/rss/mw_topstories",
            "https://www.cnbc.com/id/100003114/device/rss/rss.html",
            "https://www.reutersagency.com/feed/?best-topics=business-finance&post_type=best",
            "https://www.coindesk.com/arc/outboundfeeds/rss/",
            "https://cointelegraph.com/rss",
        ]
    )
    return urls


def _fetch_rss_feed(url: str) -> list[dict[str, Any]]:
    try:
        response = requests.get(url, timeout=_RSS_TIMEOUT_SECONDS, headers={"User-Agent": "GenesisBot/1.0"})
        if response.status_code != 200 or not response.content:
            return []
        root = ElementTree.fromstring(response.content[:1_500_000])
    except Exception:
        return []

    rows: list[dict[str, Any]] = []
    for item in root.findall(".//item")[:12]:
        title = _node_text(item, "title")
        if not title:
            continue
        link = _node_text(item, "link")
        description = _node_text(item, "description")
        published = _node_text(item, "pubDate") or _node_text(item, "published") or _node_text(item, "updated")
        source = _node_text(item, "source") or _source_from_url(url)
        image = _rss_image(item)
        rows.append(
            {
                "title": title,
                "summary": description,
                "text": description,
                "source": source,
                "publishedDate": _rss_date(published),
                "url": link,
                "link": link,
                "image_url": image,
                "thumbnail": image,
                "category": _category({"title": f"{title} {description}", "category": source}, []),
                "source_type": "rss",
            }
        )
    return rows


def _node_text(node: ElementTree.Element, tag: str) -> str:
    child = node.find(tag)
    if child is None:
        for candidate in node:
            if candidate.tag.endswith(tag):
                child = candidate
                break
    return _strip_html(child.text if child is not None else "")


def _rss_image(item: ElementTree.Element) -> str:
    for child in item.iter():
        tag = str(child.tag or "").casefold()
        if tag.endswith("content") or tag.endswith("thumbnail"):
            url = child.attrib.get("url")
            if url:
                return str(url).strip()
        if tag.endswith("enclosure"):
            url = child.attrib.get("url")
            mime = str(child.attrib.get("type") or "")
            if url and ("image" in mime or not mime):
                return str(url).strip()
    return ""


def _rss_date(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        parsed = parsedate_to_datetime(text)
        if parsed:
            parsed = parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).isoformat()
    except Exception:
        pass
    return text


def _source_from_url(url: str) -> str:
    text = str(url or "")
    if "yahoo" in text:
        return "Yahoo Finance"
    if "google" in text:
        return "Google News"
    if "cnbc" in text:
        return "CNBC"
    if "marketwatch" in text or "dowjones" in text:
        return "MarketWatch"
    if "coindesk" in text:
        return "CoinDesk"
    if "cointelegraph" in text:
        return "CoinTelegraph"
    return "RSS"


def _fetch_og_image(url: str) -> str:
    text = str(url or "").strip()
    if not text.startswith(("http://", "https://")):
        return ""
    now = time.monotonic()
    cached = _OG_CACHE.get(text)
    if cached and now - cached[0] <= _OG_TTL_SECONDS:
        return cached[1]
    image = ""
    try:
        response = requests.get(text, timeout=1, headers={"User-Agent": "GenesisBot/1.0"})
        if response.status_code == 200:
            body = response.text[:250_000]
            match = re.search(r'<meta[^>]+(?:property|name)=["\'](?:og:image|twitter:image)["\'][^>]+content=["\']([^"\']+)["\']', body, re.I)
            if not match:
                match = re.search(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+(?:property|name)=["\'](?:og:image|twitter:image)["\']', body, re.I)
            image = html.unescape(match.group(1).strip()) if match else ""
            if image and image.startswith("//"):
                image = f"https:{image}"
            elif image and not image.startswith(("http://", "https://")):
                image = urljoin(text, image)
    except Exception:
        image = ""
    _OG_CACHE[text] = (now, image)
    return image


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
        parsed = parsedate_to_datetime(text)
        if parsed:
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
            aliases = {ticker}
            if ticker == "BTC-USD":
                aliases.update({"BTC", "BITCOIN"})
            if ticker == "BZ=F":
                aliases.update({"BRENT", "CRUDE", "OIL"})
            if ticker == "GC=F":
                aliases.update({"GOLD", "ORO"})
            if any(alias in text for alias in aliases) and ticker not in tickers:
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
    text = _strip_html(raw.get("summary") or raw.get("text") or raw.get("content") or raw.get("snippet") or "")
    if not text:
        text = title
    return " ".join(text.split())[:320]


def _spanish_title(title: str, tickers: list[str], category: str) -> str:
    text = _strip_html(title)
    text = re.sub(r"\s+-\s+(Reuters|CNBC|MarketWatch|Yahoo Finance|CoinDesk|CoinTelegraph|WSJ|Wall Street Journal|The Wall Street Journal|Investing News Network|Fortune|AP News|CBS News|LancasterOnline)\s*$", "", text, flags=re.I)
    replacements = (
        (r"\bThe Minister of Finance of Chile Jorge Quiroz Rings the Nasdaq Stock Market Closing Bell\b", "El ministro de Finanzas de Chile Jorge Quiroz toca la campana de cierre del Nasdaq"),
        (r"\bStock Market Closing Bell\b", "campana de cierre del mercado accionario"),
        (r"\bClosing Bell\b", "campana de cierre"),
        (r"\bStock Market\b", "mercado accionario"),
        (r"\bToday's Crypto News\b", "Noticias cripto de hoy"),
        (r"\bCrypto News\b", "Noticias cripto"),
        (r"\bCoinbase Cuts Jobs\b", "Coinbase recorta empleos"),
        (r"\bStrategy's Bitcoin Shift\b", "giro de Strategy hacia Bitcoin"),
        (r"\bending May\b", "cerrando mayo"),
        (r"\bwould confirm\b", "confirmaria"),
        (r"\bnew bull market\b", "nuevo mercado alcista"),
        (r"\bbull market\b", "mercado alcista"),
        (r"\bTom Lee says\b", "dice Tom Lee"),
        (r"\bReclaims\b", "recupera"),
        (r"\bstocks fall from their records\b", "acciones caen desde maximos"),
        (r"\bfall from their records\b", "caen desde maximos"),
        (r"\bfrom their records\b", "desde maximos"),
        (r"\byo-yo\b", "oscilan"),
        (r"\bremind the market that\b", "recuerdan al mercado que"),
        (r"\bstill trade\b", "todavia cotizan"),
        (r"\bFundamentals\b", "fundamentales"),
        (r"\bbut\b", "pero"),
        (r"\bStock market today\b", "Mercado hoy"),
        (r"\bmarket today\b", "Mercado hoy"),
        (r"\bMarkets Rally\b", "mercados suben"),
        (r"\bPrices Plunge\b", "precios caen fuerte"),
        (r"\bCurrent price of\b", "Precio actual de"),
        (r"\bJumps Back Near\b", "vuelve cerca de"),
        (r"\bHopes for\b", "expectativas de"),
        (r"\bselling pressure\b", "presion vendedora"),
        (r"\bmillion-an-hour\b", "millones por hora"),
        (r"\bhit by\b", "afectado por"),
        (r"\babove\b", "por encima de"),
        (r"\bNear\b", "cerca de"),
        (r"\bHopes\b", "expectativas"),
        (r"\bShips\b", "buques"),
        (r"\bU\.A\.E\.\b", "EAU"),
        (r"\bOil futures Rise\b", "futuros del petroleo suben"),
        (r"\bOil Futures Rise\b", "futuros del petroleo suben"),
        (r"\bOil prices\b", "precios del petroleo"),
        (r"\bOil\b", "petroleo"),
        (r"\bfinancial markets\b", "mercados financieros"),
        (r"\bpredicted\b", "estimados"),
        (r"\bRising\b", "aumento de"),
        (r"\brising\b", "aumento de"),
        (r"\bDip\b", "ceden"),
        (r"\bdip\b", "ceden"),
        (r"\bConcerns\b", "preocupacion"),
        (r"\bconcerns\b", "preocupacion"),
        (r"\bRise\b", "suben"),
        (r"\brise\b", "suben"),
        (r"\bFires\b", "dispara"),
        (r"\bfires\b", "dispara"),
        (r"\bUS\b", "EE.UU."),
        (r"\bSlip\b", "ceden"),
        (r"\bslip\b", "ceden"),
        (r"\bJitters Build\b", "aumenta la inquietud"),
        (r"\bjitters build\b", "aumenta la inquietud"),
        (r"\bAnd\b", "y"),
        (r"\band\b", "y"),
        (r"\bby\b", "por"),
        (r"\bApril\b", "abril"),
        (r"\bMay\b", "mayo"),
        (r"\bJune\b", "junio"),
        (r"\bMarkets\b", "mercados"),
        (r"\bMarket\b", "mercado"),
        (r"\bWhy\b", "Por qué"),
        (r"\bpredicted by financial markets\b", "estimados por los mercados financieros"),
        (r"\bare missing the mark\b", "fallan el objetivo"),
        (r"\bis missing the mark\b", "falla el objetivo"),
        (r"\bwhat to watch\b", "qué vigilar"),
        (r"\bPlunge\b", "caen fuerte"),
        (r"\bplunge\b", "caen fuerte"),
        (r"\bIran Ceasefire\b", "alto el fuego en Irán"),
        (r"\bCeasefire\b", "alto el fuego"),
        (r"\bstocks\b", "acciones"),
        (r"\bstock\b", "acción"),
        (r"\bfutures\b", "futuros"),
        (r"\boil prices\b", "precios del petróleo"),
        (r"\bcrude oil\b", "petróleo crudo"),
        (r"\bBrent crude\b", "Brent"),
        (r"\bBitcoin\b", "Bitcoin"),
        (r"\bgold\b", "oro"),
        (r"\binflation\b", "inflación"),
        (r"\brates\b", "tasas"),
        (r"\bFed\b", "Fed"),
        (r"\btariffs\b", "aranceles"),
        (r"\bearnings\b", "resultados"),
        (r"\brevenue\b", "ingresos"),
        (r"\bguidance\b", "guía"),
        (r"\bdemand\b", "demanda"),
        (r"\bAI\b", "IA"),
        (r"\brallies\b", "sube"),
        (r"\brally\b", "sube"),
        (r"\brises\b", "sube"),
        (r"\bgains\b", "gana"),
        (r"\bsurges\b", "salta"),
        (r"\bfalls\b", "cae"),
        (r"\bdrops\b", "retrocede"),
        (r"\bslips\b", "cede"),
        (r"\bafter\b", "tras"),
        (r"\bas\b", "mientras"),
        (r"\bfor\b", "para"),
        (r"\bAt\b", "a"),
        (r"\bon\b", "por"),
        (r"\bamid\b", "en medio de"),
        (r"\bset to\b", "listo para"),
        (r"\bhits\b", "toca"),
        (r"\bslump\b", "caída"),
        (r"\bupdate\b", "actualización"),
        (r"\bwarns\b", "advierte"),
        (r"\bbeats\b", "supera expectativas"),
        (r"\bmisses\b", "decepciona"),
        (r"\boutlook\b", "perspectiva"),
        (r"\bshares\b", "acciones"),
        (r"\bETF\b", "ETF"),
    )
    for pattern, replacement in replacements:
        text = re.sub(pattern, replacement, text, flags=re.I)
    text = " ".join(text.split())
    if not text:
        asset = ", ".join(tickers[:2]) or category
        return f"Noticia relevante para {asset}"
    return text[:180]


def _spanish_summary(summary: str) -> str:
    text = _strip_html(summary)
    replacements = (
        (r"\bThe Minister of Finance of Chile Jorge Quiroz Rings the Nasdaq Stock Market Closing Bell\b", "El ministro de Finanzas de Chile Jorge Quiroz toca la campana de cierre del Nasdaq"),
        (r"\bStock Market Closing Bell\b", "campana de cierre del mercado accionario"),
        (r"\bClosing Bell\b", "campana de cierre"),
        (r"\bstock market\b", "mercado accionario"),
        (r"\bmarket\b", "mercado"),
        (r"\bstocks\b", "acciones"),
        (r"\binvestors\b", "inversionistas"),
        (r"\btraders\b", "operadores"),
        (r"\bdemand\b", "demanda"),
        (r"\bsupply\b", "oferta"),
        (r"\brisk\b", "riesgo"),
        (r"\brates\b", "tasas"),
        (r"\binflation\b", "inflación"),
        (r"\boil\b", "petróleo"),
        (r"\bgold\b", "oro"),
        (r"\bBitcoin\b", "Bitcoin"),
        (r"\bAI\b", "IA"),
    )
    for pattern, replacement in replacements:
        text = re.sub(pattern, replacement, text, flags=re.I)
    return " ".join(text.split())[:320] or "Resumen no disponible; revisar la fuente original."


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


def _relative_time(published_at: datetime | None) -> str:
    if not published_at:
        return "fecha no confirmada"
    delta = datetime.now(timezone.utc) - published_at
    seconds = max(int(delta.total_seconds()), 0)
    if seconds < 3600:
        return f"hace {max(seconds // 60, 1)} min"
    if seconds < 86_400:
        return f"hace {seconds // 3600} h"
    return f"hace {seconds // 86_400} d"


def _published_ts(published_at: datetime | None) -> int:
    if not published_at:
        return 0
    return int(published_at.timestamp())


def _news_age_seconds(published_at: datetime | None) -> int | None:
    if not published_at:
        return None
    return max(int((datetime.now(timezone.utc) - published_at).total_seconds()), 0)


def _recency_bucket(published_at: datetime | None) -> str:
    seconds = _news_age_seconds(published_at)
    if seconds is None:
        return "unknown"
    if seconds <= 86_400:
        return "24h"
    if seconds <= 7 * 86_400:
        return "7d"
    if seconds <= 30 * 86_400:
        return "30d"
    return "old"


def _asset_name(ticker: str) -> str:
    return {
        "BZ=F": "Brent Crude Oil",
        "BTC-USD": "Bitcoin",
        "GC=F": "Gold",
        "SPY": "S&P 500 ETF",
        "QQQ": "Nasdaq 100 ETF",
    }.get(str(ticker or "").upper(), str(ticker or "").upper())


def _asset_names_for_copy(tickers: list[str]) -> str:
    names: list[str] = []
    for ticker in tickers[:3]:
        name = _asset_name(ticker)
        if name and name not in names:
            names.append(name)
    return ", ".join(names)


def _category_photo_url(title: str, category: str, tickers: list[str]) -> str:
    text = f"{title} {category} {' '.join(tickers)}".casefold()
    if "trump" in text or "iran" in text or "geopolit" in text:
        return _CATEGORY_PHOTOS["geopolitics"]
    elif "bitcoin" in text or "btc" in text or "crypto" in text:
        return _CATEGORY_PHOTOS["crypto"]
    elif "oil" in text or "brent" in text or "crude" in text or "petroleo" in text:
        return _CATEGORY_PHOTOS["commodity"]
    elif "nvidia" in text or "nvda" in text or "ai" in text:
        return _CATEGORY_PHOTOS["tech"]
    elif "gold" in text or "oro" in text:
        return _CATEGORY_PHOTOS["gold"]
    elif "fed" in text or "inflation" in text or "rates" in text:
        return _CATEGORY_PHOTOS["macro"]
    return _CATEGORY_PHOTOS.get(str(category or "").casefold(), _CATEGORY_PHOTOS["market"])


def _takeaway(title: str, impact: str, tickers: list[str]) -> str:
    asset = _asset_names_for_copy(tickers) or "mercado"
    if impact == "bullish":
        return f"La noticia apunta a presion positiva en {asset}; Genesis busca confirmacion con precio y volumen antes de subir conviccion."
    if impact == "bearish":
        return f"La noticia puede presionar a {asset}; vigila soporte, volumen y continuidad de la reaccion."
    return f"La nota pone en foco a {asset}; Genesis revisa si el mercado confirma impacto con precio, volumen y noticias relacionadas."


def _why_it_matters(impact: str, tickers: list[str]) -> str:
    asset = _asset_names_for_copy(tickers) or "tus activos"
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


def _recency_score(published_at: datetime | None) -> int:
    if not published_at:
        return 0
    age = datetime.now(timezone.utc) - published_at
    if age <= timedelta(hours=24):
        return 5
    if age <= timedelta(days=7):
        return 3
    if age <= timedelta(days=30):
        return 1
    return 0


def _is_important(item: dict[str, Any], relevance: int, recency: int) -> bool:
    category = str(item.get("category") or "").casefold()
    impact = str(item.get("impact") or "").casefold()
    if relevance >= 3 and recency >= 1:
        return True
    if category in {"macro", "geopolitics", "commodity", "earnings"} and recency >= 1:
        return True
    if impact in {"bullish", "bearish"} and recency >= 3:
        return True
    return False


def _risk_for_impact(impact: str) -> str:
    if impact == "bullish":
        return "Riesgo: perseguir precio sin confirmacion de volumen."
    if impact == "bearish":
        return "Riesgo: aumento de volatilidad o perdida de soporte."
    return "Riesgo: impacto todavia no confirmado por precio."


def _watch_for_news(impact: str, tickers: list[str]) -> str:
    asset = _asset_names_for_copy(tickers) or "activos relacionados"
    if impact == "bullish":
        return f"Vigilar si {asset} confirma ruptura con volumen."
    if impact == "bearish":
        return f"Vigilar defensa de soporte en {asset} y reaccion del volumen."
    return f"Vigilar reaccion de precio en {asset} antes de operar."


def _set_source_status(source: str, status: str, *, elapsed_ms: int, cache_hit: bool, count: int, last_error_safe: str = "") -> None:
    _SOURCE_STATUS[source] = {
        "source": source,
        "status": status,
        "last_error_safe": last_error_safe,
        "elapsed_ms": elapsed_ms,
        "cache_hit": cache_hit,
        "count": count,
    }


def _news_id(title: str, source: object = "", published_at: object = "", url: object = "") -> str:
    raw = f"{title}|{source}|{published_at}|{url}"
    return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()[:16]


def _strip_html(value: object) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    return " ".join(text.split()).strip()


def _dedupe_key(title: object) -> str:
    text = _strip_html(title).casefold()
    text = re.sub(r"[^a-z0-9áéíóúñü\s]", " ", text)
    return " ".join(text.split())


def _is_bad_news_title(title: object) -> bool:
    text = _strip_html(title).casefold()
    blocked = (
        "contexto pendiente",
        "sin contexto",
        "genesis mantiene vigilancia",
        "stock price, news, quote",
        "stock price news quote",
        "quote & history",
        "quote and history",
        "company profile",
        "weekly market commentary",
        "market commentary",
        "weekly mercado commentary",
        "contexto relevante",
    )
    return any(token in text for token in blocked)


def _fallback_news_item(focus_tickers: list[str]) -> dict[str, Any]:
    now_dt = datetime.now(timezone.utc)
    now = now_dt.isoformat()
    tickers = focus_tickers[:5]
    image = _CATEGORY_PHOTOS["macro"]
    return {
        "id": "fallback-market-context",
        "title": "Genesis mantiene vigilancia de mercado",
        "title_es": "Genesis mantiene vigilancia de mercado",
        "original_title": "Genesis mantiene vigilancia de mercado",
        "summary": "No hay titulares externos recientes confirmados por la fuente activa; Genesis sigue usando precios, alertas, cartera y seguimiento.",
        "summary_es": "No hay titulares externos recientes confirmados por la fuente activa; Genesis sigue usando precios, alertas, cartera y seguimiento.",
        "original_summary": "No hay titulares externos recientes confirmados por la fuente activa; Genesis sigue usando precios, alertas, cartera y seguimiento.",
        "source": "Genesis",
        "published_at": now,
        "published_ts": _published_ts(now_dt),
        "age_hours": 0,
        "recency_bucket": "24h",
        "relative_time": "ahora",
        "tickers": tickers,
        "tickers_affected": tickers,
        "asset_names_affected": [_asset_name(ticker) for ticker in tickers],
        "assets": tickers,
        "impact": "neutral",
        "sentiment": "neutral",
        "tone": "neutral",
        "confidence": "low",
        "image_url": image,
        "thumbnail_url": image,
        "thumbnail": image,
        "url": "",
        "category": "macro",
        "genesis_takeaway": "Sin noticia externa confirmada; operar solo con precio, volumen y niveles.",
        "genesis_takeaway_es": "Sin noticia externa confirmada; operar solo con precio, volumen y niveles.",
        "why_it_matters": "Evita una pantalla vacia sin inventar titulares ni fuentes.",
        "why_it_matters_es": "Evita una pantalla vacia sin inventar titulares ni fuentes.",
        "what_to_watch_es": "Vigilar precio, volumen y alertas de tus activos.",
        "is_important": True,
        "is_latest": True,
        "recency_score": 1,
        "relevance_score": 1,
        "placeholder_key": "macro",
        "language": "es",
        "image_kind": "category_placeholder",
        "risk": "Riesgo: falta catalizador externo confirmado.",
        "watch": "Vigilar precio, volumen y alertas de tus activos.",
        "watch_points": ["Vigilar precio, volumen y alertas de tus activos."],
        "elapsed_ms": 0,
        "cache_hit": False,
        "provider": "internal_fallback",
    }
