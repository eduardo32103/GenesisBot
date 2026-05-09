from __future__ import annotations

import json
import logging
import os
from functools import partial
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
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
_PROXY_POST_PATHS = {
    "/api/dashboard/portfolio/paper",
    "/api/dashboard/portfolio/paper-buy",
    "/api/dashboard/portfolio/paper-remove",
    "/api/dashboard/portfolio/watchlist",
    "/api/dashboard/portfolio/watchlist/add",
    "/api/dashboard/portfolio/watchlist/remove",
    "/api/genesis/analyze-image",
    "/api/genesis/ask",
    "/api/genesis/memory/event",
}


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _local_live_sources_missing() -> bool:
    if os.getenv("PORT"):
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
            with urlopen(request, timeout=28) as response:
                data = response.read()
                status = int(getattr(response, "status", 200) or 200)
                content_type = response.headers.get("Content-Type", "application/json; charset=utf-8")
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
        if self._try_proxy_to_production(parsed, method="POST", body=body):
            return

        if parsed.path == "/api/genesis/ask":
            result = ask_genesis(
                str(body.get("message") or body.get("question") or ""),
                context=str(body.get("context") or "general"),
                ticker=str(body.get("ticker") or ""),
                panel_context=body.get("panel_context") if isinstance(body.get("panel_context"), dict) else None,
                conversation_id=str(body.get("conversation_id") or "default"),
            )
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
        if parsed.path == "/api/dashboard/news":
            payload = json.dumps(get_dashboard_news()).encode("utf-8")
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
            payload = json.dumps(get_dashboard_money_flow_detection()).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path == "/api/dashboard/money-flow/causal":
            payload = json.dumps(get_dashboard_money_flow_causal()).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path == "/api/dashboard/money-flow/jarvis":
            question = (parse_qs(parsed.query).get("q") or [""])[0]
            payload = json.dumps(get_dashboard_money_flow_jarvis(question)).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path in {"/api/dashboard/radar", "/api/dashboard/portfolio"}:
            payload = json.dumps(get_dashboard_radar()).encode("utf-8")
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
            payload = json.dumps(search_dashboard_market_ticker(query)).encode("utf-8")
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
            payload = json.dumps(get_dashboard_whales(ticker)).encode("utf-8")
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
            payload = json.dumps(get_dashboard_source_health()).encode("utf-8")
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
