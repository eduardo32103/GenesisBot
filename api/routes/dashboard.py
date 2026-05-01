from __future__ import annotations

from services.dashboard.get_alert_drilldown import get_alert_drilldown
from services.dashboard.get_alerts_snapshot import get_alerts_snapshot
from services.dashboard.get_executive_queue_snapshot import get_executive_queue_snapshot
from services.dashboard.get_fmp_dependencies_snapshot import get_fmp_dependencies_snapshot
from services.dashboard.get_genesis_answer import get_genesis_answer
from services.dashboard.get_macro_activity_snapshot import get_macro_activity_snapshot
from services.dashboard.get_money_flow_causal_snapshot import get_money_flow_causal_snapshot
from services.dashboard.get_money_flow_detection_snapshot import get_money_flow_detection_snapshot
from services.dashboard.get_money_flow_jarvis_answer import get_money_flow_jarvis_answer
from services.dashboard.get_money_flow_signal_model import get_money_flow_signal_model
from services.dashboard.get_operational_reliability_snapshot import get_operational_reliability_snapshot
from services.dashboard.get_radar_ticker_drilldown import get_dashboard_portfolio, get_dashboard_radar_ticker_drilldown
from services.dashboard.get_operational_health import get_operational_health
from services.dashboard.get_radar_snapshot import get_radar_snapshot
from services.dashboard.search_market_ticker import search_market_ticker
from services.portfolio.update_portfolio import (
    add_ticker_to_portfolio,
    remove_paper_position,
    remove_watchlist_ticker,
    simulate_paper_position,
)


def get_dashboard_health() -> dict:
    return get_operational_health()


def get_dashboard_reliability() -> dict:
    return get_operational_reliability_snapshot()


def get_dashboard_executive_queue() -> dict:
    return get_executive_queue_snapshot()


def get_dashboard_genesis(
    question: str = "",
    context: str = "general",
    ticker: str = "",
    panel_context: str = "",
) -> dict:
    return get_genesis_answer(question, context=context, ticker=ticker, panel_context=panel_context)


def get_dashboard_money_flow_model() -> dict:
    return get_money_flow_signal_model()


def get_dashboard_money_flow_detection() -> dict:
    return get_money_flow_detection_snapshot()


def get_dashboard_money_flow_causal() -> dict:
    return get_money_flow_causal_snapshot()


def get_dashboard_money_flow_jarvis(question: str = "") -> dict:
    return get_money_flow_jarvis_answer(question)


def get_dashboard_radar() -> dict:
    return get_radar_snapshot()


def get_dashboard_portfolio_snapshot() -> dict:
    return get_dashboard_portfolio()


def get_dashboard_radar_drilldown(ticker: str) -> dict:
    return get_dashboard_radar_ticker_drilldown(ticker)


def search_dashboard_market_ticker(query: str = "") -> dict:
    return search_market_ticker(query)


def add_dashboard_portfolio_ticker(ticker: str = "") -> dict:
    return add_ticker_to_portfolio(ticker)


def simulate_dashboard_portfolio_purchase(ticker: str = "", units: object = None, entry_price: object = None) -> dict:
    return simulate_paper_position(ticker, units=units, entry_price=entry_price)


def remove_dashboard_portfolio_ticker(ticker: str = "") -> dict:
    return remove_watchlist_ticker(ticker)


def remove_dashboard_portfolio_purchase(ticker: str = "") -> dict:
    return remove_paper_position(ticker)


def get_dashboard_alerts() -> dict:
    return get_alerts_snapshot()


def get_dashboard_alert_drilldown(alert_id: str) -> dict:
    return get_alert_drilldown(alert_id)


def get_dashboard_fmp_dependencies() -> dict:
    return get_fmp_dependencies_snapshot()


def get_dashboard_macro_activity() -> dict:
    return get_macro_activity_snapshot()
