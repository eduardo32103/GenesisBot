from __future__ import annotations


ROUTES = {
    "start": "app.telegram.handlers.start.handle_start",
    "analysis": "app.telegram.handlers.analysis.handle_analysis",
    "alerts": "app.telegram.handlers.alerts.handle_alerts",
    "geopolitics": "app.telegram.handlers.geopolitics.handle_geopolitics",
    "portfolio": "app.telegram.handlers.portfolio.handle_portfolio",
}
