from __future__ import annotations

import unittest
from pathlib import Path

from api.main import create_app


class DashboardMoneyFlowWebIntegrationTests(unittest.TestCase):
    def test_real_dashboard_static_files_expose_ballenas_app_screen(self) -> None:
        root = Path(__file__).resolve().parents[2]
        html = (root / "app" / "dashboard" / "index.html").read_text(encoding="utf-8")
        script = (root / "app" / "dashboard" / "app.js").read_text(encoding="utf-8")

        self.assertIn('data-view="money-flow"', html)
        self.assertIn('id="view-money-flow"', html)
        self.assertIn("Ballenas", html)
        self.assertIn('id="whales-list"', html)
        self.assertIn('id="money-flow-jarvis-form"', html)
        self.assertIn("Lectura Genesis", html)
        self.assertIn('fetch("/api/dashboard/money-flow/causal"', script)
        self.assertIn('fetch(`/api/dashboard/money-flow/jarvis?q=', script)
        self.assertIn("renderMoneyFlowSnapshot", script)
        self.assertIn("renderMoneyFlowJarvisAnswer", script)

    def test_portfolio_app_buttons_are_wired_to_real_flow_handlers(self) -> None:
        root = Path(__file__).resolve().parents[2]
        html = (root / "app" / "dashboard" / "index.html").read_text(encoding="utf-8")
        script = (root / "app" / "dashboard" / "app.js").read_text(encoding="utf-8")

        self.assertIn('id="portfolio-search-button"', html)
        self.assertIn('id="portfolio-close-modal"', html)
        self.assertIn('id="portfolio-sim-buy-button"', html)
        self.assertIn("searchAndAddPortfolioTicker", script)
        self.assertIn('searchButton.addEventListener("click", searchAndAddPortfolioTicker)', script)
        self.assertIn('data-paper-close', script)
        self.assertIn('"/api/dashboard/portfolio/watchlist/add"', script)
        self.assertIn('"/api/dashboard/portfolio/watchlist/remove"', script)
        self.assertIn('"/api/dashboard/portfolio/paper-buy"', script)
        self.assertIn('"/api/dashboard/portfolio/paper-remove"', script)

    def test_web_app_uses_existing_money_flow_backend_endpoints(self) -> None:
        app_config = create_app()

        self.assertEqual(app_config["money_flow_detection_endpoint"], "/api/dashboard/money-flow/detection")
        self.assertEqual(app_config["money_flow_causal_endpoint"], "/api/dashboard/money-flow/causal")
        self.assertEqual(app_config["money_flow_jarvis_endpoint"], "/api/dashboard/money-flow/jarvis?q={question}")
        self.assertEqual(app_config["portfolio_endpoint"], "/api/dashboard/portfolio")
        self.assertEqual(app_config["portfolio_drilldown_endpoint"], "/api/dashboard/portfolio/drilldown?ticker={symbol}")
        self.assertEqual(app_config["portfolio_remove_endpoint"], "/api/dashboard/portfolio/watchlist/remove")
        self.assertEqual(app_config["portfolio_paper_remove_endpoint"], "/api/dashboard/portfolio/paper-remove")
        self.assertNotIn("money_flow_visual_endpoint", app_config)


if __name__ == "__main__":
    unittest.main()
