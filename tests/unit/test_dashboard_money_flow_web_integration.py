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

    def test_web_app_uses_existing_money_flow_backend_endpoints(self) -> None:
        app_config = create_app()

        self.assertEqual(app_config["money_flow_detection_endpoint"], "/api/dashboard/money-flow/detection")
        self.assertEqual(app_config["money_flow_causal_endpoint"], "/api/dashboard/money-flow/causal")
        self.assertEqual(app_config["money_flow_jarvis_endpoint"], "/api/dashboard/money-flow/jarvis?q={question}")
        self.assertNotIn("money_flow_visual_endpoint", app_config)


if __name__ == "__main__":
    unittest.main()
