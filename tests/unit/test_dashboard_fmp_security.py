from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from services.dashboard.get_fmp_dependencies_snapshot import get_fmp_dependencies_snapshot


class DashboardFmpSecurityTests(unittest.TestCase):
    def test_fmp_snapshot_reports_safe_state_without_exposing_key(self) -> None:
        secret = "TEST_SECRET_DO_NOT_EXPOSE_12345"
        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot_path = Path(tmpdir) / "fmp_snapshot.json"
            snapshot_path.write_text(
                json.dumps(
                    {
                        "provider": {
                            "status": "OK",
                            "degraded": False,
                            "note": f"ready https://example.test?apikey={secret}&symbol=NVDA",
                        },
                        "last_incident": {
                            "category": "access",
                            "ticker": "NVDA",
                            "detail": f"blocked apikey={secret}",
                        },
                        "meta": {
                            "source": "runtime_snapshot",
                            "snapshot_path": f"C:/tmp/fmp_snapshot.json?apikey={secret}",
                        },
                    }
                ),
                encoding="utf-8",
            )

            with patch.dict(os.environ, {"FMP_API_KEY": secret, "FMP_LIVE_ENABLED": "true"}):
                with patch("services.dashboard.get_fmp_dependencies_snapshot._FMP_SNAPSHOT_PATH", snapshot_path):
                    payload = get_fmp_dependencies_snapshot()

        rendered = json.dumps(payload, sort_keys=True)
        self.assertTrue(payload["provider"]["key_configured"])
        self.assertTrue(payload["provider"]["live_enabled"])
        self.assertTrue(payload["provider"]["live_ready"])
        self.assertFalse(payload["security"]["secret_exposed"])
        self.assertFalse(payload["security"]["apikey_param_exposed"])
        self.assertNotIn(secret, rendered)
        self.assertNotIn("apikey=", rendered.lower())

    def test_fmp_live_defaults_off_without_flag(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot_path = Path(tmpdir) / "missing.json"
            with patch.dict(os.environ, {"FMP_API_KEY": "configured"}, clear=False):
                with patch.dict(os.environ, {"FMP_LIVE_ENABLED": ""}, clear=False):
                    with patch("services.dashboard.get_fmp_dependencies_snapshot._FMP_SNAPSHOT_PATH", snapshot_path):
                        payload = get_fmp_dependencies_snapshot()

        self.assertTrue(payload["provider"]["key_configured"])
        self.assertFalse(payload["provider"]["live_enabled"])
        self.assertFalse(payload["provider"]["live_ready"])


if __name__ == "__main__":
    unittest.main()
