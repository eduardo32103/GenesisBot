from __future__ import annotations

from datetime import datetime, timezone
import unittest
from unittest.mock import patch

from services.dashboard.get_alert_drilldown import _fetch_alert_drilldown


class _FakeCursor:
    def __init__(self, event_row, validation_rows):
        self._event_row = event_row
        self._validation_rows = validation_rows
        self._last_query = ""

    def execute(self, query, params):
        self._last_query = query

    def fetchone(self):
        if "FROM alert_events" in self._last_query:
            return self._event_row
        return None

    def fetchall(self):
        if "FROM alert_validations" in self._last_query:
            return self._validation_rows
        return []


class _FakeConnection:
    def __init__(self, event_row, validation_rows):
        self._cursor = _FakeCursor(event_row, validation_rows)
        self.committed = False
        self.closed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


class AlertDrilldownTests(unittest.TestCase):
    @patch("services.dashboard.get_alert_drilldown._connect_database")
    def test_fetch_alert_drilldown_returns_latest_validation_detail(self, mock_connect):
        event_row = (
            "alert-001",
            "geo_macro",
            "nvda",
            "Breakout confirmado",
            "Momentum y macro alineados.",
            "runtime",
            1.35,
            "completed",
            datetime(2026, 4, 20, 14, 30, tzinfo=timezone.utc),
        )
        validation_rows = [
            (
                "swing_5d",
                datetime(2026, 4, 21, 14, 30, tzinfo=timezone.utc),
                datetime(2026, 4, 23, 14, 30, tzinfo=timezone.utc),
                1.75,
                4.2,
                "ganadora",
            )
        ]
        mock_connect.return_value = _FakeConnection(event_row, validation_rows)

        detail = _fetch_alert_drilldown("postgresql://local", "alert-001")

        self.assertTrue(detail["found"])
        self.assertEqual(detail["ticker"], "NVDA")
        self.assertEqual(detail["alert_type_label"], "Geo / Macro")
        self.assertEqual(detail["horizon"], "SWING_5D")
        self.assertEqual(detail["status_label"], "Validada positiva")
        self.assertEqual(detail["validation"], "Validada | SWING_5D")
        self.assertEqual(detail["result"], "Ganadora")
        self.assertEqual(detail["score"], 1.75)
        self.assertEqual(detail["context_note"], "Momentum y macro alineados.")
        self.assertIn("No recalcula el motor", detail["reliability_note"])
        self.assertIn("2026-04-20", detail["created_at"])
        self.assertIn("2026-04-23", detail["evaluated_at"])

    @patch("services.dashboard.get_alert_drilldown._connect_database")
    def test_fetch_alert_drilldown_keeps_pending_alerts_honest(self, mock_connect):
        event_row = (
            "alert-002",
            "sentinel_news",
            "BNO",
            "Cobertura geopolitica activa",
            "",
            "runtime",
            0.55,
            "tracking",
            datetime(2026, 4, 22, 10, 0, tzinfo=timezone.utc),
        )
        validation_rows = [
            (
                "d3",
                datetime(2026, 4, 25, 10, 0, tzinfo=timezone.utc),
                None,
                None,
                None,
                "",
            )
        ]
        mock_connect.return_value = _FakeConnection(event_row, validation_rows)

        detail = _fetch_alert_drilldown("postgresql://local", "alert-002")

        self.assertTrue(detail["found"])
        self.assertEqual(detail["ticker"], "BNO")
        self.assertEqual(detail["status_label"], "Seguimiento")
        self.assertEqual(detail["validation"], "Pendiente | D3")
        self.assertEqual(detail["result"], "Sin resultado")
        self.assertIsNone(detail["score"])
        self.assertEqual(detail["title"], "Cobertura geopolitica activa")
        self.assertEqual(detail["context_note"], "Cobertura geopolitica activa")
        self.assertIn("sigue en seguimiento", detail["reliability_note"])

    @patch("services.dashboard.get_alert_drilldown._connect_database")
    def test_fetch_alert_drilldown_handles_database_unavailable(self, mock_connect):
        mock_connect.return_value = None

        detail = _fetch_alert_drilldown("postgresql://local", "alert-003")

        self.assertFalse(detail["found"])
        self.assertEqual(detail["error"], "database_unavailable")
        self.assertIn("No pude conectarme", detail["reliability_note"])


if __name__ == "__main__":
    unittest.main()
