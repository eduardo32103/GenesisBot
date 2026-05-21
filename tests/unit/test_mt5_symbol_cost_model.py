from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from services.mt5.mt5_symbol_cost_model import (
    build_symbol_cost_model,
    discover_alias,
    infer_instrument_type,
    make_cost_model_report,
    write_cost_model_report,
)


class MT5SymbolCostModelTests(unittest.TestCase):
    def test_instrument_type_detection(self) -> None:
        self.assertEqual(infer_instrument_type("BTCUSDm"), "crypto")
        self.assertEqual(infer_instrument_type("ETHUSD.r"), "crypto")
        self.assertEqual(infer_instrument_type("XAUUSD"), "metal")
        self.assertEqual(infer_instrument_type("GOLDm"), "metal")
        self.assertEqual(infer_instrument_type("USTEC"), "index")
        self.assertEqual(infer_instrument_type("EURUSD"), "forex")

    def test_cost_model_uses_symbol_specific_points(self) -> None:
        forex = build_symbol_cost_model("EURUSD", first_price=1.08)
        crypto = build_symbol_cost_model("ETHUSD", first_price=3500.0)
        metal = build_symbol_cost_model("XAUUSD", first_price=2300.0)
        index = build_symbol_cost_model("NAS100", first_price=18000.0)

        self.assertEqual(forex.instrument_type, "forex")
        self.assertEqual(forex.digits, 5)
        self.assertAlmostEqual(forex.point, 0.00001)
        self.assertEqual(crypto.instrument_type, "crypto")
        self.assertGreater(crypto.spread_points, forex.spread_points)
        self.assertEqual(metal.instrument_type, "metal")
        self.assertEqual(index.instrument_type, "index")
        for model in [forex, crypto, metal, index]:
            self.assertFalse(model.as_dict().get("broker_touched"))
            self.assertGreater(model.spread_x2_cost, model.spread_x1_5_cost)

    def test_alias_discovery_finds_known_variants(self) -> None:
        available = ["BTCUSD.r", "GOLDm", "USTEC", "SPX500m", "EURUSDm"]

        self.assertEqual(discover_alias("BTCUSD", available)["resolved_symbol"], "BTCUSD.r")
        self.assertEqual(discover_alias("XAUUSD", available)["resolved_symbol"], "GOLDm")
        self.assertEqual(discover_alias("NAS100", available)["resolved_symbol"], "USTEC")
        self.assertEqual(discover_alias("US500", available)["resolved_symbol"], "SPX500m")
        self.assertEqual(discover_alias("GBPUSD", available)["status"], "not_found")

    def test_report_outputs_are_paper_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ETHUSD_M30_20000.csv").write_text("time,open,high,low,close,volume\n2025-01-01T00:00:00+00:00,3000,3010,2990,3005,1\n", encoding="utf-8")
            (root / "XAUUSD.b_M30_20000.csv").write_text("time,open,high,low,close,volume\n2025-01-01T00:00:00+00:00,2300,2310,2290,2305,1\n", encoding="utf-8")

            result = make_cost_model_report(["ETHUSD", "XAUUSD", "MISSING"], root)
            csv_path, json_path = write_cost_model_report(result, root)

            self.assertTrue(csv_path.exists())
            self.assertTrue(json_path.exists())
            self.assertFalse(result["broker_touched"])
            self.assertFalse(result["order_executed"])
            self.assertEqual(result["order_policy"], "journal_only_no_broker")
            self.assertEqual(result["rows"][0]["instrument_type"], "crypto")
            self.assertTrue(any(row["requested_symbol"] == "XAUUSD" and row["csv_found"] for row in result["rows"]))


if __name__ == "__main__":
    unittest.main()
