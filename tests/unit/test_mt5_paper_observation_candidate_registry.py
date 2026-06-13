from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path

from scripts.run_paper_observation_candidate_registry import main as registry_script_main
from services.mt5.mt5_paper_observation_candidate_registry import (
    run_paper_observation_candidate_registry,
    validate_paper_observation_payload,
)


class MT5PaperObservationCandidateRegistryTests(unittest.TestCase):
    def test_valid_payload_passes(self) -> None:
        result = validate_paper_observation_payload(_payload())

        self.assertTrue(result["payload_valid"])
        self.assertEqual(result["validation_errors"], [])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_candidate_activated_true_fails(self) -> None:
        payload = _payload(candidate_activated=True)

        result = validate_paper_observation_payload(payload)

        self.assertFalse(result["payload_valid"])
        self.assertIn("candidate_activated_must_be_false", result["validation_errors"])

    def test_applies_to_real_trading_true_fails(self) -> None:
        payload = _payload(applies_to_real_trading=True)

        result = validate_paper_observation_payload(payload)

        self.assertFalse(result["payload_valid"])
        self.assertIn("applies_to_real_trading_must_be_false", result["validation_errors"])

    def test_broker_touched_true_fails(self) -> None:
        payload = _payload(broker_touched=True)

        result = validate_paper_observation_payload(payload)

        self.assertFalse(result["payload_valid"])
        self.assertIn("broker_touched_must_be_false", result["validation_errors"])

    def test_payload_without_gates_fails(self) -> None:
        payload = _payload()
        payload.pop("gates")

        result = validate_paper_observation_payload(payload)

        self.assertFalse(result["payload_valid"])
        self.assertIn("missing_gates", result["validation_errors"])
        self.assertIn("gates_missing", result["validation_errors"])

    def test_raw_research_artifacts_are_rejected(self) -> None:
        payload = _payload()
        payload["raw_trades"] = [{"pnl": 1.0}]
        payload["ohlc"] = [{"open": 1.0, "close": 2.0}]

        result = validate_paper_observation_payload(payload)

        self.assertFalse(result["payload_valid"])
        self.assertTrue(any(reason.startswith("raw_artifacts_not_allowed:") for reason in result["validation_errors"]))

    def test_dry_run_does_not_write(self) -> None:
        store = _RecordingStore()

        result = run_paper_observation_candidate_registry(
            payload=_payload(),
            apply=False,
            store=store,
        )

        self.assertTrue(result["payload_valid"])
        self.assertTrue(result["dry_run"])
        self.assertFalse(result["applied"])
        self.assertEqual(result["rows_written"], 0)
        self.assertEqual(store.calls, [])
        self.assertEqual(len(result["rows_to_write"]), 4)
        self.assertTrue(result["research_lesson_prepared"])
        self.assertTrue(result["profile_state_prepared"])
        self.assertTrue(result["strategy_registry_prepared"])
        self.assertFalse(result["candidate_activated"])

    def test_apply_writes_only_compact_data(self) -> None:
        store = _RecordingStore()

        result = run_paper_observation_candidate_registry(
            payload=_payload(),
            apply=True,
            store=store,
        )

        self.assertTrue(result["payload_valid"])
        self.assertFalse(result["dry_run"])
        self.assertTrue(result["applied"])
        self.assertEqual(result["rows_written"], 4)
        self.assertEqual(
            [call[0] for call in store.calls],
            [
                "mt5_strategy_registry",
                "mt5_profile_state",
                "mt5_research_lessons",
                "mt5_candidate_rotation_runs",
            ],
        )
        serialized = json.dumps([call[1] for call in store.calls], sort_keys=True)
        self.assertNotIn("raw_trades", serialized)
        self.assertNotIn("raw_ohlc", serialized)
        self.assertNotIn("ohlc_rows", serialized)
        self.assertIn("journal_only_no_broker", serialized)
        for _, payload in store.calls:
            self.assertFalse(payload.get("candidate_activated", False))
            self.assertFalse(payload.get("paper_forward_onboarding_started", False))
            self.assertFalse(payload.get("applies_to_real_trading", False))

    def test_script_dry_run_outputs_expected_safety(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "candidate.json"
            path.write_text(json.dumps(_payload(), sort_keys=True), encoding="utf-8")
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = registry_script_main(["--payload", str(path)])

        self.assertEqual(code, 0)
        text = output.getvalue()
        self.assertIn("payload_valid=True", text)
        self.assertIn("dry_run=True", text)
        self.assertIn("applied=False", text)
        self.assertIn("rows_to_write=4", text)
        self.assertIn("candidate_activated=False", text)
        self.assertIn("paper_forward_onboarding_started=False", text)
        self.assertIn("broker_touched=False", text)
        self.assertIn("order_executed=False", text)
        self.assertIn("order_policy=journal_only_no_broker", text)


def _payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": "2026-06-12.paper_observation_candidate.v1",
        "candidate_profile": "volatility_compression_breakout|mode=nr7_trailing_defensive",
        "symbol": "XAUUSD",
        "broker_symbol": "XAUUSD.b",
        "timeframe": "M15",
        "family": "volatility_compression_breakout",
        "mode": "nr7_trailing_defensive",
        "source_csv_basename": "XAUUSD.b_M15_20000.csv",
        "validation_metrics": {
            "total_closed": 1925,
            "recent_closed": 479,
            "win_rate": 82.4935,
            "recent_win_rate": 86.6388,
            "total_pf": 3.167183,
            "recent_pf": 3.92537,
            "expectancy": 0.0004261,
            "recent_expectancy": 0.00052638,
            "max_drawdown": 0.03749759,
            "consecutive_losses": 3,
            "monte_carlo_stressed_pf": 2.753907,
            "monte_carlo_stressed_expectancy": 0.00036924,
            "monte_carlo_p95_drawdown": 0.05018427,
            "spread_x2_pf": 2.638813,
            "remove_best_5_pf": 2.917227,
            "single_trade_dependency": False,
            "fragile_regime_dependency": False,
            "sample_stability_score": 94.7301,
            "source_identity_resolved": True,
        },
        "gates": {
            "source_identity_resolved": True,
            "total_closed": True,
            "recent_closed": True,
            "total_pf": True,
            "recent_pf": True,
            "monte_carlo_stressed_pf": True,
            "spread_x2_pf": True,
            "remove_best_5_pf": True,
            "single_trade_dependency_false": True,
            "fragile_regime_dependency_false": True,
            "no_registry_hit": True,
            "no_degradation_hit": True,
            "no_sibling_risk": True,
            "not_unknown_profile": True,
        },
        "recommendation": "paper_observation_review",
        "paper_observation_ready": True,
        "requires_human_approval": True,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
        "raw_trades_included": False,
        "raw_ohlc_included": False,
        "csv_payload_included": False,
    }
    merged = deepcopy(payload)
    merged.update(overrides)
    return merged


class _RecordingStore:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def healthcheck(self, *, write_test_event: bool = False) -> dict[str, object]:
        return {
            "ok": True,
            "provider": "fake",
            "db_available": True,
            "tables_ready": True,
            "db_degraded": False,
            "recommendation": "persistent_intelligence_ready",
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }

    def upsert_strategy_registry(self, payload: dict[str, object], *, critical: bool = False) -> dict[str, object]:
        self.calls.append(("mt5_strategy_registry", dict(payload)))
        return {"ok": True, "table": "mt5_strategy_registry"}

    def upsert_profile_state(self, payload: dict[str, object], *, critical: bool = False) -> dict[str, object]:
        self.calls.append(("mt5_profile_state", dict(payload)))
        return {"ok": True, "table": "mt5_profile_state"}

    def record_research_lesson(self, payload: dict[str, object], *, critical: bool = False) -> dict[str, object]:
        self.calls.append(("mt5_research_lessons", dict(payload)))
        return {"ok": True, "table": "mt5_research_lessons"}

    def record_candidate_rotation_run(self, payload: dict[str, object], *, critical: bool = False) -> dict[str, object]:
        self.calls.append(("mt5_candidate_rotation_runs", dict(payload)))
        return {"ok": True, "table": "mt5_candidate_rotation_runs"}


if __name__ == "__main__":
    unittest.main()
