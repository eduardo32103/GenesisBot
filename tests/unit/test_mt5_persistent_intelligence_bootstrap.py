from __future__ import annotations

import json
import unittest

from api.main import create_app
from api.routes.genesis import get_genesis_mt5_persistent_intelligence_bootstrap_status
from services.mt5.mt5_persistent_intelligence_bootstrap import (
    persistent_intelligence_bootstrap_status,
    run_persistent_intelligence_bootstrap,
)
from services.mt5.mt5_autonomous_learning_orchestrator import run_autonomous_learning_orchestrator


class MT5PersistentIntelligenceBootstrapTests(unittest.TestCase):
    def test_bootstrap_seeds_current_knowledge_once_and_then_skips_existing(self) -> None:
        store = _BootstrapFakeStore()

        first = run_persistent_intelligence_bootstrap(store=store)
        second = run_persistent_intelligence_bootstrap(store=store)

        self.assertEqual(first["status"], "persistent_intelligence_bootstrap_complete")
        self.assertEqual(first["seeded_degradation_rows"], 1)
        self.assertEqual(first["seeded_profile_state_rows"], 1)
        self.assertGreaterEqual(first["seeded_rejection_rows"], 7)
        self.assertEqual(first["seeded_research_lesson_rows"], 7)
        self.assertGreaterEqual(first["seeded_strategy_rows"], 8)
        self.assertEqual(first["seeded_candidate_rotation_rows"], 1)
        self.assertEqual(first["seeded_adaptive_governor_state_rows"], 1)
        self.assertGreaterEqual(first["skipped_existing_rows"], 1)
        self.assertEqual(first["errors"], [])

        self.assertEqual(second["seeded_degradation_rows"], 0)
        self.assertEqual(second["seeded_profile_state_rows"], 0)
        self.assertEqual(second["seeded_research_lesson_rows"], 0)
        self.assertEqual(second["seeded_candidate_rotation_rows"], 0)
        self.assertEqual(second["seeded_adaptive_governor_state_rows"], 0)
        self.assertGreater(second["skipped_existing_rows"], 0)
        self.assertEqual(len(store.tables["mt5_degradation_registry"]), 1)
        self.assertEqual(len(store.tables["mt5_research_lessons"]), 7)
        self.assertFalse(first["broker_touched"])
        self.assertFalse(first["order_executed"])
        self.assertEqual(first["order_policy"], "journal_only_no_broker")

    def test_bootstrap_does_not_duplicate_rejection_registry(self) -> None:
        store = _BootstrapFakeStore()

        run_persistent_intelligence_bootstrap(store=store)
        first_count = len(store.tables["mt5_research_rejection_registry"])
        run_persistent_intelligence_bootstrap(store=store)

        self.assertEqual(len(store.tables["mt5_research_rejection_registry"]), first_count)

    def test_bootstrap_aborts_safely_when_db_degraded(self) -> None:
        store = _BootstrapFakeStore(health={"db_available": False, "tables_ready": False, "db_degraded": True})

        result = run_persistent_intelligence_bootstrap(store=store)

        self.assertEqual(result["status"], "persistent_intelligence_bootstrap_aborted_db_degraded")
        self.assertEqual(result["recommendation"], "repair_persistent_db_before_bootstrap")
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["reason"], "persistent_intelligence_db_degraded")
        self.assertEqual(store.write_count, 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_bootstrap_status_endpoint_is_read_only(self) -> None:
        app = create_app()
        store = _BootstrapFakeStore()

        result = persistent_intelligence_bootstrap_status(store=store)
        route_payload = get_genesis_mt5_persistent_intelligence_bootstrap_status()

        self.assertEqual(
            app["genesis_mt5_persistent_intelligence_bootstrap_status_endpoint"],
            "/api/genesis/mt5/persistent-intelligence/bootstrap/status",
        )
        self.assertTrue(result["ok"])
        self.assertFalse(result["bootstrap_writes_enabled"])
        self.assertEqual(store.write_count, 0)
        self.assertTrue(route_payload["ok"])
        self.assertFalse(route_payload["broker_touched"])
        self.assertFalse(route_payload["order_executed"])
        self.assertEqual(route_payload["order_policy"], "journal_only_no_broker")

    def test_bootstrap_output_does_not_leak_secrets_or_touch_broker(self) -> None:
        store = _BootstrapFakeStore(secret="SUPERSECRET")

        result = run_persistent_intelligence_bootstrap(store=store)

        serialized = json.dumps(result, sort_keys=True)
        self.assertNotIn("SUPERSECRET", serialized)
        self.assertFalse(result["secrets_printed"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_one_cycle_orchestrator_does_not_apply_paper_rotation(self) -> None:
        result = run_autonomous_learning_orchestrator(
            load_persistent=False,
            load_shadow_snapshot=False,
            load_rotation=False,
            run_trade_learning=False,
            persist_events=False,
            apply_paper_rotation=False,
        )

        self.assertTrue(result["ok"])
        self.assertFalse(result["paper_rotation_applied"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")


class _BootstrapFakeStore:
    def __init__(self, *, health: dict[str, object] | None = None, secret: str = "") -> None:
        self.health = health or {"db_available": True, "tables_ready": True, "db_degraded": False}
        self.secret = secret
        self.write_count = 0
        self.tables: dict[str, list[dict[str, object]]] = {
            "mt5_degradation_registry": [],
            "mt5_research_rejection_registry": [],
            "mt5_strategy_registry": [],
            "mt5_profile_state": [],
            "mt5_research_lessons": [],
            "mt5_adaptive_governor_state": [],
            "mt5_candidate_rotation_runs": [],
        }

    def healthcheck(self, *, write_test_event: bool = False) -> dict[str, object]:
        del write_test_event
        return {
            "ok": True,
            "provider": "railway_postgres",
            "secrets_printed": False,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
            **self.health,
        }

    def upsert_degradation_registry(self, payload: dict[str, object], *, critical: bool | None = None) -> dict[str, object]:
        del critical
        return self._upsert("mt5_degradation_registry", payload, ("symbol", "timeframe", "profile"))

    def upsert_research_rejection_registry(self, payload: dict[str, object], *, critical: bool | None = None) -> dict[str, object]:
        del critical
        return self._upsert("mt5_research_rejection_registry", payload, ("symbol", "timeframe", "family_pattern"))

    def upsert_strategy_registry(self, payload: dict[str, object], *, critical: bool | None = None) -> dict[str, object]:
        del critical
        return self._upsert("mt5_strategy_registry", payload, ("symbol", "timeframe", "profile"))

    def upsert_profile_state(self, payload: dict[str, object], *, critical: bool | None = None) -> dict[str, object]:
        del critical
        return self._upsert("mt5_profile_state", payload, ("symbol", "timeframe", "profile"))

    def record_research_lesson(self, payload: dict[str, object], *, critical: bool | None = None) -> dict[str, object]:
        del critical
        self.write_count += 1
        self.tables["mt5_research_lessons"].append(dict(payload))
        return self._ok("mt5_research_lessons")

    def record_adaptive_governor_state(self, payload: dict[str, object], *, critical: bool | None = None) -> dict[str, object]:
        del critical
        self.write_count += 1
        self.tables["mt5_adaptive_governor_state"].append(dict(payload))
        return self._ok("mt5_adaptive_governor_state")

    def record_candidate_rotation_run(self, payload: dict[str, object], *, critical: bool | None = None) -> dict[str, object]:
        del critical
        return self._upsert("mt5_candidate_rotation_runs", payload, ("run_id",))

    def _safe_select(self, table: str, *, params: dict[str, str]) -> dict[str, object]:
        rows = []
        for row in self.tables.get(table, []):
            if _matches(row, params):
                rows.append(dict(row))
        return {"ok": True, "rows": rows[: int(params.get("limit") or 500)], "db_degraded": False}

    def _upsert(self, table: str, payload: dict[str, object], keys: tuple[str, ...]) -> dict[str, object]:
        self.write_count += 1
        rows = self.tables[table]
        for index, row in enumerate(rows):
            if all(str(row.get(key) or "") == str(payload.get(key) or "") for key in keys):
                rows[index] = {**row, **dict(payload)}
                return self._ok(table)
        rows.append(dict(payload))
        return self._ok(table)

    def _ok(self, table: str) -> dict[str, object]:
        return {
            "ok": True,
            "table": table,
            "db_degraded": False,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }


def _matches(row: dict[str, object], params: dict[str, str]) -> bool:
    for key, raw in params.items():
        if key in {"select", "limit"}:
            continue
        expected = str(raw or "")
        if expected.startswith("eq."):
            expected = expected[3:]
        if str(row.get(key) or "") != expected:
            return False
    return True


if __name__ == "__main__":
    unittest.main()
