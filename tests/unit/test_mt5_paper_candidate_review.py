from __future__ import annotations

import unittest

from services.mt5.mt5_paper_candidate_review import (
    active_context_review,
    resolve_paper_review_profile_name,
    review_paper_candidate,
)


class MT5PaperCandidateReviewTests(unittest.TestCase):
    def test_unknown_profile_resolves_to_stable_review_profile_without_activation(self) -> None:
        result = review_paper_candidate(_btc_candidate(), persist_review=False)

        self.assertEqual(result["candidate_profile_before"], "unknown_profile")
        self.assertEqual(result["candidate_profile_after"], "btcusd_h1_tournament_edge_candidate_paper_review_v1")
        self.assertNotIn("unknown_profile", result["candidate_profile_after"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["paper_rotation_applied"])
        self.assertFalse(result["applies_to_real_trading"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_review_preserves_metrics_and_persists_paper_review_only(self) -> None:
        store = _FakeStore()

        result = review_paper_candidate(
            _btc_candidate(),
            persist_review=True,
            store=store,
        )

        self.assertEqual(result["trades_forward"], 8)
        self.assertEqual(result["win_rate"], 75.0)
        self.assertEqual(result["profit_factor"], 19.72)
        self.assertEqual(result["recent_profit_factor"], 19.72)
        self.assertEqual(result["expectancy"], 55.12)
        self.assertTrue(result["paper_candidate_review_created"])
        self.assertTrue(result["persistent_review_write_ok"])
        self.assertEqual(len(store.upserted), 1)
        payload = store.upserted[0]
        self.assertEqual(payload["status"], "paper_review")
        self.assertFalse(payload["active"])
        self.assertFalse(payload["applies_to_paper_shadow"])
        self.assertFalse(payload["applies_to_real_trading"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])

    def test_min_sample_gate_blocks_trades_forward_below_twenty(self) -> None:
        result = review_paper_candidate(
            _btc_candidate(),
            capital_state="normal",
            adaptive_state="watch",
            risk_allowed=True,
            persist_review=False,
        )

        self.assertFalse(result["min_sample_gate"]["passed"])
        self.assertEqual(result["min_sample_gate"]["rejection_reason"], "trades_forward_below_20")
        self.assertFalse(result["review_to_observation_ready"])
        self.assertFalse(result["candidate_activated"])

    def test_active_context_review_reports_missing_active_profile_but_can_create_review_context(self) -> None:
        result = review_paper_candidate(_btc_candidate(), persist_review=False)
        active = result["active_context_review"]

        self.assertEqual(result["active_context_status"], "paper_rotation_review_missing_active_context")
        self.assertFalse(active["active_profile_exists"])
        self.assertEqual(active["missing_active_context_fields"], ["active_profile"])
        self.assertTrue(active["can_create_paper_review_context"])
        self.assertFalse(active["can_activate"])

    def test_active_context_review_reports_existing_matching_profile(self) -> None:
        review = active_context_review(
            {"symbol": "BTCUSD", "timeframe": "H1"},
            active_profiles=[{"symbol": "BTCUSD", "timeframe": "H1", "profile": "btc_h1_active"}],
            candidate_profile_name="btcusd_h1_tournament_edge_candidate_paper_review_v1",
        )

        self.assertEqual(review["active_context_status"], "active_context_ready")
        self.assertTrue(review["active_profile_exists"])
        self.assertEqual(review["active_profile_symbol"], "BTCUSD")
        self.assertEqual(review["active_profile_timeframe"], "H1")
        self.assertEqual(review["active_profile_name"], "btc_h1_active")
        self.assertEqual(review["missing_active_context_fields"], [])
        self.assertFalse(review["can_activate"])

    def test_known_profile_is_not_renamed(self) -> None:
        profile = resolve_paper_review_profile_name(
            {"symbol": "BTCUSD", "timeframe": "H1", "profile": "btcusd_h1_clean_edge_v1"}
        )

        self.assertEqual(profile, "btcusd_h1_clean_edge_v1")


def _btc_candidate() -> dict[str, object]:
    return {
        "symbol": "BTCUSD",
        "timeframe": "H1",
        "profile": "unknown_profile",
        "family": "tournament_edge",
        "trades_forward": 8,
        "win_rate": 75.0,
        "profit_factor": 19.72,
        "recent_profit_factor": 19.72,
        "expectancy": 55.12,
    }


class _FakeStore:
    def __init__(self) -> None:
        self.upserted: list[dict[str, object]] = []

    def upsert_profile_state(self, payload: dict[str, object], *, critical: bool | None = None) -> dict[str, object]:
        self.upserted.append(dict(payload))
        return {"ok": True, "db_degraded": False, "critical": bool(critical)}


if __name__ == "__main__":
    unittest.main()
