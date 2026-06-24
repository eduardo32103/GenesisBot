from __future__ import annotations

import unittest

from app.telegram.handlers.alerts import (
    AlertHandlerHooks,
    handle_alert_policy,
    handle_alert_strategy,
    handle_dashboard_alerts,
    handle_score_alerts,
)


class DummyMessage:
    pass


class AlertsHandlerTests(unittest.TestCase):
    def _build_hooks(self, *, raise_on_eval: bool = False) -> tuple[AlertHandlerHooks, list[dict], list[int]]:
        replies: list[dict] = []
        eval_limits: list[int] = []

        def evaluate_pending_alert_validations(*, limit: int) -> int:
            eval_limits.append(limit)
            if raise_on_eval:
                raise RuntimeError("forced failure")
            return 7

        def build_alert_validation_report(*, days: int, topn: int) -> str:
            return f"validation:{days}:{topn}"

        def build_alert_policy_report(*, days: int, topn: int) -> str:
            return f"policy:{days}:{topn}"

        def build_alert_strategy_report(*, days: int, topn: int) -> str:
            return f"strategy:{days}:{topn}"

        def reply_to(message: DummyMessage, text: str, **kwargs) -> None:
            replies.append({"message": message, "text": text, "kwargs": kwargs})

        hooks = AlertHandlerHooks(
            evaluate_pending_alert_validations=evaluate_pending_alert_validations,
            build_alert_validation_report=build_alert_validation_report,
            build_alert_policy_report=build_alert_policy_report,
            build_alert_strategy_report=build_alert_strategy_report,
            reply_to=reply_to,
        )
        return hooks, replies, eval_limits

    def test_score_alerts_refreshes_validation_and_replies(self) -> None:
        hooks, replies, eval_limits = self._build_hooks()
        handle_score_alerts(DummyMessage(), hooks=hooks)
        self.assertEqual(eval_limits, [80])
        self.assertEqual(replies[0]["text"], "validation:60:8")
        self.assertEqual(replies[0]["kwargs"]["parse_mode"], "HTML")

    def test_dashboard_alerts_still_replies_if_refresh_fails(self) -> None:
        hooks, replies, eval_limits = self._build_hooks(raise_on_eval=True)
        handle_dashboard_alerts(DummyMessage(), hooks=hooks)
        self.assertEqual(eval_limits, [80])
        self.assertEqual(replies[0]["text"], "validation:60:8")

    def test_policy_and_strategy_use_expected_reports(self) -> None:
        hooks, replies, _ = self._build_hooks()
        handle_alert_policy(DummyMessage(), hooks=hooks)
        handle_alert_strategy(DummyMessage(), hooks=hooks)
        self.assertEqual(replies[0]["text"], "policy:45:8")
        self.assertEqual(replies[1]["text"], "strategy:45:8")


if __name__ == "__main__":
    unittest.main()
