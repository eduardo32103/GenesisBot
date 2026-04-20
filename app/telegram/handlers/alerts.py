from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class AlertHandlerHooks:
    evaluate_pending_alert_validations: Callable[..., int]
    build_alert_validation_report: Callable[..., str]
    build_alert_policy_report: Callable[..., str]
    build_alert_strategy_report: Callable[..., str]
    reply_to: Callable[..., Any]
    logger: logging.Logger | None = None


def handle_score_alerts(message: object, *, hooks: AlertHandlerHooks) -> None:
    logger = hooks.logger or logging.getLogger("genesis.telegram.alerts")
    try:
        hooks.evaluate_pending_alert_validations(limit=80)
    except Exception as exc:
        logger.error("ALERT SCORE: error actualizando antes del reporte manual: %s", exc)
    hooks.reply_to(message, hooks.build_alert_validation_report(days=60, topn=8), parse_mode="HTML")


def handle_dashboard_alerts(message: object, *, hooks: AlertHandlerHooks) -> None:
    logger = hooks.logger or logging.getLogger("genesis.telegram.alerts")
    try:
        hooks.evaluate_pending_alert_validations(limit=80)
    except Exception as exc:
        logger.error("ALERT SCORE: error actualizando dashboard manual: %s", exc)
    hooks.reply_to(message, hooks.build_alert_validation_report(days=60, topn=8), parse_mode="HTML")


def handle_alert_policy(message: object, *, hooks: AlertHandlerHooks) -> None:
    hooks.reply_to(message, hooks.build_alert_policy_report(days=45, topn=8), parse_mode="HTML")


def handle_alert_strategy(message: object, *, hooks: AlertHandlerHooks) -> None:
    hooks.reply_to(message, hooks.build_alert_strategy_report(days=45, topn=8), parse_mode="HTML")
