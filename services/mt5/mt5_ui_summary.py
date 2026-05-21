from __future__ import annotations

import json
from pathlib import Path
from typing import Any


HUMAN_REASON_TRANSLATIONS = {
    "no_runtime_snapshot_for_requested_timeframe": "No hay lectura reciente del timeframe solicitado.",
    "risk_governor_pass": "Riesgo dentro de limites.",
    "early_forward_underperformance": "Perfil degradado por bajo rendimiento temprano.",
    "observation_only": "Solo observacion.",
    "paper_forward_candidate": "Candidato en prueba paper.",
    "lockdown": "Bloqueo total por proteccion de cuenta.",
    "risk_governor_block": "Risk Governor bloqueo la senal.",
    "daily_loss_limit_reached": "Limite diario de perdida alcanzado.",
    "weekly_loss_limit_reached": "Limite semanal de perdida alcanzado.",
    "drawdown_limit_reached": "Drawdown maximo alcanzado.",
    "consecutive_loss_lockdown": "Racha de perdidas activa; Genesis bloquea nuevas entradas.",
    "spread_too_high": "Spread demasiado alto para abrir una operacion limpia.",
    "recent_edge_negative": "La ventaja reciente esta negativa.",
    "forward_pf_below_threshold": "Profit factor forward por debajo del umbral.",
    "expectancy_negative": "Expectancy negativa.",
    "market_regime_unclear": "Regimen de mercado poco claro.",
    "snapshot_missing": "Aun no hay snapshot operativo suficiente.",
    "fast_path_snapshot_only": "Lectura rapida desde snapshot; sin orden real.",
}

_ROOT = Path(__file__).resolve().parents[2]
_ROBUST_JSON = _ROOT / "data" / "backtests" / "robust_optimizer_results.json"


def humanize_mt5_reason(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "Sin razon tecnica registrada."
    if ":" in text:
        prefix, suffix = text.split(":", 1)
        prefix_human = HUMAN_REASON_TRANSLATIONS.get(prefix, prefix.replace("_", " "))
        suffix_human = humanize_mt5_reason(suffix)
        return f"{prefix_human} {suffix_human}".strip()
    return HUMAN_REASON_TRANSLATIONS.get(text, text.replace("_", " "))


def load_robust_optimizer_payload(path: Path | None = None) -> dict[str, Any]:
    source = path or _ROBUST_JSON
    try:
        if not source.exists():
            return {"status": "robust_optimizer_missing", "recommendation": "observation_only", "candidates": []}
        payload = json.loads(source.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {"status": "robust_optimizer_invalid", "recommendation": "observation_only", "candidates": []}
    except Exception as exc:
        return {
            "status": "robust_optimizer_read_error",
            "recommendation": "observation_only",
            "candidates": [],
            "error": str(exc)[:240],
        }


def build_mt5_ui_summary(
    *,
    symbol: str = "BTCUSD",
    timeframe: str = "M30",
    risk_state: dict[str, Any] | None = None,
    decision: dict[str, Any] | None = None,
    forward_profile: dict[str, Any] | None = None,
    robust_optimizer: dict[str, Any] | None = None,
) -> dict[str, Any]:
    clean_symbol = str(symbol or "BTCUSD").upper().strip()
    clean_timeframe = str(timeframe or "M30").upper().strip()
    risk = risk_state if isinstance(risk_state, dict) else {}
    mt5_decision = decision if isinstance(decision, dict) else {}
    forward = forward_profile if isinstance(forward_profile, dict) else {}
    robust = robust_optimizer if isinstance(robust_optimizer, dict) else load_robust_optimizer_payload()
    cards = [
        _risk_card(risk),
        _decision_card(mt5_decision),
        _forward_profile_card(forward),
        _robust_optimizer_card(robust),
    ]
    blocked = any(card.get("tone") == "danger" for card in cards)
    candidate = forward.get("status") == "paper_forward_candidate" and bool(forward.get("active"))
    reading = _genesis_reading(clean_symbol, clean_timeframe, risk, mt5_decision, forward, robust)
    return {
        "ok": True,
        "status": "mt5_ui_summary_ready",
        "symbol": clean_symbol,
        "timeframe": clean_timeframe,
        "risk_state": risk.get("risk_state") or "normal",
        "allowed": bool(risk.get("allowed", True)),
        "decision": mt5_decision.get("decision") or "NO_TRADE",
        "profile_status": forward.get("status") or "observation_only",
        "paper_forward_candidate": candidate,
        "needs_attention": blocked,
        "cards": cards,
        "structured": {
            "kind": "mt5_dashboard",
            "title": f"MT5 {clean_symbol} {clean_timeframe}",
            "summary": reading,
            "cards": cards,
            "warning": "No real trading todavia.",
        },
        "genesis_reading": reading,
        "human_translations": dict(HUMAN_REASON_TRANSLATIONS),
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _risk_card(risk: dict[str, Any]) -> dict[str, Any]:
    allowed = bool(risk.get("allowed", True))
    state = str(risk.get("risk_state") or "normal")
    reason = str(risk.get("reason") or ("risk_governor_pass" if allowed else "risk_governor_block"))
    tone = "danger" if state == "lockdown" or not allowed else "warning" if state in {"caution", "defensive"} else "safe"
    return {
        "kind": "risk_state",
        "title": "Estado de Riesgo",
        "tone": tone,
        "headline": humanize_mt5_reason(state),
        "human_reason": humanize_mt5_reason(reason),
        "rows": [
            _row("Estado", humanize_mt5_reason(state), state),
            _row("Puede abrir paper", _yes_no(allowed), allowed),
            _row("Razon", humanize_mt5_reason(reason), reason),
            _row("Perdida diaria", _pct(risk.get("daily_loss_pct")), risk.get("daily_loss_pct")),
            _row("Perdida semanal", _pct(risk.get("weekly_loss_pct")), risk.get("weekly_loss_pct")),
            _row("Drawdown actual", _pct(risk.get("current_drawdown_pct")), risk.get("current_drawdown_pct")),
            _row("Racha de perdidas", str(int(_num(risk.get("consecutive_losses")))), risk.get("consecutive_losses")),
            _row("Spread", "OK" if risk.get("spread_ok", True) else "Alto", risk.get("spread_ok")),
            _row("Edge reciente", "OK" if risk.get("edge_ok", True) else "Debil", risk.get("edge_ok")),
            _row("Multiplicador sugerido", _number_text(risk.get("suggested_lot_multiplier")), risk.get("suggested_lot_multiplier")),
        ],
        "broker_protected": True,
    }


def _decision_card(decision: dict[str, Any]) -> dict[str, Any]:
    action = str(decision.get("decision") or "NO_TRADE")
    reason = str(decision.get("reason") or "snapshot_missing")
    profile = str(decision.get("strategy_profile") or "")
    paper_profile = str(decision.get("paper_forward_candidate_profile") or "")
    return {
        "kind": "mt5_decision",
        "title": "Decision MT5",
        "tone": "safe" if action in {"WAIT", "NO_TRADE"} else "warning",
        "headline": f"{action} paper-only",
        "human_reason": humanize_mt5_reason(reason),
        "rows": [
            _row("Decision", action, action),
            _row("Razon", humanize_mt5_reason(reason), reason),
            _row("Estrategia", profile or "Sin perfil activo", profile),
            _row("Perfil candidato", paper_profile or "Ninguno", paper_profile),
            _row("Broker", "Broker protegido", False),
            _row("Orden ejecutada", _yes_no(bool(decision.get("order_executed"))), decision.get("order_executed")),
            _row("Politica", decision.get("order_policy") or "journal_only_no_broker", decision.get("order_policy")),
        ],
        "broker_protected": not bool(decision.get("broker_touched")) and not bool(decision.get("order_executed")),
    }


def _forward_profile_card(forward: dict[str, Any]) -> dict[str, Any]:
    status = str(forward.get("status") or "observation_only")
    degraded = bool(forward.get("degraded"))
    reason = str(forward.get("degradation_reason") or forward.get("degrade_reason") or status)
    tone = "danger" if degraded else "warning" if status == "paper_forward_candidate" else "safe"
    return {
        "kind": "forward_profile",
        "title": "Perfil Forward",
        "tone": tone,
        "headline": humanize_mt5_reason(status),
        "human_reason": humanize_mt5_reason(reason),
        "rows": [
            _row("Estado", humanize_mt5_reason(status), status),
            _row("Perfil", forward.get("profile") or "Sin candidato", forward.get("profile")),
            _row("Activo", _yes_no(bool(forward.get("active"))), forward.get("active")),
            _row("Degradado", _yes_no(degraded), degraded),
            _row("Razon degradacion", humanize_mt5_reason(reason), reason),
            _row("Aplica a paper", _yes_no(bool(forward.get("applies_to_paper_shadow"))), forward.get("applies_to_paper_shadow")),
            _row("Aplica a real", "No", False),
            _row("Trades forward", str(int(_num(forward.get("trades_forward")))), forward.get("trades_forward")),
            _row("Win rate", _pct(forward.get("win_rate")), forward.get("win_rate")),
            _row("Profit factor", _number_text(forward.get("profit_factor")), forward.get("profit_factor")),
            _row("Expectancy", _number_text(forward.get("expectancy")), forward.get("expectancy")),
            _row("Max drawdown", _number_text(forward.get("max_drawdown")), forward.get("max_drawdown")),
        ],
        "broker_protected": True,
    }


def _robust_optimizer_card(robust: dict[str, Any]) -> dict[str, Any]:
    candidates = robust.get("candidates") if isinstance(robust.get("candidates"), list) else []
    best = robust.get("best_profile") if isinstance(robust.get("best_profile"), dict) else {}
    recommendation = str(robust.get("recommendation") or best.get("recommendation") or "observation_only")
    reasons = best.get("pass_fail_reasons") if isinstance(best.get("pass_fail_reasons"), list) else []
    candidate_count = len(candidates)
    best_profile = str(best.get("profile") or "Sin perfil robusto")
    why = "Candidato paper-forward" if candidate_count else _rejection_text(reasons)
    return {
        "kind": "robust_optimizer",
        "title": "Robust Optimizer",
        "tone": "warning" if candidate_count else "safe",
        "headline": humanize_mt5_reason(recommendation),
        "human_reason": why,
        "rows": [
            _row("Recomendacion", humanize_mt5_reason(recommendation), recommendation),
            _row("Candidatos", str(candidate_count), candidate_count),
            _row("Mejor perfil", best_profile, best.get("profile")),
            _row("Por que", why, reasons),
            _row("PF", _number_text(best.get("profit_factor")), best.get("profit_factor")),
            _row("Expectancy", _number_text(best.get("expectancy")), best.get("expectancy")),
            _row("Drawdown", _number_text(best.get("max_drawdown")), best.get("max_drawdown")),
            _row("Aviso", "No real trading todavia", False),
        ],
        "candidates": candidates[:5],
        "best_profile": best,
        "broker_protected": True,
    }


def _genesis_reading(
    symbol: str,
    timeframe: str,
    risk: dict[str, Any],
    decision: dict[str, Any],
    forward: dict[str, Any],
    robust: dict[str, Any],
) -> str:
    risk_text = humanize_mt5_reason(risk.get("reason") or "risk_governor_pass")
    decision_text = humanize_mt5_reason(decision.get("reason") or "snapshot_missing")
    profile_status = humanize_mt5_reason(forward.get("status") or "observation_only")
    robust_rec = humanize_mt5_reason(robust.get("recommendation") or "observation_only")
    return (
        f"{symbol} {timeframe}: {profile_status}. Risk Governor: {risk_text}. "
        f"Decision MT5: {decision.get('decision') or 'NO_TRADE'} por {decision_text}. "
        f"Robust optimizer: {robust_rec}. Broker protegido: broker_touched=false, order_executed=false."
    )


def _rejection_text(reasons: list[Any]) -> str:
    clean = [humanize_mt5_reason(item) for item in reasons if item]
    if not clean:
        return "No hay candidato robusto activo; mantener observation_only."
    return "; ".join(clean[:3])


def _row(label: str, value: Any, raw: Any = None) -> dict[str, Any]:
    return {"label": label, "value": str(value), "raw": raw}


def _yes_no(value: bool) -> str:
    return "Si" if value else "No"


def _num(value: Any) -> float:
    try:
        if value in (None, "", "None"):
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def _number_text(value: Any) -> str:
    number = _num(value)
    return f"{number:.4f}".rstrip("0").rstrip(".") if number else "0"


def _pct(value: Any) -> str:
    return f"{_num(value):.2f}%"
