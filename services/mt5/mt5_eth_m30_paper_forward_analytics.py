from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

from services.mt5.mt5_eth_m30_paper_forward_candidate import ETH_M30_CANDIDATE_PROFILE, ETH_M30_PROFILE_RULES


DEFAULT_MONITOR_CSV = Path("data") / "backtests" / "multisymbol" / "eth_m30_paper_forward_monitor_log.csv"
DEFAULT_MONITOR_JSON = Path("data") / "backtests" / "multisymbol" / "eth_m30_paper_forward_monitor_log.json"
DEFAULT_OUTPUT_DIR = Path("data") / "backtests" / "multisymbol"
SUMMARY_FILENAME = "eth_m30_paper_forward_analytics_summary.md"
JSON_FILENAME = "eth_m30_paper_forward_analytics.json"


def run_eth_m30_paper_forward_analytics(
    *,
    csv_path: str | Path = DEFAULT_MONITOR_CSV,
    json_path: str | Path | None = DEFAULT_MONITOR_JSON,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    shadow_trades: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    snapshots = load_eth_m30_monitor_snapshots(csv_path=csv_path, json_path=json_path)
    result = analyze_eth_m30_paper_forward_snapshots(snapshots, shadow_trades=shadow_trades)
    paths = write_eth_m30_paper_forward_analytics_outputs(result, output_dir=output_dir)
    result["output_paths"] = {name: str(path) for name, path in paths.items()}
    return result


def load_eth_m30_monitor_snapshots(*, csv_path: str | Path, json_path: str | Path | None = None) -> list[dict[str, Any]]:
    json_candidate = Path(json_path) if json_path else None
    if json_candidate and json_candidate.exists():
        data = json.loads(json_candidate.read_text(encoding="utf-8"))
        if isinstance(data, dict) and isinstance(data.get("snapshots"), list):
            return [item for item in data["snapshots"] if isinstance(item, dict)]
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
    csv_candidate = Path(csv_path)
    if not csv_candidate.exists():
        return []
    with csv_candidate.open("r", newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def analyze_eth_m30_paper_forward_snapshots(
    snapshots: list[dict[str, Any]],
    *,
    shadow_trades: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    rows = [_extract_row(item) for item in snapshots if isinstance(item, dict)]
    total = len(rows)
    complete_count = sum(1 for row in rows if row["runtime_snapshot_complete"])
    bar_context_count = sum(1 for row in rows if row["runtime_snapshot_context"] == "bar_context")
    active_true_count = sum(1 for row in rows if row["active"])
    applies_to_paper_shadow_count = sum(1 for row in rows if row["applies_to_paper_shadow"])
    risk_blocks = sum(1 for row in rows if row["risk_governor_blocked"])
    open_shadow_total = sum(int(_number(row["open_shadow_count"]) or 0) for row in rows)
    latest_open_shadow = int(_number(rows[-1]["open_shadow_count"]) or 0) if rows else 0
    observed_broker_touched = sum(1 for row in rows if row["broker_touched"])
    observed_order_executed = sum(1 for row in rows if row["order_executed"])
    order_policies = _counts(row["order_policy"] or "journal_only_no_broker" for row in rows)
    decision_counts = _counts(row["decision"] or "UNKNOWN" for row in rows)
    reason_counts = _counts(row["decision_reason"] or "unknown" for row in rows)
    score_rows = [row for row in rows if row.get("score") is not None]
    score_thresholds = _score_thresholds()
    near_threshold = _near_threshold_counts(rows, score_thresholds)
    bottleneck = _score_component_bottleneck(rows, score_thresholds)
    bottleneck_reasons = _bottleneck_reason_ranking(rows)
    near_miss_counts = _near_miss_counts(rows)
    top_near_misses = _top_near_miss_rows(rows)
    momentum_gate = _momentum_gate_report(rows, score_thresholds)
    max_open_diagnostic = _max_open_trades_diagnostic(rows, shadow_trades or [])
    shadow_stats = _shadow_trade_stats(shadow_trades or [], rows)
    recommendation_actions = _recommendation_actions(
        total,
        reason_counts,
        risk_blocks,
        observed_broker_touched,
        observed_order_executed,
        bottleneck_component=str(bottleneck.get("dominant_component") or ""),
        shadow_occupancy_inconsistent=bool(max_open_diagnostic.get("inconsistency_detected")),
    )
    result = {
        "ok": True,
        "status": "eth_m30_paper_forward_analytics_ready",
        "symbol": "ETHUSD",
        "timeframe": "M30",
        "profile": ETH_M30_CANDIDATE_PROFILE,
        "samples_total": total,
        "runtime_snapshot_complete_count": complete_count,
        "runtime_snapshot_complete_pct": _pct(complete_count, total),
        "bar_context_count": bar_context_count,
        "bar_context_pct": _pct(bar_context_count, total),
        "active_true_count": active_true_count,
        "active_true_pct": _pct(active_true_count, total),
        "applies_to_paper_shadow_count": applies_to_paper_shadow_count,
        "applies_to_paper_shadow_pct": _pct(applies_to_paper_shadow_count, total),
        "decision_counts": decision_counts,
        "top_decision_reasons": reason_counts,
        "risk_governor_block_count": risk_blocks,
        "score_thresholds": score_thresholds,
        "score_distributions": {
            "score": _distribution(row["score"] for row in score_rows),
            "trend_score": _distribution(row["trend_score"] for row in rows),
            "momentum_score": _distribution(row["momentum_score"] for row in rows),
            "volatility_score": _distribution(row["volatility_score"] for row in rows),
        },
        "near_threshold_counts": near_threshold,
        "near_miss_counts": near_miss_counts,
        "score_gap_distribution": _distribution(_score_gap(row) for row in rows),
        "momentum_gap_distribution": _distribution(row["momentum_gap_to_threshold"] for row in rows),
        "score_component_bottleneck": bottleneck,
        "bottleneck_component_ranking": bottleneck.get("component_ranking", []),
        "bottleneck_reason_ranking": bottleneck_reasons,
        "top_near_miss_timestamps": top_near_misses,
        "momentum_gate_report": momentum_gate,
        "score_pass_momentum_fail_count": momentum_gate.get("score_pass_momentum_fail_count", 0),
        "top_momentum_near_misses": momentum_gate.get("top_momentum_near_misses", []),
        "momentum_fail_by_session": momentum_gate.get("momentum_fail_by_session", {}),
        "momentum_fail_by_regime": momentum_gate.get("momentum_fail_by_regime", {}),
        "max_open_trades_diagnostic": max_open_diagnostic,
        "shadow_occupancy_inconsistency": bool(max_open_diagnostic.get("inconsistency_detected")),
        "human_bottleneck_explanation": _human_bottleneck_explanation(momentum_gate, bottleneck, max_open_diagnostic),
        "session_distribution": _counts(row["session"] or "unknown" for row in rows),
        "regime_distribution": _counts(row["market_regime"] or "unknown" for row in rows),
        "spread_distribution": _distribution(row["spread"] for row in rows),
        "open_shadow_trades_observed_sum": open_shadow_total,
        "open_shadow_trades_latest": latest_open_shadow,
        "closed_shadow_trades": shadow_stats["closed_shadow_trades"],
        "paper_pnl": shadow_stats["paper_pnl"],
        "shadow_trade_stats": shadow_stats,
        "observed_broker_touched_count": observed_broker_touched,
        "observed_order_executed_count": observed_order_executed,
        "observed_order_policy_counts": order_policies,
        "recommendation": recommendation_actions[0] if recommendation_actions else "continue_observation",
        "recommendation_actions": recommendation_actions,
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        "applies_to_real_trading": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
        "created_at": _now(),
    }
    result["summary_markdown"] = eth_m30_paper_forward_analytics_summary_markdown(result)
    return result


def write_eth_m30_paper_forward_analytics_outputs(result: dict[str, Any], *, output_dir: str | Path = DEFAULT_OUTPUT_DIR) -> dict[str, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / JSON_FILENAME
    summary_path = root / SUMMARY_FILENAME
    json_path.write_text(json.dumps(_jsonable(result), indent=2, sort_keys=True, ensure_ascii=True), encoding="utf-8")
    summary_path.write_text(str(result.get("summary_markdown") or eth_m30_paper_forward_analytics_summary_markdown(result)), encoding="utf-8")
    return {"json": json_path, "summary": summary_path}


def eth_m30_paper_forward_analytics_summary_markdown(result: dict[str, Any]) -> str:
    bottleneck = result.get("score_component_bottleneck") if isinstance(result.get("score_component_bottleneck"), dict) else {}
    distributions = result.get("score_distributions") if isinstance(result.get("score_distributions"), dict) else {}
    near = result.get("near_threshold_counts") if isinstance(result.get("near_threshold_counts"), dict) else {}
    near_misses = result.get("near_miss_counts") if isinstance(result.get("near_miss_counts"), dict) else {}
    max_open = result.get("max_open_trades_diagnostic") if isinstance(result.get("max_open_trades_diagnostic"), dict) else {}
    lines = [
        "# ETHUSD M30 Paper-Forward Analytics",
        "",
        "Paper-only diagnostic for ETHUSD/M30 observation logs. It does not change strategy, thresholds, promoted profiles, forward state, or broker state.",
        "",
        "## Observation",
        f"- Samples total: `{result.get('samples_total', 0)}`",
        f"- Runtime snapshot complete: `{result.get('runtime_snapshot_complete_count', 0)}` (`{result.get('runtime_snapshot_complete_pct', 0.0)}%`)",
        f"- Bar context: `{result.get('bar_context_count', 0)}` (`{result.get('bar_context_pct', 0.0)}%`)",
        f"- Active true count: `{result.get('active_true_count', 0)}`",
        f"- Applies to paper shadow count: `{result.get('applies_to_paper_shadow_count', 0)}`",
        f"- Decision counts: `{result.get('decision_counts', {})}`",
        f"- Top decision reasons: `{result.get('top_decision_reasons', {})}`",
        f"- RiskGovernor blocks: `{result.get('risk_governor_block_count', 0)}`",
        "",
        "## Score Components",
        f"- Score distribution: `{distributions.get('score', {})}`",
        f"- Trend distribution: `{distributions.get('trend_score', {})}`",
        f"- Momentum distribution: `{distributions.get('momentum_score', {})}`",
        f"- Volatility distribution: `{distributions.get('volatility_score', {})}`",
        f"- Near threshold counts: `{near}`",
        f"- Near-miss counts by score gap: `{near_misses}`",
        f"- Score gap distribution: `{result.get('score_gap_distribution', {})}`",
        f"- Momentum gap distribution: `{result.get('momentum_gap_distribution', {})}`",
        f"- Bottleneck: `{bottleneck.get('dominant_component', '')}`",
        f"- Bottleneck reason: `{bottleneck.get('reason', '')}`",
        f"- Bottleneck ranking: `{result.get('bottleneck_component_ranking', [])}`",
        f"- Bottleneck reason ranking: `{result.get('bottleneck_reason_ranking', [])}`",
        f"- Top near-miss timestamps: `{result.get('top_near_miss_timestamps', [])}`",
        f"- Score passed but momentum failed: `{result.get('score_pass_momentum_fail_count', 0)}`",
        f"- Momentum fail by session: `{result.get('momentum_fail_by_session', {})}`",
        f"- Momentum fail by regime: `{result.get('momentum_fail_by_regime', {})}`",
        f"- Top momentum near-misses: `{result.get('top_momentum_near_misses', [])}`",
        f"- Human explanation: `{result.get('human_bottleneck_explanation', '')}`",
        "",
        "## Context",
        f"- Session distribution: `{result.get('session_distribution', {})}`",
        f"- Regime distribution: `{result.get('regime_distribution', {})}`",
        f"- Spread distribution: `{result.get('spread_distribution', {})}`",
        "",
        "## Shadow/Paper",
        f"- Open shadow trades observed sum: `{result.get('open_shadow_trades_observed_sum', 0)}`",
        f"- Open shadow trades latest: `{result.get('open_shadow_trades_latest', 0)}`",
        f"- Closed shadow trades: `{result.get('closed_shadow_trades', 0)}`",
        f"- Paper P/L: `{result.get('paper_pnl', 0.0)}`",
        "",
        "## Max Open Trades Diagnostic",
        f"- Max-open blocks: `{max_open.get('max_open_block_count', 0)}`",
        f"- Inconsistency detected: `{max_open.get('inconsistency_detected', False)}`",
        f"- Inconsistent rows: `{max_open.get('inconsistent_rows', [])}`",
        f"- Block rows: `{max_open.get('max_open_block_rows', [])}`",
        f"- Shadow store review: `{max_open.get('shadow_store_review', {})}`",
        "",
        "## Recommendation",
    ]
    for action in result.get("recommendation_actions", []):
        lines.append(f"- `{action}`")
    lines.extend(
        [
            "",
            "## Safety",
            f"- broker_touched=`{result.get('broker_touched', False)}`",
            f"- order_executed=`{result.get('order_executed', False)}`",
            f"- order_policy=`{result.get('order_policy', 'journal_only_no_broker')}`",
            "- No real trading.",
            "- No order_send.",
            "- No threshold relaxation.",
            "- No automatic promotion.",
        ]
    )
    return "\n".join(lines) + "\n"


def _extract_row(snapshot: dict[str, Any]) -> dict[str, Any]:
    raw = snapshot.get("raw") if isinstance(snapshot.get("raw"), dict) else {}
    decision = raw.get("decision") if isinstance(raw.get("decision"), dict) else {}
    forward = raw.get("forward_profile_state") if isinstance(raw.get("forward_profile_state"), dict) else {}
    tick = decision.get("last_tick") if isinstance(decision.get("last_tick"), dict) else {}
    open_trades = raw.get("shadow_trades_open") if isinstance(raw.get("shadow_trades_open"), dict) else {}
    reason = snapshot.get("decision_reason") or decision.get("reason") or ""
    risk_allowed = _bool(snapshot.get("risk_governor_allowed"), default=_bool(decision.get("risk_governor_allowed"), default=True))
    risk_reason = str(snapshot.get("risk_governor_reason") or decision.get("risk_governor_reason") or "").strip()
    thresholds = _score_thresholds()
    score = _first_number(snapshot.get("strategy_score"), snapshot.get("score"), decision.get("strategy_score"), tick.get("score"), tick.get("final_score"), tick.get("entry_quality_score"), decision.get("score"))
    min_score = _first_number(
        snapshot.get("min_score"),
        decision.get("min_score"),
        (decision.get("component_thresholds") or {}).get("score") if isinstance(decision.get("component_thresholds"), dict) else None,
    )
    if min_score is None and score is not None:
        min_score = thresholds["score"]
    score_gap = _first_number(snapshot.get("score_gap_to_threshold"), decision.get("score_gap_to_threshold"))
    if score_gap is None and score is not None and min_score is not None:
        score_gap = score - min_score
    trend_score = _first_number(snapshot.get("trend_score"), decision.get("trend_score"), tick.get("trend_score"))
    momentum_score = _first_number(snapshot.get("momentum_score"), decision.get("momentum_score"), tick.get("momentum_score"))
    volatility_score = _first_number(snapshot.get("volatility_score"), decision.get("volatility_score"), tick.get("volatility_score"))
    min_momentum_score = _first_number(snapshot.get("min_momentum_score"), decision.get("min_momentum_score"), thresholds["momentum_score"])
    min_trend_score = _first_number(snapshot.get("min_trend_score"), decision.get("min_trend_score"), thresholds["trend_score"])
    min_volatility_score = _first_number(snapshot.get("min_volatility_score"), decision.get("min_volatility_score"), thresholds["volatility_score"])
    momentum_gap = _first_number(snapshot.get("momentum_gap_to_threshold"), decision.get("momentum_gap_to_threshold"))
    if momentum_gap is None and momentum_score is not None and min_momentum_score is not None:
        momentum_gap = momentum_score - min_momentum_score
    trend_gap = _first_number(snapshot.get("trend_gap_to_threshold"), decision.get("trend_gap_to_threshold"))
    if trend_gap is None and trend_score is not None and min_trend_score is not None:
        trend_gap = trend_score - min_trend_score
    volatility_gap = _first_number(snapshot.get("volatility_gap_to_threshold"), decision.get("volatility_gap_to_threshold"))
    if volatility_gap is None and volatility_score is not None and min_volatility_score is not None:
        volatility_gap = volatility_score - min_volatility_score
    failed_components = _failed_components(snapshot.get("failed_components"), decision.get("failed_components"))
    if not failed_components:
        failed_components = _derive_failed_components(
            score=score,
            min_score=min_score,
            trend_score=trend_score,
            momentum_score=momentum_score,
            volatility_score=volatility_score,
            thresholds=thresholds,
        )
    open_shadow_count = _number(snapshot.get("open_shadow_count") or open_trades.get("open_count")) or 0
    shadow_open_endpoint_count = _number(snapshot.get("shadow_open_endpoint_count") or open_trades.get("open_count")) or 0
    risk_governor_open_count = (
        _first_number(
            snapshot.get("risk_governor_open_trades_count"),
            decision.get("risk_governor_open_trades_count"),
            ((decision.get("risk_governor") if isinstance(decision.get("risk_governor"), dict) else {}) or {}).get("open_trades_count"),
        )
        or 0
    )
    source_of_open_trade_count = str(snapshot.get("source_of_open_trade_count") or decision.get("source_of_open_trade_count") or "").strip()
    if not source_of_open_trade_count:
        if shadow_open_endpoint_count:
            source_of_open_trade_count = "shadow_trades_open_endpoint"
        elif open_shadow_count:
            source_of_open_trade_count = "monitor_open_shadow_count"
        elif risk_governor_open_count:
            source_of_open_trade_count = str(snapshot.get("risk_governor_open_trades_source") or decision.get("risk_governor_open_trades_source") or "risk_governor")
        else:
            source_of_open_trade_count = "none"
    return {
        "timestamp": snapshot.get("timestamp") or "",
        "symbol": snapshot.get("symbol") or decision.get("symbol") or "ETHUSD",
        "timeframe": snapshot.get("timeframe") or decision.get("timeframe") or "M30",
        "active": _bool(snapshot.get("active"), default=_bool(forward.get("active"))),
        "applies_to_paper_shadow": _bool(snapshot.get("applies_to_paper_shadow"), default=_bool(forward.get("applies_to_paper_shadow"))),
        "decision": snapshot.get("decision") or decision.get("decision") or "",
        "decision_reason": reason,
        "risk_governor_blocked": str(reason).startswith("risk_governor_block") or (not risk_allowed and risk_reason not in {"", "risk_governor_pass"}),
        "runtime_snapshot_complete": _bool(
            snapshot.get("runtime_snapshot_complete"),
            default=_bool(decision.get("runtime_snapshot_complete"), default=_bool(forward.get("runtime_snapshot_complete"), default=_bool(tick.get("runtime_snapshot_complete")))),
        ),
        "runtime_snapshot_context": str(
            snapshot.get("runtime_snapshot_context") or decision.get("runtime_snapshot_context") or forward.get("runtime_snapshot_context") or tick.get("runtime_snapshot_context") or ""
        ).strip(),
        "score": score,
        "min_score": min_score,
        "score_gap_to_threshold": score_gap,
        "trend_score": trend_score,
        "min_trend_score": min_trend_score,
        "trend_gap_to_threshold": trend_gap,
        "momentum_score": momentum_score,
        "min_momentum_score": min_momentum_score,
        "momentum_gap_to_threshold": momentum_gap,
        "volatility_score": volatility_score,
        "min_volatility_score": min_volatility_score,
        "volatility_gap_to_threshold": volatility_gap,
        "failed_components": failed_components,
        "market_regime": str(snapshot.get("market_regime") or tick.get("market_regime") or tick.get("regime") or decision.get("market_regime") or "").strip(),
        "session": str(snapshot.get("session") or tick.get("session") or decision.get("session") or "").strip(),
        "hour": _number(tick.get("hour") or decision.get("hour")),
        "spread": _first_number(snapshot.get("spread"), tick.get("spread"), decision.get("spread")),
        "open_shadow_count": open_shadow_count,
        "open_shadow_trade_ids": _id_list(snapshot.get("open_shadow_trade_ids"), decision.get("open_shadow_trade_ids"), open_trades.get("trades")),
        "blocking_shadow_trade_id": str(snapshot.get("blocking_shadow_trade_id") or decision.get("blocking_shadow_trade_id") or decision.get("risk_governor_open_trade_id") or ""),
        "risk_governor_reason": risk_reason,
        "risk_governor_open_trades_count": risk_governor_open_count,
        "risk_governor_open_trades_source": str(snapshot.get("risk_governor_open_trades_source") or decision.get("risk_governor_open_trades_source") or ""),
        "risk_governor_open_trade_id": str(snapshot.get("risk_governor_open_trade_id") or decision.get("risk_governor_open_trade_id") or ""),
        "risk_governor_open_trade_status": str(snapshot.get("risk_governor_open_trade_status") or decision.get("risk_governor_open_trade_status") or ""),
        "shadow_open_endpoint_count": shadow_open_endpoint_count,
        "source_of_open_trade_count": source_of_open_trade_count,
        "shadow_occupancy_inconsistent": _bool(snapshot.get("shadow_occupancy_inconsistent")),
        "broker_touched": _bool(snapshot.get("broker_touched"), default=_bool(decision.get("broker_touched"))),
        "order_executed": _bool(snapshot.get("order_executed"), default=_bool(decision.get("order_executed"))),
        "order_policy": str(snapshot.get("order_policy") or decision.get("order_policy") or forward.get("order_policy") or "journal_only_no_broker"),
    }


def _score_thresholds() -> dict[str, float]:
    return {
        "score": float(_number(ETH_M30_PROFILE_RULES.get("min_score")) or 58.0),
        "momentum_score": float(_number(ETH_M30_PROFILE_RULES.get("min_momentum_score")) or 50.0),
        "trend_score": float(_number(ETH_M30_PROFILE_RULES.get("min_trend_score")) or 50.0),
        "volatility_score": float(_number(ETH_M30_PROFILE_RULES.get("min_volatility_score")) or 35.0),
    }


def _near_threshold_counts(rows: list[dict[str, Any]], thresholds: dict[str, float]) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    for key, threshold in thresholds.items():
        values = [_number(row.get(key)) for row in rows]
        values = [float(value) for value in values if value is not None]
        below_values = [value for value in values if value < threshold]
        result[key] = {
            "observed": len(values),
            "below": len(below_values),
            "within_1_below": sum(1 for value in below_values if threshold - 1 <= value < threshold),
            "within_2_below": sum(1 for value in below_values if threshold - 2 <= value < threshold),
            "within_3_below": sum(1 for value in below_values if threshold - 3 <= value < threshold),
            "within_5_below": sum(1 for value in below_values if threshold - 5 <= value < threshold),
            "within_10_below": sum(1 for value in below_values if threshold - 10 <= value < threshold),
            "passed": sum(1 for value in values if value >= threshold),
        }
    return result


def _near_miss_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    gaps = [_score_gap(row) for row in rows]
    gaps = [float(gap) for gap in gaps if gap is not None and gap < 0]
    return {
        "within_1": sum(1 for gap in gaps if -1.0 <= gap < 0.0),
        "within_2": sum(1 for gap in gaps if -2.0 <= gap < 0.0),
        "within_3": sum(1 for gap in gaps if -3.0 <= gap < 0.0),
        "within_5": sum(1 for gap in gaps if -5.0 <= gap < 0.0),
    }


def _top_near_miss_rows(rows: list[dict[str, Any]], *, limit: int = 10) -> list[dict[str, Any]]:
    candidates = []
    for row in rows:
        gap = _score_gap(row)
        if gap is None or gap >= 0:
            continue
        candidates.append(
            {
                "timestamp": row.get("timestamp") or "",
                "decision_reason": row.get("decision_reason") or "",
                "score": row.get("score"),
                "min_score": row.get("min_score") or _score_thresholds()["score"],
                "score_gap_to_threshold": round(float(gap), 4),
                "momentum_score": row.get("momentum_score"),
                "trend_score": row.get("trend_score"),
                "volatility_score": row.get("volatility_score"),
                "failed_components": row.get("failed_components") or [],
            }
        )
    return sorted(candidates, key=lambda item: item["score_gap_to_threshold"], reverse=True)[:limit]


def _score_gap(row: dict[str, Any]) -> float | None:
    gap = _number(row.get("score_gap_to_threshold"))
    if gap is not None:
        return gap
    score = _number(row.get("score"))
    threshold = _number(row.get("min_score")) or _score_thresholds()["score"]
    if score is None:
        return None
    return score - threshold


def _momentum_gate_report(rows: list[dict[str, Any]], thresholds: dict[str, float]) -> dict[str, Any]:
    score_pass_momentum_fail = [
        row
        for row in rows
        if (_number(row.get("score")) is not None and float(_number(row.get("score")) or 0.0) >= float(row.get("min_score") or thresholds["score"]))
        and _momentum_failed(row)
    ]
    momentum_fail_rows = [row for row in rows if _momentum_failed(row)]
    return {
        "score_pass_momentum_fail_count": len(score_pass_momentum_fail),
        "score_pass_momentum_fail_pct": _pct(len(score_pass_momentum_fail), len(rows)),
        "momentum_fail_count": len(momentum_fail_rows),
        "momentum_gap_distribution": _distribution(row.get("momentum_gap_to_threshold") for row in rows),
        "top_momentum_near_misses": _top_momentum_near_miss_rows(rows),
        "momentum_fail_by_session": _counts(row.get("session") or "unknown" for row in momentum_fail_rows),
        "momentum_fail_by_regime": _counts(row.get("market_regime") or "unknown" for row in momentum_fail_rows),
        "high_score_momentum_low_is_repeated": len(score_pass_momentum_fail) >= 3,
    }


def _momentum_failed(row: dict[str, Any]) -> bool:
    failed = row.get("failed_components") if isinstance(row.get("failed_components"), list) else []
    if "momentum_below_threshold" in failed:
        return True
    reason = str(row.get("decision_reason") or "")
    if "momentum_score_low" in reason:
        return True
    gap = _number(row.get("momentum_gap_to_threshold"))
    return gap is not None and gap < 0


def _top_momentum_near_miss_rows(rows: list[dict[str, Any]], *, limit: int = 10) -> list[dict[str, Any]]:
    candidates = []
    for row in rows:
        gap = _number(row.get("momentum_gap_to_threshold"))
        if gap is None or gap >= 0:
            continue
        candidates.append(
            {
                "timestamp": row.get("timestamp") or "",
                "decision_reason": row.get("decision_reason") or "",
                "strategy_score": row.get("score"),
                "score_gap_to_threshold": _score_gap(row),
                "momentum_score": row.get("momentum_score"),
                "min_momentum_score": row.get("min_momentum_score"),
                "momentum_gap_to_threshold": round(float(gap), 4),
                "trend_score": row.get("trend_score"),
                "volatility_score": row.get("volatility_score"),
                "session": row.get("session") or "",
                "market_regime": row.get("market_regime") or "",
                "failed_components": row.get("failed_components") or [],
            }
        )
    return sorted(candidates, key=lambda item: item["momentum_gap_to_threshold"], reverse=True)[:limit]


def _max_open_trades_diagnostic(rows: list[dict[str, Any]], shadow_trades: list[dict[str, Any]]) -> dict[str, Any]:
    blocked = [row for row in rows if str(row.get("decision_reason") or "") == "risk_governor_block:max_open_trades_reached"]
    block_rows = [_max_open_row(row) for row in blocked]
    inconsistent = [
        item
        for item in block_rows
        if int(_number(item.get("open_shadow_count")) or 0) == 0
        and int(_number(item.get("shadow_open_endpoint_count")) or 0) == 0
    ]
    visible_blocks = [
        item
        for item in block_rows
        if int(_number(item.get("open_shadow_count")) or 0) > 0
        or int(_number(item.get("shadow_open_endpoint_count")) or 0) > 0
        or bool(item.get("open_shadow_trade_ids"))
    ]
    journal_open = [trade for trade in shadow_trades if str(trade.get("status") or trade.get("lifecycle_status") or "").casefold() == "open"]
    journal_closed = [trade for trade in shadow_trades if str(trade.get("status") or trade.get("lifecycle_status") or "").casefold() == "closed"]
    return {
        "max_open_block_count": len(blocked),
        "max_open_block_rows": block_rows[:10],
        "inconsistency_detected": bool(inconsistent),
        "inconsistent_rows": inconsistent[:10],
        "shadow_store_review": {
            "blocked_rows_with_visible_open_trade": len(visible_blocks),
            "blocked_rows_with_endpoint_zero": sum(1 for item in block_rows if int(_number(item.get("shadow_open_endpoint_count")) or 0) == 0),
            "latest_open_shadow_count": int(_number(rows[-1].get("open_shadow_count")) or 0) if rows else 0,
            "journal_shadow_trades_available": bool(shadow_trades),
            "journal_open_trades": len(journal_open),
            "journal_closed_trades": len(journal_closed),
        },
        "recommendation": "investigate_stale_runtime_open_shadow_trade_without_relaxing_risk_governor" if inconsistent else "no_shadow_occupancy_inconsistency_detected",
    }


def _max_open_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "timestamp": row.get("timestamp") or "",
        "decision_reason": row.get("decision_reason") or "",
        "open_shadow_count": int(_number(row.get("open_shadow_count")) or 0),
        "open_shadow_trade_ids": row.get("open_shadow_trade_ids") if isinstance(row.get("open_shadow_trade_ids"), list) else [],
        "shadow_open_endpoint_count": int(_number(row.get("shadow_open_endpoint_count")) or 0),
        "blocking_shadow_trade_id": row.get("blocking_shadow_trade_id") or "",
        "risk_governor_open_trades_count": int(_number(row.get("risk_governor_open_trades_count")) or 0),
        "risk_governor_open_trades_source": row.get("risk_governor_open_trades_source") or "",
        "risk_governor_open_trade_id": row.get("risk_governor_open_trade_id") or "",
        "risk_governor_open_trade_status": row.get("risk_governor_open_trade_status") or "",
        "source_of_open_trade_count": row.get("source_of_open_trade_count") or "",
    }


def _human_bottleneck_explanation(
    momentum_gate: dict[str, Any],
    bottleneck: dict[str, Any],
    max_open_diagnostic: dict[str, Any],
) -> str:
    score_pass_momentum_fail = int(momentum_gate.get("score_pass_momentum_fail_count") or 0)
    dominant = str(bottleneck.get("dominant_component") or "")
    if score_pass_momentum_fail:
        return (
            f"El score total ya supera el minimo en {score_pass_momentum_fail} muestras, "
            "pero la compuerta de momentum sigue por debajo del minimo; no conviene relajar umbrales sin mas observacion."
        )
    if bool(max_open_diagnostic.get("inconsistency_detected")):
        return "RiskGovernor reporto max_open_trades mientras el endpoint open mostro cero; revisar estado shadow stale antes de tocar estrategia."
    if dominant:
        return f"El cuello dominante observado es {dominant}; mantener observacion paper-only."
    return "No hay suficientes muestras de componentes para explicar el cuello con confianza."


def _score_component_bottleneck(rows: list[dict[str, Any]], thresholds: dict[str, float]) -> dict[str, Any]:
    score_low_rows = [row for row in rows if "score_too_low" in str(row.get("decision_reason") or "")]
    target_rows = score_low_rows or rows
    component_stats: dict[str, dict[str, Any]] = {}
    for key in ("momentum_score", "trend_score", "volatility_score"):
        threshold = thresholds[key]
        values = [_number(row.get(key)) for row in target_rows]
        values = [float(value) for value in values if value is not None]
        margins = [value - threshold for value in values]
        component_stats[key] = {
            "observed": len(values),
            "below_threshold_count": sum(1 for margin in margins if margin < 0),
            "avg_margin": round(mean(margins), 4) if margins else None,
            "avg_value": round(mean(values), 4) if values else None,
            "threshold": threshold,
        }
    ranked = sorted(
        component_stats.items(),
        key=lambda item: (
            -(item[1]["below_threshold_count"] or 0),
            item[1]["avg_margin"] if item[1]["avg_margin"] is not None else 9999,
        ),
    )
    dominant = ranked[0][0] if ranked else "unknown"
    score_values = [_number(row.get("score")) for row in target_rows]
    score_values = [float(value) for value in score_values if value is not None]
    score_avg = round(mean(score_values), 4) if score_values else None
    reason = "no_score_samples"
    if dominant != "unknown":
        reason = f"{dominant} has the weakest threshold margin during score_too_low samples"
    if ranked and (ranked[0][1]["below_threshold_count"] or 0) == 0 and score_avg is not None and score_avg < thresholds["score"]:
        dominant = "composite_score_threshold"
        reason = "components mostly pass, but weighted composite score remains below min_score"
    return {
        "dominant_component": dominant,
        "reason": reason,
        "score_too_low_samples": len(score_low_rows),
        "score_avg": score_avg,
        "score_threshold": thresholds["score"],
        "component_stats": component_stats,
        "component_ranking": [
            {
                "component": key,
                "below_threshold_count": stats.get("below_threshold_count", 0),
                "avg_margin": stats.get("avg_margin"),
                "avg_value": stats.get("avg_value"),
                "threshold": stats.get("threshold"),
            }
            for key, stats in ranked
        ],
    }


def _bottleneck_reason_ranking(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counters = {
        "momentum_below_threshold": 0,
        "score_below_threshold": 0,
        "session_blocked": 0,
        "regime_blocked": 0,
        "risk_governor_block": 0,
    }
    for row in rows:
        reason = str(row.get("decision_reason") or "")
        failed = row.get("failed_components") if isinstance(row.get("failed_components"), list) else []
        if "momentum_below_threshold" in failed or "momentum_score_low" in reason:
            counters["momentum_below_threshold"] += 1
        if "score_below_threshold" in failed or "score_too_low" in reason:
            counters["score_below_threshold"] += 1
        if "session_blocked" in reason or "off_session" in reason:
            counters["session_blocked"] += 1
        if "regime_blocked" in reason or "regime" in reason or "chop_guard" in reason:
            counters["regime_blocked"] += 1
        if bool(row.get("risk_governor_blocked")) or reason.startswith("risk_governor_block"):
            counters["risk_governor_block"] += 1
    return [
        {"reason": reason, "count": count}
        for reason, count in sorted(counters.items(), key=lambda item: (-item[1], item[0]))
        if count
    ]


def _shadow_trade_stats(shadow_trades: list[dict[str, Any]], rows: list[dict[str, Any]]) -> dict[str, Any]:
    open_from_rows = max((int(_number(row.get("open_shadow_count")) or 0) for row in rows), default=0)
    closed = [trade for trade in shadow_trades if str(trade.get("status") or trade.get("lifecycle_status") or "").casefold() == "closed"]
    open_items = [trade for trade in shadow_trades if str(trade.get("status") or trade.get("lifecycle_status") or "").casefold() == "open"]
    pnl = sum(float(_number(trade.get("pnl") or trade.get("realized_pnl") or trade.get("profit") or 0.0) or 0.0) for trade in closed)
    return {
        "open_shadow_trades": max(open_from_rows, len(open_items)),
        "closed_shadow_trades": len(closed),
        "paper_pnl": round(pnl, 6),
    }


def _recommendation_actions(
    total: int,
    reason_counts: dict[str, int],
    risk_blocks: int,
    broker_count: int,
    order_count: int,
    *,
    bottleneck_component: str = "",
    shadow_occupancy_inconsistent: bool = False,
) -> list[str]:
    if broker_count or order_count:
        return ["halt_observation", "investigate_safety_violation", "no_real_trading"]
    actions = ["continue_observation"]
    if total < 100:
        actions.append("collect_more_samples")
    if any("score_too_low" in reason for reason in reason_counts):
        actions.append("investigate_score_components")
    if bottleneck_component == "momentum_score":
        actions.append("investigate_momentum_gate")
    if risk_blocks:
        actions.append("review_risk_governor_blocks_without_relaxing")
    if shadow_occupancy_inconsistent:
        actions.append("fix_shadow_occupancy_mismatch")
    actions.extend(["do_not_relax_thresholds_yet", "no_real_trading"])
    return list(dict.fromkeys(actions))


def _distribution(values: Any) -> dict[str, Any]:
    clean = [float(value) for value in (_number(item) for item in values) if value is not None]
    if not clean:
        return {"count": 0, "min": None, "max": None, "avg": None, "median": None}
    ordered = sorted(clean)
    return {
        "count": len(ordered),
        "min": round(ordered[0], 6),
        "max": round(ordered[-1], 6),
        "avg": round(mean(ordered), 6),
        "median": round(median(ordered), 6),
    }


def _counts(values: Any) -> dict[str, int]:
    counter = Counter(str(value or "unknown") for value in values)
    return dict(counter.most_common(10))


def _pct(count: int, total: int) -> float:
    return round((float(count) / float(total) * 100.0), 4) if total else 0.0


def _bool(value: object, *, default: bool = False) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().casefold() in {"1", "true", "yes", "y", "on"}


def _number(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _first_number(*values: object) -> float | None:
    for value in values:
        parsed = _number(value)
        if parsed is not None:
            return parsed
    return None


def _failed_components(*values: object) -> list[str]:
    for value in values:
        if isinstance(value, list):
            return [str(item) for item in value]
        if isinstance(value, str) and value.strip():
            return [item.strip() for item in value.split(",") if item.strip()]
    return []


def _id_list(*values: object) -> list[str]:
    ids: list[str] = []
    for value in values:
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    candidate = str(item.get("shadow_trade_id") or item.get("trade_id") or item.get("id") or "").strip()
                    if candidate:
                        ids.append(candidate)
                else:
                    candidate = str(item or "").strip()
                    if candidate:
                        ids.append(candidate)
            continue
        if isinstance(value, str) and value.strip():
            raw = value.strip()
            if raw.startswith("["):
                try:
                    parsed = json.loads(raw.replace("'", '"'))
                    ids.extend(_id_list(parsed))
                    continue
                except (TypeError, ValueError, json.JSONDecodeError):
                    pass
            ids.extend(item.strip() for item in raw.split(",") if item.strip())
    return list(dict.fromkeys(ids))


def _derive_failed_components(
    *,
    score: float | None,
    min_score: float | None,
    trend_score: float | None,
    momentum_score: float | None,
    volatility_score: float | None,
    thresholds: dict[str, float],
) -> list[str]:
    failed: list[str] = []
    if score is not None and min_score is not None and score < min_score:
        failed.append("score_below_threshold")
    if momentum_score is not None and momentum_score < thresholds["momentum_score"]:
        failed.append("momentum_below_threshold")
    if trend_score is not None and trend_score < thresholds["trend_score"]:
        failed.append("trend_below_threshold")
    if volatility_score is not None and volatility_score < thresholds["volatility_score"]:
        failed.append("volatility_below_threshold")
    return failed


def _jsonable(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items() if key != "summary_markdown"}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return value


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
