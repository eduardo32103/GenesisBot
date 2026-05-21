from __future__ import annotations

import csv
import json
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from services.mt5.mt5_backtester import _load_bars, _metrics, _number, _safety, _settings
from services.mt5.mt5_capital_preservation_optimizer import (
    CAPITAL_PRESERVATION_PROFILES,
    CAPITAL_PRESERVATION_TIMEFRAMES,
    _PROFILE_PARAMS,
    _capital_decision_from_history,
    _common_entry_block,
    _config,
    _effective_min_score,
    _effective_min_volatility,
    _fast_risk_block,
    _market_features,
    _settings_for_capital_config,
    _simulate_capital_preservation,
)
from services.mt5.mt5_config import get_mt5_config


FILTER_COLUMNS = [
    "passed_regime_filter",
    "failed_regime_filter",
    "passed_spread_filter",
    "failed_spread_filter",
    "passed_volatility_filter",
    "failed_volatility_filter",
    "passed_trend_filter",
    "failed_trend_filter",
    "passed_pullback_filter",
    "failed_pullback_filter",
    "passed_rsi_filter",
    "failed_rsi_filter",
    "passed_ema_filter",
    "failed_ema_filter",
    "passed_score_threshold",
    "failed_score_threshold",
]


def run_entry_funnel_diagnostics(body: dict[str, Any] | None = None) -> dict[str, Any]:
    body = body or {}
    started = time.monotonic()
    symbol = str(body.get("symbol") or "BTCUSD").upper().strip()
    csv_dir = Path(str(body.get("csv_dir") or Path("data") / "backtests"))
    timeframes = _requested_list(body.get("timeframes"), CAPITAL_PRESERVATION_TIMEFRAMES)
    profiles = [
        profile
        for profile in _requested_list(body.get("profiles"), CAPITAL_PRESERVATION_PROFILES)
        if profile in _PROFILE_PARAMS
    ]
    max_bars = max(50, min(int(_number(body.get("max_bars")) or 5000), 10000))
    spread_points = float(_number(body.get("spread_points")) or 25.0)
    timeout_seconds = float(_number(body.get("timeout_seconds")) or 30.0)
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    warnings: list[str] = []

    for timeframe in timeframes:
        csv_path = Path(str(body.get("csv_path") or csv_dir / f"{symbol}_{timeframe}_5000.csv"))
        base_body = {
            "symbol": symbol,
            "timeframe": timeframe,
            "csv_path": str(csv_path),
            "max_bars": max_bars,
            "spread_points": spread_points,
            "save_results": False,
            "source": "mt5_csv",
            "timeout_seconds": max(1.0, min(timeout_seconds, 20.0)),
        }
        base_settings = _settings(base_body, get_mt5_config())
        bars, load_warnings = _load_bars(base_body, base_settings)
        warnings.extend(load_warnings)
        bars = bars[-base_settings.max_bars :]
        if not bars:
            errors.append({"timeframe": timeframe, "error": "csv_bars_not_loaded", "csv_path": str(csv_path)})
            continue

        for profile in profiles:
            if time.monotonic() - started > timeout_seconds:
                errors.append({"timeframe": timeframe, "profile": profile, "error": "diagnostics_timeout_guard"})
                break
            row = diagnose_profile_funnel(
                bars,
                base_settings,
                profile,
                source_csv=str(csv_path),
                started=time.monotonic(),
                timeout_seconds=timeout_seconds,
            )
            rows.append(row)

    rows.sort(key=lambda item: (item.get("opened_shadow_trade_count", 0), item.get("generated_signal_count", 0)), reverse=True)
    summary = _aggregate_summary(rows)
    return {
        "ok": True,
        "status": "mt5_entry_funnel_diagnostics_completed",
        "symbol": symbol,
        "timeframes": timeframes,
        "profiles": profiles,
        "results": rows,
        "summary": summary,
        "errors": errors,
        "warnings": warnings,
        "live_runtime_mutated": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        "shadow_trades_mutated": False,
        "duration_ms": int((time.monotonic() - started) * 1000),
        **_safety(),
    }


def diagnose_profile_funnel(
    bars: list[dict[str, Any]],
    base_settings: Any,
    profile: str,
    *,
    source_csv: str = "",
    started: float | None = None,
    timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    started = started or time.monotonic()
    params = dict(_PROFILE_PARAMS[profile])
    config = _config(
        profile,
        1.2,
        3,
        float(params.get("min_score") or 65.0),
        float(params.get("max_spread_points") or 25.0),
        True,
        bool(params.get("avoid_chop", True)),
        2,
        2,
        True,
        True,
        True,
        True,
        bool(params.get("session_filter")),
        bool(params.get("partial_exit")),
        bool(params.get("atr_trailing")),
        True,
        0.1,
    )
    settings = _settings_for_capital_config(base_settings, config)
    counters: Counter[str] = Counter()
    no_trade_reasons: Counter[str] = Counter()
    buy_sell: Counter[str] = Counter()
    session_hours: Counter[str] = Counter()
    regimes: Counter[str] = Counter()
    risk_blocks = 0
    generated = 0
    actionable = 0
    spread_sum = 0.0
    atr_sum = 0.0
    features_seen = 0
    timed_out = False
    max_iterations = len(bars) + 5

    for index in range(1, len(bars)):
        if index > max_iterations:
            no_trade_reasons["loop_guard"] += 1
            break
        if time.monotonic() - started > timeout_seconds:
            timed_out = True
            no_trade_reasons["timeout_guard"] += 1
            break
        counters["bars_evaluated"] += 1
        history = bars[max(0, index - 80) : index]
        features = _market_features(history)
        if not features:
            no_trade_reasons["insufficient_history"] += 1
            _count_all_filter_failures(counters, "insufficient_history")
            continue
        features_seen += 1
        spread_sum += float(settings.spread_points)
        atr_sum += float(_number(features.get("atr")) or 0.0)
        regimes[str(features.get("regime") or "unknown")] += 1
        if features.get("hour") is not None:
            session_hours[str(int(features["hour"]))] += 1

        filter_state = _filter_state(features, settings, config, params)
        for name, passed in filter_state.items():
            counters[f"passed_{name}"] += int(passed)
            counters[f"failed_{name}"] += int(not passed)

        common_reason = _common_entry_block(features, settings, config, params)
        decision = _capital_decision_from_history(history, settings, config)
        reason = str(common_reason or decision.get("reason") or "no_edge")
        if not decision.get("actionable"):
            no_trade_reasons[reason] += 1
            continue

        generated += 1
        side = str(decision.get("side") or "unknown")
        buy_sell[side] += 1
        risk_reason = _fast_risk_block(settings, [], config, str(decision.get("regime") or features.get("regime") or "trend"))
        if risk_reason:
            risk_blocks += 1
            no_trade_reasons[f"risk_governor_{risk_reason}"] += 1
            continue
        actionable += 1

    trades, _no_trade_count, blocked, sim_state = _simulate_capital_preservation(settings, bars, config, started)
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    metrics = _metrics(closed, initial_balance=settings.initial_balance)
    opened = len(trades)
    row: dict[str, Any] = {
        "timeframe": settings.timeframe,
        "profile": profile,
        "source_csv": source_csv,
        "bars_loaded": len(bars),
        "bars_evaluated": int(counters["bars_evaluated"]),
        "generated_signal_count": generated,
        "actionable_signal_count": actionable,
        "blocked_by_risk_governor": risk_blocks + int(sim_state.get("risk_governor_blocks") or 0),
        "opened_shadow_trade_count": opened,
        "closed_trade_count": len(closed),
        "buy_signals": int(buy_sell.get("buy", 0)),
        "sell_signals": int(buy_sell.get("sell", 0)),
        "avg_spread": round(spread_sum / max(features_seen, 1), 4),
        "avg_atr": round(atr_sum / max(features_seen, 1), 6),
        "no_trade_reason_counts": dict(no_trade_reasons + Counter(blocked)),
        "top_no_trade_reasons": _top_items(no_trade_reasons + Counter(blocked), 10),
        "top_restrictive_filters": _top_restrictive_filters(counters),
        "buy_sell_signal_distribution": dict(buy_sell),
        "session_hour_distribution": dict(sorted(session_hours.items(), key=lambda item: int(item[0]))),
        "regime_distribution": dict(regimes),
        "exit_reason_counts": dict(Counter(str(trade.get("exit_reason") or "unknown") for trade in closed)),
        "win_rate": metrics["win_rate"],
        "profit_factor": metrics["profit_factor"],
        "expectancy": metrics["expectancy"],
        "max_drawdown": metrics["max_drawdown"],
        "timed_out": timed_out,
        "live_runtime_mutated": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        "shadow_trades_mutated": False,
        **_safety(),
    }
    for column in FILTER_COLUMNS:
        row[column] = int(counters[column])
    row["restrictiveness_score"] = _restrictiveness_score(row)
    return row


def write_entry_funnel_outputs(result: dict[str, Any], output_dir: Path | str) -> tuple[Path, Path, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    csv_path = root / "entry_funnel_diagnostics.csv"
    json_path = root / "entry_funnel_diagnostics.json"
    summary_path = root / "entry_funnel_summary.md"
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    headers = [
        "timeframe",
        "profile",
        "bars_loaded",
        "bars_evaluated",
        *FILTER_COLUMNS,
        "generated_signal_count",
        "actionable_signal_count",
        "blocked_by_risk_governor",
        "opened_shadow_trade_count",
        "closed_trade_count",
        "buy_signals",
        "sell_signals",
        "avg_spread",
        "avg_atr",
        "restrictiveness_score",
        "top_no_trade_reasons",
        "top_restrictive_filters",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    **row,
                    "top_no_trade_reasons": json.dumps(row.get("top_no_trade_reasons") or [], ensure_ascii=True),
                    "top_restrictive_filters": json.dumps(row.get("top_restrictive_filters") or [], ensure_ascii=True),
                }
            )
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True), encoding="utf-8")
    summary_path.write_text(entry_funnel_summary_markdown(result), encoding="utf-8")
    return csv_path, json_path, summary_path


def entry_funnel_summary_markdown(result: dict[str, Any]) -> str:
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    lines = [
        "# MT5 Entry Funnel Diagnostics",
        "",
        "Paper-only diagnostics. No broker touched, no live orders, no promoted profile mutation.",
        "",
    ]
    if not rows:
        lines.extend(["No rows were generated.", ""])
    else:
        most_opportunity = sorted(rows, key=lambda row: (int(row.get("actionable_signal_count") or 0), int(row.get("opened_shadow_trade_count") or 0)), reverse=True)[:5]
        most_restrictive = sorted(rows, key=lambda row: float(row.get("restrictiveness_score") or 0.0), reverse=True)[:5]
        lines.extend(["## Timeframes With Most Opportunity", ""])
        for row in most_opportunity:
            lines.append(
                f"- `{row.get('timeframe')} {row.get('profile')}` generated `{row.get('generated_signal_count')}` "
                f"signals, actionable `{row.get('actionable_signal_count')}`, opened `{row.get('opened_shadow_trade_count')}`."
            )
        lines.extend(["", "## Most Restrictive Profiles", ""])
        for row in most_restrictive:
            blockers = ", ".join(f"{item['reason']}={item['count']}" for item in (row.get("top_restrictive_filters") or [])[:3])
            lines.append(
                f"- `{row.get('timeframe')} {row.get('profile')}` restrictiveness `{row.get('restrictiveness_score')}`; {blockers}."
            )
        lines.extend(["", "## Recommendations", ""])
        lines.extend(_recommendations(rows))
    lines.extend(
        [
            "",
            "## Safety",
            "- No martingale.",
            "- No grid.",
            "- MaxOpenTrades remains 1 in simulation.",
            "- Recommendation: no real trading. Use diagnostics to design more paper-only variants.",
            "- broker_touched=false",
            "- order_executed=false",
            "- order_policy=journal_only_no_broker",
        ]
    )
    return "\n".join(lines) + "\n"


def _filter_state(features: dict[str, Any], settings: Any, config: Any, params: dict[str, Any]) -> dict[str, bool]:
    min_trend = float(_number(params.get("min_trend_score")) or 0.0)
    min_momentum = float(_number(params.get("min_momentum_score")) or 0.0)
    max_rsi_buy = float(_number(params.get("max_rsi_for_buy")) or 100.0)
    min_rsi_sell = float(_number(params.get("min_rsi_for_sell")) or 0.0)
    min_score = _effective_min_score(features, settings, params)
    score = _diagnostic_score(features, str(params.get("strategy_family") or "legacy"))
    trend_pass = features["trend_score"] >= min_trend or (100.0 - features["trend_score"]) >= max(30.0, min_trend * 0.65)
    momentum_pass = features["momentum_score"] >= min_momentum or features["momentum_score"] <= (100.0 - min_momentum)
    state = {
        "spread_filter": settings.spread_points <= config.spread_max,
        "volatility_filter": (not config.volatility_filter) or features["volatility_score"] >= _effective_min_volatility(features, settings, params),
        "regime_filter": _regime_pass(features, config, params),
        "trend_filter": trend_pass and momentum_pass,
        "pullback_filter": _setup_pass(features, params),
        "rsi_filter": min_rsi_sell <= features["rsi"] <= max_rsi_buy,
        "ema_filter": features["distance20_atr"] <= float(_number(params.get("ema_distance_max_atr")) or 2.4),
        "score_threshold": score >= min_score,
    }
    return state


def _regime_pass(features: dict[str, Any], config: Any, params: dict[str, Any]) -> bool:
    allowed = str(params.get("allowed_regime") or "").casefold()
    if allowed and features["regime"] != allowed:
        return False
    if config.anti_chop_filter and bool(params.get("avoid_chop")) and features["regime"] == "chop":
        return str(params.get("strategy_family") or "") in {"mean_reversion", "liquidity_sweep_reversal", "volatility_squeeze"}
    return True


def _setup_pass(features: dict[str, Any], params: dict[str, Any]) -> bool:
    family = str(params.get("strategy_family") or "legacy").casefold()
    if family == "breakout_pullback":
        broke_up = features["recent_high"] > features["prior_high"]
        broke_down = features["recent_low"] < features["prior_low"]
        pullback_buy = features["low"] <= features["ema20"] + features["atr"] * 0.45 and features["close"] > features["ema20"]
        pullback_sell = features["high"] >= features["ema20"] - features["atr"] * 0.45 and features["close"] < features["ema20"]
        return (broke_up and pullback_buy) or (broke_down and pullback_sell)
    if family == "trend_continuation":
        return (features["close"] > features["ema20"] > features["ema50"]) or (features["close"] < features["ema20"] < features["ema50"])
    if family == "mean_reversion":
        return features["distance20_atr"] >= 0.45
    if family == "volatility_squeeze":
        return features["previous_range"] > 0 and features["recent_range"] <= features["previous_range"] * 0.8
    if family == "liquidity_sweep_reversal":
        swept_low = features["low"] < features["recent_low"] and features["close"] > features["recent_low"]
        swept_high = features["high"] > features["recent_high"] and features["close"] < features["recent_high"]
        return swept_low or swept_high
    if family == "ema_rsi_confirmed":
        return (features["close"] > features["ema20"] > features["ema50"]) or (features["close"] < features["ema20"] < features["ema50"])
    return True


def _diagnostic_score(features: dict[str, Any], family: str) -> float:
    family = family.casefold()
    if family in {"breakout_pullback", "ema_rsi_confirmed"}:
        weights = (0.40, 0.35, 0.25)
    elif family == "trend_continuation":
        weights = (0.48, 0.34, 0.18)
    elif family in {"liquidity_sweep_reversal", "mean_reversion"}:
        weights = (0.25, 0.30, 0.45)
    else:
        weights = (0.34, 0.34, 0.32)
    return (
        features["trend_score"] * weights[0]
        + features["momentum_score"] * weights[1]
        + features["volatility_score"] * weights[2]
    )


def _count_all_filter_failures(counters: Counter[str], reason: str) -> None:
    for column in FILTER_COLUMNS:
        if column.startswith("failed_"):
            counters[column] += 1
    counters[f"failed_{reason}"] += 1


def _top_restrictive_filters(counters: Counter[str]) -> list[dict[str, Any]]:
    items = []
    for key, value in counters.items():
        if key.startswith("failed_") and key in FILTER_COLUMNS:
            items.append({"reason": key.replace("failed_", ""), "count": int(value)})
    items.sort(key=lambda item: item["count"], reverse=True)
    return items[:10]


def _top_items(counter: Counter[str], limit: int) -> list[dict[str, Any]]:
    return [{"reason": key, "count": int(value)} for key, value in counter.most_common(limit)]


def _restrictiveness_score(row: dict[str, Any]) -> float:
    evaluated = max(1, int(row.get("bars_evaluated") or 0))
    failures = sum(int(row.get(column) or 0) for column in FILTER_COLUMNS if column.startswith("failed_"))
    generated = int(row.get("generated_signal_count") or 0)
    opened = int(row.get("opened_shadow_trade_count") or 0)
    return round((failures / evaluated) * 10.0 + max(0, 20 - generated) + max(0, 10 - opened), 4)


def _aggregate_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    filter_blocks: Counter[str] = Counter()
    reason_blocks: Counter[str] = Counter()
    timeframe_opportunity: defaultdict[str, int] = defaultdict(int)
    for row in rows:
        timeframe_opportunity[str(row.get("timeframe") or "")] += int(row.get("actionable_signal_count") or 0)
        for item in row.get("top_restrictive_filters") or []:
            filter_blocks[str(item.get("reason"))] += int(item.get("count") or 0)
        reasons = row.get("no_trade_reason_counts") if isinstance(row.get("no_trade_reason_counts"), dict) else {}
        for reason, count in reasons.items():
            reason_blocks[str(reason)] += int(count)
    best_timeframe = max(timeframe_opportunity.items(), key=lambda item: item[1])[0] if timeframe_opportunity else ""
    return {
        "top_filter_blocks": _top_items(filter_blocks, 10),
        "top_no_trade_reasons": _top_items(reason_blocks, 10),
        "timeframe_opportunity": dict(timeframe_opportunity),
        "best_timeframe_by_actionable_signals": best_timeframe,
        "recommendation": "diagnose_then_expand_signals_no_real_trading",
        **_safety(),
    }


def _recommendations(rows: list[dict[str, Any]]) -> list[str]:
    summary = _aggregate_summary(rows)
    top_filters = summary.get("top_filter_blocks") or []
    top_reasons = summary.get("top_no_trade_reasons") or []
    best_timeframe = summary.get("best_timeframe_by_actionable_signals") or "unknown"
    lines = [
        f"- Timeframe with most raw opportunity: `{best_timeframe}`.",
    ]
    if top_filters:
        lines.append(f"- Most restrictive filter: `{top_filters[0]['reason']}` with `{top_filters[0]['count']}` failures.")
    if top_reasons:
        lines.append(f"- Most common no-trade reason: `{top_reasons[0]['reason']}` with `{top_reasons[0]['count']}` bars.")
    lines.append("- Build balanced variants by easing the dominant blocker one step at a time, then rerun capital preservation gates.")
    lines.append("- Do not promote profiles from funnel counts alone; funnel only explains opportunity loss.")
    return lines


def _requested_list(value: Any, default: list[str]) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [part.strip() for part in value.split(",") if part.strip()]
    return list(default)
