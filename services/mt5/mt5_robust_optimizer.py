from __future__ import annotations

import random
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

from services.mt5.mt5_backtester import (
    BacktestSettings,
    _FILTER_PROFILES,
    _load_bars,
    _metrics,
    _number,
    _reason_counts,
    _settings,
    _settings_for_profile,
    _simulate,
    _safety,
    _walk_forward_metrics,
)
from services.mt5.mt5_config import MT5RuntimeConfig, get_mt5_config


ROBUST_PROFILES = [
    "anti_chop_v2_safe",
    "quality_v3_conservative",
    "trend_v2_drawdown_guard",
    "momentum_v2_filtered",
    "rsi_reversal_v2_confirmed",
    "low_drawdown_v1",
    "capital_preservation_v1",
    "anti_chop_v1",
    "quality_strict",
]
ROBUST_TIMEFRAMES = ["M15", "M30", "H1"]


class MT5RobustOptimizer:
    """Cold-path robustness optimizer. It never mutates live forward/profile state."""

    def __init__(self, *, config: MT5RuntimeConfig | None = None) -> None:
        self.config = config or get_mt5_config()

    def run(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        started = time.monotonic()
        body = dict(payload or {})
        symbol = str(body.get("symbol") or "BTCUSD").upper().strip()
        csv_dir = Path(str(body.get("csv_dir") or Path("data") / "backtests"))
        timeframes = _requested_list(body.get("timeframes"), ROBUST_TIMEFRAMES)
        profiles = [profile for profile in _requested_list(body.get("profiles"), ROBUST_PROFILES) if profile in _FILTER_PROFILES]
        max_bars = int(_number(body.get("max_bars")) or 5000)
        rows: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        for timeframe in timeframes:
            csv_path = Path(str(body.get(f"csv_path_{timeframe.lower()}") or csv_dir / f"{symbol}_{timeframe}_5000.csv"))
            if not csv_path.exists():
                errors.append({"timeframe": timeframe, "path": str(csv_path), "error": "csv_not_found"})
                continue
            csv_text = csv_path.read_text(encoding="utf-8-sig")
            settings = _settings(
                {
                    **body,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "csv_text": csv_text,
                    "max_bars": max_bars,
                    "save_results": False,
                },
                self.config,
            )
            bars, warnings = _load_bars({"csv_text": csv_text}, settings)
            bars = bars[: settings.max_bars]
            if not bars:
                errors.append({"timeframe": timeframe, "path": str(csv_path), "error": "no_bars_loaded", "warnings": warnings})
                continue
            for profile in profiles:
                for rr in _requested_numbers(body.get("rr_values"), [1.2]):
                    for time_stop in _requested_numbers(body.get("time_stop_minutes"), [15.0]):
                        profile_settings = _settings_for_profile(settings, profile, {})
                        profile_settings = replace(
                            profile_settings,
                            min_rr=max(1.0, float(rr)),
                            time_stop_bars=max(1, int((time_stop + _timeframe_minutes(timeframe) - 1) // _timeframe_minutes(timeframe))),
                            risk_pct=float(_requested_numbers(body.get("risk_pct_values"), [0.1])[0]),
                        )
                        rows.append(self._evaluate(profile_settings, bars, source_csv=str(csv_path), time_stop_min=float(time_stop)))
        rows.sort(key=lambda item: (item["recommendation"] != "paper_forward_candidate", -float(item["institutional_score"])))
        candidates = [item for item in rows if item["recommendation"] == "paper_forward_candidate"]
        return {
            "ok": True,
            "status": "mt5_robust_optimizer_completed",
            "symbol": symbol,
            "timeframes": timeframes,
            "profiles": profiles,
            "results": rows,
            "candidates": candidates,
            "best_profile": candidates[0] if candidates else (rows[0] if rows else None),
            "recommendation": "paper_forward_candidate" if candidates else "reject",
            "genesis_reading": _summary_reading(candidates, rows),
            "errors": errors,
            "live_runtime_mutated": False,
            "promoted_profile_mutated": False,
            "shadow_trades_mutated": False,
            **_safety(),
            "duration_ms": int((time.monotonic() - started) * 1000),
        }

    def _evaluate(self, settings: BacktestSettings, bars: list[dict[str, Any]], *, source_csv: str, time_stop_min: float) -> dict[str, Any]:
        started = time.monotonic()
        trades, no_trade_count, blocked = _simulate(settings, bars, started, prefix=f"robust-{settings.filter_profile}")
        full = _metrics(trades, initial_balance=settings.initial_balance)
        windows = _window_metrics_from_trades(settings, bars, trades)
        split = _walk_forward_metrics(settings, bars, {"train_ratio": 0.6})
        train_summary = dict(split.get("train_summary") or {})
        test_summary = dict(split.get("test_summary") or {})
        monte_carlo = _monte_carlo(trades, initial_balance=settings.initial_balance, max_drawdown_limit=5000.0)
        gate = _candidate_gate(full, windows, train_summary, test_summary, monte_carlo, settings)
        score = _institutional_score(full, windows, train_summary, test_summary, monte_carlo)
        recommendation = "paper_forward_candidate" if gate["passed"] else "reject"
        return {
            "timeframe": settings.timeframe,
            "profile": settings.filter_profile,
            "rr": settings.min_rr,
            "time_stop_min": time_stop_min,
            "risk_pct": settings.risk_pct,
            "source_csv": source_csv,
            "closed": full["closed"],
            "wins": full["wins"],
            "losses": full["losses"],
            "win_rate": full["win_rate"],
            "profit_factor": full["profit_factor"],
            "expectancy": full["expectancy"],
            "max_drawdown": full["max_drawdown"],
            "net_pnl": full["net_pnl"],
            "buy_pf": full["buy_pf"],
            "sell_pf": full["sell_pf"],
            "buy_win_rate": full["buy_win_rate"],
            "sell_win_rate": full["sell_win_rate"],
            "exit_reason_counts": full["exit_reason_counts"],
            "side_stats": full["side_stats"],
            "regime_stats": full["regime_stats"],
            "hour_stats": full["hour_stats"],
            "no_trade_count": no_trade_count,
            "blocked_reason_counts": _reason_counts(blocked),
            "train_pf": train_summary.get("profit_factor", 0.0),
            "test_pf": test_summary.get("profit_factor", 0.0),
            "train_expectancy": train_summary.get("expectancy", 0.0),
            "test_expectancy": test_summary.get("expectancy", 0.0),
            "train_drawdown": train_summary.get("max_drawdown", 0.0),
            "test_drawdown": test_summary.get("max_drawdown", 0.0),
            "windows": windows,
            "monte_carlo": monte_carlo,
            "institutional_score": score,
            "recommendation": recommendation,
            "candidate": gate["passed"],
            "pass_fail_reasons": gate["reasons"],
            "guardrails": _paper_forward_guardrails(max_drawdown=3000.0 if full["max_drawdown"] <= 3000 else 5000.0),
            "applies_to_paper_shadow": recommendation == "paper_forward_candidate",
            "applies_to_real_trading": False,
            "live_runtime_mutated": False,
            "promoted_profile_mutated": False,
            "shadow_trades_mutated": False,
            **_safety(),
        }


def _window_metrics_from_trades(settings: BacktestSettings, bars: list[dict[str, Any]], trades: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    midpoint = max(3, len(bars) // 2)
    windows = {
        "first_half": (0, midpoint),
        "second_half": (midpoint, len(bars)),
        "last_1000": (max(0, len(bars) - 1000), len(bars)),
        "last_2000": (max(0, len(bars) - 2000), len(bars)),
    }
    payload: dict[str, dict[str, Any]] = {}
    for name, (start, end) in windows.items():
        scoped = [
            trade
            for trade in trades
            if trade.get("lifecycle_status") == "closed"
            and start <= int(_number(trade.get("opened_index")) or 0) < end
        ]
        if not scoped:
            payload[name] = {"closed": 0, "profit_factor": 0.0, "expectancy": 0.0, "max_drawdown": 0.0}
            continue
        summary = _metrics(scoped, initial_balance=settings.initial_balance)
        payload[name] = {
            "closed": summary["closed"],
            "profit_factor": summary["profit_factor"],
            "expectancy": summary["expectancy"],
            "max_drawdown": summary["max_drawdown"],
            "win_rate": summary["win_rate"],
            "no_trade_count": 0,
            "blocked_reason_counts": {},
        }
    return payload


def _candidate_gate(
    full: dict[str, Any],
    windows: dict[str, dict[str, Any]],
    train: dict[str, Any],
    test: dict[str, Any],
    monte_carlo: dict[str, Any],
    settings: BacktestSettings,
) -> dict[str, Any]:
    reasons: list[str] = []
    min_closed = 50 if settings.timeframe.upper().startswith("H") else 75
    if int(full.get("closed") or 0) < min_closed:
        reasons.append("sample_too_small")
    if float(full.get("profit_factor") or 0.0) < 1.20:
        reasons.append("pf_below_1_20")
    if float(full.get("expectancy") or 0.0) <= 0:
        reasons.append("expectancy_not_positive")
    if float(full.get("win_rate") or 0.0) < 45.0:
        reasons.append("win_rate_below_45")
    if float(full.get("max_drawdown") or 0.0) > 5000.0:
        reasons.append("drawdown_above_5000")
    for name, metrics in windows.items():
        if int(metrics.get("closed") or 0) >= 10 and float(metrics.get("profit_factor") or 0.0) < 1.0:
            reasons.append(f"{name}_pf_below_1")
        if int(metrics.get("closed") or 0) >= 10 and float(metrics.get("expectancy") or 0.0) < -0.05:
            reasons.append(f"{name}_expectancy_strong_negative")
    if int(test.get("closed") or 0) >= 10 and float(test.get("profit_factor") or 0.0) < 1.0:
        reasons.append("test_pf_below_1")
    if float(full.get("buy_pf") or 0.0) > 0 and float(full.get("buy_pf") or 0.0) < 0.75:
        reasons.append("buy_side_destructive")
    if float(full.get("sell_pf") or 0.0) > 0 and float(full.get("sell_pf") or 0.0) < 0.75:
        reasons.append("sell_side_destructive")
    if not monte_carlo.get("passed"):
        reasons.extend([f"monte_carlo_{reason}" for reason in monte_carlo.get("fail_reasons", [])])
    return {"passed": not reasons, "reasons": reasons or ["passes_institutional_rules"]}


def _monte_carlo(trades: list[dict[str, Any]], *, initial_balance: float, max_drawdown_limit: float, simulations: int = 1000) -> dict[str, Any]:
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    pnls = [float(_number(trade.get("pnl")) or 0.0) for trade in closed]
    if not pnls:
        return {"passed": False, "fail_reasons": ["no_closed_trades"], "risk_of_ruin": 1.0, "max_drawdown_p95": 0.0, "profit_factor_stressed": 0.0, "expectancy_stressed": 0.0}
    rng = random.Random(1337)
    drawdowns: list[float] = []
    ruin_count = 0
    for _ in range(simulations):
        sample = [rng.choice(pnls) for _ in pnls]
        drawdown = _max_drawdown_from_pnls(sample, initial_balance)
        drawdowns.append(drawdown)
        if drawdown > max_drawdown_limit:
            ruin_count += 1
    drawdowns.sort()
    p95_index = min(len(drawdowns) - 1, int(len(drawdowns) * 0.95))
    stressed = _stress_pnls(pnls)
    pf_stressed = _profit_factor(stressed)
    expectancy_stressed = sum(stressed) / len(stressed) if stressed else 0.0
    fail_reasons: list[str] = []
    risk_of_ruin = ruin_count / simulations
    if risk_of_ruin > 0.05:
        fail_reasons.append("risk_of_ruin_high")
    if drawdowns[p95_index] > max_drawdown_limit:
        fail_reasons.append("drawdown_p95_above_limit")
    if pf_stressed < 1.05:
        fail_reasons.append("stressed_pf_below_1_05")
    if expectancy_stressed < 0:
        fail_reasons.append("stressed_expectancy_negative")
    return {
        "passed": not fail_reasons,
        "fail_reasons": fail_reasons,
        "simulations": simulations,
        "risk_of_ruin": round(risk_of_ruin, 4),
        "max_drawdown_p95": round(drawdowns[p95_index], 6),
        "profit_factor_stressed": round(pf_stressed, 4),
        "expectancy_stressed": round(expectancy_stressed, 6),
        "removed_best_5": len(pnls) >= 5,
    }


def _institutional_score(full: dict[str, Any], windows: dict[str, dict[str, Any]], train: dict[str, Any], test: dict[str, Any], monte_carlo: dict[str, Any]) -> float:
    score = 0.0
    pf = float(full.get("profit_factor") or 0.0)
    expectancy = float(full.get("expectancy") or 0.0)
    closed = int(full.get("closed") or 0)
    drawdown = float(full.get("max_drawdown") or 0.0)
    capped_pf = min(pf, 3.0)
    capped_test_pf = min(float(test.get("profit_factor") or 0.0), 3.0)
    score += max(0.0, capped_pf - 1.0) * 120.0
    score += max(0.0, expectancy) * 500.0
    score += min(closed, 200) * 0.35
    score -= max(0.0, 1.2 - pf) * 150.0
    score -= drawdown / 80.0
    score -= max(0, 75 - closed) * 4.0
    score += max(0.0, capped_test_pf - 1.0) * 50.0
    for metrics in windows.values():
        if int(metrics.get("closed") or 0) >= 10:
            score -= max(0.0, 1.0 - float(metrics.get("profit_factor") or 0.0)) * 45.0
            score -= max(0.0, -float(metrics.get("expectancy") or 0.0)) * 250.0
    if not monte_carlo.get("passed"):
        score -= 150.0
    score -= float(monte_carlo.get("risk_of_ruin") or 0.0) * 200.0
    return round(score, 4)


def _stress_pnls(pnls: list[float]) -> list[float]:
    sorted_pnls = sorted(pnls, reverse=True)
    trim_count = min(5, max(0, len(sorted_pnls) // 10))
    trimmed = sorted_pnls[trim_count:] if trim_count else list(pnls)
    return [value * 0.9 if value > 0 else value * 1.1 for value in trimmed]


def _max_drawdown_from_pnls(pnls: list[float], initial_balance: float) -> float:
    equity = initial_balance
    peak = initial_balance
    max_dd = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return max_dd


def _profit_factor(values: list[float]) -> float:
    gross_win = sum(value for value in values if value > 0)
    gross_loss = abs(sum(value for value in values if value < 0))
    if gross_loss <= 0:
        return gross_win if gross_win > 0 else 0.0
    return gross_win / gross_loss


def _paper_forward_guardrails(*, max_drawdown: float) -> dict[str, Any]:
    return {
        "early_guardrail_min_trades": 10,
        "early_pf_min": 0.9,
        "early_expectancy_min": 0.0,
        "early_win_rate_min": 40.0,
        "main_guardrail_min_trades": 50,
        "main_pf_min": 1.15,
        "main_expectancy_min": 0.0,
        "max_forward_drawdown": max_drawdown,
        "degrade_to": "observation_only",
    }


def _summary_reading(candidates: list[dict[str, Any]], rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "No hubo datos para evaluar. Mantener observation_only."
    if not candidates:
        return "Ningun perfil paso robustez institucional. Recomendacion: reject/observation_only, nunca real trading."
    best = candidates[0]
    return (
        f"Mejor candidato paper-forward: {best['timeframe']} {best['profile']} "
        f"PF {best['profit_factor']} DD {best['max_drawdown']}. No promover a real trading."
    )


def _requested_list(raw: Any, default: list[str]) -> list[str]:
    if isinstance(raw, str):
        values = [part.strip() for part in raw.split(",")]
    elif isinstance(raw, list):
        values = [str(part or "").strip() for part in raw]
    else:
        values = list(default)
    return [value for value in values if value]


def _requested_numbers(raw: Any, default: list[float]) -> list[float]:
    values = _requested_list(raw, [str(value) for value in default])
    parsed = [float(_number(value) or 0.0) for value in values]
    return [value for value in parsed if value > 0] or default


def _timeframe_minutes(timeframe: str) -> int:
    value = str(timeframe or "H1").upper().strip()
    if value.startswith("M"):
        return max(1, int(_number(value[1:]) or 1))
    if value.startswith("H"):
        return max(1, int(_number(value[1:]) or 1) * 60)
    return 60
