from __future__ import annotations

from typing import Any

from services.trading_intelligence.strategy_metrics import calculate_strategy_metrics


class WalkForwardValidator:
    """Simple train/test and rolling-window validation for research only."""

    def evaluate(self, trades: list[dict[str, Any]] | None) -> dict[str, Any]:
        trades = trades or []
        if len(trades) < 12:
            return {
                "status": "insufficient_sample",
                "accepted": False,
                "out_of_sample_score": 0.0,
                "train": {},
                "test": {},
                "rolling_windows": [],
                "notes": ["No hay suficientes trades para aceptar edge fuera de muestra."],
            }
        split = max(6, int(len(trades) * 0.6))
        train = calculate_strategy_metrics(trades[:split])
        test = calculate_strategy_metrics(trades[split:])
        windows = _rolling_windows(trades)
        positive_windows = sum(1 for row in windows if row["net_profit"] > 0 and row["profit_factor"] >= 1.0)
        window_ratio = positive_windows / len(windows) if windows else 0.0
        accepted = (
            train["profit_factor"] >= 1.15
            and test["profit_factor"] >= 1.05
            and test["expectancy"] > 0
            and window_ratio >= 0.5
        )
        score = 0.0
        if train["profit_factor"] >= 1.15:
            score += 25
        if test["profit_factor"] >= 1.05:
            score += 30
        if test["expectancy"] > 0:
            score += 25
        score += window_ratio * 20
        return {
            "status": "evaluated",
            "accepted": accepted,
            "out_of_sample_score": round(score, 2),
            "train": train,
            "test": test,
            "rolling_windows": windows,
            "positive_window_ratio": round(window_ratio, 4),
            "notes": ["No se acepta un perfil si solo gana en train y se destruye en test."],
        }


def validate_walk_forward(trades: list[dict[str, Any]] | None) -> dict[str, Any]:
    return WalkForwardValidator().evaluate(trades)


def _rolling_windows(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    window_size = max(4, len(trades) // 4)
    windows: list[dict[str, Any]] = []
    for start in range(0, len(trades), window_size):
        chunk = trades[start : start + window_size]
        if len(chunk) < 3:
            continue
        metrics = calculate_strategy_metrics(chunk)
        windows.append(
            {
                "start": start,
                "end": start + len(chunk),
                "profit_factor": metrics["profit_factor"],
                "net_profit": metrics["net_profit"],
                "expectancy": metrics["expectancy"],
            }
        )
    return windows
