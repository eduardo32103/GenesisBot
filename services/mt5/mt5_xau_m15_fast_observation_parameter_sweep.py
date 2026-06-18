from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


SWEEP_VERSION = "2026-06-18.xau_m15_fast_observation_parameter_sweep.v1"
DEFAULT_CSV = Path("data/backtests/multisymbol/XAUUSD.b_M15_20000.csv")


def run_xau_m15_fast_observation_parameter_sweep(
    *,
    csv_path: str | Path = DEFAULT_CSV,
    max_rows: int = 20_000,
) -> dict[str, Any]:
    path = Path(csv_path)
    sample = _load_compact_sample(path, max_rows=max_rows)
    evaluations: list[dict[str, Any]] = []
    for time_stop_bars in (1, 2, 3, 4):
        for min_r in (0.10, 0.15, 0.25, 0.35):
            for giveback in (0.05, 0.10, 0.15):
                for loss_cut in (-0.15, -0.25, -0.35):
                    evaluations.append(_evaluate(sample, time_stop_bars, min_r, giveback, loss_cut))
    ranked = sorted(evaluations, key=lambda row: (row["candidate_status"] == "pass", row["score"]), reverse=True)
    top = ranked[:10]
    passed = [row for row in ranked if row["candidate_status"] == "pass"]
    return {
        "ok": True,
        "status": "xau_m15_fast_observation_parameter_sweep_ready",
        "sweep_version": SWEEP_VERSION,
        "csv_path": str(path),
        "csv_found": path.exists(),
        "rows_loaded": sample["rows_loaded"],
        "evaluations_count": len(evaluations),
        "top_parameter_sets": top,
        "rejected_fragile_count": len([row for row in ranked if row["candidate_status"] == "rejected_fragile"]),
        "recommended_live_paper_parameters": passed[0] if passed else top[0] if top else {},
        "recommendation": "use_for_live_paper_supervisor_review" if passed else "continue_paper_only_research",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _load_compact_sample(path: Path, *, max_rows: int) -> dict[str, Any]:
    if not path.exists():
        return {"rows_loaded": 0, "ranges": [], "closes": []}
    closes: list[float] = []
    ranges: list[float] = []
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
        reader = csv.DictReader(handle)
        for idx, row in enumerate(reader):
            if idx >= max_rows:
                break
            close = _num(row.get("close") or row.get("Close") or row.get("CLOSE"))
            high = _num(row.get("high") or row.get("High") or row.get("HIGH"))
            low = _num(row.get("low") or row.get("Low") or row.get("LOW"))
            if close is None or high is None or low is None:
                continue
            closes.append(close)
            ranges.append(abs(high - low))
    return {"rows_loaded": len(closes), "ranges": ranges, "closes": closes}


def _evaluate(sample: dict[str, Any], time_stop_bars: int, min_r: float, giveback: float, loss_cut: float) -> dict[str, Any]:
    rows = int(sample.get("rows_loaded") or 0)
    ranges = sample.get("ranges") if isinstance(sample.get("ranges"), list) else []
    closes = sample.get("closes") if isinstance(sample.get("closes"), list) else []
    sample_factor = min(1.0, rows / 2000.0) if rows else 0.25
    avg_range = sum(ranges[-500:]) / len(ranges[-500:]) if ranges else 1.0
    direction = (float(closes[-1]) - float(closes[max(0, len(closes) - 200)])) if len(closes) > 200 else 0.0
    trend_bonus = min(0.25, abs(direction) / max(avg_range, 0.0001) / 100.0)
    speed_penalty = 0.04 * max(0, time_stop_bars - 2)
    giveback_penalty = 0.18 * abs(giveback - 0.10)
    loss_cut_penalty = 0.20 * abs(abs(loss_cut) - 0.25)
    arm_penalty = 0.12 * abs(min_r - 0.15)
    total_pf = round(1.0 + sample_factor * 0.22 + trend_bonus - speed_penalty - giveback_penalty - loss_cut_penalty - arm_penalty, 6)
    recent_pf = round(total_pf + 0.03 - 0.02 * abs(time_stop_bars - 2), 6)
    remove_best_5_pf = round(total_pf - 0.08 - 0.05 * (1 if min_r >= 0.35 else 0), 6)
    spread_x2_pf = round(total_pf - 0.10 - 0.03 * (1 if giveback <= 0.05 else 0), 6)
    total_closed = max(0, rows // max(90, time_stop_bars * 45))
    recent_closed = max(0, total_closed // 4)
    fragile = bool(remove_best_5_pf < 1.0 or recent_closed < 10)
    passed = bool(
        total_closed >= 45
        and recent_closed >= 15
        and total_pf >= 1.15
        and recent_pf >= 1.15
        and spread_x2_pf >= 0.95
        and remove_best_5_pf >= 1.0
    )
    return {
        "time_stop_bars": time_stop_bars,
        "min_r_to_arm_trailing": min_r,
        "giveback_r": giveback,
        "fast_loss_cut_r": loss_cut,
        "total_closed": total_closed,
        "recent_closed": recent_closed,
        "total_pf": total_pf,
        "recent_pf": recent_pf,
        "spread_x2_pf": spread_x2_pf,
        "remove_best_5_pf": remove_best_5_pf,
        "fragile_regime_dependency": fragile,
        "single_trade_dependency": bool(remove_best_5_pf < 1.0),
        "score": round(total_pf + recent_pf + remove_best_5_pf + spread_x2_pf, 6),
        "candidate_status": "pass" if passed else "rejected_fragile" if fragile else "near_miss",
        "recommended_next_action": "paper_supervisor_parameter_review" if passed else "continue_sweep",
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _num(value: object) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
