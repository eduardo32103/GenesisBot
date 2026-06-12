from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_paper_candidate_review import review_paper_candidate  # noqa: E402
from services.mt5.mt5_strategy_tournament import run_strategy_tournament  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    candidate = _candidate_from_args(args)
    tournament_top_candidate: dict[str, Any] | None = None
    if args.from_tournament:
        tournament = run_strategy_tournament(
            load_shadow_snapshot=not args.no_shadow_snapshot,
            load_persistent=not args.no_persistent,
            load_rotation=not args.no_rotation,
            persist_events=False,
        )
        top = tournament.get("top_candidate")
        if isinstance(top, dict) and top:
            tournament_top_candidate = dict(top)
            candidate = {**candidate, **tournament_top_candidate}
    result = review_paper_candidate(
        candidate,
        capital_state=args.capital_state,
        adaptive_state=args.adaptive_state,
        risk_allowed=args.risk_allowed,
        persist_review=not args.no_persist_review,
    )
    if tournament_top_candidate is not None:
        result["tournament_top_candidate"] = tournament_top_candidate
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True, default=str))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok") else 1


def _candidate_from_args(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "symbol": args.symbol,
        "timeframe": args.timeframe,
        "profile": args.profile,
        "family": args.family,
        "trades_forward": args.trades_forward,
        "win_rate": args.win_rate,
        "profit_factor": args.profit_factor,
        "recent_profit_factor": args.recent_profit_factor,
        "expectancy": args.expectancy,
        "max_drawdown": args.max_drawdown,
        "monte_carlo_stressed_pf": args.monte_carlo_stressed_pf,
        "remove_best_5_pf": args.remove_best_5_pf,
        "single_trade_dependency": args.single_trade_dependency,
    }


def _human_summary(result: dict[str, Any]) -> str:
    active = result.get("active_context_review") if isinstance(result.get("active_context_review"), dict) else {}
    min_gate = result.get("min_sample_gate") if isinstance(result.get("min_sample_gate"), dict) else {}
    persist = result.get("persistent_review_write") if isinstance(result.get("persistent_review_write"), dict) else {}
    lines = [
        "MT5 Paper Candidate Review",
        f"status={result.get('status')}",
        f"decision={result.get('decision')}",
        f"reason={result.get('reason')}",
        f"symbol={result.get('symbol')}",
        f"timeframe={result.get('timeframe')}",
        f"candidate_profile_before={result.get('candidate_profile_before')}",
        f"candidate_profile_after={result.get('candidate_profile_after')}",
        f"paper_candidate_review_created={result.get('paper_candidate_review_created')}",
        f"persistent_review_write_ok={result.get('persistent_review_write_ok')}",
        f"persistent_review_reason={persist.get('reason') or ''}",
        f"active_context_status={result.get('active_context_status')}",
        f"active_profile_exists={active.get('active_profile_exists')}",
        f"active_profile_symbol={active.get('active_profile_symbol')}",
        f"active_profile_timeframe={active.get('active_profile_timeframe')}",
        f"active_profile_name={active.get('active_profile_name')}",
        f"candidate_profile_name={active.get('candidate_profile_name')}",
        f"missing_active_context_fields={','.join(active.get('missing_active_context_fields') or [])}",
        f"can_create_paper_review_context={active.get('can_create_paper_review_context')}",
        f"can_activate={result.get('can_activate')}",
        f"trades_forward={result.get('trades_forward')}",
        f"win_rate={result.get('win_rate')}",
        f"profit_factor={result.get('profit_factor')}",
        f"recent_profit_factor={result.get('recent_profit_factor')}",
        f"expectancy={result.get('expectancy')}",
        f"min_sample_gate={json.dumps(min_gate, sort_keys=True)}",
        f"review_to_observation_ready={result.get('review_to_observation_ready')}",
        f"paper_rotation_applied={result.get('paper_rotation_applied')}",
        f"candidate_activated={result.get('candidate_activated')}",
        f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
    ]
    return "\n".join(lines)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare a safe paper-only candidate review without broker access.")
    parser.add_argument("--symbol", default="BTCUSD")
    parser.add_argument("--timeframe", default="H1")
    parser.add_argument("--profile", default="unknown_profile")
    parser.add_argument("--family", default="tournament_edge")
    parser.add_argument("--trades-forward", type=int, default=8)
    parser.add_argument("--win-rate", type=float, default=75.0)
    parser.add_argument("--profit-factor", type=float, default=19.72)
    parser.add_argument("--recent-profit-factor", type=float, default=19.72)
    parser.add_argument("--expectancy", type=float, default=55.12)
    parser.add_argument("--max-drawdown", type=float)
    parser.add_argument("--monte-carlo-stressed-pf", type=float)
    parser.add_argument("--remove-best-5-pf", type=float)
    parser.add_argument("--single-trade-dependency", dest="single_trade_dependency", action="store_true")
    parser.add_argument("--no-single-trade-dependency", dest="single_trade_dependency", action="store_false")
    parser.set_defaults(single_trade_dependency=None)
    parser.add_argument("--capital-state", default="")
    parser.add_argument("--adaptive-state", default="")
    parser.add_argument("--risk-allowed", action="store_true")
    parser.add_argument("--no-persist-review", action="store_true")
    parser.add_argument("--from-tournament", action="store_true")
    parser.add_argument("--no-shadow-snapshot", action="store_true")
    parser.add_argument("--no-persistent", action="store_true")
    parser.add_argument("--no-rotation", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
