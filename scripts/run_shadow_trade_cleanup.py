from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_shadow_trade_cleanup import run_shadow_trade_cleanup  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_shadow_trade_cleanup(
        apply_paper_cleanup=args.apply_paper_cleanup,
        max_open_shadow_trades=args.max_open_shadow_trades,
        max_profile_open_shadows=args.max_profile_open_shadows,
        stale_hours=args.stale_hours,
        load_shadow_snapshot=not args.no_shadow_snapshot,
        load_persistent_db=not args.no_persistent_db,
        require_live_db=args.require_live_db,
        expected_live_capital_count=args.expected_live_capital_count,
        confirm_source_fingerprint=args.confirm_source_fingerprint,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok") else 1


def _human_summary(result: dict[str, Any]) -> str:
    lines = [
        "MT5 Shadow Trade Cleanup",
        f"mode={result.get('mode')}",
        f"open_shadow_trades_before={result.get('open_shadow_trades_before')}",
        f"open_shadow_trades_after={result.get('open_shadow_trades_after')}",
        f"cleanup_candidates={len(result.get('cleanup_candidates') or [])}",
        f"closed_paper_only={result.get('closed_paper_only')}",
        f"skipped_unsafe={len(result.get('skipped_unsafe') or [])}",
        f"history_deleted={result.get('history_deleted')}",
        f"metrics_reset={result.get('metrics_reset')}",
        f"losses_reset={result.get('losses_reset')}",
        f"capital_protection_relaxed={result.get('capital_protection_relaxed')}",
        f"risk_governor_relaxed={result.get('risk_governor_relaxed')}",
        f"paper_rotation_applied={result.get('paper_rotation_applied')}",
        f"candidate_activated={result.get('candidate_activated')}",
        f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
        f"source_guard={result.get('source_guard')}",
        "",
        "Candidates:",
    ]
    lines.extend(_row_lines(result.get("cleanup_candidates") or []))
    lines.extend(["", "Skipped unsafe:"])
    lines.extend(_row_lines(result.get("skipped_unsafe") or []))
    return "\n".join(lines)


def _row_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    lines: list[str] = []
    for row in rows[:20]:
        reasons = ",".join(str(item) for item in row.get("reasons") or row.get("skip_reasons") or [])
        lines.append(
            f"- {row.get('shadow_trade_id')} {row.get('symbol')} {row.get('timeframe')} "
            f"{row.get('profile')} reasons={reasons}"
        )
    return lines


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dry-run or apply safe paper-only shadow trade cleanup.")
    parser.add_argument("--apply-paper-cleanup", action="store_true")
    parser.add_argument("--max-open-shadow-trades", type=int, default=3)
    parser.add_argument("--max-profile-open-shadows", type=int, default=1)
    parser.add_argument("--stale-hours", type=float, default=12.0)
    parser.add_argument("--no-shadow-snapshot", action="store_true")
    parser.add_argument("--no-persistent-db", action="store_true")
    parser.add_argument("--require-live-db", action="store_true")
    parser.add_argument("--expected-live-capital-count", type=int, default=None)
    parser.add_argument("--confirm-source-fingerprint", default="")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
