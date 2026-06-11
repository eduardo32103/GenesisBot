from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_persistent_schema import PERSISTENT_INTELLIGENCE_SCHEMA_VERSION, get_persistent_intelligence_schema_sql


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    sql = _schema_sql(include_rls=args.include_rls)
    output = args.output
    if args.railway_file and not output:
        output = "persistent_intelligence_schema_railway.sql"
    if output:
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(sql, encoding="utf-8")
        print(f"wrote_schema_sql={path}")
    else:
        print(sql)
    instruction_stream = sys.stdout if output else sys.stderr
    print("instruction=Execute this SQL in the Postgres query console, not in a bash shell.", file=instruction_stream)
    print("bash_instruction=For bash or Railway shell, use scripts/run_persistent_intelligence_apply_schema.py.", file=instruction_stream)
    print("broker_touched=false", file=instruction_stream)
    print("order_executed=false", file=instruction_stream)
    print("order_policy=journal_only_no_broker", file=instruction_stream)
    return 0


def _schema_sql(*, include_rls: bool = False) -> str:
    header = "\n".join(
        [
            f"-- {PERSISTENT_INTELLIGENCE_SCHEMA_VERSION}",
            "-- Idempotent schema for Genesis MT5 Persistent Intelligence Store.",
            "-- Safety: no DROP, no TRUNCATE, no DELETE, no OHLC/tick/raw CSV storage.",
            "",
        ]
    )
    return header + get_persistent_intelligence_schema_sql(include_rls=include_rls).strip() + "\n"


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Emit idempotent Supabase SQL for Genesis MT5 persistent intelligence.")
    parser.add_argument("--output", default="", help="Optional path to write SQL instead of printing it.")
    parser.add_argument("--railway-file", action="store_true", help="Write persistent_intelligence_schema_railway.sql in the repo root.")
    parser.add_argument("--include-rls", action="store_true", help="Include optional RLS enable statements.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
