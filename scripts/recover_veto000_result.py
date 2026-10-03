from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BASE = "https://nwduaycuofeggjtfdwsy.supabase.co/functions/v1/pa-cross-sectional-reversal-dev-v1"
ROUTES = ("veto_public", "veto_pool", "veto_pgb", "veto_direct")
OUT = Path("data/research_outputs/veto000_recovery.json")


def fetch(route: str, timeout: int = 35) -> dict[str, Any]:
    url = f"{BASE}?read={route}"
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "GenesisBot-VETO000-Recovery/1.0",
        },
        method="GET",
    )
    started = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw = response.read().decode("utf-8", errors="replace")
            status = int(response.status)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        status = int(exc.code)
    except Exception as exc:
        return {
            "route": route,
            "http": None,
            "elapsed_s": round(time.time() - started, 3),
            "transport_error": f"{type(exc).__name__}: {exc}",
        }

    try:
        body: Any = json.loads(raw)
    except Exception:
        body = {"raw": raw[:2000]}

    return {
        "route": route,
        "http": status,
        "elapsed_s": round(time.time() - started, 3),
        "body": body,
    }


def extract_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
    body = result.get("body")
    if not isinstance(body, dict):
        return []
    rows = body.get("rows")
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def main() -> int:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "purpose": "read_only_recovery_of_persisted_VETO000_result",
        "supabase_project": "nwduaycuofeggjtfdwsy",
        "mutates_database": False,
        "reruns_strategy": False,
        "attempts": [],
        "recovered": False,
        "recovered_route": None,
        "rows": [],
    }

    for round_no in range(1, 3):
        for route in ROUTES:
            result = fetch(route)
            result["round"] = round_no
            report["attempts"].append(result)
            rows = extract_rows(result)
            print(
                json.dumps(
                    {
                        "round": round_no,
                        "route": route,
                        "http": result.get("http"),
                        "elapsed_s": result.get("elapsed_s"),
                        "rows": len(rows),
                        "body_ok": (
                            result.get("body", {}).get("ok")
                            if isinstance(result.get("body"), dict)
                            else None
                        ),
                        "error": (
                            result.get("body", {}).get("error")
                            if isinstance(result.get("body"), dict)
                            else result.get("transport_error")
                        ),
                    },
                    ensure_ascii=False,
                )
            )
            if rows:
                report["recovered"] = True
                report["recovered_route"] = route
                report["rows"] = rows
                OUT.write_text(
                    json.dumps(report, indent=2, ensure_ascii=False),
                    encoding="utf-8",
                )
                print(f"RECOVERED VETO000: {len(rows)} persisted row(s) via {route}")
                return 0
        if round_no == 1:
            time.sleep(15)

    OUT.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print("VETO000 payload not recovered in this run; artifact contains all route diagnostics.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
