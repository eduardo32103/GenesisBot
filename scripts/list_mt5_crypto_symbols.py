from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_crypto_symbol_discovery import discover_mt5_crypto_symbols  # noqa: E402


def main() -> int:
    result = discover_mt5_crypto_symbols()
    print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
