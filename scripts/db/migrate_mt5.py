from __future__ import annotations

import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.genesis.memory_store import MemoryStore


def main() -> int:
    os.environ["GENESIS_DB_MIGRATIONS_ENABLED"] = "true"
    store = MemoryStore()
    backend = getattr(store, "backend", "unknown")
    print(f"Genesis MT5 DB migration completed using backend={backend}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
