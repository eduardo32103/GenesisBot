from __future__ import annotations

"""Optional local MT5 Python bridge.

This file is intentionally journal-only by default. It must run on the same PC/VPS
where MetaTrader 5 is installed. The backend phase does not execute broker orders.
"""

from services.mt5.mt5_bridge import mt5_health


def main() -> None:
    health = mt5_health()
    print("Genesis MT5 local bridge placeholder")
    print(f"status={health['status']} order_policy={health['order_policy']} broker_touched={health['broker_touched']}")
    print("Install MetaTrader5 package and wire demo-only polling here when explicitly enabled.")


if __name__ == "__main__":
    main()
