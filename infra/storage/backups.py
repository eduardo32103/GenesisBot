from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass(frozen=True)
class BackupCommandResult:
    tracked_tickers: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class RecoveryCommandResult:
    tracked_tickers: list[str] = field(default_factory=list)


def extract_recovery_payload(command_text: str) -> str | None:
    command_parts = str(command_text or "").split(" ", 1)
    if len(command_parts) < 2:
        return None
    payload = command_parts[1].strip()
    return payload or None


def execute_manual_backup(
    *,
    save_state: Callable[[], None],
    get_tracked_tickers: Callable[[], list[str]],
) -> BackupCommandResult:
    save_state()
    return BackupCommandResult(tracked_tickers=list(get_tracked_tickers() or []))


def execute_manual_recovery(
    *,
    b64_payload: str,
    restore_from_b64: Callable[[str], None],
    save_state: Callable[[], None],
    get_tracked_tickers: Callable[[], list[str]],
) -> RecoveryCommandResult:
    restore_from_b64(b64_payload)
    save_state()
    return RecoveryCommandResult(tracked_tickers=list(get_tracked_tickers() or []))


def refresh_smc_after_recovery(
    *,
    tracked_tickers: list[str],
    fetch_and_analyze_stock: Callable[[str], Any],
    update_smc_memory: Callable[[str, Any], None],
) -> None:
    for ticker in tracked_tickers:
        analysis = fetch_and_analyze_stock(ticker)
        if analysis:
            update_smc_memory(ticker, analysis)
