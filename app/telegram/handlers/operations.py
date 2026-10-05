from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from infra.db.health import probe_database_version
from infra.storage.backups import (
    execute_manual_backup,
    execute_manual_recovery,
    extract_recovery_payload,
    refresh_smc_after_recovery,
)


@dataclass
class OperationsHandlerHooks:
    get_db_connection: Callable[[], Any]
    print_line: Callable[[str], None]
    save_state: Callable[[], None]
    restore_from_b64: Callable[[str], None]
    get_tracked_tickers: Callable[[], list[str]]
    fetch_and_analyze_stock: Callable[[str], Any]
    update_smc_memory: Callable[[str, Any], None]
    reply_to: Callable[..., Any]


def handle_check_db(message: object, *, hooks: OperationsHandlerHooks) -> None:
    del message
    hooks.print_line("🔳 Intentando conectar con Supabase (Timeout de 5 segundos)...")
    result = probe_database_version(hooks.get_db_connection)
    if result.ok:
        hooks.print_line(
            "✅ CONEXIÓN ESTABLECIDA\n"
            "PostgreSQL OK. Base de datos en línea y funcional.\n\n"
            f"Detalle: {result.version}"
        )
        return
    hooks.print_line(
        "❌ ERROR DE RED O AUTENTICACIÓN\n"
        "Supabase ha rechazado la conexión.\n\n"
        f"Log Técnico: {result.error}"
    )


def handle_backup(message: object, *, hooks: OperationsHandlerHooks) -> None:
    result = execute_manual_backup(
        save_state=hooks.save_state,
        get_tracked_tickers=hooks.get_tracked_tickers,
    )
    hooks.reply_to(
        message,
        f"✅ Backup forzado completado.\n📊 {len(result.tracked_tickers)} activos guardados en Telegram Cloud.",
    )


def handle_recover(message: object, *, hooks: OperationsHandlerHooks) -> None:
    payload = extract_recovery_payload(getattr(message, "text", ""))
    if not payload:
        hooks.reply_to(
            message,
            "⚠️ Restauración crítica.\nUso: `/recover [STRING_BASE64_DEL_LOG]`",
            parse_mode="Markdown",
        )
        return

    try:
        result = execute_manual_recovery(
            b64_payload=payload,
            restore_from_b64=hooks.restore_from_b64,
            save_state=hooks.save_state,
            get_tracked_tickers=hooks.get_tracked_tickers,
        )
        hooks.reply_to(
            message,
            "✅ **¡RECUPERACIÓN EXITOSA!**\n"
            f"Se restauraron {len(result.tracked_tickers)} activos.\n"
            "El backup ya fue guardado en Telegram.",
            parse_mode="Markdown",
        )
        refresh_smc_after_recovery(
            tracked_tickers=result.tracked_tickers,
            fetch_and_analyze_stock=hooks.fetch_and_analyze_stock,
            update_smc_memory=hooks.update_smc_memory,
        )
    except Exception as exc:
        hooks.reply_to(message, f"❌ Error en recuperación: `{exc}`", parse_mode="Markdown")
