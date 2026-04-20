from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class TelegramRuntimeHooks:
    update_runtime_lock: Callable[..., None]
    wait_for_bot_leader_lock: Callable[[], None]
    acquire_bot_leader_lock: Callable[[], bool]
    log_telegram_boot_diagnostics: Callable[[], None]
    start_bot_leader_heartbeat: Callable[[], None]
    background_loop_proactivo: Callable[[], None]


@dataclass
class TelegramRuntimeState:
    instance_id: str
    instance_hostname: str
    instance_pid: int
    bot_lock_force_after_seconds: int
    bot_lock_heartbeat_seconds: int


def run_telegram_runtime(bot: Any, hooks: TelegramRuntimeHooks, state: TelegramRuntimeState, logger: logging.Logger | None = None) -> None:
    logger = logger or logging.getLogger("genesis.telegram.runtime")

    logger.info(
        "Identidad de esta instancia | self=%s | host=%s | pid=%s | takeover_forzado=%ss | heartbeat=%ss",
        state.instance_id,
        state.instance_hostname,
        state.instance_pid,
        state.bot_lock_force_after_seconds,
        state.bot_lock_heartbeat_seconds,
    )
    logger.info("Iniciando Genesis 1.0 - Persistencia: Telegram Cloud + SQLite local + Base64 logs")
    hooks.update_runtime_lock(stage="boot", notes="arranque inicial", heartbeat=False)
    hooks.wait_for_bot_leader_lock()
    hooks.update_runtime_lock(stage="boot", notes="lock adquirido", heartbeat=False)

    background_thread = threading.Thread(target=hooks.background_loop_proactivo, daemon=True)
    background_thread.start()

    logger.info("Limpiando webhook para evitar conflictos getUpdates...")
    try:
        hooks.update_runtime_lock(stage="boot", notes="limpiando webhook", heartbeat=False)
        bot.delete_webhook(drop_pending_updates=True)
        time.sleep(1)
    except Exception as exc:
        logger.warning("Webhook clear error (ignorado): %s", exc)
        hooks.update_runtime_lock(stage="boot", notes=f"webhook clear error: {exc}", heartbeat=False)

    hooks.log_telegram_boot_diagnostics()
    hooks.update_runtime_lock(stage="boot", notes="diagnostico telegram completado", heartbeat=False)
    hooks.start_bot_leader_heartbeat()
    logger.info("Iniciando Telegram polling...")

    while True:
        if not hooks.acquire_bot_leader_lock():
            hooks.update_runtime_lock(stage="esperando_lock", notes="liderazgo perdido antes de polling", heartbeat=False)
            hooks.wait_for_bot_leader_lock()
            hooks.log_telegram_boot_diagnostics()
        try:
            hooks.update_runtime_lock(stage="polling", notes="infinity_polling activo", heartbeat=True)
            logger.info("Genesis esta vivo y escuchando...")
            bot.infinity_polling(timeout=10, long_polling_timeout=5)
        except Exception as exc:
            hooks.update_runtime_lock(stage="polling_error", notes=str(exc)[:240], heartbeat=True)
            logger.error("Telegram polling caido: %s", exc)
            wait_seconds = 30 if "409" in str(exc) else 5
            if "409" in str(exc):
                try:
                    bot.delete_webhook(drop_pending_updates=False)
                except Exception:
                    pass
                if not hooks.acquire_bot_leader_lock():
                    hooks.wait_for_bot_leader_lock()
                    continue
            hooks.log_telegram_boot_diagnostics()
            logger.info("Reconectando en %s segundos...", wait_seconds)
            time.sleep(wait_seconds)
