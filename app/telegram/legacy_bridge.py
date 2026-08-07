from __future__ import annotations

from functools import wraps
from typing import Any, Iterable

from app.telegram.handlers.operations import (
    OperationsHandlerHooks,
    handle_backup,
    handle_check_db,
    handle_recover,
)


def _normalize_commands(commands: Any) -> set[str]:
    if commands is None:
        return set()
    if isinstance(commands, str):
        return {commands}
    if isinstance(commands, Iterable):
        return {str(command) for command in commands}
    return {str(commands)}


def _build_operations_hooks(func: Any) -> OperationsHandlerHooks:
    globals_map = func.__globals__
    return OperationsHandlerHooks(
        get_db_connection=globals_map["get_db_connection"],
        print_line=print,
        save_state=globals_map["save_state_to_telegram"],
        restore_from_b64=globals_map["_restore_from_b64"],
        get_tracked_tickers=globals_map["get_tracked_tickers"],
        fetch_and_analyze_stock=globals_map["fetch_and_analyze_stock"],
        update_smc_memory=globals_map["update_smc_memory"],
        reply_to=globals_map["bot"].reply_to,
    )


def _chat_is_allowed(message: Any, func: Any) -> bool:
    globals_map = func.__globals__
    expected_chat_id = str(globals_map.get("CHAT_ID") or "").strip()
    if not expected_chat_id:
        return True
    chat = getattr(message, "chat", None)
    actual_chat_id = str(getattr(chat, "id", "")).strip()
    return actual_chat_id == expected_chat_id


def _wrap_check_db(func: Any):
    @wraps(func)
    def wrapper(message: Any, *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        hooks = _build_operations_hooks(func)
        handle_check_db(message, hooks=hooks)

    return wrapper


def _wrap_recover(func: Any):
    @wraps(func)
    def wrapper(message: Any, *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        if not _chat_is_allowed(message, func):
            return
        hooks = _build_operations_hooks(func)
        handle_recover(message, hooks=hooks)

    return wrapper


def _wrap_backup(func: Any):
    @wraps(func)
    def wrapper(message: Any, *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        if not _chat_is_allowed(message, func):
            return
        hooks = _build_operations_hooks(func)
        handle_backup(message, hooks=hooks)

    return wrapper


_COMMAND_WRAPPERS = {
    "check_db": _wrap_check_db,
    "recover": _wrap_recover,
    "backup": _wrap_backup,
}


def install_legacy_operations_bridge(telebot_module: Any) -> bool:
    telebot_class = getattr(telebot_module, "TeleBot", None)
    if telebot_class is None:
        return False

    if getattr(telebot_class, "_genesis_operations_bridge_installed", False):
        return True

    original_message_handler = telebot_class.message_handler

    def patched_message_handler(self: Any, *args: Any, **kwargs: Any):
        decorator = original_message_handler(self, *args, **kwargs)
        commands = _normalize_commands(kwargs.get("commands"))

        def register(func: Any):
            wrapped = func
            for command_name, wrapper_builder in _COMMAND_WRAPPERS.items():
                if command_name in commands:
                    wrapped = wrapper_builder(func)
                    break
            return decorator(wrapped)

        return register

    telebot_class.message_handler = patched_message_handler
    telebot_class._genesis_operations_bridge_installed = True
    telebot_class._genesis_original_message_handler = original_message_handler
    return True
