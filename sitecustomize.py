from __future__ import annotations

import logging

try:
    import telebot
except ModuleNotFoundError:
    telebot = None

if telebot is not None:
    try:
        from app.telegram.legacy_bridge import install_legacy_operations_bridge

        install_legacy_operations_bridge(telebot)
    except Exception as exc:  # pragma: no cover - defensive startup bridge
        logging.getLogger(__name__).warning(
            "No pude instalar el bridge legado de operaciones: %s",
            exc,
        )
