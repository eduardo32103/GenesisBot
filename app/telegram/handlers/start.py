from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

try:
    from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup
except ImportError:  # pragma: no cover - fallback para pruebas locales sin pyTelegramBotAPI
    class InlineKeyboardButton:
        def __init__(self, text: str, callback_data: str) -> None:
            self.text = text
            self.callback_data = callback_data

    class KeyboardButton:
        def __init__(self, text: str) -> None:
            self.text = text

    class InlineKeyboardMarkup:
        def __init__(self, row_width: int = 2) -> None:
            self.row_width = row_width
            self.keyboard: list[list[InlineKeyboardButton]] = []

        def add(self, *buttons: InlineKeyboardButton) -> None:
            self.keyboard.append(list(buttons))

    class ReplyKeyboardMarkup:
        def __init__(self, resize_keyboard: bool = True, row_width: int = 2) -> None:
            self.resize_keyboard = resize_keyboard
            self.row_width = row_width
            self.keyboard: list[list[KeyboardButton]] = []

        def add(self, *buttons: KeyboardButton) -> None:
            self.keyboard.append(list(buttons))


DEFAULT_MENU_LABELS = {
    "geopolitics": "🌍 Geopolítica",
    "whales": "🐋 Radar de Ballenas",
    "smc": "🦅 Niveles SMC",
    "wallet": "💼 Mi Cartera",
}


@dataclass
class StartHandlerHooks:
    restore_state: Callable[[], None]
    get_tracked_tickers: Callable[[], list[str]]
    make_card: Callable[..., str]
    send_message: Callable[..., Any]
    reply_to: Callable[..., Any]


def build_start_inline_keyboard(labels: dict[str, str] | None = None) -> InlineKeyboardMarkup:
    current_labels = labels or DEFAULT_MENU_LABELS
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton(text=current_labels["geopolitics"], callback_data="geopolitics"),
        InlineKeyboardButton(text=current_labels["whales"], callback_data="super_radar_24h"),
    )
    markup.add(
        InlineKeyboardButton(text=current_labels["smc"], callback_data="smc_levels"),
        InlineKeyboardButton(text=current_labels["wallet"], callback_data="wallet_status"),
    )
    return markup


def build_start_reply_keyboard(labels: dict[str, str] | None = None) -> ReplyKeyboardMarkup:
    current_labels = labels or DEFAULT_MENU_LABELS
    reply_kbd = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    reply_kbd.add(
        KeyboardButton(current_labels["geopolitics"]),
        KeyboardButton(current_labels["whales"]),
    )
    reply_kbd.add(
        KeyboardButton(current_labels["smc"]),
        KeyboardButton(current_labels["wallet"]),
    )
    return reply_kbd


def handle_start(
    message: object,
    *,
    hooks: StartHandlerHooks,
    labels: dict[str, str] | None = None,
) -> None:
    chat = getattr(message, "chat", None)
    chat_id = getattr(chat, "id", None)
    if chat_id is None:
        raise ValueError("El mensaje no contiene chat.id")

    hooks.restore_state()
    tracked_tickers = hooks.get_tracked_tickers() or []

    reply_keyboard = build_start_reply_keyboard(labels)
    inline_keyboard = build_start_inline_keyboard(labels)

    hooks.send_message(
        chat_id,
        "🔄 Inicializando Base de Operaciones...",
        reply_markup=reply_keyboard,
    )

    reply_text = hooks.make_card(
        "GÉNESIS 1.0",
        [
            "✅ Bot iniciado correctamente.",
            f"📊 <b>Radar activo:</b> {len(tracked_tickers)} activos",
            "🛡️ <b>Persistencia:</b> cartera protegida y lista para operar",
            "🎛️ Usa los botones de abajo o el panel flotante para navegar",
        ],
        icon="🧠",
    )
    hooks.reply_to(message, reply_text, reply_markup=inline_keyboard, parse_mode="HTML")
