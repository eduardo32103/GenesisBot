from __future__ import annotations

import unittest

from app.telegram.handlers.start import (
    DEFAULT_MENU_LABELS,
    StartHandlerHooks,
    build_start_inline_keyboard,
    build_start_reply_keyboard,
    handle_start,
)


class DummyChat:
    def __init__(self, chat_id: int) -> None:
        self.id = chat_id


class DummyMessage:
    def __init__(self, chat_id: int) -> None:
        self.chat = DummyChat(chat_id)


class StartHandlerTests(unittest.TestCase):
    def test_build_start_inline_keyboard_uses_expected_callbacks(self) -> None:
        markup = build_start_inline_keyboard()
        callback_data = [button.callback_data for row in markup.keyboard for button in row]
        self.assertEqual(
            callback_data,
            ["geopolitics", "super_radar_24h", "smc_levels", "wallet_status"],
        )

    def test_build_start_reply_keyboard_uses_default_labels(self) -> None:
        markup = build_start_reply_keyboard()
        labels = [button.text for row in markup.keyboard for button in row]
        self.assertEqual(
            labels,
            [
                DEFAULT_MENU_LABELS["geopolitics"],
                DEFAULT_MENU_LABELS["whales"],
                DEFAULT_MENU_LABELS["smc"],
                DEFAULT_MENU_LABELS["wallet"],
            ],
        )

    def test_handle_start_restores_state_and_sends_boot_messages(self) -> None:
        sent_messages: list[dict] = []
        replied_messages: list[dict] = []
        restore_calls: list[str] = []

        def restore_state() -> None:
            restore_calls.append("restore")

        def get_tracked_tickers() -> list[str]:
            return ["NVDA", "MSFT", "BTC-USD"]

        def make_card(title: str, lines: list[str], icon: str | None = None) -> str:
            return f"{icon}|{title}|{'||'.join(lines)}"

        def send_message(chat_id: int, text: str, **kwargs) -> None:
            sent_messages.append({"chat_id": chat_id, "text": text, "kwargs": kwargs})

        def reply_to(message: DummyMessage, text: str, **kwargs) -> None:
            replied_messages.append({"message": message, "text": text, "kwargs": kwargs})

        hooks = StartHandlerHooks(
            restore_state=restore_state,
            get_tracked_tickers=get_tracked_tickers,
            make_card=make_card,
            send_message=send_message,
            reply_to=reply_to,
        )

        handle_start(DummyMessage(12345), hooks=hooks)

        self.assertEqual(restore_calls, ["restore"])
        self.assertEqual(len(sent_messages), 1)
        self.assertEqual(sent_messages[0]["chat_id"], 12345)
        self.assertIn("Inicializando Base de Operaciones", sent_messages[0]["text"])
        self.assertEqual(len(replied_messages), 1)
        self.assertIn("Radar activo:</b> 3 activos", replied_messages[0]["text"])
        self.assertEqual(replied_messages[0]["kwargs"]["parse_mode"], "HTML")


if __name__ == "__main__":
    unittest.main()
