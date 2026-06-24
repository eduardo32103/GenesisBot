from __future__ import annotations

from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from app.telegram.legacy_bridge import install_legacy_operations_bridge


class _FakeTeleBot:
    def __init__(self, token: str = "") -> None:
        self.token = token
        self.registered: list[tuple[dict, object]] = []
        self.replies: list[tuple[object, str, dict]] = []

    def message_handler(self, *args, **kwargs):
        del args

        def decorator(func):
            self.registered.append((kwargs, func))
            return func

        return decorator

    def reply_to(self, message, text, **kwargs):
        self.replies.append((message, text, kwargs))


_ORIGINAL_MESSAGE_HANDLER = _FakeTeleBot.message_handler


class _LegacyBridgeTests(TestCase):
    def setUp(self) -> None:
        _FakeTeleBot._genesis_operations_bridge_installed = False
        _FakeTeleBot.message_handler = _ORIGINAL_MESSAGE_HANDLER
        if hasattr(_FakeTeleBot, "_genesis_original_message_handler"):
            delattr(_FakeTeleBot, "_genesis_original_message_handler")
        self.telebot_module = SimpleNamespace(TeleBot=_FakeTeleBot)
        install_legacy_operations_bridge(self.telebot_module)

    def _message(self, text: str = "/cmd", chat_id: str = "123") -> object:
        return SimpleNamespace(text=text, chat=SimpleNamespace(id=chat_id))

    def _attach_runtime_globals(self, func, *, bot) -> None:
        func.__globals__["get_db_connection"] = lambda: "conn"
        func.__globals__["save_state_to_telegram"] = lambda: None
        func.__globals__["_restore_from_b64"] = lambda payload: payload
        func.__globals__["get_tracked_tickers"] = lambda: ["NVDA"]
        func.__globals__["fetch_and_analyze_stock"] = lambda ticker: {"ticker": ticker}
        func.__globals__["update_smc_memory"] = lambda ticker, payload: (ticker, payload)
        func.__globals__["bot"] = bot
        func.__globals__["CHAT_ID"] = "123"

    def test_check_db_command_is_wrapped(self) -> None:
        bot = _FakeTeleBot()

        def legacy_handler(message):
            raise AssertionError("No debe ejecutarse el bloque legacy")

        legacy_handler.__name__ = "test_db"
        decorator = bot.message_handler(commands=["check_db"])
        decorator(legacy_handler)
        registered = bot.registered[-1][1]

        with (
            patch("app.telegram.legacy_bridge._build_operations_hooks", return_value=object()),
            patch("app.telegram.legacy_bridge.handle_check_db") as handler,
        ):
            message = self._message("/check_db")
            registered(message)
            handler.assert_called_once()
            self.assertIs(handler.call_args.args[0], message)

    def test_recover_keeps_chat_guard(self) -> None:
        bot = _FakeTeleBot()

        def legacy_handler(message):
            raise AssertionError("No debe ejecutarse el bloque legacy")

        legacy_handler.__name__ = "cmd_recover"
        self._attach_runtime_globals(legacy_handler, bot=bot)

        decorator = bot.message_handler(commands=["recover"])
        decorator(legacy_handler)
        registered = bot.registered[-1][1]

        with (
            patch("app.telegram.legacy_bridge._build_operations_hooks", return_value=object()),
            patch("app.telegram.legacy_bridge.handle_recover") as handler,
        ):
            registered(self._message("/recover payload", chat_id="999"))
            handler.assert_not_called()

            message = self._message("/recover payload", chat_id="123")
            registered(message)
            handler.assert_called_once()
            self.assertIs(handler.call_args.args[0], message)

    def test_backup_keeps_chat_guard(self) -> None:
        bot = _FakeTeleBot()

        def legacy_handler(message):
            raise AssertionError("No debe ejecutarse el bloque legacy")

        legacy_handler.__name__ = "cmd_backup"
        self._attach_runtime_globals(legacy_handler, bot=bot)

        decorator = bot.message_handler(commands=["backup"])
        decorator(legacy_handler)
        registered = bot.registered[-1][1]

        with (
            patch("app.telegram.legacy_bridge._build_operations_hooks", return_value=object()),
            patch("app.telegram.legacy_bridge.handle_backup") as handler,
        ):
            registered(self._message("/backup", chat_id="999"))
            handler.assert_not_called()

            message = self._message("/backup", chat_id="123")
            registered(message)
            handler.assert_called_once()
            self.assertIs(handler.call_args.args[0], message)
