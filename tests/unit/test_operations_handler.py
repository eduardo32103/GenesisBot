from __future__ import annotations

import unittest

from app.telegram.handlers.operations import OperationsHandlerHooks, handle_backup, handle_check_db, handle_recover


class DummyChat:
    def __init__(self, chat_id: int) -> None:
        self.id = chat_id


class DummyMessage:
    def __init__(self, text: str = "", chat_id: int = 12345) -> None:
        self.text = text
        self.chat = DummyChat(chat_id)


class FakeCursor:
    def __init__(self, version: str) -> None:
        self.version = version
        self.executed: list[str] = []

    def execute(self, query: str) -> None:
        self.executed.append(query)

    def fetchone(self) -> tuple[str]:
        return (self.version,)


class FakeConnection:
    def __init__(self, version: str) -> None:
        self.cursor_instance = FakeCursor(version)

    def cursor(self) -> FakeCursor:
        return self.cursor_instance


class OperationsHandlerTests(unittest.TestCase):
    def _build_hooks(
        self,
        *,
        connection_factory=None,
        tracked_tickers: list[str] | None = None,
        recover_error: Exception | None = None,
        analysis_by_ticker: dict[str, object] | None = None,
    ) -> tuple[OperationsHandlerHooks, dict[str, list]]:
        prints: list[str] = []
        replies: list[dict] = []
        saved_states: list[str] = []
        restored_payloads: list[str] = []
        analyzed_tickers: list[str] = []
        updated_memory: list[tuple[str, object]] = []

        tracked_tickers = tracked_tickers or []
        analysis_by_ticker = analysis_by_ticker or {}

        def get_db_connection():
            if callable(connection_factory):
                return connection_factory()
            return connection_factory

        def print_line(text: str) -> None:
            prints.append(text)

        def save_state() -> None:
            saved_states.append("saved")

        def restore_from_b64(payload: str) -> None:
            if recover_error is not None:
                raise recover_error
            restored_payloads.append(payload)

        def get_tracked_tickers() -> list[str]:
            return list(tracked_tickers)

        def fetch_and_analyze_stock(ticker: str):
            analyzed_tickers.append(ticker)
            return analysis_by_ticker.get(ticker)

        def update_smc_memory(ticker: str, analysis: object) -> None:
            updated_memory.append((ticker, analysis))

        def reply_to(message: DummyMessage, text: str, **kwargs) -> None:
            replies.append({"message": message, "text": text, "kwargs": kwargs})

        hooks = OperationsHandlerHooks(
            get_db_connection=get_db_connection,
            print_line=print_line,
            save_state=save_state,
            restore_from_b64=restore_from_b64,
            get_tracked_tickers=get_tracked_tickers,
            fetch_and_analyze_stock=fetch_and_analyze_stock,
            update_smc_memory=update_smc_memory,
            reply_to=reply_to,
        )
        state = {
            "prints": prints,
            "replies": replies,
            "saved_states": saved_states,
            "restored_payloads": restored_payloads,
            "analyzed_tickers": analyzed_tickers,
            "updated_memory": updated_memory,
        }
        return hooks, state

    def test_handle_check_db_prints_success_status(self) -> None:
        hooks, state = self._build_hooks(connection_factory=FakeConnection("PostgreSQL 16.0"))
        handle_check_db(DummyMessage("/check_db"), hooks=hooks)
        self.assertEqual(len(state["prints"]), 2)
        self.assertIn("Intentando conectar con Supabase", state["prints"][0])
        self.assertIn("CONEXIÓN ESTABLECIDA", state["prints"][1])
        self.assertIn("PostgreSQL 16.0", state["prints"][1])

    def test_handle_check_db_prints_error_when_connection_missing(self) -> None:
        hooks, state = self._build_hooks(connection_factory=None)
        handle_check_db(DummyMessage("/check_db"), hooks=hooks)
        self.assertEqual(len(state["prints"]), 2)
        self.assertIn("ERROR DE RED O AUTENTICACIÓN", state["prints"][1])
        self.assertIn("conn es None", state["prints"][1])

    def test_handle_backup_saves_and_replies(self) -> None:
        hooks, state = self._build_hooks(tracked_tickers=["NVDA", "MSFT"])
        handle_backup(DummyMessage("/backup"), hooks=hooks)
        self.assertEqual(state["saved_states"], ["saved"])
        self.assertEqual(len(state["replies"]), 1)
        self.assertIn("Backup forzado completado", state["replies"][0]["text"])
        self.assertIn("2 activos", state["replies"][0]["text"])

    def test_handle_recover_requires_payload(self) -> None:
        hooks, state = self._build_hooks()
        handle_recover(DummyMessage("/recover"), hooks=hooks)
        self.assertEqual(len(state["replies"]), 1)
        self.assertIn("Uso: `/recover", state["replies"][0]["text"])
        self.assertEqual(state["replies"][0]["kwargs"]["parse_mode"], "Markdown")

    def test_handle_recover_restores_replies_and_refreshes_memory(self) -> None:
        hooks, state = self._build_hooks(
            tracked_tickers=["NVDA", "BTC-USD"],
            analysis_by_ticker={"NVDA": {"smc": "ok"}, "BTC-USD": None},
        )
        handle_recover(DummyMessage("/recover ABC123"), hooks=hooks)
        self.assertEqual(state["restored_payloads"], ["ABC123"])
        self.assertEqual(state["saved_states"], ["saved"])
        self.assertEqual(len(state["replies"]), 1)
        self.assertIn("RECUPERACIÓN EXITOSA", state["replies"][0]["text"])
        self.assertEqual(state["analyzed_tickers"], ["NVDA", "BTC-USD"])
        self.assertEqual(state["updated_memory"], [("NVDA", {"smc": "ok"})])

    def test_handle_recover_replies_with_error_when_restore_fails(self) -> None:
        hooks, state = self._build_hooks(recover_error=RuntimeError("payload inválido"))
        handle_recover(DummyMessage("/recover ABC123"), hooks=hooks)
        self.assertEqual(len(state["replies"]), 1)
        self.assertIn("Error en recuperación", state["replies"][0]["text"])
        self.assertEqual(state["replies"][0]["kwargs"]["parse_mode"], "Markdown")


if __name__ == "__main__":
    unittest.main()
