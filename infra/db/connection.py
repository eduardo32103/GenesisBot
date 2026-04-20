from __future__ import annotations

import logging
import ssl
import threading
import time
import urllib.parse
from dataclasses import dataclass, field

import pg8000.dbapi


DEFAULT_BOOTSTRAP_STATEMENTS = (
    "CREATE TABLE IF NOT EXISTS wallet (user_id BIGINT, ticker TEXT, is_investment INTEGER DEFAULT 0, amount_usd REAL DEFAULT 0.0, entry_price REAL DEFAULT 0.0, timestamp TEXT, PRIMARY KEY (user_id, ticker))",
    "CREATE TABLE IF NOT EXISTS global_stats (key TEXT PRIMARY KEY, value REAL)",
    "CREATE TABLE IF NOT EXISTS seen_events (hash_id TEXT PRIMARY KEY, timestamp TEXT)",
    "CREATE TABLE IF NOT EXISTS runtime_locks (lock_name TEXT PRIMARY KEY, instance_id TEXT, hostname TEXT, pid BIGINT, started_at TEXT, claimed_at TEXT, last_heartbeat TEXT, stage TEXT, notes TEXT)",
    "CREATE TABLE IF NOT EXISTS alert_events (alert_id TEXT PRIMARY KEY, alert_type TEXT, ticker TEXT, direction TEXT, entry_price REAL, created_at TEXT, title TEXT, summary TEXT, source TEXT, signal_strength REAL DEFAULT 0.0, metadata_json TEXT, status TEXT DEFAULT 'tracking')",
    "CREATE TABLE IF NOT EXISTS alert_validations (alert_id TEXT, horizon_key TEXT, scheduled_at TEXT, evaluated_at TEXT, current_price REAL, return_pct REAL, signed_return_pct REAL, outcome_label TEXT, score_value REAL, PRIMARY KEY (alert_id, horizon_key))",
    "CREATE TABLE IF NOT EXISTS alert_policy_audit (decision_id TEXT PRIMARY KEY, created_at TEXT, alert_type TEXT, ticker TEXT, raw_signal_strength REAL, normalized_strength REAL, required_strength REAL, was_allowed INTEGER DEFAULT 0, reason TEXT, context_json TEXT)",
)


@dataclass(frozen=True)
class DatabaseConfig:
    url: str


@dataclass
class DatabaseManager:
    database_url: str
    logger: logging.Logger | None = None
    connect_timeout: int = 10
    bootstrap_statements: tuple[str, ...] = DEFAULT_BOOTSTRAP_STATEMENTS
    _local: threading.local = field(default_factory=threading.local, init=False, repr=False)
    _bootstrap_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.logger is None:
            self.logger = logging.getLogger("genesis.db")

    def get_connection(self):
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                return conn
            except Exception:
                self.logger.warning("Conexion existente caida, reconectando...")
                try:
                    conn.close()
                except Exception:
                    pass
                self._local.conn = None

        if not self.database_url:
            self.logger.warning("DATABASE_URL no configurada")
            return None

        for attempt in range(1, 4):
            conn = None
            try:
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE

                parsed = urllib.parse.urlparse(self.database_url)
                conn = pg8000.dbapi.connect(
                    user=parsed.username,
                    password=parsed.password,
                    host=parsed.hostname,
                    port=parsed.port or 6543,
                    database=parsed.path[1:],
                    ssl_context=ctx,
                    timeout=self.connect_timeout,
                )

                with self._bootstrap_lock:
                    cursor = conn.cursor()
                    for statement in self.bootstrap_statements:
                        cursor.execute(statement)
                    conn.commit()

                self._local.conn = conn
                self.logger.info("Conexion exitosa a Supabase (intento %s/3)", attempt)
                return conn
            except Exception as exc:
                self.logger.error("Error de conexion a Supabase (intento %s/3): %s", attempt, exc)
                try:
                    if conn is not None:
                        conn.close()
                except Exception:
                    pass
                self._local.conn = None
                if attempt < 3:
                    time.sleep(attempt * 2)

        self.logger.error("No se pudo conectar a Supabase despues de 3 intentos")
        return None

    def init_db(self) -> None:
        self.get_connection()

    def close_thread_connection(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            return
        try:
            conn.close()
        finally:
            self._local.conn = None


def build_database_config(url: str) -> DatabaseConfig:
    return DatabaseConfig(url=url)
