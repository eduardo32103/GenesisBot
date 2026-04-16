import os, pg8000.dbapi, telebot
import ssl
import urllib.parse
import logging
import base64
import requests
import re
import xml.etree.ElementTree as ET
import pandas as pd
# yfinance ELIMINADO â€” Todo via FMP Pro
import threading
import time
import json
from collections import deque
from telebot.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from datetime import datetime, timedelta

# ConfiguraciÃ³n extendida de logs para Railway
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY') # Volvemos a requerir OpenAI para visiÃ³n
# Canal privado donde el bot fija el backup (puede ser el mismo CHAT_ID o un canal dedicado)
BACKUP_CHAT_ID = os.environ.get('BACKUP_CHAT_ID', CHAT_ID)
FMP_API_KEY = "".join(c for c in os.environ.get('FMP_API_KEY', '') if ord(c) < 128).strip()

if not TELEGRAM_TOKEN or not CHAT_ID:
    logging.critical("Falta TELEGRAM_TOKEN o CHAT_ID. Saliendo.")
    exit()

if not os.environ.get('FMP_API_KEY'):
    logging.warning("âš ï¸ FMP_API_KEY no configurada. El motor de precios FMP no funcionarÃ¡.")
else:
    logging.info(f"âœ… FMP_API_KEY cargada correctamente ({len(os.environ.get('FMP_API_KEY'))} caracteres).")

bot = telebot.TeleBot(TELEGRAM_TOKEN)

# --- BASE DE DATOS LOCAL/REMOTA (PostgreSQL) ---
DATABASE_URL = os.environ.get('DATABASE_URL')
USE_RAM_MODE = False
RAM_WALLET = {}
RAM_PNL = 0.0
DATA_DIR = os.environ.get('DATA_DIR', '.')
os.makedirs(DATA_DIR, exist_ok=True)

_global_db_conn = None

def get_db_connection():
    global _global_db_conn
    # Si ya hay conexiÃ³n activa, verificarla
    if _global_db_conn is not None:
        try:
            c = _global_db_conn.cursor()
            c.execute("SELECT 1")
            c.fetchone()
            return _global_db_conn
        except:
            print("DEBUG DB: ConexiÃ³n existente caÃ­da, reconectando...")
            _global_db_conn = None

    url = os.environ.get('DATABASE_URL')
    if not url:
        print("DEBUG DB: DATABASE_URL no configurada")
        return None

    # Reintentar 3 veces con backoff
    for attempt in range(1, 4):
        try:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

            r = urllib.parse.urlparse(url)

            _global_db_conn = pg8000.dbapi.connect(
                user=r.username,
                password=r.password,
                host=r.hostname,
                port=r.port or 6543,
                database=r.path[1:],
                ssl_context=ctx,
                timeout=10
            )
            print(f"âœ… ConexiÃ³n exitosa a Supabase (intento {attempt}/3)")

            # Crear tablas si no existen
            cr = _global_db_conn.cursor()
            cr.execute("CREATE TABLE IF NOT EXISTS wallet (user_id BIGINT, ticker TEXT, is_investment INTEGER DEFAULT 0, amount_usd REAL DEFAULT 0.0, entry_price REAL DEFAULT 0.0, timestamp TEXT, PRIMARY KEY (user_id, ticker))")
            cr.execute("CREATE TABLE IF NOT EXISTS global_stats (key TEXT PRIMARY KEY, value REAL)")
            cr.execute("CREATE TABLE IF NOT EXISTS seen_events (hash_id TEXT PRIMARY KEY, timestamp TEXT)")
            _global_db_conn.commit()

            return _global_db_conn

        except Exception as e:
            print(f"âŒ Error de conexiÃ³n a Supabase (intento {attempt}/3): {e}")
            _global_db_conn = None
            if attempt < 3:
                wait = attempt * 2  # 2s, 4s
                print(f"DEBUG DB: Reintentando en {wait}s...")
                time.sleep(wait)

    print("âŒ FATAL: No se pudo conectar a Supabase despuÃ©s de 3 intentos")
    return None

def init_db():
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS wallet (user_id BIGINT, ticker TEXT, is_investment INTEGER, amount_usd REAL, entry_price REAL, timestamp TEXT, PRIMARY KEY (user_id, ticker));''')
        c.execute('''CREATE TABLE IF NOT EXISTS global_stats (key TEXT PRIMARY KEY, value REAL)''')
        c.execute('''CREATE TABLE IF NOT EXISTS seen_events (hash_id TEXT PRIMARY KEY, timestamp TEXT)''')
        conn.commit()
        pass # conn.close() delegado a pooling global
    except Exception as e:
        print(f"âŒ Error: No se pudo conectar a Supabase -> {e}")
        logging.error(f"Error init_db: {e}")

init_db()

# =====================================================================
# PERSISTENCIA REAL: TELEGRAM COMO BASE DE DATOS
# El bot guarda el estado completo de la cartera como un mensaje
# en Telegram. Railway no puede borrar mensajes de Telegram.
# =====================================================================
BACKUP_PREFIX = "ðŸ”GENESIS_BACKUP_V2ðŸ”"
_last_backup_msg_id = None  # Cache del message_id del Ãºltimo backup

def _build_backup_payload():
    """Construye el JSON completo del estado actual"""
    portfolio = get_all_portfolio_data()
    realized = get_realized_pnl()
    return {
        "portfolio": portfolio,
        "global_stats": {"realized_pnl": realized},
        "timestamp": datetime.now().isoformat()
    }

def _save_local_portfolio_json():
    """Guarda portfolio.json como respaldo local persistente en disco"""
    try:
        portfolio = get_all_portfolio_data()
        realized = get_realized_pnl()
        backup_data = {}
        for tk, info in portfolio.items():
            backup_data[tk] = {
                "is_investment": int(info.get("is_investment", 0)),
                "amount_usd": float(info.get("amount_usd", 0)),
                "entry_price": float(info.get("entry_price", 0)),
                "timestamp": info.get("timestamp", "")
            }

        # Guardar en mÃºltiples ubicaciones para mÃ¡xima persistencia
        script_dir = os.path.dirname(os.path.abspath(__file__))
        paths = [
            os.path.join(script_dir, 'portfolio.json'),
            os.path.join(DATA_DIR, 'portfolio.json'),
        ]
        for path in paths:
            try:
                with open(path, 'w') as f:
                    json.dump(backup_data, f, ensure_ascii=False, indent=2)
                logging.debug(f"Portfolio guardado en {path}")
            except Exception:
                pass
    except Exception as e:
        logging.debug(f"Error guardando portfolio local: {e}")

def _load_backup_msg_id():
    """Carga el message_id del Ãºltimo backup desde un archivo local"""
    paths = [
        os.path.join(DATA_DIR, '.backup_msg_id'),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), '.backup_msg_id'),
    ]
    for p in paths:
        try:
            if os.path.exists(p):
                with open(p, 'r') as f:
                    return int(f.read().strip())
        except Exception:
            pass
    return None

def _save_backup_msg_id(msg_id):
    """Guarda el message_id del backup en disco para sobrevivir reinicios"""
    paths = [
        os.path.join(DATA_DIR, '.backup_msg_id'),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), '.backup_msg_id'),
    ]
    for p in paths:
        try:
            with open(p, 'w') as f:
                f.write(str(msg_id))
        except Exception:
            pass

def save_state_to_telegram():
    """Guarda el estado completo en Telegram de forma INVISIBLE y PERSISTENTE.
    Estrategia: editar siempre el mismo mensaje (nunca borrarlo)."""
    global _last_backup_msg_id

    # SIEMPRE guardar en disco local primero
    _save_local_portfolio_json()

    try:
        payload = _build_backup_payload()
        # Verificar que el payload tenga datos reales antes de guardar
        portfolio = payload.get("portfolio", {})
        has_real_data = any(
            info.get("is_investment") or info.get("entry_price", 0) > 0
            for info in portfolio.values()
        ) if portfolio else False

        if not portfolio:
            logging.info("BACKUP: Portfolio vacÃ­o, no se guarda backup.")
            return

        json_str = json.dumps(payload, ensure_ascii=False)
        b64 = base64.b64encode(json_str.encode('utf-8')).decode('utf-8')
        backup_text = f"{BACKUP_PREFIX}\n{b64}"

        logging.info(f"BACKUP_DB_LOG: {len(portfolio)} activos ({len(b64)} bytes)")

        # Cargar msg_id desde disco si no lo tenemos en memoria
        if not _last_backup_msg_id:
            _last_backup_msg_id = _load_backup_msg_id()

        # ESTRATEGIA 1: Editar el mensaje existente (invisible para Eduardo)
        if _last_backup_msg_id:
            try:
                bot.edit_message_text(
                    backup_text,
                    chat_id=BACKUP_CHAT_ID,
                    message_id=_last_backup_msg_id
                )
                logging.info(f"âœ… Backup actualizado (msg_id: {_last_backup_msg_id})")
                return
            except Exception as e:
                logging.debug(f"No se pudo editar backup msg {_last_backup_msg_id}: {e}")
                _last_backup_msg_id = None

        # ESTRATEGIA 2: Crear mensaje nuevo y fijarlo
        msg = bot.send_message(BACKUP_CHAT_ID, backup_text, disable_notification=True)
        _last_backup_msg_id = msg.message_id
        _save_backup_msg_id(msg.message_id)
        logging.info(f"âœ… Backup nuevo enviado (msg_id: {_last_backup_msg_id})")

        # Fijar el mensaje para que SIEMPRE sea recuperable
        try:
            bot.pin_chat_message(BACKUP_CHAT_ID, msg.message_id, disable_notification=True)
            logging.info("ðŸ“Œ Backup fijado en el chat.")
        except Exception as e:
            logging.debug(f"No se pudo fijar backup: {e}")

        # Si va al chat principal, borrar SOLO despuÃ©s de fijarlo
        if str(BACKUP_CHAT_ID) == str(CHAT_ID):
            try:
                time.sleep(0.5)
                bot.delete_message(CHAT_ID, msg.message_id)
                _last_backup_msg_id = None  # Ya no podemos editarlo
                logging.info("Backup borrado del chat visible (pero queda en pinned).")
            except Exception:
                pass

    except Exception as e:
        logging.error(f"Error guardando backup en Telegram: {e}")

def restore_state_from_telegram():
    """Recupera la cartera usando TODAS las fuentes disponibles.
    Prioridad: Pinned message > Saved msg_id > Updates > portfolio.json"""
    global _last_backup_msg_id

    existing = get_tracked_tickers()
    if existing:
        logging.info(f"DB local SQLite OK y persistente: {len(existing)} activos en radar.")
        return True

    logging.info("ðŸ”„ DB local vacÃ­a o sin inversiones. Buscando backup...")

    # === FUENTE 1: Mensaje fijado (MÃS CONFIABLE â€” sobrevive reinicios) ===
    try:
        chat_info = bot.get_chat(BACKUP_CHAT_ID)
        pinned = chat_info.pinned_message
        if pinned and pinned.text and pinned.text.startswith(BACKUP_PREFIX):
            b64_data = pinned.text.replace(BACKUP_PREFIX, "").strip()
            if b64_data:
                _restore_from_b64(b64_data)
                _last_backup_msg_id = pinned.message_id
                _save_backup_msg_id(pinned.message_id)
                logging.info(f"âœ… RESTAURACIÃ“N desde mensaje FIJADO (msg_id: {pinned.message_id})")
                return True
    except Exception as e:
        logging.debug(f"Pinned message check failed: {e}")

    # === FUENTE 2: Message ID guardado en disco ===
    saved_id = _load_backup_msg_id()
    if saved_id:
        try:
            # No podemos leer un mensaje por ID directamente, pero si es el pinned ya lo probamos arriba
            # Intentar con forward trick: reenviar el mensaje a nosotros mismos para leerlo
            logging.info(f"Backup msg_id guardado en disco: {saved_id}")
        except Exception:
            pass

    # === FUENTE 3: Updates recientes de Telegram ===
    try:
        updates = bot.get_updates(limit=100, timeout=5)
        for update in reversed(updates):
            msg = update.message or update.edited_message
            if msg and msg.text and msg.text.startswith(BACKUP_PREFIX):
                b64_data = msg.text.replace(BACKUP_PREFIX, "").strip()
                if b64_data:
                    _restore_from_b64(b64_data)
                    _last_backup_msg_id = msg.message_id
                    _save_backup_msg_id(msg.message_id)
                    logging.info(f"âœ… RESTAURACIÃ“N desde updates (msg_id: {msg.message_id})")
                    return True
    except Exception as e:
        logging.debug(f"Updates check failed: {e}")

    # === FUENTE 4: portfolio.json del repositorio/disco ===
    # Si todo falla, no borrar la DB actual, simplemente decir que no hay backup remoto
    logging.warning("âš ï¸ No se encontrÃ³ NINGÃšN respaldo en cloud. Cartera usarÃ¡ solo SQLite local.")
    return False

def _restore_from_b64(b64_data):
    """Restaura la base de datos desde un string Base64"""
    global RAM_WALLET, RAM_PNL
    try:
        json_str = base64.b64decode(b64_data).decode('utf-8')
        payload = json.loads(json_str)

        portfolio = payload.get("portfolio", {})
        stats = payload.get("global_stats", {})

        conn = get_db_connection()
        if not conn: return
        try:
            c = conn.cursor()
            for tk, info in portfolio.items():
                c.execute('''
                    INSERT INTO wallet (ticker, is_investment, amount_usd, entry_price, timestamp) 
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (ticker) DO UPDATE SET 
                    is_investment = EXCLUDED.is_investment, amount_usd = EXCLUDED.amount_usd, 
                    entry_price = EXCLUDED.entry_price, timestamp = EXCLUDED.timestamp
                ''', (tk, int(info.get("is_investment", 0)), float(info.get("amount_usd", 0)), float(info.get("entry_price", 0)), info.get("timestamp", datetime.now().isoformat())))

            rpnl = stats.get("realized_pnl", 0)
            if rpnl:
                c.execute('''
                    INSERT INTO global_stats (key, value) VALUES ('realized_pnl', %s)
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                ''', (float(rpnl),))
            conn.commit()
        finally:
            pass # conn.close() delegado a pooling global

        logging.info(f"Restaurados {len(portfolio)} activos desde backup Base64.")
    except Exception as e:
        logging.error(f"Error restaurando B64: {e}")

def _restore_from_repo_json():
    """Ãšltimo recurso: lee portfolio.json que estÃ¡ en el repositorio Git"""
    # Intentar varias rutas posibles
    script_dir = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.environ.get('DATA_DIR', '.')
    possible_paths = [
        os.path.join(script_dir, 'portfolio.json'),
        'portfolio.json',
        os.path.join(DATA_DIR, 'portfolio.json')
    ]

    for json_path in possible_paths:
        if os.path.exists(json_path):
            try:
                with open(json_path, 'r') as f:
                    legacy = json.load(f)

                conn = get_db_connection()
                if not conn: return False
                try:
                    c = conn.cursor()
                    for tk, val in legacy.items():
                        if isinstance(val, (int, float)):
                            c.execute('''INSERT INTO wallet (user_id, ticker, is_investment, amount_usd, entry_price, timestamp) 
                                         VALUES (%s, %s, 1, 1000.0, %s, %s) ON CONFLICT (user_id, ticker) DO NOTHING''',
                                (int(CHAT_ID), tk, float(val), datetime.now().isoformat()))
                        elif isinstance(val, dict):
                            c.execute('''INSERT INTO wallet (user_id, ticker, is_investment, amount_usd, entry_price, timestamp) 
                                         VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (user_id, ticker) DO NOTHING''',
                                (int(CHAT_ID), tk, int(val.get('is_investment', 1)), float(val.get('amount_usd', 1000.0)), float(val.get('entry_price', 0.0)), datetime.now().isoformat()))
                    conn.commit()
                finally:
                    pass # conn.close() delegado a pooling global

                logging.info(f"âœ… RestauraciÃ³n desde portfolio.json ({json_path}) exitosa: {len(legacy)} activos.")
                return True
            except Exception as e:
                logging.error(f"Error leyendo {json_path}: {e}")

    logging.warning("âš ï¸ No se encontrÃ³ ningÃºn respaldo. Cartera inicia vacÃ­a.")
    return False


# --- MAPEO DURO Y ALIAS VISUAL  ---
def remap_ticker(ticker_input):
    tk = ticker_input.upper()
    if ":" in tk:
        tk = tk.split(":")[-1]
    if tk in ["LCO", "BRENT", "PETROLEO"]: return "BZ=F"
    if tk in ["ORO", "GOLD", "GC"]: return "GC=F"
    if tk in ["BTC", "BITCOIN"]: return "BTC-USD"
    if tk in ["ETH", "ETHEREUM"]: return "ETH-USD"
    if tk in ["SOL", "SOLANA"]: return "SOL-USD"
    if tk in ["BNB"]: return "BNB-USD"
    if tk in ["XRP", "RIPPLE"]: return "XRP-USD"
    if tk in ["ADA", "CARDANO"]: return "ADA-USD"
    if tk in ["DOGE", "DOGECOIN"]: return "DOGE-USD"
    if tk in ["AVAX", "AVALANCHE"]: return "AVAX-USD"
    if tk in ["DOT", "POLKADOT"]: return "DOT-USD"
    if tk in ["LINK", "CHAINLINK"]: return "LINK-USD"
    return tk

def get_display_name(ticker_key):
    mapping = {
        "BZ=F": "LCO (PetrÃ³leo Brent)",
        "GC=F": "Oro (Gold)",
        "BTC-USD": "BTC (Bitcoin)",
        "ETH-USD": "ETH (Ethereum)",
        "SOL-USD": "SOL (Solana)",
        "BNB-USD": "BNB (Binance Coin)",
        "XRP-USD": "XRP (Ripple)",
        "ADA-USD": "ADA (Cardano)",
        "DOGE-USD": "DOGE (Dogecoin)",
        "AVAX-USD": "AVAX (Avalanche)",
        "DOT-USD": "DOT (Polkadot)",
        "LINK-USD": "LINK (Chainlink)",
    }
    return mapping.get(ticker_key, ticker_key)

# --- CONTROLADORES DE BASE DE DATOS (PostgreSQL) ---
def check_and_add_seen_event(event_hash):
    conn = get_db_connection()
    if not conn: return False
    try:
        c = conn.cursor()
        c.execute('SELECT 1 FROM seen_events WHERE hash_id = %s', (event_hash,))
        if c.fetchone(): return True
        c.execute('INSERT INTO seen_events (hash_id, timestamp) VALUES (%s, %s)', (event_hash, datetime.now().isoformat()))
        conn.commit()
    finally:
        pass # conn.close() delegado a pooling global
    return False

def purge_old_events():
    cutoff_date = (datetime.now() - timedelta(days=7)).isoformat()
    conn = get_db_connection()
    if not conn: return
    try:
        c = conn.cursor()
        c.execute('DELETE FROM seen_events WHERE timestamp < %s', (cutoff_date,))
        conn.commit()
    finally:
        pass # conn.close() delegado a pooling global

def get_tracked_tickers():
    conn = get_db_connection()
    if not conn:
        print("DEBUG WALLET: Sin conexiÃ³n a DB, retornando lista vacÃ­a")
        return []
    try:
        c = conn.cursor()
        c.execute('SELECT ticker FROM wallet WHERE user_id = %s', (int(CHAT_ID),))
        tickers = [row[0] for row in c.fetchall()]
        print(f"DEBUG WALLET: Cargadas {len(tickers)} acciones de la base de datos: {tickers}")
        return tickers
    except Exception as e:
        print(f"DEBUG WALLET: Error leyendo tickers de Supabase: {e}")
        return []
    finally:
        pass

def get_all_portfolio_data():
    pf = {}
    conn = get_db_connection()
    if not conn: return pf
    try:
        c = conn.cursor()
        c.execute('SELECT * FROM wallet WHERE user_id = %s', (int(CHAT_ID),))
        for row in c.fetchall():
            pf[row[1]] = {"is_investment": bool(row[2]), "amount_usd": row[3], "entry_price": row[4], "timestamp": row[5]}
    finally:
        pass # conn.close() delegado a pooling global
    return pf

def add_ticker(ticker):
    ticker = remap_ticker(ticker)
    conn = get_db_connection()
    if not conn: return "DB_ERROR"
    try:
        c = conn.cursor()
        c.execute('SELECT 1 FROM wallet WHERE user_id = %s AND ticker = %s', (int(CHAT_ID), ticker))
        if not c.fetchone():
            c.execute('INSERT INTO wallet (user_id, ticker, is_investment, amount_usd, entry_price, timestamp) VALUES (%s, %s, 0, 0, 0, %s)', (int(CHAT_ID), ticker, datetime.now().isoformat()))
            conn.commit()
            save_state_to_telegram()  # â† PERSISTENCIA EN TELEGRAM
            return True
        return False
    except Exception as e:
        logging.error(f"Error ADD: {e}")
        return "DB_ERROR"
    finally:
        if conn: pass # conn.close() delegado a pooling global

def remove_ticker(ticker):
    ticker = remap_ticker(ticker)
    conn = get_db_connection()
    if not conn: return False
    try:
        c = conn.cursor()
        c.execute('SELECT 1 FROM wallet WHERE user_id = %s AND ticker = %s', (int(CHAT_ID), ticker))
        if c.fetchone():
            c.execute('DELETE FROM wallet WHERE user_id = %s AND ticker = %s', (int(CHAT_ID), ticker))
            conn.commit()
            save_state_to_telegram()  # â† PERSISTENCIA EN TELEGRAM
            if ticker in SMC_LEVELS_MEMORY: del SMC_LEVELS_MEMORY[ticker]
            return True
    finally:
        pass # conn.close() delegado a pooling global
    return False

def add_investment(ticker, amount_usd, entry_price):
    ticker = remap_ticker(ticker)
    timestamp = datetime.now().isoformat()
    conn = get_db_connection()
    if not conn: return
    try:
        c = conn.cursor()
        c.execute('SELECT 1 FROM wallet WHERE user_id = %s AND ticker = %s', (int(CHAT_ID), ticker))
        if c.fetchone():
            c.execute('UPDATE wallet SET is_investment = 1, amount_usd = %s, entry_price = %s, timestamp = %s WHERE user_id = %s AND ticker = %s', (amount_usd, entry_price, timestamp, int(CHAT_ID), ticker))
        else:
            c.execute('INSERT INTO wallet (user_id, ticker, is_investment, amount_usd, entry_price, timestamp) VALUES (%s, %s, 1, %s, %s, %s)', (int(CHAT_ID), ticker, amount_usd, entry_price, timestamp))
        conn.commit()
    finally:
        if conn: pass # conn.close() delegado a pooling global
    save_state_to_telegram()  # â† PERSISTENCIA EN TELEGRAM

def close_investment(ticker):
    ticker = remap_ticker(ticker)
    conn = get_db_connection()
    if not conn: return
    try:
        c = conn.cursor()
        c.execute('UPDATE wallet SET is_investment = 0, amount_usd = 0, entry_price = 0 WHERE user_id = %s AND ticker = %s', (int(CHAT_ID), ticker))
        conn.commit()
    finally:
        pass # conn.close() delegado a pooling global
    save_state_to_telegram()  # â† PERSISTENCIA EN TELEGRAM

def get_investments():
    invs = {}
    conn = get_db_connection()
    if not conn: return invs
    try:
        c = conn.cursor()
        c.execute('SELECT ticker, amount_usd, entry_price FROM wallet WHERE user_id = %s AND is_investment = 1', (int(CHAT_ID),))
        for row in c.fetchall():
            invs[row[0]] = {'amount_usd': row[1], 'entry_price': row[2]}
    finally:
        pass # conn.close() delegado a pooling global
    return invs

def add_realized_pnl(prof_usd):
    conn = get_db_connection()
    if not conn: return
    try:
        c = conn.cursor()
        c.execute('SELECT value FROM global_stats WHERE key = %s', ("realized_pnl",))
        res = c.fetchone()
        cur_pnl = res[0] if res else 0.0
        new_val = cur_pnl + float(prof_usd)
        if res: c.execute('UPDATE global_stats SET value = %s WHERE key = %s', (new_val, "realized_pnl"))
        else: c.execute('INSERT INTO global_stats (key, value) VALUES (%s, %s)', ("realized_pnl", new_val))
        conn.commit()
    finally:
        pass # conn.close() delegado a pooling global
    save_state_to_telegram()  # â† PERSISTENCIA EN TELEGRAM

def get_realized_pnl():
    conn = get_db_connection()
    if not conn: return 0.0
    try:
        c = conn.cursor()
        c.execute('SELECT value FROM global_stats WHERE key = %s', ("realized_pnl",))
        res = c.fetchone()
        return res[0] if res else 0.0
    finally:
        pass # conn.close() delegado a pooling global

def reset_realized_pnl():
    """Resetea la ganancia mensual acumulada a $0.00"""
    conn = get_db_connection()
    if not conn: return
    try:
        c = conn.cursor()
        c.execute('''INSERT INTO global_stats (key, value) VALUES ('realized_pnl', 0.0)
                     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value''')
        conn.commit()
    finally:
        pass # conn.close() delegado a pooling global
    save_state_to_telegram()
    logging.info("ðŸ”„ PnL mensual reseteado a $0.00")

def reset_total_db():
    """RESET RADICAL: borra TODAS las inversiones, PnL y contabilidad"""
    conn = get_db_connection()
    if not conn: return
    try:
        c = conn.cursor()
        c.execute('UPDATE wallet SET is_investment = 0, amount_usd = 0, entry_price = 0 WHERE user_id = %s', (int(CHAT_ID),))
        c.execute('''INSERT INTO global_stats (key, value) VALUES ('realized_pnl', 0.0)
                     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value''')
        conn.commit()
    finally:
        pass # conn.close() delegado a pooling global
    logging.info("âš ï¸ RESET TOTAL ejecutado: inversiones y PnL eliminados")


WHALE_MEMORY = deque(maxlen=5)
SMC_LEVELS_MEMORY = {}
LAST_KNOWN_PRICES = {}  # Cache de Ãºltimo precio vÃ¡lido por ticker
LAST_KNOWN_ANALYSIS = {}  # Cache de Ãºltimo anÃ¡lisis SMC completo por ticker
last_whale_alert = {} # Memoria Anti-Spam para Ballenas
WHALE_HISTORY_DB = {} # Acumulador de flujos 24H

# Tickers de respaldo para Brent Crude
BRENT_FALLBACK_CHAIN = ["BZ=F", "CO=F", "BNO"]
BRENT_MIN_VALID_PRICE = 50.0  # Si el precio es menor a esto, es un ERROR de Yahoo

# ----------------- NÃšCLEO DE MERCADO E INTELIGENCIA -----------------
def fmt_price(val):
    """Formatea precio con decimales REALES del exchange, sin ceros de relleno"""
    s = f"{val:.6f}".rstrip('0')
    parts = s.split('.')
    if len(parts) == 2 and len(parts[1]) < 2:
        s = f"{val:.2f}"
    elif s.endswith('.'):
        s = f"{val:.2f}"
    return s

# === MOTOR FMP (Financial Modeling Prep) â€” FUENTE ÃšNICA DE PRECIOS ===
# Endpoint: https://financialmodelingprep.com/stable/quote?symbol={SYMBOL}&apikey={KEY}

# Mapeo de tickers internos -> sÃ­mbolos FMP
FMP_SYMBOL_MAP = {
    "BTC-USD": "BTCUSD", "ETH-USD": "ETHUSD", "SOL-USD": "SOLUSD",
    "BNB-USD": "BNBUSD", "XRP-USD": "XRPUSD", "ADA-USD": "ADAUSD",
    "DOGE-USD": "DOGEUSD", "DOT-USD": "DOTUSD", "AVAX-USD": "AVAXUSD",
    "MATIC-USD": "MATICUSD", "LINK-USD": "LINKUSD",
    "BZ=F": "BZUSD",   # Brent Crude Oil
    "GC=F": "GCUSD",   # Gold
}

def _is_crypto_ticker(tk):
    """Detecta si un ticker es una criptomoneda"""
    return tk.endswith('-USD')

def _get_fmp_symbol(tk):
    """Convierte ticker interno al sÃ­mbolo FMP correcto"""
    if tk in FMP_SYMBOL_MAP:
        return FMP_SYMBOL_MAP[tk]
    # Crypto auto-map: BTC-USD -> BTCUSD
    if tk.endswith('-USD'):
        return tk.replace('-USD', 'USD')
    # Acciones y ETFs van directo: NVDA, IXC, BNO
    return tk

_FMP_LAST_ERROR = {}  # Cache global para diagnÃ³stico del Ãºltimo error FMP

def _fetch_fmp_quote(tk):
    """Consulta precio en vivo EXCLUSIVAMENTE desde FMP - /stable/quote"""
    global _FMP_LAST_ERROR

    if not FMP_API_KEY:
        logging.error("ERROR: La variable FMP_API_KEY no existe en el sistema")
        _FMP_LAST_ERROR[tk] = "FMP_API_KEY no detectada."
        return None

    fmp_symbol = _get_fmp_symbol(tk)
    symbols_to_try = [fmp_symbol]

    # Soporte Crypto exhaustivo
    if _is_crypto_ticker(tk) or tk in ['BTC', 'ETH', 'SOL', 'MARA', 'XRP', 'DOGE']:
        base = tk.replace('-USD', '')
        symbols_to_try = [f"{base}USD", base, tk]
    elif tk == "BZ=F":
        symbols_to_try = ["BZUSD", "BCOUSD"]
    elif tk == "GC=F":
        symbols_to_try = ["GCUSD", "XAUUSD"]

    for symbol in symbols_to_try:
        try:
            # === STABLE API (nuevo) ===
            url = f"https://financialmodelingprep.com/stable/quote?symbol={symbol}&apikey={FMP_API_KEY}"
            resp = requests.get(url, timeout=10)

            print(f"DEBUG FMP HTTP Status: {resp.status_code} para {symbol}")
            if resp.status_code != 200:
                print(f"DEBUG FMP Respuesta Cruda: {resp.text[:300]}")

            if resp.status_code == 200:
                data = resp.json()
                if not data or len(data) == 0:
                    print(f"DEBUG FMP: respuesta vac\u00eda para {symbol}")
                    continue
                
                # /stable/quote puede devolver lista o dict
                if isinstance(data, list):
                    quote = data[0] if data[0] is not None else {}
                elif isinstance(data, dict):
                    quote = data
                else:
                    print(f"DEBUG FMP: formato inesperado para {symbol}: {str(data)[:200]}")
                    continue

                if isinstance(quote, dict):
                    price = quote.get('price')
                    if price is None:
                        print(f"DEBUG FMP: precio nulo para {symbol}. Datos: {quote}")
                        continue
                        
                    price = float(price)
                    volume = float(quote.get('volume', 0) or 0)
                    avg_volume = float(quote.get('avgVolume', 0) or 0)
                    change = float(quote.get('change', 0) or 0)
                    pe = float(quote.get('pe', 0) or 0)
                    if price > 0:
                        logging.info(f"FMP OK {tk} ({symbol}): ${fmt_price(price)}")
                        return {'price': price, 'vol': volume, 'volume': volume, 'avgVolume': avg_volume, 'change': change, 'pe': pe}
                    else:
                        print(f"DEBUG FMP: precio=0 para {symbol}. Datos: {quote}")

            elif resp.status_code in (401, 403):
                _FMP_LAST_ERROR[tk] = f"{resp.status_code} - Key rechazada o plan insuficiente"
                logging.error(f"FMP: {resp.status_code} para {symbol}. Verifica FMP_API_KEY en Railway.")
                return None

        except Exception as e:
            logging.error(f"FMP error fetching {symbol}: {e}")
            print(f"DEBUG FMP ExcepciÃ³n: {e}")

    _FMP_LAST_ERROR[tk] = "Activo no encontrado en FMP"
    logging.warning(f"FMP fallÃ³ para {tk}. Activo no localizado.")
    return None
def _sanity_check_price(tk, new_price):
    """Verifica que el precio no sea basura (desviaciÃ³n >50% vs Ãºltimo conocido)"""
    if tk in LAST_KNOWN_PRICES:
        last_price = LAST_KNOWN_PRICES[tk]['price']
        if last_price > 0:
            change_pct = abs(new_price - last_price) / last_price
            if change_pct > 0.50:
                logging.warning(f"âš ï¸ SANITY CHECK FALLIDO para {tk}: ${new_price:.2f} vs Ãºltimo ${last_price:.2f} ({change_pct*100:.1f}%)")
                return False
    if not _is_crypto_ticker(tk) and new_price < 0.50:
        logging.warning(f"âš ï¸ SANITY CHECK: {tk} precio ${new_price:.4f} demasiado bajo.")
        return False
    if new_price <= 0:
        return False
    return True

def get_safe_ticker_price(ticker, force_validation=False):
    """MOTOR MAESTRO: FMP como fuente ÃšNICA de precios en vivo"""
    tk = remap_ticker(ticker)

    # FMP es la fuente Ãºnica para TODOS los activos
    result = _fetch_fmp_quote(tk)
    if result and _sanity_check_price(tk, result['price']):
        LAST_KNOWN_PRICES[tk] = result
        return result

    # Cache como Ãºnico respaldo si FMP no responde
    if tk in LAST_KNOWN_PRICES:
        logging.warning(f"{tk}: FMP no respondiÃ³. Usando cache: ${fmt_price(LAST_KNOWN_PRICES[tk]['price'])}")
        return LAST_KNOWN_PRICES[tk]

    logging.error(f"{tk}: FMP fallÃ³ y no hay cache disponible.")
    return None

def verify_1m_realtime_data(ticker):
    """Obtiene precio Y volumen en tiempo real via FMP quote."""
    tk = remap_ticker(ticker)
    try:
        fmp_data = _fetch_fmp_quote(tk)
        if fmp_data:
            return {'price': fmp_data['price'], 'vol': fmp_data.get('volume', 0)}
    except Exception as e:
        print(f"DEBUG verify_1m: Error FMP para {tk}: {e}")
    safe = get_safe_ticker_price(tk)
    if safe:
        return {'price': safe['price'], 'vol': 0}
    return None


def _fetch_fmp_news(limit=10):
    print("📰 Noticias: No disponibles temporalmente")
    return []


def _fetch_google_news_fallback(limit=8):
    """Fallback: Google News RSS para cuando FMP no devuelve noticias"""
    all_news = []
    try:
        for query in ["financial+markets+today", "stock+market+economy", "bitcoin+crypto+market"]:
            url = f"https://news.google.com/rss/search?q={query}&hl=en"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                root = ET.fromstring(resp.text)
                for item in root.findall('.//item')[:3]:
                    title = item.find('title').text
                    if title and title not in all_news:
                        all_news.append(title)
    except Exception:
        pass
    return all_news[:limit]


def check_geopolitical_news():
    """Monitor automÃ¡tico unificado: FMP news + sentiment + wallet cross-reference"""
    try:
        HIGH_IMPACT_KEYWORDS = ["war", "attack", "strike", "escalation", "missile", "sanction",
                                "embargo", "explosion", "guerra", "ataque", "tensiÃ³n", "misil",
                                "sanciones", "rates", "fed", "trump", "powell", "crash", "recession",
                                "tariff", "default", "crisis"]
        news_alerts = []

        fmp_news = _fetch_fmp_news(15)
        if fmp_news:
            for article in fmp_news:
                title = article.get('title', '') or article.get('text', '') or ''
                if title and any(re.search(rf"\b{kw}\b", title, re.IGNORECASE) for kw in HIGH_IMPACT_KEYWORDS):
                    news_alerts.append(title)
                    if len(news_alerts) >= 5: break

        if not news_alerts:
            try:
                search_url = "https://news.google.com/rss/search?q=geopolitics+OR+Trump+OR+rates+OR+war+OR+economy"
                response = requests.get(search_url, timeout=5)
                if response.status_code == 200:
                    root = ET.fromstring(response.text)
                    for item in root.findall('.//item'):
                        title = item.find('title').text
                        if any(re.search(rf"\b{kw}\b", title, re.IGNORECASE) for kw in HIGH_IMPACT_KEYWORDS):
                            news_alerts.append(title)
                            if len(news_alerts) >= 5: break
            except Exception:
                pass

        return news_alerts
    except Exception as e:
        logging.error(f"Error en check_geopolitical_news: {e}")
        return []


def gpt_advanced_geopolitics(news_list, manual=False):
    if not news_list or not OPENAI_API_KEY: return None
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)
    news_text = "\n".join([f"- {n}" for n in news_list])
    if manual:
        prompt = f"Titulares globales:\n{news_text}\nHaz un resumen y dime quÃ© moverÃ­a el mercado hoy. RESPONDE ESTRICTAMENTE EN ESPAÃ‘OL."
    else:
        prompt = (f"Titulares recientes:\n{news_text}\nAnaliza si hay algo de nivel 'Alto Impacto' (>2%). Si no lo hay, responde 'TRANQUILIDAD'.\nSi lo hay: 'âš ï¸ ALERTA URGENTE: [Resumen] - Impacto en [AcciÃ³n/Sector]'\nRESPONDE ESTRICTA Y ÃšNICAMENTE EN ESPAÃ‘OL.")
    try:
        res = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=600
        ).choices[0].message.content.strip()
        if not manual and ("TRANQUILIDAD" in res.upper() and len(res) < 20): return None
        return res
    except: return None


# =====================================================================
# MOTOR DE INTELIGENCIA UNIFICADO GÃ‰NESIS
# Integra: FMP Sentiment + Wallet Cross-Reference + Whale Radar
# =====================================================================

# Cache global de contexto de riesgo geopolÃ­tico (para cruce con ballenas)
GENESIS_RISK_CONTEXT = {
    'sentiment_global': 0.0,       # Sentimiento promedio del mercado (-1 a 1)
    'high_risk_tickers': [],        # Tickers con sentimiento muy negativo
    'last_update': None,            # Timestamp de Ãºltima actualizaciÃ³n
    'news_digest': [],              # Ãšltimas noticias procesadas con sentimiento
}


def _classify_sentiment(score):
    """Traduce score de FMP a semÃ¡foro de riesgo con porcentajes"""
    try:
        s = float(score)
    except (TypeError, ValueError):
        return {'label': 'Neutral', 'icon': 'ðŸŸ¡', 'bull_pct': 50, 'bear_pct': 50, 'raw': 0.0}

    if s > 0.3:
        bull = min(95, int(50 + s * 50))
        return {'label': 'Alcista', 'icon': 'ðŸŸ¢', 'bull_pct': bull, 'bear_pct': 100 - bull, 'raw': s}
    elif s < -0.3:
        bear = min(95, int(50 + abs(s) * 50))
        return {'label': 'Bajista', 'icon': 'ðŸ”´', 'bull_pct': 100 - bear, 'bear_pct': bear, 'raw': s}
    else:
        return {'label': 'Neutral', 'icon': 'ðŸŸ¡', 'bull_pct': 50, 'bear_pct': 50, 'raw': s}


def _extract_mentioned_tickers(text, wallet_tickers):
    """Detecta quÃ© tickers de la wallet se mencionan en un texto"""
    mentioned = []
    text_upper = text.upper()
    for tk in wallet_tickers:
        clean_tk = tk.replace('-USD', '').replace('=F', '').upper()
        display = get_display_name(tk).upper()
        # Buscar el ticker crudo, el display name, o palabras clave
        aliases = [clean_tk]
        if 'BTC' in clean_tk: aliases.extend(['BITCOIN', 'BTC', 'CRYPTO'])
        if 'ETH' in clean_tk: aliases.extend(['ETHEREUM', 'ETH'])
        if 'SOL' in clean_tk: aliases.extend(['SOLANA'])
        if 'NVDA' in clean_tk: aliases.extend(['NVIDIA'])
        if 'MARA' in clean_tk: aliases.extend(['MARATHON', 'MARA'])
        if 'GC' in clean_tk: aliases.extend(['GOLD', 'ORO'])
        if 'BZ' in clean_tk: aliases.extend(['BRENT', 'OIL', 'PETRÃ“LEO', 'PETROLEO'])
        if 'XRP' in clean_tk: aliases.extend(['RIPPLE'])

        for alias in aliases:
            if alias in text_upper:
                if tk not in mentioned:
                    mentioned.append(tk)
                break
    return mentioned


def _fetch_fmp_news_with_sentiment(limit=12):
    """Fetch FMP news y extrae sentiment score de cada artÃ­culo"""
    raw_news = _fetch_fmp_news(limit)
    processed = []

    for article in raw_news:
        title = article.get('title', '') or ''
        if not title:
            continue

        # FMP puede incluir 'sentiment' directamente en el payload
        sentiment_raw = article.get('sentiment', None)
        if sentiment_raw is None:
            # Inferencia bÃ¡sica por keywords si FMP no da sentiment
            sentiment_raw = _infer_sentiment_from_title(title)

        sentiment = _classify_sentiment(sentiment_raw)
        symbol = article.get('symbol', '') or article.get('tickers', '') or ''
        site = article.get('site', '') or article.get('source', '') or ''
        url = article.get('url', '') or article.get('link', '') or ''

        processed.append({
            'title': title,
            'symbol': symbol,
            'source': site,
            'url': url,
            'sentiment': sentiment,
        })

    return processed


def _infer_sentiment_from_title(title):
    """Inferencia rÃ¡pida de sentimiento por keywords cuando FMP no lo provee"""
    t = title.lower()
    BEARISH = ['crash', 'plunge', 'drop', 'fall', 'war', 'crisis', 'recession',
               'default', 'sanction', 'tariff', 'sell-off', 'dump', 'decline',
               'fears', 'slump', 'downturn', 'bankruptcy', 'layoffs', 'collapse']
    BULLISH = ['surge', 'rally', 'soar', 'gain', 'bull', 'record', 'breakout',
               'growth', 'boost', 'optimism', 'recovery', 'upgrade', 'beat',
               'profit', 'expansion', 'all-time high', 'moon']

    bear_hits = sum(1 for kw in BEARISH if kw in t)
    bull_hits = sum(1 for kw in BULLISH if kw in t)

    if bear_hits > bull_hits:
        return -0.3 - (bear_hits * 0.15)  # MÃ¡s keywords = mÃ¡s negativo
    elif bull_hits > bear_hits:
        return 0.3 + (bull_hits * 0.15)
    return 0.0


def _get_whale_context_for_ticker(ticker):
    """Busca el Ãºltimo movimiento de ballena relevante para un ticker"""
    for w in reversed(list(WHALE_MEMORY)):
        if w['ticker'] == ticker:
            minutes_ago = int((datetime.now() - w['timestamp']).total_seconds() / 60)
            is_crypto = '-USD' in w['ticker']
            vol_str = f"${w['vol_approx']:,} USD" if is_crypto else f"{w['vol_approx']:,} unidades"
            return {
                'vol_str': vol_str,
                'type': w['type'],
                'minutes_ago': minutes_ago,
                'vol_approx': w['vol_approx'],
            }
    return None


# === SISTEMA DE TRADUCCIÃ“N AUTOMÃTICA AL ESPAÃ‘OL ===

# Diccionario de tÃ©rminos financieros inglÃ©s â†’ espaÃ±ol
_FINANCIAL_DICT = {
    'bull market': 'mercado alcista', 'bear market': 'mercado bajista',
    'interest rates': 'tasas de interÃ©s', 'interest rate': 'tasa de interÃ©s',
    'rate hike': 'alza de tasas', 'rate cut': 'recorte de tasas',
    'earnings': 'ganancias', 'revenue': 'ingresos', 'profit': 'beneficio',
    'loss': 'pÃ©rdida', 'losses': 'pÃ©rdidas',
    'surge': 'alza fuerte', 'surges': 'sube fuertemente',
    'plunge': 'desplome', 'plunges': 'se desploma',
    'rally': 'rally alcista', 'rallies': 'repunta',
    'crash': 'desplome', 'crashes': 'se desploma',
    'drop': 'caÃ­da', 'drops': 'cae',
    'rise': 'alza', 'rises': 'sube',
    'gain': 'ganancia', 'gains': 'ganancias',
    'fall': 'caÃ­da', 'falls': 'cae',
    'soar': 'se dispara', 'soars': 'se dispara',
    'decline': 'descenso', 'declines': 'desciende',
    'volatility': 'volatilidad', 'volatile': 'volÃ¡til',
    'downturn': 'recesiÃ³n', 'recession': 'recesiÃ³n',
    'inflation': 'inflaciÃ³n', 'deflation': 'deflaciÃ³n',
    'tariff': 'arancel', 'tariffs': 'aranceles',
    'sanction': 'sanciÃ³n', 'sanctions': 'sanciones',
    'trade war': 'guerra comercial', 'trade deal': 'acuerdo comercial',
    'federal reserve': 'Reserva Federal', 'the fed': 'la Fed',
    'treasury': 'Tesoro', 'bond': 'bono', 'bonds': 'bonos',
    'yield': 'rendimiento', 'yields': 'rendimientos',
    'stock': 'acciÃ³n', 'stocks': 'acciones',
    'shares': 'acciones', 'share': 'acciÃ³n',
    'market cap': 'capitalizaciÃ³n de mercado',
    'all-time high': 'mÃ¡ximo histÃ³rico', 'record high': 'mÃ¡ximo histÃ³rico',
    'all-time low': 'mÃ­nimo histÃ³rico',
    'breakout': 'ruptura alcista', 'breakdown': 'ruptura bajista',
    'support': 'soporte', 'resistance': 'resistencia',
    'sell-off': 'venta masiva', 'selloff': 'venta masiva',
    'buyback': 'recompra de acciones',
    'dividend': 'dividendo', 'dividends': 'dividendos',
    'outperform': 'supera expectativas', 'underperform': 'por debajo de expectativas',
    'upgrade': 'mejora de calificaciÃ³n', 'downgrade': 'rebaja de calificaciÃ³n',
    'bullish': 'alcista', 'bearish': 'bajista',
    'outlook': 'perspectiva', 'forecast': 'pronÃ³stico',
    'growth': 'crecimiento', 'expansion': 'expansiÃ³n',
    'layoffs': 'despidos', 'hiring': 'contrataciones',
    'bankruptcy': 'bancarrota', 'default': 'impago',
    'crisis': 'crisis', 'recovery': 'recuperaciÃ³n',
    'quarter': 'trimestre', 'quarterly': 'trimestral',
    'annual': 'anual', 'yearly': 'anual',
    'report': 'reporte', 'reports': 'reportes',
    'warns': 'advierte', 'warning': 'advertencia',
    'announces': 'anuncia', 'announcement': 'anuncio',
    'launch': 'lanzamiento', 'launches': 'lanza',
    'deal': 'acuerdo', 'merger': 'fusiÃ³n', 'acquisition': 'adquisiciÃ³n',
    'investor': 'inversionista', 'investors': 'inversionistas',
    'traders': 'operadores', 'analyst': 'analista', 'analysts': 'analistas',
    'ahead of': 'antes de', 'amid': 'en medio de',
    'despite': 'a pesar de', 'due to': 'debido a',
    'according to': 'segÃºn', 'following': 'tras',
    'higher': 'mÃ¡s alto', 'lower': 'mÃ¡s bajo',
    'strong': 'fuerte', 'weak': 'dÃ©bil',
    'bitcoin': 'Bitcoin', 'ethereum': 'Ethereum',
    'cryptocurrency': 'criptomoneda', 'crypto': 'cripto',
}


def _quick_translate_financial(text):
    """TraducciÃ³n rÃ¡pida por diccionario â€” reemplaza tÃ©rminos financieros comunes"""
    result = text
    # Ordenar por longitud descendente para evitar reemplazos parciales
    sorted_terms = sorted(_FINANCIAL_DICT.items(), key=lambda x: len(x[0]), reverse=True)
    for eng, esp in sorted_terms:
        # Reemplazo case-insensitive preservando capitalizaciÃ³n del contexto
        pattern = re.compile(re.escape(eng), re.IGNORECASE)
        result = pattern.sub(esp, result)
    return result


def _translate_titles_to_spanish(titles):
    """Traduce una lista de tÃ­tulos al espaÃ±ol usando OpenAI (batch).
    Si OpenAI no estÃ¡ disponible, usa traducciÃ³n por diccionario."""
    if not titles:
        return titles

    # Si hay OpenAI, traducciÃ³n por lotes (mÃ¡s natural)
    if OPENAI_API_KEY and len(titles) > 0:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=OPENAI_API_KEY)
            numbered = "\n".join([f"{i+1}. {t}" for i, t in enumerate(titles)])
            prompt = (
                f"Traduce estos titulares financieros al ESPAÃ‘OL con vocabulario profesional de mercados.\n"
                f"Usa tÃ©rminos como: mercado alcista, tasas de interÃ©s, rendimiento, volatilidad, arancel, etc.\n"
                f"MantÃ©n los nombres propios (empresas, personas, paÃ­ses) sin traducir.\n"
                f"Devuelve SOLO las traducciones numeradas, sin explicaciones.\n\n"
                f"{numbered}"
            )
            res = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1500
            ).choices[0].message.content.strip()

            # Parsear respuesta numerada
            translated = []
            for line in res.split('\n'):
                line = line.strip()
                if line and line[0].isdigit():
                    # Quitar "1. ", "2. ", etc.
                    clean = re.sub(r'^\d+[\.\)]\s*', '', line)
                    if clean:
                        translated.append(clean)

            if len(translated) >= len(titles) * 0.5:  # Al menos 50% traducido
                # Rellenar los que falten con diccionario
                while len(translated) < len(titles):
                    idx = len(translated)
                    translated.append(_quick_translate_financial(titles[idx]))
                return translated

        except Exception as e:
            logging.debug(f"Error en traducciÃ³n batch OpenAI: {e}")

    # Fallback: traducciÃ³n por diccionario
    return [_quick_translate_financial(t) for t in titles]


def genesis_strategic_report(manual=True):
    """REPORTE ESTRATÃ‰GICO UNIFICADO GÃ‰NESIS
    Integra: FMP Sentiment + Wallet Cross-Reference + Whale Data + IA
    TODO el contenido se entrega en ESPAÃ‘OL."""
    global GENESIS_RISK_CONTEXT

    wallet_tickers = get_tracked_tickers()

    # === PASO 1: Fetch noticias con sentimiento ===
    news_data = _fetch_fmp_news_with_sentiment(12)

    # Fallback: Google News si FMP falla
    if not news_data:
        google_fallback = _fetch_google_news_fallback(8)
        if google_fallback:
            for title in google_fallback:
                sent_raw = _infer_sentiment_from_title(title)
                news_data.append({
                    'title': title,
                    'symbol': '',
                    'source': 'Google News',
                    'url': '',
                    'sentiment': _classify_sentiment(sent_raw),
                })

    # Sin noticias de ninguna fuente
    if not news_data:
        return "â˜• Sin eventos de riesgo detectados en este momento. Vigilancia activa."

    # === PASO 1.5: Traducir tÃ­tulos al espaÃ±ol ===
    titles_to_translate = [n['title'] for n in news_data]
    translated = _translate_titles_to_spanish(titles_to_translate)
    for i, news in enumerate(news_data):
        if i < len(translated) and translated[i]:
            news['title_es'] = translated[i]
        else:
            news['title_es'] = _quick_translate_financial(news['title'])

    # === PASO 2: Cross-reference con wallet ===
    wallet_alerts = []    # Noticias que tocan activos de Eduardo
    general_news = []     # Noticias generales del mercado

    for news in news_data:
        mentioned = _extract_mentioned_tickers(news['title'], wallet_tickers)
        if news['symbol']:
            # TambiÃ©n buscar por symbol explÃ­cito de FMP
            for tk in wallet_tickers:
                fmp_sym = _get_fmp_symbol(tk)
                if news['symbol'].upper() in [fmp_sym, tk.replace('-USD', ''), tk]:
                    if tk not in mentioned:
                        mentioned.append(tk)

        news['affected_tickers'] = mentioned
        if mentioned:
            wallet_alerts.append(news)
        else:
            general_news.append(news)

    # === PASO 3: Calcular sentimiento global ===
    all_sentiments = [n['sentiment']['raw'] for n in news_data if n['sentiment']['raw'] != 0]
    avg_sentiment = sum(all_sentiments) / len(all_sentiments) if all_sentiments else 0.0
    global_risk = _classify_sentiment(avg_sentiment)

    # Actualizar contexto global (para cruce con ballenas en el loop)
    GENESIS_RISK_CONTEXT = {
        'sentiment_global': avg_sentiment,
        'high_risk_tickers': [tk for news in wallet_alerts
                               for tk in news['affected_tickers']
                               if news['sentiment']['raw'] < -0.3],
        'last_update': datetime.now(),
        'news_digest': news_data[:6],
    }

    # === PASO 4: Construir reporte ===
    lines = []
    lines.append("ðŸŒ <b>REPORTE ESTRATÃ‰GICO GÃ‰NESIS</b> ðŸŒ")
    lines.append("\u2500" * 28)

    # --- Alertas de wallet primero (mÃ¡ximo 4) ---
    if wallet_alerts:
        lines.append("")
        lines.append("ðŸš¨ <b>ALERTAS EN TU CARTERA:</b>")
        lines.append("")
        for news in wallet_alerts[:4]:
            s = news['sentiment']
            affected = ", ".join([get_display_name(tk) for tk in news['affected_tickers']])
            whale_note = ""
            for tk in news['affected_tickers']:
                wctx = _get_whale_context_for_ticker(tk)
                if wctx:
                    whale_note = f"\nðŸ‹ <b>Ballena:</b> {wctx['vol_str']} ({wctx['type']}) hace {wctx['minutes_ago']}min"
                    break

            lines.append(f"ðŸ“° <b>Noticia:</b> {news['title_es'][:140]}")
            lines.append(f"ðŸŽ¯ <b>Activos afectados:</b> {affected}")
            lines.append(f"{s['icon']} <b>Riesgo:</b> {s['bull_pct']}% Alcista / {s['bear_pct']}% Bajista ({s['label']})")
            if whale_note:
                lines.append(whale_note)
            lines.append(f"ðŸ’¡ <b>AnÃ¡lisis:</b> SegÃºn el sentimiento del mercado, la probabilidad de impacto en tu cartera es <b>{s['bear_pct']}%</b>.")
            lines.append("")
    else:
        lines.append("")
        lines.append("âœ… <b>Sin alertas directas para tu cartera.</b>")
        lines.append("")

    # --- Panorama general (mÃ¡ximo 3 noticias) ---
    lines.append("\u2500" * 28)
    lines.append("ðŸ“Š <b>PANORAMA MACRO:</b>")
    lines.append("")
    top_general = sorted(general_news, key=lambda x: abs(x['sentiment']['raw']), reverse=True)[:3]
    for news in top_general:
        s = news['sentiment']
        src = f" ({news['source']})" if news['source'] else ""
        lines.append(f"{s['icon']} {news['title_es'][:120]}{src}")
    if not top_general:
        lines.append("â˜• Sin noticias macro relevantes.")

    # --- Datos de Ballenas integrados ---
    lines.append("")
    lines.append("\u2500" * 28)
    lines.append("ðŸ³ <b>RADAR BALLENAS INTEGRADO:</b>")
    lines.append("")
    if WHALE_MEMORY:
        for w in list(WHALE_MEMORY)[::-1][:3]:
            is_crypto = '-USD' in w['ticker']
            vol_str = f"${w['vol_approx']:,} USD" if is_crypto else f"{w['vol_approx']:,} unidades"
            minutes_ago = int((datetime.now() - w['timestamp']).total_seconds() / 60)
            # Cruzar con riesgo geopolÃ­tico
            risk_tag = ""
            if w['ticker'] in GENESIS_RISK_CONTEXT.get('high_risk_tickers', []):
                risk_tag = " âš ï¸ <b>[ZONA DE RIESGO]</b>"
            lines.append(f"ðŸ‹ <b>{get_display_name(w['ticker'])}</b> | {vol_str} | {w['type']} | {minutes_ago}min{risk_tag}")
    else:
        lines.append("ðŸŒŠ OcÃ©ano tranquilo. Sin anomalÃ­as.")

    # --- Sentimiento resumen ---
    lines.append("")
    lines.append("\u2500" * 28)
    lines.append(f"ðŸŽ¯ <b>SENTIMIENTO GLOBAL:</b> {global_risk['icon']} {global_risk['label']} â€” {global_risk['bull_pct']}% Alcista / {global_risk['bear_pct']}% Bajista")
    lines.append("\u2500" * 28)

    # === PASO 5: IA avanzada (si OpenAI estÃ¡ disponible) ===
    if manual and OPENAI_API_KEY and (wallet_alerts or top_general):
        try:
            all_titles = [n['title_es'] for n in (wallet_alerts + top_general)[:6]]
            wallet_str = ", ".join([get_display_name(tk) for tk in wallet_tickers])
            sentiments_str = "\n".join([f"- {n['title_es'][:60]} ({n['sentiment']['label']})" for n in news_data[:5]])

            from openai import OpenAI
            client = OpenAI(api_key=OPENAI_API_KEY)
            prompt = (
                f"Eres GÃ‰NESIS, un sistema de inteligencia estratÃ©gica de mercados financieros.\n\n"
                f"NOTICIAS DEL DÃA CON SENTIMIENTO:\n{sentiments_str}\n\n"
                f"WALLET DE EDUARDO: {wallet_str}\n\n"
                f"SENTIMIENTO GLOBAL: {global_risk['label']} ({avg_sentiment:.2f})\n\n"
                f"INSTRUCCIONES OBLIGATORIAS:\n"
                f"1. Redacta TODO en ESPAÃ‘OL con vocabulario financiero profesional.\n"
                f"2. Usa tÃ©rminos como: mercado alcista, tasas de interÃ©s, rendimiento, volatilidad, liquidez, soporte, resistencia, presiÃ³n vendedora/compradora.\n"
                f"3. En 3-4 lÃ­neas, explica cÃ³mo estas noticias afectan DIRECTAMENTE los activos de Eduardo.\n"
                f"4. Da UNA recomendaciÃ³n estratÃ©gica clara: Mantener / Vigilar de cerca / Reducir exposiciÃ³n / Aprovechar oportunidad.\n"
                f"5. PROHIBIDO responder en inglÃ©s. Todo debe ser 100% en espaÃ±ol.\n"
            )
            res = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=500
            ).choices[0].message.content.strip()

            lines.append("")
            lines.append("ðŸ§  <b>ANÃLISIS IA GÃ‰NESIS:</b>")
            lines.append(res)
        except Exception as e:
            logging.error(f"OpenAI strategic error: {e}")

    return "\n".join(lines)


def generar_reporte_macro_manual():
    """Wrapper para el botÃ³n GeopolÃ­tica â€” usa el motor unificado"""
    return genesis_strategic_report(manual=True)


def fetch_intraday_data(ticker):
    """Obtiene precio + volumen EXCLUSIVAMENTE de FMP Pro para detecciÃ³n de ballenas."""
    tk = remap_ticker(ticker)

    if not FMP_API_KEY:
        print(f"DEBUG INTRADAY: FMP_API_KEY es None, saltando {tk}")
        return None

    # PASO 1: Obtener precio + volumen actual del dÃ­a via FMP quote
    fmp_data = _fetch_fmp_quote(tk)
    if not fmp_data:
        print(f"DEBUG INTRADAY: FMP quote fallo para {tk}")
        return None

    price = fmp_data['price']
    if not price:
        return None

    latest_vol = float(fmp_data.get('volume', 0) or 0)
    quote_avg_vol = float(fmp_data.get('avgVolume', 0) or 0)
    change = float(fmp_data.get('change', 0) or 0)
    vol_type = "Compra ðŸŸ¢" if change >= 0 else "Venta ðŸ”´"

    # PASO 2: Obtener historial de volumen REAL de los Ãºltimos 30 dÃ­as
    avg_vol = 0
    fmp_sym = _get_fmp_symbol(tk)
    if _is_crypto_ticker(tk):
        fmp_sym = tk.replace('-USD', '') + 'USD'

    try:
        # Endpoint moderno para evitar Legacy Endpoint 403
        url = f"https://financialmodelingprep.com/api/v3/technical_indicator/1day/{fmp_sym}?type=sma&period=10&apikey={FMP_API_KEY}"
        resp = requests.get(url, timeout=12)

        if resp.status_code == 200:
            hist = resp.json()

            if isinstance(hist, list) and len(hist) >= 5:
                # hist viene mÃ¡s reciente primero, tomar Ãºltimos 10 dÃ­as para RVOL como pidiÃ³ el usuario
                recent_vols = []
                for day in hist[:10]:
                    v = float(day.get('volume', 0) or 0)
                    if v > 0:
                        recent_vols.append(v)

                if recent_vols:
                    avg_vol = sum(recent_vols) / len(recent_vols)
                    print(f"DEBUG INTRADAY {tk}: historial OK | {len(recent_vols)} dias | avg_vol={avg_vol:,.0f} | latest_vol={latest_vol:,.0f} | spike={latest_vol/avg_vol:.2f}x" if avg_vol > 0 else f"DEBUG INTRADAY {tk}: avg_vol=0")
                else:
                    print(f"DEBUG INTRADAY {tk}: historial tiene 0 volumenes positivos de {len(hist)} registros")
            else:
                print(f"ERROR: FMP no devolviÃ³ historial para {tk}. Estructura: {str(raw)[:100]}")
        else:
            print(f"DEBUG INTRADAY {tk}: historial HTTP {resp.status_code} para {fmp_sym}")
            # Mostrar el error exacto para diagnosticar
            print(f"DEBUG INTRADAY {tk}: respuesta: {resp.text[:200]}")

    except Exception as e:
        print(f"DEBUG INTRADAY {tk}: historial error: {e}")

    # PASO 3: Si el historial fallÃ³, usar avgVolume del quote como fallback
    if avg_vol == 0 and quote_avg_vol > 0:
        avg_vol = quote_avg_vol
        print(f"DEBUG INTRADAY {tk}: usando avgVolume del quote como fallback | avg_vol={avg_vol:,.0f} | latest_vol={latest_vol:,.0f}")
        if avg_vol > 0 and latest_vol > 0:
            print(f"DEBUG INTRADAY {tk}: spike={latest_vol/avg_vol:.2f}x")

    # PASO 4: Asegurar validez estad\u00edstica del Volumen
    if avg_vol < 1000:
        print(f"DEBUG INTRADAY {tk}: Ignorando activo por avg_vol muy bajo o inv\u00e1lido ({avg_vol}). Evita Spam.")
        return None

    return {
        'ticker': tk,
        'latest_vol': latest_vol,
        'avg_vol': avg_vol,
        'vol_type': vol_type,
        'latest_price': price
    }
def fetch_and_analyze_stock(ticker):
    """Calcula RSI, MACD, SMC usando datos diarios de FMP."""
    clean_ticker = str(ticker).strip().upper()
    tk = remap_ticker(clean_ticker)
    print(f"DEBUG SMC: Consultando niveles para {tk}...")
    try:
        safe_check = get_safe_ticker_price(tk)
        if not safe_check:
            print(f"DEBUG SMC: get_safe_ticker_price fallÃ³ para {tk}")
            return "\u26a0\ufe0f Error de conexi\u00f3n con FMP"
            
        def _get_fallback_smc():
            latest_price = safe_check['price']
            pe = safe_check.get('pe', 0.0)
            return {
                'ticker': tk, 'price': latest_price, 'rsi': 50.0, 'macd_line': 0.0, 'macd_signal': 0.0, 
                'smc_sup': latest_price * 0.95, 'smc_res': latest_price * 1.05, 'smc_trend': "Alcista (\u26a0\ufe0f)", 
                'order_block': latest_price, 'take_profit': latest_price * 1.05, 'stop_loss': latest_price * 0.95 * 0.98,
                'rvol': 1.0, 'pe': pe
            }

        # Obtener historial diario de FMP
        fmp_sym = _get_fmp_symbol(tk)
        if _is_crypto_ticker(tk):
            fmp_sym = tk.replace('-USD', '') + 'USD'
        
        import urllib.parse
        ticker_clean = "".join(c for c in str(fmp_sym) if ord(c) < 128).strip().upper()
        safe_ticker = urllib.parse.quote(ticker_clean)
        
        print(f"DEBUG: Enviando petici\u00f3n SMC para: {safe_ticker}")
        url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{safe_ticker}?apikey={FMP_API_KEY}"
        
        try:
            resp = requests.get(url, timeout=5)
            print(f"DEBUG SMC: Ticker {safe_ticker} | Status: {resp.status_code}")
        except UnicodeEncodeError as e:
            print(f"ERROR CR\u00cdTICO SMC (Unicode): El ticker {safe_ticker} tiene caracteres ocultos. {e}")
            return _get_fallback_smc()
        except Exception as e:
            print(f"ERROR CR\u00cdTICO SMC (Red): {e}")
            return _get_fallback_smc()
        
        if resp.status_code != 200:
            print(f"CR\u00cdTICO: Error o Acceso denegado al ticker {safe_ticker}. HTTP {resp.status_code}")
            return _get_fallback_smc()

        raw = resp.json()
        hist = []
        if isinstance(raw, list):
            hist = raw
        elif isinstance(raw, dict):
            if 'historical' in raw:
                hist = raw['historical']
            elif 'historicalStockList' in raw:
                hist = raw['historicalStockList']
                if hist and isinstance(hist[0], dict) and 'historical' in hist[0]:
                    hist = hist[0]['historical']
        
        if not hist or not isinstance(hist, list) or len(hist) < 5:
            print(f"DEBUG SMC: El ticker {safe_ticker} no devolvi\u00f3 indicadores.")
            return _get_fallback_smc()

        # FMP viene en orden reciente-primero, revertir para cÃ¡lculos
        hist = list(reversed(hist[:100]))  # Ãšltimos 100 dÃ­as max para cÃ¡lculos limpios y rÃ¡pidos

        closes = pd.Series([float(d.get('close', 0)) for d in hist])
        volumes = pd.Series([float(d.get('volume', 0) or 0) for d in hist])

        if len(closes) < 15:
            print(f"DEBUG SMC: closes length {len(closes)} < 15 para {safe_ticker}")
            return _get_fallback_smc()

        # RSI
        delta = closes.diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)
        ema_up = up.ewm(com=13, adjust=False).mean()
        ema_down = down.ewm(com=13, adjust=False).mean()
        rs = ema_up / ema_down
        rsi_series = 100 - (100 / (1 + rs))
        rsi_series[ema_down == 0] = 100

        # MACD
        macd_line = closes.ewm(span=12, adjust=False).mean() - closes.ewm(span=26, adjust=False).mean()
        macd_signal = macd_line.ewm(span=9, adjust=False).mean()

        # Extracci\u00f3n directa del array reverso (de m\u00e1s viejo a m\u00e1s nuevo)
        recent_month_data = hist[-20:] # los \u00faltimos 20 d\u00edas de la lista invertida (los m\u00e1s recientes cronol\u00f3gicamente)
        
        smc_res = float(max([float(d.get('high', 0)) for d in recent_month_data])) if recent_month_data else latest_price
        smc_sup = float(min([float(d.get('low', float('inf'))) for d in recent_month_data])) if recent_month_data else latest_price
        latest_price = float(recent_month_data[-1].get('close', latest_price)) if recent_month_data else latest_price
        
        smc_trend = "Alcista \ud83d\udfe2" if latest_price > closes.ewm(span=20).mean().iloc[-1] else "Bajista \ud83d\udd34"
        
        vol_month = volumes.iloc[-20:]
        order_block_price = float(closes.iloc[vol_month.idxmax()]) if vol_month.max() > 0 else latest_price

        # C\u00c1LCULO DE TP/SL: Take Profit en la siguiente Resistencia SMC y Stop Loss 2% abajo del Soporte SMC
        take_profit = smc_res
        stop_loss = smc_sup * 0.98

        # VARIABLE 1 (RVOL): Volumen actual / Promedio de 10 d\u00edas
        recent_10_vols = volumes.iloc[-10:]
        avg_10_vol = float(recent_10_vols.mean()) if len(recent_10_vols) > 0 else 1.0
        latest_vol = safe_check.get('volume', float(volumes.iloc[-1]))
        rvol = float(latest_vol / avg_10_vol) if avg_10_vol > 0 else 0.0

        # VARIABLE 3 (VALOR): Usa el RSI y el P/E.
        pe = safe_check.get('pe', 0.0)

        result = {
            'ticker': tk, 
            'price': latest_price, 
            'rsi': latest_rsi, 
            'macd_line': float(macd_line.iloc[-1]), 
            'macd_signal': float(macd_signal.iloc[-1]), 
            'smc_sup': smc_sup, 
            'smc_res': smc_res, 
            'smc_trend': smc_trend, 
            'order_block': order_block_price,
            'take_profit': take_profit,
            'stop_loss': stop_loss,
            'rvol': rvol,
            'pe': pe
        }
        LAST_KNOWN_ANALYSIS[tk] = result
        return result
    except Exception as e:
        print(f"ERROR CR\u00cdTICO SMC: {e}")
        try:
            return _get_fallback_smc()
        except:
            return "\u26a0\ufe0f Error t\u00e9cnico al calcular niveles."

def update_smc_memory(ticker, analysis):
    tk = remap_ticker(ticker)
    SMC_LEVELS_MEMORY[tk] = {'sup': analysis['smc_sup'], 'res': analysis['smc_res'], 'update_date': datetime.now()}

def analyze_breakout_gpt(ticker, level_type, price):
    tk = remap_ticker(ticker)
    display_name = get_display_name(tk)
    if not OPENAI_API_KEY: return "Â¿QuÃ© hacer? Mantener cautela."
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)

    # === Recopilar contexto unificado GÃ‰NESIS ===
    # Contexto geopolÃ­tico
    geo_context = "Sin datos geopolÃ­ticos recientes."
    risk_ctx = GENESIS_RISK_CONTEXT
    if risk_ctx.get('last_update'):
        global_s = _classify_sentiment(risk_ctx['sentiment_global'])
        geo_context = f"Sentimiento global del mercado: {global_s['label']} ({global_s['bull_pct']}% Alcista / {global_s['bear_pct']}% Bajista)."
        if risk_ctx.get('news_digest'):
            top_news = [n.get('title_es', n.get('title', ''))[:60] for n in risk_ctx['news_digest'][:3]]
            geo_context += f"\nNoticias clave: {'; '.join(top_news)}"
        if tk in risk_ctx.get('high_risk_tickers', []):
            geo_context += f"\nâš ï¸ {display_name} estÃ¡ en ZONA DE RIESGO GEOPOLÃTICO."

    # Contexto de ballenas
    whale_context = "Sin movimientos de ballena recientes en este activo."
    wctx = _get_whale_context_for_ticker(tk)
    if wctx:
        whale_context = f"Ballena detectada: {wctx['vol_str']} ({wctx['type']}) hace {wctx['minutes_ago']} minutos."

    prompt = (f"Eres GÃ‰NESIS, analista institucional senior de un fondo de cobertura.\n\n"
              f"EVENTO: El activo {display_name} acaba de romper su nivel de {level_type} (Smart Money Concept) en ${fmt_price(price)} verificado vÃ­a FMP.\n\n"
              f"CONTEXTO GEOPOLÃTICO:\n{geo_context}\n\n"
              f"CONTEXTO BALLENAS:\n{whale_context}\n\n"
              f"INSTRUCCIONES OBLIGATORIAS:\n"
              f"1. EvalÃºa esta ruptura cruzando: direcciÃ³n del precio, sentimiento geopolÃ­tico, y movimientos de ballenas.\n"
              f"2. Da un consejo claro: Â¿COMPRAR, VENDER o MANTENER? Resalta tu elecciÃ³n en negrita.\n"
              f"3. Asigna un PORCENTAJE DE CONFIANZA (ejemplo: 75%, 85%, 92%) basado en cuÃ¡ntas seÃ±ales convergen:\n"
              f"   - Si ruptura + ballenas + sentimiento apuntan en la misma direcciÃ³n = 85-95%\n"
              f"   - Si hay seÃ±ales mixtas = 60-75%\n"
              f"   - Si hay contradicciÃ³n fuerte = 50-65%\n"
              f"4. Formato: 1 pÃ¡rrafo de mÃ¡ximo 4 lÃ­neas. ESPAÃ‘OL ESTRICTO con vocabulario financiero profesional.\n"
              f"5. Termina con: 'ðŸŽ¯ Confianza: [X]%'\n")
    try:
        return client.chat.completions.create(model="gpt-4o", messages=[{"role": "user", "content": prompt}], max_tokens=400).choices[0].message.content.strip()
    except Exception as e:
        logging.error(f"Fallo OpenAI breakout: {e}")
        return "Â¿QuÃ© hacer? Esperar confirmaciÃ³n de volumen en la siguiente hora. ðŸŽ¯ Confianza: 50%"

def perform_deep_analysis(ticker):
    tk = remap_ticker(ticker)
    display_name = get_display_name(tk)

    # PASO 0: Obtener precio VERIFICADO de FMP ANTES de todo
    # Este precio es SAGRADO â€” viene directo del exchange via FMP
    verified_price = None
    fmp_data = _fetch_fmp_quote(tk)
    if fmp_data:
        verified_price = fmp_data['price']
        logging.info(f"ANÃLISIS {tk}: precio FMP verificado = ${fmt_price(verified_price)}")

    # PASO 1: Obtener indicadores tÃ©cnicos (RSI, MACD, SMC) via FMP historical
    tech = fetch_and_analyze_stock(tk)

    # SIEMPRE imponer el precio FMP quote sobre el precio del historial
    if tech and verified_price:
        tech['price'] = verified_price

    # Si no hay tech pero sÃ­ tenemos precio verificado
    if not tech and not verified_price:
        live = get_safe_ticker_price(tk)
        if live:
            verified_price = live['price']

    # Precio final a inyectar en el prompt (INNEGOCIABLE)
    final_price = verified_price or (tech['price'] if tech else None)

    # === HARD-STOP: SIN PRECIO VERIFICADO = SIN ANÃLISIS ===
    if not final_price:
        fmp_sym = _get_fmp_symbol(tk)
        diag = _FMP_LAST_ERROR.get(tk, 'Sin informaciÃ³n de error')
        _key_len = len(FMP_API_KEY) if FMP_API_KEY else 0
        return (f"âš ï¸ <b>Error de conexiÃ³n con FMP</b>\n\n"
                f"No se pudo obtener el precio de {display_name} "
                f"(sÃ­mbolo: {fmp_sym}).\n\n"
                f"ðŸ” <b>DiagnÃ³stico:</b>\n<code>{diag}</code>\n\n"
                f"ðŸ”‘ Key cargada: {'SÃ­' if FMP_API_KEY else 'NO'} "
                f"({_key_len} chars)\n"
                f"ðŸ›‘ AnÃ¡lisis BLOQUEADO para evitar datos inventados.")

    if tech:
        tech_block = (
            f"--- DATOS EN VIVO (calculados por el sistema, NO los inventes) ---\n"
            f"â€¢ Precio EXACTO en vivo: ${fmt_price(final_price)}\n"
            f"â€¢ RSI (14 perÃ­odos): {tech['rsi']:.2f}\n"
            f"â€¢ MACD LÃ­nea: {tech['macd_line']:.4f}\n"
            f"â€¢ MACD SeÃ±al: {tech['macd_signal']:.4f}\n"
            f"â€¢ Tendencia SMC: {tech['smc_trend']}\n"
            f"â€¢ Buy-side Liquidity (Soporte SMC): ${fmt_price(tech['smc_sup'])}\n"
            f"â€¢ Sell-side Liquidity (Resistencia SMC): ${fmt_price(tech['smc_res'])}\n"
            f"â€¢ Order Block Institucional: ${fmt_price(tech['order_block'])}\n"
            f"--- FIN DE DATOS EN VIVO ---"
        )
    elif final_price:
        tech_block = (
            f"--- DATOS EN VIVO ---\n"
            f"â€¢ Precio EXACTO en vivo: ${fmt_price(final_price)}\n"
            f"â€¢ Indicadores tÃ©cnicos: No disponibles (mercado cerrado o sin historial)\n"
            f"--- FIN DE DATOS EN VIVO ---"
        )
    else:
        tech_block = "--- DATOS EN VIVO: No disponibles en este momento ---"

    # PASO 2: Noticias recientes del activo
    news_str = "No hay noticias recientes disponibles."
    try:
        fmp_news = _fetch_fmp_news(5)
        if fmp_news:
            news_titles = [n.get('title', '') for n in fmp_news if n.get('title')]
            if news_titles:
                news_str = "\n".join([f"- {t}" for t in news_titles[:5]])
    except: pass

    # PASO 3: Prompt blindado anti-alucinaciÃ³n hiper-detallado para GPT-4o
    price_str = f"${fmt_price(final_price)}" if final_price else "N/A"
    prompt = (
        f"ActÃºa como GÃ‰NESIS, un analista financiero institucional senior (modelo GPT-4o).\n\n"
        f"ACTIVO: {display_name} ({tk})\n\n"
        f"{tech_block}\n\n"
        f"NOTICIAS RECIENTES:\n{news_str}\n\n"
        f"REGLAS INQUEBRANTABLES:\n"
        f"1. El precio REAL Y VERIFICADO de {display_name} en este momento es {price_str} (proveedor: FMP). Tienes PROHIBIDO inventar, adivinar o usar otro precio.\n"
        f"2. Basa tu anÃ¡lisis EXCLUSIVAMENTE en los datos numÃ©ricos proporcionados arriba.\n"
        f"3. Realiza una fusiÃ³n de perspectivas: cruza los niveles mecÃ¡nicos de 'Smart Money Concepts' (Bloques de Ã³rdenes y vacÃ­os de liquidez) con el indicador de Tendencia SMC.\n"
        f"4. EvalÃºa exhaustivamente si el precio actual sugiere que los algoritmos institucionales estÃ¡n acumulando en zona de demanda o distribuyendo en zona de oferta.\n"
        f"5. Combina el pulso macro de las noticias y detalla de quÃ© manera afectan los niveles tÃ©cnicos.\n\n"
        f"FORMATO DE RESPUESTA EN GITHUB MARKDOWN:\n"
        f"ðŸ“Š **AnÃ¡lisis Smart Money (SMC):** [Profundiza sobre liquidez, imbalances y el order block actual]\n"
        f"ðŸ“° **Contexto Macro / Institucional:** [Tu lectura de cÃ³mo el flujo de impacto altera la tÃ©cnica]\n"
        f"ðŸŽ¯ **VEREDICTO FINAL:** [COMPRAR / VENDER / MANTENER] + [JustificaciÃ³n institucional en 2 lÃ­neas]\n\n"
        f"RESPONDE ESTRICTAMENTE EN ESPAÃ‘OL."
    )

    if not OPENAI_API_KEY: return "Error: API KEY de OpenAI no configurada."
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        return client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=800
        ).choices[0].message.content.strip()
    except Exception as e:
        logging.error(f"Fallo al analizar con OpenAI: {e}")
        return f"Fallo al analizar: {e}"


def build_wallet_dashboard():
    investments = get_investments()
    realized_pnl = get_realized_pnl()

    if not investments and realized_pnl == 0:
        return ("---\nðŸ’Ž *ESTADO GLOBAL DE TU WALLET* ðŸ’Ž\n---\n"
                "ðŸ’¹ <b>Capital Operativo Activo:</b> $0.00\n"
                "ðŸ’° <b>Ganancia Mensual Acumulada:</b> $0.00 USD\n"
                "ðŸ“ˆ <b>Rendimiento M/M:</b> [0.00%]\n"
                "ðŸ“Š <b>Estatus:</b> [âšª SIN OPERACIONES]\n"
                "ðŸŽ¯ <b>Meta del Mes (10%):</b> [â–‘â–‘â–‘â–‘â–‘â–‘â–‘â–‘â–‘â–‘] 0%\n---")

    if not investments and realized_pnl != 0:
        return ("---\nðŸ’Ž *ESTADO GLOBAL DE TU WALLET* ðŸ’Ž\n---\n"
                "ðŸ’¹ <b>Capital Operativo Activo:</b> $0.00\n"
                f"ðŸ’µ <b>Ganancia Mensual (Acumulado Ventas):</b> {'+' if realized_pnl>=0 else ''}${realized_pnl:,.2f} USD\n"
                "ðŸ“ˆ <b>Rendimiento M/M:</b> [0.00%]\n"
                "ðŸ“Š <b>Estatus:</b> [âšª SIN POSICIONES ABIERTAS]\n"
                "ðŸŽ¯ <b>Meta del Mes (10%):</b> [â–‘â–‘â–‘â–‘â–‘â–‘â–‘â–‘â–‘â–‘] 0%\n---")

    total_invested = 0.0
    total_current = 0.0
    details = []

    for tk, dt in investments.items():
        init_amount = dt['amount_usd']
        entry_p = dt['entry_price']
        intra = fetch_intraday_data(tk)

        display_name = get_display_name(tk)

        if intra:
            live_price = intra['latest_price']
            roi_percent = (live_price - entry_p) / entry_p if entry_p > 0 else 0
            curr_val = init_amount * (1 + roi_percent)

            total_invested += init_amount
            total_current += curr_val

            sign = "+" if roi_percent >= 0 else ""
            details.append(f"â€¢ {display_name}: {sign}{roi_percent*100:.2f}% (${fmt_price(live_price)})")
        else:
            # Mercado cerrado - mostrar activo sin ocultar
            total_invested += init_amount
            total_current += init_amount
            details.append(f"â€¢ {display_name}: â³ Mercado cerrado (entrada: ${fmt_price(entry_p)})")



    total_roi = (total_current - total_invested) / total_invested if total_invested > 0 else 0
    sign_roi = "+" if total_roi >= 0 else ""
    status_icon = "ðŸŸ¢ EN GANANCIAS" if total_roi >= 0 else "ðŸ”´ EN PÃ‰RDIDAS"

    goal = 0.10
    progress_ratio = max(0, min(1, total_roi / goal))

    filled_blocks = int(progress_ratio * 10)
    empty_blocks = 10 - filled_blocks
    bar = "â–“" * filled_blocks + "â–‘" * empty_blocks
    progress_text = f"{int(progress_ratio*100)}% completado"

    report = []
    report.append("---")
    report.append("ðŸ’Ž <b>ESTADO GLOBAL DE TU WALLET</b> ðŸ’Ž")
    report.append("---")
    report.append(f"ðŸ’¹ <b>Rendimiento M/M (Activo):</b> [{sign_roi}{total_roi*100:.2f}%]")
    report.append(f"ðŸ“Š <b>Estatus:</b> [{status_icon}]")
    report.append(f"ðŸŽ¯ <b>Meta del Mes (10%):</b> [{bar}] {progress_text}")
    if realized_pnl != 0:
        report.append(f"ðŸ’µ <b>Acumulado en Ventas (Mes):</b> {'+' if realized_pnl>=0 else ''}${realized_pnl:,.2f} USD")
    report.append("---")
    if details:
        report.append("<i>(Detalle por activo)</i>")
        report.extend(details)
    return "\n".join(report)

# ----------------- CONTROLADORES TELEBOT (NLP & ACCIONES DIRECTAS) -----------------

@bot.message_handler(commands=['check_db'])
def test_db(message):
    print("â ³ Intentando conectar con Supabase (Timeout de 5 segundos)...")
    try:
        conn = get_db_connection()
        if not conn:
             print("â Œ ERROR DE RED O AUTENTICACIÃ“N: conn es None")
             return
        c = conn.cursor()
        c.execute('SELECT version();')
        v = c.fetchone()[0]
        print(f"âœ… CONEXIÃ“N ESTABLECIDA\nPostgreSQL OK. Base de Datos en lÃ­nea y funcional.\n\nDetalle: {v}")
    except Exception as e:
        print(f"â Œ ERROR DE RED O AUTENTICACIÃ“N\nSupabase ha rechazado la conexiÃ³n.\n\nLog TÃ©cnico: {e}")

@bot.message_handler(commands=['clear_all'])
def command_clear_all(message):
    if str(message.chat.id) != str(CHAT_ID): return
    conn = get_db_connection()
    if not conn:
        bot.reply_to(message, "ðŸš¨ Error: No hay conexiÃ³n a Supabase.")
        return
    try:
        c = conn.cursor()
        c.execute('TRUNCATE TABLE wallet')
        conn.commit()
    except Exception as e:
        bot.reply_to(message, f"âŒ Fallo al limpiar DB: {e}")
    finally:
        pass # conn.close() delegado a pooling global

@bot.message_handler(commands=['start'])
def cmd_start(message):
    if str(message.chat.id) != str(CHAT_ID): return
    restore_state_from_telegram()
    tkrs = get_tracked_tickers()
    
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton(text="🛡️ Geopolítica", callback_data="geopolitics"),
        InlineKeyboardButton(text="🐋 Radar de Ballenas", callback_data="radar_institucional")
    )
    markup.add(
        InlineKeyboardButton(text="🦅 Niveles SMC", callback_data="smc_levels"),
        InlineKeyboardButton(text="💰 Mi Wallet / Estado", callback_data="wallet_status")
    )
    
    reply_text = """---
🐋 <b>GENESIS 1.0 - TRADING INSTITUCIONAL</b> 📈
---
✅ Bot iniciado correctamente.
📊 Radar: """ + str(len(tkrs)) + """ activos.
🛡️ Persistencia activa. Tu cartera está segura."""
    bot.reply_to(message, reply_text, reply_markup=markup, parse_mode="HTML")
@bot.message_handler(commands=['reset_pnl'])
def cmd_reset_pnl(message):
    """Comando oculto para resetear la ganancia mensual a $0.00"""
    if str(message.chat.id) != str(CHAT_ID): return
    reset_realized_pnl()
    bot.reply_to(message, "ðŸ”„ <b>PnL Mensual Reseteado</b>\n\nâœ… Ganancia Mensual Acumulada: <b>$0.00 USD</b>\nâœ… Contabilidad limpia desde este momento.", parse_mode="HTML")

@bot.message_handler(commands=['reset_total'])
def cmd_reset_total(message):
    """RESET RADICAL: borra todo el historial contable"""
    if str(message.chat.id) != str(CHAT_ID): return
    reset_total_db()
    bot.reply_to(message, (
        "âš ï¸ <b>SISTEMA REINICIADO</b>\n\n"
        "ðŸ—‘ï¸ Todo el historial contable ha sido eliminado.\n"
        "ðŸ’¹ Capital Operativo: <b>$0.00</b>\n"
        "ðŸ’° Ganancia Mensual: <b>$0.00 USD</b>\n"
        "ðŸ“ˆ Rendimiento: <b>0.00%</b>\n\n"
        "âœ… Wallet limpia. Los activos en tu radar siguen activos para monitoreo SMC."
    ), parse_mode="HTML")


@bot.message_handler(commands=['recover'])
def cmd_recover(message):
    """Herramienta de Carga CrÃ­tica de Respaldo por Base64"""
    if str(message.chat.id) != str(CHAT_ID): return
    try:
        command_parts = message.text.split(' ', 1)
        if len(command_parts) < 2:
            bot.reply_to(message, "âš ï¸ RestauraciÃ³n CrÃ­tica.\nUso: `/recover [STRING_BASE64_DEL_LOG]`", parse_mode="Markdown")
            return

        b64_str = command_parts[1].strip()
        _restore_from_b64(b64_str)
        save_state_to_telegram()  # Guardar inmediatamente en Telegram

        tkrs = get_tracked_tickers()
        bot.reply_to(message, f"âœ… **Â¡RECUPERACIÃ“N EXITOSA!**\nSe restauraron {len(tkrs)} activos.\nEl backup ya fue guardado en Telegram.", parse_mode="Markdown")

        for tk in tkrs:
            val = fetch_and_analyze_stock(tk)
            if val: update_smc_memory(tk, val)

    except Exception as e:
        bot.reply_to(message, f"âŒ Error en recuperaciÃ³n: `{e}`", parse_mode="Markdown")

@bot.message_handler(commands=['backup'])
def cmd_backup(message):
    """Forzar un backup manual visible"""
    if str(message.chat.id) != str(CHAT_ID): return
    save_state_to_telegram()
    tkrs = get_tracked_tickers()
    bot.reply_to(message, f"âœ… Backup forzado completado.\nðŸ“Š {len(tkrs)} activos guardados en Telegram Cloud.")

from openai import OpenAI

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    if str(message.chat.id) != str(CHAT_ID): return
    msg = bot.reply_to(message, "ðŸ‘ï¸ Analizando grÃ¡fica con GÃ‰NESIS Vision (GPT-4o OpenAI)...")
    try:
        if not OPENAI_API_KEY:
            bot.edit_message_text("âš ï¸ Error de configuraciÃ³n de modelo: OPENAI_API_KEY no detectada.", chat_id=message.chat.id, message_id=msg.message_id)
            return

        file_info = bot.get_file(message.photo[-1].file_id)
        image_bytes = bot.download_file(file_info.file_path)
        base_img = base64.b64encode(image_bytes).decode('utf-8')
        
        client = OpenAI(api_key=OPENAI_API_KEY)
        
        prompt = (
            "ActÃºa como una herramienta educativa de anÃ¡lisis tÃ©cnico. Este anÃ¡lisis es puramente para fines de estudio y simulaciÃ³n, no es asesorÃ­a financiera. Analiza la siguiente imagen de manera objetiva.\n\n"
            "Analiza bajo conceptos Smart Money Concepts (SMC). PROHIBIDO explicar quÃ© significa SMC, Order Blocks, BOS o CHoCH. CERO TEORÃA.\n\n"
            "Tu respuesta DEBE seguir ESTRICTAMENTE este formato, sin agregar introducciones ni despedidas. Tono frÃ­o, analÃ­tico y directo a los datos duros:\n\n"
            "ðŸ“Š CONTEXTO TÃ‰CNICO: [1 o 2 lÃ­neas sobre la tendencia actual y la acciÃ³n del precio evaluando liquidez y estructura].\n"
            "ðŸŽ¯ NIVELES CLAVE: [Soportes, Resistencias u Order Blocks con PRECIOS EXACTOS segÃºn la grÃ¡fica].\n"
            "âš ï¸ RIESGO DE INVERSIÃ“N: [Bajo / Medio / Alto] - [RazÃ³n tÃ©cnica directa].\n"
            "âš–ï¸ SESGO DIRECCIONAL: [Fuerte Alcista / Fuerte Bajista / Neutral / Esperar ConfirmaciÃ³n] - [JustificaciÃ³n descriptiva en una lÃ­nea, ej. 'Alta probabilidad de rebote en FVG en $150']."
        )

        res = client.chat.completions.create(
            model="gpt-4o", 
            messages=[
                {"role": "user", "content": [
                    {"type": "text", "text": prompt}, 
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base_img}"}}
                ]}
            ], 
            max_tokens=800
        )
        
        bot.edit_message_text(f"---\nðŸ“Š *REPORTE VISUAL GÃ‰NESIS*\n---\n{res.choices[0].message.content.strip()}", chat_id=message.chat.id, message_id=msg.message_id, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Error de visiÃ³n OpenAI: {e}")
        bot.edit_message_text("\u26a0\ufe0f Error de configuraci\u00f3n de modelo", chat_id=message.chat.id, message_id=msg.message_id)

@bot.message_handler(func=lambda message: True, content_types=['text'])
def handle_text(message):
    if str(message.chat.id) != str(CHAT_ID): return
    text = message.text.strip()

    # Ignorar mensajes de backup del bot
    if text.startswith(BACKUP_PREFIX): return

    # === BOTONES MENÃš RÃPIDO ===
    if text == "ðŸ’° Mi Wallet / Estado" or "CÃ“MO VOY" in text.upper() or "RESUMEN" in text.upper():
        bot.reply_to(message, "ðŸ’° Extrayendo datos robustos y valuando mÃ©tricas live...")
        bot.send_message(message.chat.id, build_wallet_dashboard(), parse_mode="HTML")
        return

    if text == "\ud83d\udc33 Radar Ballenas":
        bot.reply_to(message, "\ud83d\udc33 Analizando flujos transaccionales institucionales (24H)...")
        try:
            import datetime
            now_t = datetime.datetime.now()
            report = ["---", "\ud83d\udc33 <b>REPORTE DE BALLENAS (\u00daLTIMAS 24H)</b> \ud83d\udc33", "---"]
            
            total_net = 0
            assets = []
            
            for tk, events in list(WHALE_HISTORY_DB.items()):
                # Filtrar \u00faltimas 24h (86400 segundos)
                recent = [e for e in events if (now_t - e['timestamp']).total_seconds() <= 86400]
                WHALE_HISTORY_DB[tk] = recent # Purga de memoria
                
                if not recent: continue
                
                entradas_events = [e['vol_usd'] for e in recent if "Compra" in e['type']]
                salidas_events = [e['vol_usd'] for e in recent if "Venta" in e['type']]
                
                entradas = sum(entradas_events)
                salidas = sum(salidas_events)
                neto = entradas - salidas
                total_net += neto
                
                assets.append({
                    'tk': tk, 
                    'entradas': entradas, 
                    'salidas': salidas, 
                    'neto': neto,
                    'n_entradas': len(entradas_events),
                    'n_salidas': len(salidas_events)
                })
                
            if not assets:
                report.append("\ud83c\udf0a Oc\u00e9ano tranquilo. Sin flujo institucional en las \u00faltimas 24h.")
                bot.send_message(message.chat.id, "\n".join(report), parse_mode="HTML")
                return
                
            # Orden de mayor a menor flujo de inter\u00e9s absoluto
            assets.sort(key=lambda x: abs(x['neto']), reverse=True)
            
            for a in assets:
                t_name = get_display_name(a['tk'])
                sign = "+" if a['neto'] > 0 else ""
                pres = "Presi\u00f3n Alcista \ud83d\udcc8" if a['neto'] > 0 else "Presi\u00f3n Bajista \ud83d\udcc9"
                report.extend([
                    f"\ud83e\ude99 <b>{t_name} ({a['tk']}):</b>",
                    f"\u2022 \ud83d\udfe2 ENTRADAS: ${a['entradas']:,.0f} USD (De {a['n_entradas']} ballenas)",
                    f"\u2022 \ud83d\udd34 SALIDAS: ${a['salidas']:,.0f} USD (De {a['n_salidas']} ballenas)",
                    f"\u2022 \ud83d\udcca NETO: {sign}${a['neto']:,.0f} USD ({pres})",
                    ""
                ])
                
            report.append("---")
            report.append("<b>TOTAL MERCADO (Cartera):</b>")
            t_sign = "+" if total_net > 0 else ""
            report.append(f"\ud83d\udcb0 Flujo Total: <b>{t_sign}${total_net:,.0f} USD</b>")
            
            bot.send_message(message.chat.id, "\n".join(report), parse_mode="HTML")
        except Exception as e:
            logging.error(f"Error en Reporte de Ballenas 24H: {e}")
            bot.send_message(message.chat.id, "\u26a0\ufe0f Error generando reporte de flujos institucionales.", parse_mode="HTML")
        return

    if text == "ðŸŒŽ GeopolÃ­tica":
        bot.reply_to(message, "ðŸŒ Generando Reporte EstratÃ©gico Unificado GÃ‰NESIS...")
        try:
            report = generar_reporte_macro_manual()
            if report:
                bot.send_message(message.chat.id, report, parse_mode="HTML")
            else:
                bot.send_message(message.chat.id, "â˜• Sin eventos de riesgo detectados en este momento. Vigilancia activa.", parse_mode="HTML")
        except Exception as e:
            logging.error(f"Error en GeopolÃ­tica: {e}")
            bot.send_message(message.chat.id, "â˜• Sin eventos de riesgo detectados en este momento. Vigilancia activa.", parse_mode="HTML")
        return

    if text == "ðŸ“‰ SMC / Mi Cartera":
        bot.reply_to(message, "ðŸ“‰ Limpiando cachÃ© y forzando datos frescos de exchange...")
        report_lines = ["---", "ðŸ¦… *GÃ‰NESIS: SMC / NIVELES CRÃTICOS*", "---"]
        tkrs = get_tracked_tickers()

        if not tkrs:
             bot.send_message(message.chat.id, "Tu cartera estÃ¡ vacÃ­a.", parse_mode="HTML")
             return

        # REFRESH FORZADO: limpiar cachÃ© de precios para obligar consulta fresca
        for raw_tk in tkrs:
            tk = remap_ticker(raw_tk)
            LAST_KNOWN_PRICES.pop(tk, None)

        for raw_tk in tkrs:
            import time
            time.sleep(0.2)
            tk = remap_ticker(raw_tk)
            analysis = fetch_and_analyze_stock(tk)
            d_name = get_display_name(tk)

            if analysis and isinstance(analysis, dict):
                precio = analysis['price']
                soporte = analysis['smc_sup']
                resistencia = analysis['smc_res']
                
                if precio < soporte:
                    veredicto = "COMPRA \ud83d\udfe2"
                elif precio > resistencia:
                    veredicto = "VENTA \ud83d\udd34"
                else:
                    veredicto = "MANTENER \u26a0\ufe0f"

                report_lines.extend([
                    f"\ud83c\udfe6 <b>RESEARCH: {d_name}</b>",
                    f"\ud83d\udcb0 <b>Precio:</b> ${fmt_price(precio)}",
                    f"\ud83d\udcc9 <b>Soporte (Piso):</b> ${fmt_price(soporte)}",
                    f"\ud83d\udcc8 <b>Resistencia (Techo):</b> ${fmt_price(resistencia)}",
                    "",
                    "\ud83c\udfaf <b>NIVELES T\u00c1CTICOS:</b>",
                    f"\u2022 \ud83d\udfe2 TP (Toma de ganancia): ${fmt_price(analysis['take_profit'])}",
                    f"\u2022 \ud83d\udd34 SL (Stop Loss): ${fmt_price(analysis['stop_loss'])}",
                    "",
                    f"\u2696\ufe0f <b>VEREDICTO:</b> <b>{veredicto}</b>",
                    "---"
                ])
            elif isinstance(analysis, str):
                report_lines.extend([f"\ud83c\udfe6 <b>RESEARCH: {d_name}</b>", f"\u2022 {analysis}", "---"])
            else:
                report_lines.extend([f"\ud83c\udfe6 <b>{d_name}</b>", f"\u2022 \u26a0\ufe0f Niveles SMC no disponibles para este ticker en este momento", "---"])

        texto_smc = "\n".join(report_lines)
        final_text = texto_smc.encode('utf-8', 'ignore').decode('utf-8')
        bot.send_message(message.chat.id, final_text, parse_mode="HTML")
        return

    # === EXPRESIONES REGULARES INTELIGENTES NLP ===
    if re.search(r'(?i)\bANALIZA\b\s+([A-Za-z0-9\-]+)', text):
        match = re.search(r'(?i)\bANALIZA\b\s+([A-Za-z0-9\-]+)', text)
        if match:
            tk = remap_ticker(match.group(1))
            display_name = get_display_name(tk)
            bot.reply_to(message, f"ðŸ” AnÃ¡lisis Profundo Institucional en {display_name}...")
            bot.send_message(message.chat.id, f"---\nðŸ¦ *RESEARCH: {display_name}*\n---\n{perform_deep_analysis(tk)}", parse_mode="HTML")
        return

    if re.search(r'(?i)\b(?:ELIMINA|BORRA|BORRAR|ELIMINAR)\b\s+([A-Za-z0-9\-]+)', text):
        match = re.search(r'(?i)\b(?:ELIMINA|BORRA|BORRAR|ELIMINAR)\b\s+([A-Za-z0-9\-]+)', text)
        if match:
             raw_input = match.group(1)
             tk = remap_ticker(raw_input)
             display_name = get_display_name(tk)
             if remove_ticker(tk):
                 bot.reply_to(message, f"---\nâœ… *GESTIÃ“N DE CARTERA*\n---\nâœ… [ {display_name} ] ha sido borrado del radar.\n\nâœ… Guardado en Base de Datos Blindada. Esta informaciÃ³n no se borrarÃ¡ aunque el bot se reinicie.", parse_mode="HTML")
             else:
                 bot.reply_to(message, f"âš ï¸ El activo {display_name} no residÃ­a en tu radar.")
        return

    if re.search(r'(?i)\b(?:AGREGA|AÃ‘ADE|AGREGAR)\b\s+([A-Za-z0-9\-]+)', text):
        match = re.search(r'(?i)\b(?:AGREGA|AÃ‘ADE|AGREGAR)\b\s+([A-Za-z0-9\-]+)', text)
        if match:
             raw_input = match.group(1).upper()
             tk = remap_ticker(raw_input)
             display_name = get_display_name(tk)

             validation = get_safe_ticker_price(tk)
             if validation is None:
                 bot.reply_to(message, "âš ï¸ Activo no encontrado en FMP. No se agregÃ³.")
                 return
             res = add_ticker(tk)
             if res == "DB_ERROR":
                 bot.reply_to(message, f"ðŸš¨ ERROR DE BASE DE DATOS: No se pudo conectar a Supabase. Revisa tu DATABASE_URL.")
             elif res == True:
                 bot.reply_to(message, f"---\nâœ… *GESTIÃ“N DE CARTERA*\n---\nâœ… [ {display_name} ] aÃ±adido al radar SMC.\n\nâœ… Guardado directamente en Supabase (Sin cachÃ©s).", parse_mode="HTML")
             else:
                 bot.reply_to(message, f"âš ï¸ El activo {display_name} ya existe en tu DB centralizada (Supabase).")
        return

    if re.search(r'(?i)\bCOMPR[EÃ‰]\b', text):
        match = re.search(r'(?i)\bCOMPR[EÃ‰]\b\s+(?:DE\s+)?\$?(\d+(?:\.\d+)?)\s+(?:EN\s+|DE\s+|ACCIONES\s+DE\s+)?([A-Za-z0-9\-]+)', text)
        if match:
            amt = match.group(1)
            tk = remap_ticker(match.group(2))
            display_name = get_display_name(tk)

            bot.reply_to(message, f"ðŸ’¸ Consultando precio de fijaciÃ³n para {display_name}...")
            intra = fetch_intraday_data(tk)
            if intra:
                add_investment(tk, amt, intra['latest_price'])
                bot.send_message(message.chat.id, f"---\nâœ… *CAPITAL REGISTRADO*\n---\nâ€¢ Activo: {display_name}\nâ€¢ Capital Invertido: ${float(amt):,.2f} USD\nâ€¢ Entrada: ${fmt_price(intra['latest_price'])}\n\nâœ… Guardado en Base de Datos Blindada. Esta informaciÃ³n no se borrarÃ¡ aunque el bot se reinicie.", parse_mode="HTML")
            else:
                bot.reply_to(message, f"âŒ No pude fijar el precio real de {display_name} ahora. Mercado cerrado temporalmente.")
        return

    if re.search(r'(?i)\bVEND[IÃ]\b', text):
        match = re.search(r'(?i)\bVEND[IÃ]\b\s+(?:TODO\s+)?(?:DE\s+)?\$?(?:\d+(?:\.\d+)?\s+(?:EN\s+|DE\s+|ACCIONES\s+DE\s+)?)?([A-Za-z0-9\-]+)', text)
        if match:
            tk = remap_ticker(match.group(1))
            display_name = get_display_name(tk)

            investments = get_investments()
            if tk in investments:
                bot.reply_to(message, f"ðŸ’¸ Procesando cierre institucional para {display_name}...")
                entry = investments[tk]['entry_price']
                amt = investments[tk]['amount_usd']

                intra = fetch_intraday_data(tk)
                if intra:
                    live_price = intra['latest_price']
                    roi = (live_price - entry) / entry if entry > 0 else 0
                    prof = amt * roi
                    sign = "+" if prof >= 0 else ""
                    icon = "ðŸŸ¢" if prof >= 0 else "ðŸ”´"
                    final_usd = amt + prof

                    close_investment(tk)
                    add_realized_pnl(prof)

                    ans_str = (
                        f"---\nâœ… *GESTIÃ“N DE CARTERA: CIERRE*\n---\n"
                        f"âœ… [ {display_name} ] liquidado al precio de ${fmt_price(live_price)}\n"
                        f"ðŸ’° <b>Capital Retirado:</b> ${final_usd:,.2f} USD\n"
                        f"{icon} <b>Ganancia Mensual Sumada:</b> {sign}${prof:,.2f} USD ({sign}{roi*100:.2f}%)\n\n"
                        f"âœ… Guardado en Base de Datos Blindada. Esta informaciÃ³n no se borrarÃ¡ aunque el bot se reinicie."
                    )
                    bot.send_message(message.chat.id, ans_str, parse_mode="HTML")
                else:
                    bot.reply_to(message, f"âŒ No pude contactar al mercado para saldar la liquidaciÃ³n de {display_name}.")
            else:
                 bot.reply_to(message, f"âš ï¸ No tienes capital invertido en {display_name}. Usa 'Elimina {display_name}' para detener rastreo.")
        return


# ----------------- MODO CENTINELA: VIGILANCIA DE NOTICIAS POR ACTIVO -----------------
_SENTINEL_TICK_INTERVAL = 40  # Cada 40 ticks de 30s = ~20 minutos

def verificar_noticias_cartera():
    """Vigila noticias especÃ­ficas de los activos en la cartera de Eduardo"""
    tkrs = get_tracked_tickers()
    if not tkrs:
        return

    for raw_tk in tkrs:
        tk = remap_ticker(raw_tk)
        display_name = get_display_name(tk)

        try:
            # Usar FMP news en lugar de yfinance
            fmp_sym = _get_fmp_symbol(tk)
            if _is_crypto_ticker(tk):
                fmp_sym = tk.replace('-USD', '') + 'USD'
            url = f"https://financialmodelingprep.com/stable/stock-news?symbol={fmp_sym}&limit=3&apikey={FMP_API_KEY}"
            resp = requests.get(url, timeout=10)
            news_list = resp.json() if resp.status_code == 200 else []
            if not isinstance(news_list, list):
                news_list = []
        except Exception:
            continue

        for article in news_list[:3]:  # Solo las 3 mÃ¡s recientes
            title = article.get('title', '')
            if not title:
                continue

            # Deduplicar con hash: no alertar la misma noticia dos veces
            news_hash = f"SENTINEL_{tk}_{hash(title) % 100000}"
            if check_and_add_seen_event(news_hash):
                continue  # Ya la vimos

            # Pasar por GPT para anÃ¡lisis de riesgo
            if not GEMINI_API_KEY:
                continue

            try:
                client = genai.Client(api_key=GEMINI_API_KEY)
                prompt = (
                    f"ActÃºa como GÃ‰NESIS, un gestor de riesgos senior de un fondo institucional (con base en Gemini 3.1 Pro).\n"
                    f"Analiza esta noticia del activo {display_name} ({tk}):\n"
                    f"Titular: \"{title}\"\n\n"
                    f"REGLAS ESTRICTAS:\n"
                    f"- Si la noticia es NEUTRAL, de relleno, o sin impacto real en el precio local, responde EXACTAMENTE: 'NEUTRAL'\n"
                    f"- Si la noticia tiene impacto REAL (positivo o negativo), predice el impacto en las zonas de oferta/demanda y genera una alerta con este formato:\n"
                    f"  ðŸ“° Suceso: [Resumen de 1 lÃ­nea]\n"
                    f"  ðŸ’¡ Sugerencia Institucional: [Vender / Vigilar / Hold / Comprar]\n"
                    f"  âš¡ Impacto Estimado: [Alto / Medio] en la liquidez\n"
                    f"RESPONDE EN ESPAÃ‘OL."
                )

                res = client.models.generate_content(
                    model="gemini-1.5-pro",
                    contents=prompt,
                ).text.strip()

                # Filtro de ruido: si GPT dice NEUTRAL, silencio total
                if "NEUTRAL" in res.upper() and len(res) < 30:
                    continue

                # Alerta que SÃ amerita atenciÃ³n
                alert_msg = (
                    f"---\nðŸš¨ *CENTINELA GÃ‰NESIS: ALERTA DE ACTIVO* ðŸš¨\n---\n"
                    f"ðŸ“ˆ Activo: <b>{display_name}</b>\n"
                    f"{res}\n"
                    f"---"
                )
                bot.send_message(CHAT_ID, alert_msg, parse_mode="HTML")

            except Exception as e:
                logging.debug(f"Sentinel GPT error for {tk}: {e}")
                continue


# Cache de precios de referencia para detectar movimientos >3%
_PROTECTION_BASELINE = {}  # {ticker: {'price': float, 'timestamp': datetime}}

def monitor_proteccion_activos():
    """SISTEMA GÃ‰NESIS: Monitor de protecciÃ³n de activos en la wallet.
    Compara precio FMP en vivo vs precio de referencia. Alerta si >3% de movimiento."""
    investments = get_investments()
    if not investments:
        return

    for tk_key, inv_data in investments.items():
        tk = remap_ticker(tk_key)
        display_name = get_display_name(tk)

        # Obtener precio FMP en vivo
        live_data = get_safe_ticker_price(tk)
        if not live_data:
            continue

        current_price = live_data['price']

        # Establecer baseline si no existe
        if tk not in _PROTECTION_BASELINE:
            _PROTECTION_BASELINE[tk] = {'price': current_price, 'timestamp': datetime.now()}
            continue

        baseline = _PROTECTION_BASELINE[tk]['price']
        if baseline <= 0:
            _PROTECTION_BASELINE[tk] = {'price': current_price, 'timestamp': datetime.now()}
            continue

        # Calcular variaciÃ³n porcentual
        pct_change = ((current_price - baseline) / baseline) * 100

        # Solo alertar si movimiento > 3%
        if abs(pct_change) < 3.0:
            continue

        # Deduplicar: no alertar dos veces por el mismo rango de movimiento
        alert_hash = f"PROT_{tk}_{int(pct_change)}"
        if check_and_add_seen_event(alert_hash):
            continue

        # Actualizar baseline al precio actual
        _PROTECTION_BASELINE[tk] = {'price': current_price, 'timestamp': datetime.now()}

        # Determinar direcciÃ³n
        direction = "ðŸ“‰ CAÃDA" if pct_change < 0 else "ðŸ“ˆ SUBIDA"
        emoji = "ðŸ”´" if pct_change < 0 else "ðŸŸ¢"

        # Obtener contexto SMC si estÃ¡ disponible
        smc_context = ""
        smc = SMC_LEVELS_MEMORY.get(tk)
        if smc:
            if current_price < smc.get('sup', 0):
                smc_context = f"\nâš ï¸ Precio POR DEBAJO del Soporte SMC (${fmt_price(smc['sup'])}). Zona de riesgo."
            elif current_price > smc.get('res', 0):
                smc_context = f"\nâœ… Precio POR ENCIMA de Resistencia SMC (${fmt_price(smc['res'])}). Posible breakout."
            else:
                smc_context = f"\nðŸ“Š Rango SMC: Soporte ${fmt_price(smc['sup'])} | Resistencia ${fmt_price(smc['res'])}"

        # Generar veredicto con IA si estÃ¡ disponible
        veredicto = ""
        if GEMINI_API_KEY:
            try:
                client = genai.Client(api_key=GEMINI_API_KEY)
                prompt = (
                    f"Eres GÃ‰NESIS (Gemini 3.1 Pro), un sistema de protecciÃ³n de activos enfocado en la prevenciÃ³n de riesgos y la estrategia Smart Money.\n"
                    f"Activo protegido: {display_name} ({tk})\n"
                    f"Precio real actual de FMP: ${fmt_price(current_price)}\n"
                    f"DesviaciÃ³n anÃ³mala detectada: {pct_change:+.2f}% en las Ãºltimas horas\n"
                    f"Contexto SMC en vivo:\n{smc_context}\n\n"
                    f"Analiza profunda pero rÃ¡pidamente esta desviaciÃ³n en relaciÃ³n a la liquidez del Order Block.\n"
                    f"Da un VEREDICTO en 2 lÃ­neas: Â¿Mantener, vender parcial, o reforzar posiciÃ³n institucional? Justifica mecÃ¡nicamente.\n"
                    f"ESPAÃ‘OL ESTRICTO."
                )
                res = client.models.generate_content(
                    model="gemini-1.5-pro",
                    contents=prompt,
                ).text.strip()
                veredicto = f"\n\nðŸ§  <b>VEREDICTO GÃ‰NESIS:</b>\n{res}"
            except Exception as e:
                logging.debug(f"Protection GPT error: {e}")

        # Construir y enviar alerta
        entry_price = inv_data.get('entry_price', 0)
        entry_info = f"\nðŸŽ¯ Precio de entrada: ${fmt_price(entry_price)}" if entry_price > 0 else ""

        alert_msg = (
            f"---\nðŸš¨ <b>SISTEMA GÃ‰NESIS â€” PROTECCIÃ“N DE ACTIVOS</b> ðŸš¨\n---\n\n"
            f"{emoji} <b>{direction} DETECTADA</b>\n\n"
            f"ðŸ’° Activo: <b>{display_name}</b>\n"
            f"ðŸ“‰ Movimiento: <b>{pct_change:+.2f}%</b>\n"
            f"ðŸ’µ Precio FMP: <b>${fmt_price(current_price)}</b>{entry_info}"
            f"{smc_context}"
            f"{veredicto}\n\n---"
        )

        try:
            bot.send_message(CHAT_ID, alert_msg, parse_mode="HTML")
        except Exception as e:
            logging.error(f"Error enviando alerta de protecciÃ³n para {tk}: {e}")


# ----------------- BUCLE CENTINELA HFT PRECISIÃ“N QUIRÃšRGICA -----------------
def boot_smc_levels_once():
    logging.info("Arrancando Centinela QuirÃºrgico (30s)...")

    # PASO CRÃTICO: Restaurar datos ANTES de hacer cualquier otra cosa
    restore_state_from_telegram()

    tkrs = get_tracked_tickers()
    logging.info(f"Activos cargados en radar: {len(tkrs)} â†’ {tkrs}")

    for tk in tkrs:
        val = fetch_and_analyze_stock(tk)
        if val: update_smc_memory(tk, val)

    # PASO INICIAL: Poblar contexto geopolÃ­tico al arrancar
    try:
        print("DEBUG BOOT: Inicializando contexto geopolitico...")
        genesis_strategic_report(manual=False)
        print(f"DEBUG BOOT: Contexto listo. Sentimiento: {GENESIS_RISK_CONTEXT.get('sentiment_global', 'N/A')} | High risk: {GENESIS_RISK_CONTEXT.get('high_risk_tickers', [])}")
    except Exception as e:
        print(f"DEBUG BOOT: Error inicializando contexto geo: {e}")

def background_loop_proactivo():
    """BUCLE DE ALTA LATENCIA CON DOBLE VERIFICACIÃ“N Y ANTI-SPAM (TTL 7 DÃAS)"""
    boot_smc_levels_once()
    sentinel_tick_counter = 0  # Contador para noticias de cartera cada ~20 min
    protection_tick_counter = 0  # Contador para monitor de protecciÃ³n cada ~5 min
    geo_refresh_counter = 0  # Contador para refrescar contexto geopolÃ­tico
    _PROTECTION_INTERVAL = 10  # ~5 minutos (10 ticks * 30s)
    _GEO_REFRESH_INTERVAL = 20  # ~10 minutos (20 ticks * 30s)
    loop_counter = 0  # Contador total de ciclos para heartbeat
    while True:
        try:
            time.sleep(30)
            now = datetime.now()
            purge_old_events()
            sentinel_tick_counter += 1
            protection_tick_counter += 1
            geo_refresh_counter += 1
            loop_counter += 1

            # === HEARTBEAT: log cada ciclo ===
            tracked = get_tracked_tickers()
            print(f"DEBUG HEARTBEAT [{now.strftime('%H:%M:%S')}]: Ciclo #{loop_counter} | {len(tracked)} activos en radar | Whale memory: {len(WHALE_MEMORY)}")

            raw_news = check_geopolitical_news()
            unique_news = []
            for n_title in raw_news:
                nws_id = f"NWS_{n_title}"
                if not check_and_add_seen_event(nws_id):
                    unique_news.append(n_title)

            if unique_news:
                ai_threat_evaluation = gpt_advanced_geopolitics(unique_news, manual=False)
                if ai_threat_evaluation:
                     bot.send_message(CHAT_ID, f"---\nðŸš¨ *VIGILANCIA GLOBAL ALTO RIESGO*\n---\n{ai_threat_evaluation}", parse_mode="HTML")

            # === REFRESCAR CONTEXTO GEOPOLÃTICO: cada ~10 minutos ===
            if geo_refresh_counter >= _GEO_REFRESH_INTERVAL:
                geo_refresh_counter = 0
                try:
                    print(f"DEBUG GEO REFRESH: Actualizando contexto geopolitico...")
                    genesis_strategic_report(manual=False)  # Actualiza GENESIS_RISK_CONTEXT sin enviar
                    print(f"DEBUG GEO REFRESH: Contexto actualizado. Sentimiento: {GENESIS_RISK_CONTEXT.get('sentiment_global', 'N/A')} | High risk: {GENESIS_RISK_CONTEXT.get('high_risk_tickers', [])}")
                except Exception as e:
                    print(f"DEBUG GEO REFRESH ERROR: {e}")

            # === MODO CENTINELA: verificar noticias de activos cada ~20 minutos ===
            if sentinel_tick_counter >= _SENTINEL_TICK_INTERVAL:
                sentinel_tick_counter = 0
                try:
                    verificar_noticias_cartera()
                except Exception as e:
                    logging.error(f"Error en Centinela de Noticias: {e}")

            # === MONITOR DE PROTECCIÃ“N DE ACTIVOS: cada ~5 minutos ===
            if protection_tick_counter >= _PROTECTION_INTERVAL:
                protection_tick_counter = 0
                try:
                    monitor_proteccion_activos()
                except Exception as e:
                    logging.error(f"Error en Monitor de ProtecciÃ³n: {e}")

            # === ESCANEO DE ACTIVOS: precios, rupturas, ballenas ===
            whale_scan_count = 0
            whale_detected_count = 0
            for tk in tracked:
                try:
                    intra = fetch_intraday_data(tk)
                    if not intra:
                        print(f"DEBUG WHALE SCAN: {tk} -> fetch_intraday_data devolvio None")
                        continue
                    cur_price = intra['latest_price']
                    display_name = get_display_name(tk)
                    whale_scan_count += 1

                    # === GUARDIA DE COHERENCIA: bloquear alertas si el precio es ilÃ³gico ===
                    price_is_reliable = True
                    if tk in LAST_KNOWN_PRICES:
                        last_p = LAST_KNOWN_PRICES[tk]['price']
                        if last_p > 0 and abs(cur_price - last_p) / last_p > 0.50:
                            logging.warning(f"ðŸš« ALERTA BLOQUEADA para {tk}: ${cur_price:.2f} vs Ãºltimo ${last_p:.2f} (>50% de desviaciÃ³n). Error de API probable.")
                            price_is_reliable = False

                    # Rupturas Doble Verificadas â€” SOLO si el precio es confiable
                    # Rupturas Doble Verificadas â€” SOLO si el precio es confiable
                    topol = SMC_LEVELS_MEMORY.get(tk)
                    analysis = LAST_KNOWN_ANALYSIS.get(tk)
                    if topol and analysis and price_is_reliable:
                        rsi = analysis.get('rsi', 50)
                        avg_v = intra.get('avg_vol', 1)
                        rvol = (intra.get('latest_vol', 1) / avg_v) if avg_v > 0 else 1
                    
                        if rsi < 35:
                            reason = f"RSI en {rsi:.1f} indica sobreventa extrema."
                        elif rsi > 65:
                            reason = f"RSI en {rsi:.1f} se\u00f1ala sobrecompra (riesgo de recorte)."
                        elif rvol >= 1.5:
                            reason = f"Presi\u00f3n de volumen inusual ({rvol:.1f}x por encima de la media)."
                        else:
                            reason = f"La estructura SMC t\u00e9cnica dicta la fuerza de la zona."

                        # L\u00f3gica 1: Ruptura Ascendente
                        if cur_price > topol['res']:
                            hash_brk = f"BRK_UP_{tk}_{topol['res']}"
                            if not check_and_add_seen_event(hash_brk):
                                msg = f"\ud83d\ude80 <b>RUPTURA DE RESISTENCIA EN {display_name}</b>.\nImpulso alcista detectado en ${fmt_price(cur_price)}.\n\nðŸ§  {reason}\n\u2696\ufe0f <b>Veredicto:</b> COMPRAR / MANTENER."
                                bot.send_message(CHAT_ID, f"---\n{msg}\n---", parse_mode="HTML")

                        # L\u00f3gica 2: Ruptura Descendente
                        elif cur_price < topol['sup']:
                            hash_drp = f"BRK_DWN_{tk}_{topol['sup']}"
                            if not check_and_add_seen_event(hash_drp):
                                msg = f"\u26a0\ufe0f <b>RUPTURA DE SOPORTE EN {display_name}</b>.\nPosible ca\u00edda detectada (perdi\u00f3 soporte de ${fmt_price(topol['sup'])} a ${fmt_price(cur_price)}).\n\nðŸ§  {reason}\n\u2696\ufe0f <b>Veredicto:</b> VENDER / CORTAR P\u00c9RDIDAS."
                                bot.send_message(CHAT_ID, f"---\n{msg}\n---", parse_mode="HTML")
                            
                        # L\u00f3gica 3: Zona de Acumulaci\u00f3n (Cerca del Soporte)
                        elif topol['sup'] <= cur_price <= (topol['sup'] * 1.015):
                            hash_acc = f"ACCUM_{tk}_{topol['sup']}"
                            if not check_and_add_seen_event(hash_acc):
                                msg = f"\ud83d\udc8e <b>ZONA DE ACUMULACI\u00d3N en {display_name}</b>.\nLas instituciones est\u00e1n comprando aqu\u00ed (muy cerca del Order Block de ${fmt_price(topol['sup'])}).\n\nðŸ§  {reason}\n\u2696\ufe0f <b>Veredicto:</b> OPORTUNIDAD DE COMPRA."
                                bot.send_message(CHAT_ID, f"---\n{msg}\n---", parse_mode="HTML")

                    # Ballenas â€” con cruce geopolÃ­tico GENESIS
                    # UMBRAL TEMPORAL REDUCIDO PARA TESTING (original: crypto=5.0, stocks=2.5)
                    if intra['avg_vol'] > 0 and price_is_reliable:
                        is_crypto = '-USD' in tk
                        whale_threshold = 2.0 if is_crypto else 1.5
                        spike = intra['latest_vol'] / intra['avg_vol']

                        # DEBUG: logear ratios de volumen significativos
                        if spike > 1.0:
                            print(f"DEBUG WHALE SCAN {tk}: latest_vol={intra['latest_vol']:,.0f} | avg_vol={intra['avg_vol']:,.0f} | spike={spike:.2f}x | threshold={whale_threshold}x | {'WHALE!' if spike >= whale_threshold else 'no trigger'}")

                        if spike >= whale_threshold:
                            if tk in last_whale_alert and (current_time - last_whale_alert[tk]) < 7200:
                                continue # Cooldown de 2 horas activo
                        
                            rt = verify_1m_realtime_data(tk)
                            valid_vol = int(rt['vol']) if rt else int(intra['latest_vol'])
                        
                            is_crypto = '-USD' in tk
                            vol_usd = valid_vol if is_crypto else (valid_vol * cur_price)
                        
                            min_elite_vol = 1_000_000
                            if vol_usd < min_elite_vol:
                                continue # Filtro \u00c9lite Institucional ($1M USD Minimo)
                            # === INICIO DE SMART MONEY FILTER ===
                            topol_whale = SMC_LEVELS_MEMORY.get(tk, {})
                            analysis_whale = LAST_KNOWN_ANALYSIS.get(tk, {})
                            rsi_w = analysis_whale.get('rsi', 50) if analysis_whale else 50
                            
                            try:
                                # Solo si precio est\u00e1 en zonas SMC
                                if 'sup' in topol_whale and 'res' in topol_whale:
                                    w_sup = topol_whale['sup']
                                    w_res = topol_whale['res']
                                    
                                    if intra['vol_type'] == 'Compra' and cur_price > (w_sup * 1.05): continue # Ignora compras caras
                                    if intra['vol_type'] == 'Venta' and cur_price < (w_res * 0.95): continue # Ignora ventas baratas
                                    
                                    if intra['vol_type'] == 'Compra':
                                        smart_msg = "\ud83d\udd25 <b>BALLENA DE ALTA CONVICCI\u00d3N DETECTADA:</b>\nEntrada institucional en zona de soporte t\u00e9cnico.\nProbabilidad de \u00e9xito: ALTA."
                                    else:
                                        smart_msg = "\ud83d\udd25 <b>BALLENA VENDEDORA DE ALTA CONVICCI\u00d3N:</b>\nSalida institucional en zona de resistencia t\u00e9cnica.\nProbabilidad de reversi\u00f3n bajista: ALTA."
                                else:
                                    continue # Si no tiene niveles validos, bloquear
                            except:
                                pass # Si no hay datos SMC, que no rompa el c\u00f3digo
                                
                            # Si pasa y no hay smart_msg, es porque pas\u00f3 el except pero se filtr\u00f3 mal, mejor asegurar
                            if 'smart_msg' not in locals():
                                smart_msg = "\ud83d\udd25 <b>BALLENA HFT DETECTADA.</b>"
                            # === FIN DE SMART MONEY FILTER ===
                            
                            whale_hash_id = f"WHL_SMART_{tk}_{valid_vol}"

                        if not check_and_add_seen_event(whale_hash_id):
                            last_whale_alert[tk] = current_time # Registrar el env\u00edo s\u00f3lo si es nuevo
                            whale_detected_count += 1
                            note = "\n<i>[Confirmando volumen institucional...]</i>" if not rt or rt['vol'] < intra['latest_vol'] else ""
                            WHALE_MEMORY.append({"ticker": tk, "vol_approx": valid_vol, "type": intra['vol_type'], "timestamp": now})
                        
                            if tk not in WHALE_HISTORY_DB:
                                WHALE_HISTORY_DB[tk] = []
                        
                            WHALE_HISTORY_DB[tk].append({
                                "type": intra['vol_type'],
                                "vol_usd": float(vol_usd),
                                "timestamp": now
                            })
                        
                            if is_crypto:
                                vol_display = f"${valid_vol:,} USD"
                            else:
                                vol_display = f"{valid_vol:,} unidades"

                            print(f"DEBUG WHALE SMART DETECTADA: {display_name} vol={vol_display} tipo={intra['vol_type']} spike={spike:.2f}x")

                            bot_msg = f"---\n{smart_msg}\n---\n<b>{display_name} ({tk})</b>\n\ud83d\udcb0 Capital transferido: <b>${vol_usd:,.0f} USD</b>\n\ud83d\udcca Riesgo T\u00e9cnico: RSI {rsi_w:.1f} | Precio: ${fmt_price(cur_price)}{note}"
                            bot.send_message(CHAT_ID, bot_msg, parse_mode="HTML")
                        else:
                            if intra['avg_vol'] == 0:
                                print(f"DEBUG WHALE SCAN {tk}: avg_vol=0, no se puede calcular spike")
                except Exception as e:
                    logging.error(f'Error ticker {tk}: {e}')
                    continue
            # === HEARTBEAT BALLENAS: resumen del escaneo ===
            print(f"DEBUG WHALE SCAN COMPLETADO: {whale_scan_count}/{len(tracked)} escaneados | {whale_detected_count} ballenas detectadas")

        except Exception as e:
            print(f"DEBUG ERROR HFT LOOP: {e}")
            logging.error(f"Error HFT: {e}")

@bot.callback_query_handler(func=lambda call: call.data == "radar_institucional")
def callback_whale_radar(call):
    try:
        bot.answer_callback_query(call.id, "🐋 Conectando con Wall Street...")
    except:
        pass
    
    try:
        tkrs = get_tracked_tickers()
        if not tkrs:
            bot.send_message(call.message.chat.id, "✅ Tu radar está vacío.")
            return

        import os
        import requests
        api_key = os.environ.get("FMP_API_KEY")
        
        syms = ",".join(tkrs)
        url = f"https://financialmodelingprep.com/api/v3/quote/{syms}?apikey={api_key}"
        
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code != 200:
                raise ValueError(f"HTTP {resp.status_code}")
            data = resp.json()
        except:
            data = []
            
        report = ["--- 🐋 <b>RADAR DE BALLENAS</b> ---"]
        ballenas_count = 0
        
        if isinstance(data, list):
            for q in data:
                tk = q.get("symbol", "UNKNOWN")
                vol = q.get("volume", 0)
                avg_vol = q.get("avgVolume", 0)
                price = q.get("price", 0)
                change = q.get("changesPercentage", 0)
                
                if avg_vol > 0 and vol > (avg_vol * 2):
                    ballenas_count += 1
                    estado = "🟢 COMPRA MASIVA" if change > 0 else "🔴 VENTA MASIVA"
                    report.append(f"🪙 <b>{tk}</b>: {estado}")
                    report.append(f"   • Precio: ${price:.2f} ({change:+.2f}%)")
                    report.append(f"   • Volumen Actual: {vol:,}")
                    report.append(f"   • Promedio: {avg_vol:,} ({(vol/avg_vol):.1f}x)")
                    report.append("")
                
        if ballenas_count == 0:
            msg = """--- 🐋 RADAR DE BALLENAS ---
Mercado en calma. No hay movimientos institucionales de alto valor en este momento.
---"""
            bot.send_message(call.message.chat.id, msg)
            return
            
        bot.send_message(call.message.chat.id, "\\n".join(report), parse_mode="HTML")
        
    except Exception as e:
        print(f"ERROR RADAR: {e}")
        try:
            bot.send_message(call.message.chat.id, f"⚠️ Error interno en radar: {e}")
        except:
            pass

# ----------------- MAIN -----------------
def main():
    logging.info("Iniciando GÃ©nesis 1.0 â€” Persistencia: Telegram Cloud + SQLite local + Base64 logs")
    t = threading.Thread(target=background_loop_proactivo, daemon=True)
    t.start()
    
    # 1. FORZAR CIERRE DE CONEXIÃ“N: Elimina conflicto getUpdates
    print("DEBUG BOOT: Limpiando webhook para evitar conflictos getUpdates...")
    try:
        import time
        bot.delete_webhook(drop_pending_updates=True)
        time.sleep(1)
    except Exception as e:
        print(f"DEBUG BOOT: Webhook clear error (ignorado): {e}")

    # Polling con auto-reconexion
    print("DEBUG BOOT: Iniciando Telegram polling...")
    print(">>> SISTEMA GENESIS ACTIVO <<<")
    while True:
        try:
            print("GENESIS ESTA VIVO Y ESCUCHANDO...")
            bot.infinity_polling(timeout=10, long_polling_timeout=5)
        except Exception as e:
            print(f"X TELEGRAM POLLING CAIDO: {e}")
            print("DEBUG: Reconectando en 5 segundos...")
            import time
            time.sleep(5)
            print("DEBUG: Reintentando polling...")

if __name__ == "__main__":
    main()
