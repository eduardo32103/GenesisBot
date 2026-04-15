import os, pg8000.dbapi, telebot
import ssl
import urllib.parse
import logging
import base64
import requests
import re
import xml.etree.ElementTree as ET
import pandas as pd
import yfinance as yf
import threading
import time
import json
from collections import deque
from telebot.types import ReplyKeyboardMarkup, KeyboardButton
from datetime import datetime, timedelta

# Configuración extendida de logs para Railway
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY') # Volvemos a requerir OpenAI para visión
# Canal privado donde el bot fija el backup (puede ser el mismo CHAT_ID o un canal dedicado)
BACKUP_CHAT_ID = os.environ.get('BACKUP_CHAT_ID', CHAT_ID)
FMP_API_KEY = os.environ.get('FMP_API_KEY')

if not TELEGRAM_TOKEN or not CHAT_ID:
    logging.critical("Falta TELEGRAM_TOKEN o CHAT_ID. Saliendo.")
    exit()

if not os.environ.get('FMP_API_KEY'):
    logging.warning("⚠️ FMP_API_KEY no configurada. El motor de precios FMP no funcionará.")
else:
    logging.info(f"✅ FMP_API_KEY cargada correctamente ({len(os.environ.get('FMP_API_KEY'))} caracteres).")

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
    try:
        if _global_db_conn is not None:
             try:
                 c = _global_db_conn.cursor()
                 c.execute("SELECT 1")
                 c.fetchone()
                 return _global_db_conn
             except:
                 _global_db_conn = None
        
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        
        url = os.environ.get('DATABASE_URL')
        if not url: return None
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
        print("✅ Conexión exitosa a Supabase desde backend.")
        
        # Iniciar tabla
        cr = _global_db_conn.cursor()
        cr.execute("CREATE TABLE IF NOT EXISTS wallet (user_id BIGINT, ticker TEXT, is_investment INTEGER DEFAULT 0, amount_usd REAL DEFAULT 0.0, entry_price REAL DEFAULT 0.0, timestamp TEXT, PRIMARY KEY (user_id, ticker))")
        cr.execute("CREATE TABLE IF NOT EXISTS global_stats (key TEXT PRIMARY KEY, value REAL)")
        cr.execute("CREATE TABLE IF NOT EXISTS seen_events (hash_id TEXT PRIMARY KEY, timestamp TEXT)")
        _global_db_conn.commit()
        
        return _global_db_conn
    except Exception as e:
        print(f"❌ Error de conexión silencioso: {e}")
        _global_db_conn = None
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
        print(f"❌ Error: No se pudo conectar a Supabase -> {e}")
        logging.error(f"Error init_db: {e}")

init_db()

# =====================================================================
# PERSISTENCIA REAL: TELEGRAM COMO BASE DE DATOS
# El bot guarda el estado completo de la cartera como un mensaje
# en Telegram. Railway no puede borrar mensajes de Telegram.
# =====================================================================
BACKUP_PREFIX = "🔐GENESIS_BACKUP_V2🔐"
_last_backup_msg_id = None  # Cache del message_id del último backup

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

        # Guardar en múltiples ubicaciones para máxima persistencia
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
    """Carga el message_id del último backup desde un archivo local"""
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
            logging.info("BACKUP: Portfolio vacío, no se guarda backup.")
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
                logging.info(f"✅ Backup actualizado (msg_id: {_last_backup_msg_id})")
                return
            except Exception as e:
                logging.debug(f"No se pudo editar backup msg {_last_backup_msg_id}: {e}")
                _last_backup_msg_id = None

        # ESTRATEGIA 2: Crear mensaje nuevo y fijarlo
        msg = bot.send_message(BACKUP_CHAT_ID, backup_text, disable_notification=True)
        _last_backup_msg_id = msg.message_id
        _save_backup_msg_id(msg.message_id)
        logging.info(f"✅ Backup nuevo enviado (msg_id: {_last_backup_msg_id})")

        # Fijar el mensaje para que SIEMPRE sea recuperable
        try:
            bot.pin_chat_message(BACKUP_CHAT_ID, msg.message_id, disable_notification=True)
            logging.info("📌 Backup fijado en el chat.")
        except Exception as e:
            logging.debug(f"No se pudo fijar backup: {e}")

        # Si va al chat principal, borrar SOLO después de fijarlo
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

    logging.info("🔄 DB local vacía o sin inversiones. Buscando backup...")

    # === FUENTE 1: Mensaje fijado (MÁS CONFIABLE — sobrevive reinicios) ===
    try:
        chat_info = bot.get_chat(BACKUP_CHAT_ID)
        pinned = chat_info.pinned_message
        if pinned and pinned.text and pinned.text.startswith(BACKUP_PREFIX):
            b64_data = pinned.text.replace(BACKUP_PREFIX, "").strip()
            if b64_data:
                _restore_from_b64(b64_data)
                _last_backup_msg_id = pinned.message_id
                _save_backup_msg_id(pinned.message_id)
                logging.info(f"✅ RESTAURACIÓN desde mensaje FIJADO (msg_id: {pinned.message_id})")
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
                    logging.info(f"✅ RESTAURACIÓN desde updates (msg_id: {msg.message_id})")
                    return True
    except Exception as e:
        logging.debug(f"Updates check failed: {e}")

    # === FUENTE 4: portfolio.json del repositorio/disco ===
    # Si todo falla, no borrar la DB actual, simplemente decir que no hay backup remoto
    logging.warning("⚠️ No se encontró NINGÚN respaldo en cloud. Cartera usará solo SQLite local.")
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
    """Último recurso: lee portfolio.json que está en el repositorio Git"""
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

                logging.info(f"✅ Restauración desde portfolio.json ({json_path}) exitosa: {len(legacy)} activos.")
                return True
            except Exception as e:
                logging.error(f"Error leyendo {json_path}: {e}")

    logging.warning("⚠️ No se encontró ningún respaldo. Cartera inicia vacía.")
    return False


# --- MAPEO DURO Y ALIAS VISUAL  ---
def remap_ticker(ticker_input):
    tk = ticker_input.upper()
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
        "BZ=F": "LCO (Petróleo Brent)",
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
    if not conn: return []
    try:
        c = conn.cursor()
        c.execute('SELECT ticker FROM wallet WHERE user_id = %s', (int(CHAT_ID),))
        return [row[0] for row in c.fetchall()]
    finally:
        pass # conn.close() delegado a pooling global

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
            save_state_to_telegram()  # ← PERSISTENCIA EN TELEGRAM
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
            save_state_to_telegram()  # ← PERSISTENCIA EN TELEGRAM
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
    save_state_to_telegram()  # ← PERSISTENCIA EN TELEGRAM

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
    save_state_to_telegram()  # ← PERSISTENCIA EN TELEGRAM

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
    save_state_to_telegram()  # ← PERSISTENCIA EN TELEGRAM

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
    logging.info("🔄 PnL mensual reseteado a $0.00")

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
    logging.info("⚠️ RESET TOTAL ejecutado: inversiones y PnL eliminados")


WHALE_MEMORY = deque(maxlen=5)
SMC_LEVELS_MEMORY = {}
LAST_KNOWN_PRICES = {}  # Cache de último precio válido por ticker
LAST_KNOWN_ANALYSIS = {}  # Cache de último análisis SMC completo por ticker

# Tickers de respaldo para Brent Crude
BRENT_FALLBACK_CHAIN = ["BZ=F", "CO=F", "BNO"]
BRENT_MIN_VALID_PRICE = 50.0  # Si el precio es menor a esto, es un ERROR de Yahoo

# ----------------- NÚCLEO DE MERCADO E INTELIGENCIA -----------------
def fmt_price(val):
    """Formatea precio con decimales REALES del exchange, sin ceros de relleno"""
    s = f"{val:.6f}".rstrip('0')
    parts = s.split('.')
    if len(parts) == 2 and len(parts[1]) < 2:
        s = f"{val:.2f}"
    elif s.endswith('.'):
        s = f"{val:.2f}"
    return s

# === MOTOR FMP (Financial Modeling Prep) — FUENTE ÚNICA DE PRECIOS ===
# Endpoint: https://financialmodelingprep.com/stable/quote?symbol={SYMBOL}&apikey={KEY}

# Mapeo de tickers internos -> símbolos FMP
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
    """Convierte ticker interno al símbolo FMP correcto"""
    if tk in FMP_SYMBOL_MAP:
        return FMP_SYMBOL_MAP[tk]
    # Crypto auto-map: BTC-USD -> BTCUSD
    if tk.endswith('-USD'):
        return tk.replace('-USD', 'USD')
    # Acciones y ETFs van directo: NVDA, IXC, BNO
    return tk

_FMP_LAST_ERROR = {}  # Cache global para diagnóstico del último error FMP

def _fetch_fmp_quote(tk):
    """Consulta precio en vivo EXCLUSIVAMENTE desde FMP - /stable/quote"""
    global _FMP_LAST_ERROR

    if not FMP_API_KEY:
        logging.error("ERROR: La variable FMP_API_KEY no existe en el sistema")
        _FMP_LAST_ERROR[tk] = "FMP_API_KEY no detectada."
        return None

    fmp_symbol = _get_fmp_symbol(tk)
    symbols_to_try = [fmp_symbol]

    # Soporte Crypto exhaustivo: BTC -> BTCUSD, BTC, BTC-USD
    if _is_crypto_ticker(tk):
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
                if not data:
                    print(f"DEBUG FMP: respuesta vacía para {symbol}")
                    continue
                # /stable/quote puede devolver lista o dict
                if isinstance(data, list) and len(data) > 0:
                    quote = data[0]
                elif isinstance(data, dict) and data.get('price'):
                    quote = data
                else:
                    print(f"DEBUG FMP: formato inesperado para {symbol}: {str(data)[:200]}")
                    continue

                if isinstance(quote, dict):
                    price = float(quote.get('price', 0) or 0)
                    volume = float(quote.get('volume', 0) or 0)
                    if price > 0:
                        logging.info(f"FMP OK {tk} ({symbol}): ${fmt_price(price)}")
                        return {'price': price, 'vol': volume}
                    else:
                        print(f"DEBUG FMP: precio=0 para {symbol}. Datos: {quote}")

            elif resp.status_code in (401, 403):
                _FMP_LAST_ERROR[tk] = f"{resp.status_code} - Key rechazada o plan insuficiente"
                logging.error(f"FMP: {resp.status_code} para {symbol}. Verifica FMP_API_KEY en Railway.")
                return None

        except Exception as e:
            logging.error(f"FMP error fetching {symbol}: {e}")
            print(f"DEBUG FMP Excepción: {e}")

    _FMP_LAST_ERROR[tk] = "Activo no encontrado en FMP"
    logging.warning(f"FMP falló para {tk}. Activo no localizado.")
    return None
def _sanity_check_price(tk, new_price):
    """Verifica que el precio no sea basura (desviación >50% vs último conocido)"""
    if tk in LAST_KNOWN_PRICES:
        last_price = LAST_KNOWN_PRICES[tk]['price']
        if last_price > 0:
            change_pct = abs(new_price - last_price) / last_price
            if change_pct > 0.50:
                logging.warning(f"⚠️ SANITY CHECK FALLIDO para {tk}: ${new_price:.2f} vs último ${last_price:.2f} ({change_pct*100:.1f}%)")
                return False
    if not _is_crypto_ticker(tk) and new_price < 0.50:
        logging.warning(f"⚠️ SANITY CHECK: {tk} precio ${new_price:.4f} demasiado bajo.")
        return False
    if new_price <= 0:
        return False
    return True

def get_safe_ticker_price(ticker, force_validation=False):
    """MOTOR MAESTRO: FMP como fuente ÚNICA de precios en vivo"""
    tk = remap_ticker(ticker)

    # FMP es la fuente única para TODOS los activos
    result = _fetch_fmp_quote(tk)
    if result and _sanity_check_price(tk, result['price']):
        LAST_KNOWN_PRICES[tk] = result
        return result

    # Cache como único respaldo si FMP no responde
    if tk in LAST_KNOWN_PRICES:
        logging.warning(f"{tk}: FMP no respondió. Usando cache: ${fmt_price(LAST_KNOWN_PRICES[tk]['price'])}")
        return LAST_KNOWN_PRICES[tk]

    logging.error(f"{tk}: FMP falló y no hay cache disponible.")
    return None

def verify_1m_realtime_data(ticker):
    return get_safe_ticker_price(ticker)


def _fetch_fmp_news(limit=10):
    """Extrae noticias del mercado via FMP — usa SOLO endpoints /stable/"""
    if not FMP_API_KEY:
        return []

    all_news = []

    # === INTENTO 1: /stable/news/all (noticias generales) ===
    try:
        url = f"https://financialmodelingprep.com/stable/news/all?limit={limit}&apikey={FMP_API_KEY}"
        resp = requests.get(url, timeout=8)
        logging.info(f"LOG FMP [stable/news/all]: Status {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                return data
    except Exception as e:
        logging.debug(f"FMP stable/news/all error: {e}")

    # === INTENTO 2: /stable/news (general) ===
    try:
        url = f"https://financialmodelingprep.com/stable/news?limit={limit}&apikey={FMP_API_KEY}"
        resp = requests.get(url, timeout=8)
        logging.info(f"LOG FMP [stable/news]: Status {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                return data
    except Exception as e:
        logging.debug(f"FMP stable/news error: {e}")

    # === INTENTO 3: /stable/news por ticker específico ===
    default_tickers = ["AAPL", "NVDA", "BTCUSD", "SPY", "MSFT"]
    for ticker in default_tickers:
        try:
            url = f"https://financialmodelingprep.com/stable/news?symbol={ticker}&limit=3&apikey={FMP_API_KEY}"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    all_news.extend(data)
                    if len(all_news) >= limit:
                        break
        except Exception:
            pass

    if all_news:
        logging.info(f"FMP noticias por ticker: {len(all_news)} artículos recopilados.")
        return all_news[:limit]

    # === INTENTO 4: /stable/stock-news por ticker ===
    for ticker in default_tickers[:3]:
        try:
            url = f"https://financialmodelingprep.com/stable/stock-news?symbol={ticker}&limit=3&apikey={FMP_API_KEY}"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    all_news.extend(data)
        except Exception:
            pass

    if all_news:
        return all_news[:limit]

    logging.warning("FMP: Todos los endpoints de noticias fallaron.")
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
    """Monitor automático unificado: FMP news + sentiment + wallet cross-reference"""
    try:
        HIGH_IMPACT_KEYWORDS = ["war", "attack", "strike", "escalation", "missile", "sanction",
                                "embargo", "explosion", "guerra", "ataque", "tensión", "misil",
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
        prompt = f"Titulares globales:\n{news_text}\nHaz un resumen y dime qué movería el mercado hoy. RESPONDE ESTRICTAMENTE EN ESPAÑOL."
    else:
        prompt = (f"Titulares recientes:\n{news_text}\nAnaliza si hay algo de nivel 'Alto Impacto' (>2%). Si no lo hay, responde 'TRANQUILIDAD'.\nSi lo hay: '⚠️ ALERTA URGENTE: [Resumen] - Impacto en [Acción/Sector]'\nRESPONDE ESTRICTA Y ÚNICAMENTE EN ESPAÑOL.")
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
# MOTOR DE INTELIGENCIA UNIFICADO GÉNESIS
# Integra: FMP Sentiment + Wallet Cross-Reference + Whale Radar
# =====================================================================

# Cache global de contexto de riesgo geopolítico (para cruce con ballenas)
GENESIS_RISK_CONTEXT = {
    'sentiment_global': 0.0,       # Sentimiento promedio del mercado (-1 a 1)
    'high_risk_tickers': [],        # Tickers con sentimiento muy negativo
    'last_update': None,            # Timestamp de última actualización
    'news_digest': [],              # Últimas noticias procesadas con sentimiento
}


def _classify_sentiment(score):
    """Traduce score de FMP a semáforo de riesgo con porcentajes"""
    try:
        s = float(score)
    except (TypeError, ValueError):
        return {'label': 'Neutral', 'icon': '🟡', 'bull_pct': 50, 'bear_pct': 50, 'raw': 0.0}

    if s > 0.3:
        bull = min(95, int(50 + s * 50))
        return {'label': 'Alcista', 'icon': '🟢', 'bull_pct': bull, 'bear_pct': 100 - bull, 'raw': s}
    elif s < -0.3:
        bear = min(95, int(50 + abs(s) * 50))
        return {'label': 'Bajista', 'icon': '🔴', 'bull_pct': 100 - bear, 'bear_pct': bear, 'raw': s}
    else:
        return {'label': 'Neutral', 'icon': '🟡', 'bull_pct': 50, 'bear_pct': 50, 'raw': s}


def _extract_mentioned_tickers(text, wallet_tickers):
    """Detecta qué tickers de la wallet se mencionan en un texto"""
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
        if 'BZ' in clean_tk: aliases.extend(['BRENT', 'OIL', 'PETRÓLEO', 'PETROLEO'])
        if 'XRP' in clean_tk: aliases.extend(['RIPPLE'])

        for alias in aliases:
            if alias in text_upper:
                if tk not in mentioned:
                    mentioned.append(tk)
                break
    return mentioned


def _fetch_fmp_news_with_sentiment(limit=12):
    """Fetch FMP news y extrae sentiment score de cada artículo"""
    raw_news = _fetch_fmp_news(limit)
    processed = []

    for article in raw_news:
        title = article.get('title', '') or ''
        if not title:
            continue

        # FMP puede incluir 'sentiment' directamente en el payload
        sentiment_raw = article.get('sentiment', None)
        if sentiment_raw is None:
            # Inferencia básica por keywords si FMP no da sentiment
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
    """Inferencia rápida de sentimiento por keywords cuando FMP no lo provee"""
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
        return -0.3 - (bear_hits * 0.15)  # Más keywords = más negativo
    elif bull_hits > bear_hits:
        return 0.3 + (bull_hits * 0.15)
    return 0.0


def _get_whale_context_for_ticker(ticker):
    """Busca el último movimiento de ballena relevante para un ticker"""
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


def genesis_strategic_report(manual=True):
    """REPORTE ESTRATÉGICO UNIFICADO GÉNESIS
    Integra: FMP Sentiment + Wallet Cross-Reference + Whale Data + IA"""
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
        return "☕ Sin eventos de riesgo detectados en este momento. Vigilancia activa."

    # === PASO 2: Cross-reference con wallet ===
    wallet_alerts = []    # Noticias que tocan activos de Eduardo
    general_news = []     # Noticias generales del mercado

    for news in news_data:
        mentioned = _extract_mentioned_tickers(news['title'], wallet_tickers)
        if news['symbol']:
            # También buscar por symbol explícito de FMP
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
    lines.append("🌍 <b>REPORTE ESTRATÉGICO GÉNESIS</b> 🌍")
    lines.append("\u2500" * 28)

    # --- Alertas de wallet primero (máximo 4) ---
    if wallet_alerts:
        lines.append("")
        lines.append("🚨 <b>ALERTAS EN TU CARTERA:</b>")
        lines.append("")
        for news in wallet_alerts[:4]:
            s = news['sentiment']
            affected = ", ".join([get_display_name(tk) for tk in news['affected_tickers']])
            whale_note = ""
            for tk in news['affected_tickers']:
                wctx = _get_whale_context_for_ticker(tk)
                if wctx:
                    whale_note = f"\n🐋 <b>Ballena:</b> {wctx['vol_str']} ({wctx['type']}) hace {wctx['minutes_ago']}min"
                    break

            lines.append(f"📰 <b>Noticia:</b> {news['title'][:120]}")
            lines.append(f"🎯 <b>Activos afectados:</b> {affected}")
            lines.append(f"{s['icon']} <b>Riesgo:</b> {s['bull_pct']}% Alcista / {s['bear_pct']}% Bajista ({s['label']})")
            if whale_note:
                lines.append(whale_note)
            lines.append(f"💡 <b>Análisis:</b> Basado en el sentimiento FMP, la probabilidad de impacto en tu cartera es <b>{s['bear_pct']}%</b>.")
            lines.append("")
    else:
        lines.append("")
        lines.append("✅ <b>Sin alertas directas para tu cartera.</b>")
        lines.append("")

    # --- Panorama general (máximo 3 noticias) ---
    lines.append("\u2500" * 28)
    lines.append("📊 <b>PANORAMA MACRO:</b>")
    lines.append("")
    top_general = sorted(general_news, key=lambda x: abs(x['sentiment']['raw']), reverse=True)[:3]
    for news in top_general:
        s = news['sentiment']
        src = f" ({news['source']})" if news['source'] else ""
        lines.append(f"{s['icon']} {news['title'][:100]}{src}")
    if not top_general:
        lines.append("☕ Sin noticias macro relevantes.")

    # --- Datos de Ballenas integrados ---
    lines.append("")
    lines.append("\u2500" * 28)
    lines.append("🐳 <b>RADAR BALLENAS INTEGRADO:</b>")
    lines.append("")
    if WHALE_MEMORY:
        for w in list(WHALE_MEMORY)[::-1][:3]:
            is_crypto = '-USD' in w['ticker']
            vol_str = f"${w['vol_approx']:,} USD" if is_crypto else f"{w['vol_approx']:,} unidades"
            minutes_ago = int((datetime.now() - w['timestamp']).total_seconds() / 60)
            # Cruzar con riesgo geopolítico
            risk_tag = ""
            if w['ticker'] in GENESIS_RISK_CONTEXT.get('high_risk_tickers', []):
                risk_tag = " ⚠️ <b>[ZONA DE RIESGO]</b>"
            lines.append(f"🐋 <b>{get_display_name(w['ticker'])}</b> | {vol_str} | {w['type']} | {minutes_ago}min{risk_tag}")
    else:
        lines.append("🌊 Océano tranquilo. Sin anomalías.")

    # --- Sentimiento resumen ---
    lines.append("")
    lines.append("\u2500" * 28)
    lines.append(f"🎯 <b>SENTIMIENTO GLOBAL:</b> {global_risk['icon']} {global_risk['label']} — {global_risk['bull_pct']}% Alcista / {global_risk['bear_pct']}% Bajista")
    lines.append("\u2500" * 28)

    # === PASO 5: IA avanzada (si OpenAI está disponible) ===
    if manual and OPENAI_API_KEY and (wallet_alerts or top_general):
        try:
            all_titles = [n['title'] for n in (wallet_alerts + top_general)[:6]]
            wallet_str = ", ".join([get_display_name(tk) for tk in wallet_tickers])
            sentiments_str = ", ".join([f"{n['title'][:40]}... ({n['sentiment']['label']})" for n in news_data[:5]])

            from openai import OpenAI
            client = OpenAI(api_key=OPENAI_API_KEY)
            prompt = (
                f"Eres GÉNESIS, un sistema de inteligencia estratégica de mercados.\n\n"
                f"NOTICIAS CON SENTIMIENTO:\n{sentiments_str}\n\n"
                f"WALLET DE EDUARDO: {wallet_str}\n\n"
                f"SENTIMIENTO GLOBAL: {global_risk['label']} ({avg_sentiment:.2f})\n\n"
                f"INSTRUCCIÓN:\n"
                f"1. En 3-4 líneas, explica cómo estas noticias afectan DIRECTAMENTE a la wallet de Eduardo.\n"
                f"2. Da UNA recomendación estratégica clara (Hold/Vigilar/Reducir/Aprovechar).\n"
                f"3. RESPONDE EN ESPAÑOL.\n"
            )
            res = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=500
            ).choices[0].message.content.strip()

            lines.append("")
            lines.append("🧠 <b>ANÁLISIS IA GÉNESIS:</b>")
            lines.append(res)
        except Exception as e:
            logging.error(f"OpenAI strategic error: {e}")

    return "\n".join(lines)


def generar_reporte_macro_manual():
    """Wrapper para el botón Geopolítica — usa el motor unificado"""
    return genesis_strategic_report(manual=True)


def fetch_intraday_data(ticker):
    """Obtiene datos intradía usando EXCLUSIVAMENTE FMP."""
    tk = remap_ticker(ticker)
    
    print(f"DEBUG: Buscando 'FMP_API_KEY'... ¿Encontrada?: {os.environ.get('FMP_API_KEY') is not None}")
    api_key = os.environ.get('FMP_API_KEY')
    if not api_key:
        logging.error("ERROR: La variable FMP_API_KEY no existe en el sistema")
        return None
        
    try:
        safe_check = get_safe_ticker_price(tk)
        if not safe_check:
            logging.warning(f"fetch_intraday_data: FMP no devolvió precio para {tk}")
            return None

        price = safe_check.get('price')
        if not price:
            logging.warning(f"Respuesta inesperada de FMP: {safe_check}")
            return None

        fmp_sym = _get_fmp_symbol(tk)
        if _is_crypto_ticker(tk):
            fmp_sym = tk.replace('-USD', '') + 'USD'

        url = f"https://financialmodelingprep.com/stable/historical-chart/5min/{fmp_sym}?apikey={api_key}"
        resp = requests.get(url, timeout=10)

        print(f"DEBUG FMP HTTP Status: {resp.status_code} para historical {fmp_sym}")
        if resp.status_code != 200 or not resp.text:
            print(f"DEBUG FMP Respuesta Cruda: {resp.text[:300]}")
            logging.warning(f"FMP historical-chart: {resp.status_code} para {fmp_sym}")
            return {'ticker': tk, 'latest_vol': 0, 'avg_vol': 0, 'vol_type': 'N/A', 'latest_price': price}

        data = resp.json()
        if not data or not isinstance(data, list) or len(data) == 0:
            logging.warning(f"Respuesta inesperada de FMP: {str(data)[:200]}")
            return {'ticker': tk, 'latest_vol': 0, 'avg_vol': 0, 'vol_type': 'N/A', 'latest_price': price}

        volumes = [float(c.get('volume', 0) or 0) for c in data if isinstance(c, dict)]
        latest = data[0] if isinstance(data[0], dict) else {}
        close_p = float(latest.get('close', price) or price)
        open_p  = float(latest.get('open', price) or price)
        latest_vol = float(latest.get('volume', 0) or 0)
        avg_vol = (sum(volumes) / len(volumes)) if volumes else 0

        vol_type = "Compra 🟢" if close_p >= open_p else "Venta 🔴"
        return {
            'ticker': tk,
            'latest_vol': latest_vol,
            'avg_vol': avg_vol,
            'vol_type': vol_type,
            'latest_price': price
        }
    except Exception as e:
        logging.error(f"fetch_intraday_data error para {tk}: {e}")
        return None
def fetch_and_analyze_stock(ticker):
    tk = remap_ticker(ticker)
    try:
        safe_check = get_safe_ticker_price(tk)
        if not safe_check: return None

        # Usar Ticker.history() para evitar bugs de MultiIndex con yf.download()
        ticker_obj = yf.Ticker(tk)
        data = ticker_obj.history(period="6mo", interval="1d")
        if data.empty: return None

        close_prices = data['Close']; volume = data['Volume']

        delta = close_prices.diff()
        up = delta.clip(lower=0); down = -1 * delta.clip(upper=0)
        ema_up = up.ewm(com=13, adjust=False).mean(); ema_down = down.ewm(com=13, adjust=False).mean()
        rs = ema_up / ema_down
        rsi_series = 100 - (100 / (1 + rs)); rsi_series[ema_down == 0] = 100

        macd_line = close_prices.ewm(span=12, adjust=False).mean() - close_prices.ewm(span=26, adjust=False).mean()
        macd_signal = macd_line.ewm(span=9, adjust=False).mean()
        latest_price = safe_check['price']
        latest_rsi = float(rsi_series.iloc[-1])

        smc_trend = "Alcista 🟢" if latest_price > close_prices.ewm(span=20).mean().iloc[-1] else "Bajista 🔴"
        recent_month = close_prices.iloc[-22:]
        smc_sup = float(recent_month.min()); smc_res = float(recent_month.max())
        vol_month = volume.iloc[-22:]; order_block_price = float(close_prices.loc[vol_month.idxmax()])

        result = {'ticker': tk, 'price': latest_price, 'rsi': latest_rsi, 'macd_line': float(macd_line.iloc[-1]), 'macd_signal': float(macd_signal.iloc[-1]), 'smc_sup': smc_sup, 'smc_res': smc_res, 'smc_trend': smc_trend, 'order_block': order_block_price}
        LAST_KNOWN_ANALYSIS[tk] = result  # Cachear último análisis válido
        return result
    except: return None

def update_smc_memory(ticker, analysis):
    tk = remap_ticker(ticker)
    SMC_LEVELS_MEMORY[tk] = {'sup': analysis['smc_sup'], 'res': analysis['smc_res'], 'update_date': datetime.now()}

def analyze_breakout_gpt(ticker, level_type, price):
    tk = remap_ticker(ticker)
    display_name = get_display_name(tk)
    if not OPENAI_API_KEY: return "¿Qué hacer? Mantener cautela."
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)
    prompt = (f"Eres GÉNESIS, analista institucional.\n"
              f"El activo {display_name} acaba de romper su nivel de {level_type} (Smart Money Concept) en exactamente ${fmt_price(price)} verificado vía FMP.\n"
              f"Instrucción: Evalúa esta ruptura intradiaria con perspectiva de liquidez institucional.\n"
              f"Da un consejo corto de 1 párrafo: ¿Qué hacer ahora? (Elige y resalta COMPRAR, VENDER o MANTENER) y explica mecánicamente por qué. ESPAÑOL ESTRICTO.")
    try:
        return client.chat.completions.create(model="gpt-4o", messages=[{"role": "user", "content": prompt}]).choices[0].message.content.strip()
    except Exception as e:
        logging.error(f"Fallo OpenAI breakout: {e}")
        return "¿Qué hacer? Esperar confirmación de volumen en la siguiente hora."

def perform_deep_analysis(ticker):
    tk = remap_ticker(ticker)
    display_name = get_display_name(tk)

    # PASO 0: Obtener precio VERIFICADO de FMP ANTES de todo
    # Este precio es SAGRADO — viene directo del exchange via FMP
    verified_price = None
    fmp_data = _fetch_fmp_quote(tk)
    if fmp_data:
        verified_price = fmp_data['price']
        logging.info(f"ANÁLISIS {tk}: precio FMP verificado = ${fmt_price(verified_price)}")

    # PASO 1: Obtener indicadores técnicos (RSI, MACD, SMC) via yfinance history
    tech = fetch_and_analyze_stock(tk)

    # SIEMPRE imponer el precio FMP sobre lo que yfinance diga en los indicadores
    if tech and verified_price:
        tech['price'] = verified_price

    # Si no hay tech pero sí tenemos precio verificado
    if not tech and not verified_price:
        live = get_safe_ticker_price(tk)
        if live:
            verified_price = live['price']

    # Precio final a inyectar en el prompt (INNEGOCIABLE)
    final_price = verified_price or (tech['price'] if tech else None)

    # === HARD-STOP: SIN PRECIO VERIFICADO = SIN ANÁLISIS ===
    if not final_price:
        fmp_sym = _get_fmp_symbol(tk)
        diag = _FMP_LAST_ERROR.get(tk, 'Sin información de error')
        _key_len = len(FMP_API_KEY) if FMP_API_KEY else 0
        return (f"⚠️ <b>Error de conexión con FMP</b>\n\n"
                f"No se pudo obtener el precio de {display_name} "
                f"(símbolo: {fmp_sym}).\n\n"
                f"🔍 <b>Diagnóstico:</b>\n<code>{diag}</code>\n\n"
                f"🔑 Key cargada: {'Sí' if FMP_API_KEY else 'NO'} "
                f"({_key_len} chars)\n"
                f"🛑 Análisis BLOQUEADO para evitar datos inventados.")

    if tech:
        tech_block = (
            f"--- DATOS EN VIVO (calculados por el sistema, NO los inventes) ---\n"
            f"• Precio EXACTO en vivo: ${fmt_price(final_price)}\n"
            f"• RSI (14 períodos): {tech['rsi']:.2f}\n"
            f"• MACD Línea: {tech['macd_line']:.4f}\n"
            f"• MACD Señal: {tech['macd_signal']:.4f}\n"
            f"• Tendencia SMC: {tech['smc_trend']}\n"
            f"• Buy-side Liquidity (Soporte SMC): ${fmt_price(tech['smc_sup'])}\n"
            f"• Sell-side Liquidity (Resistencia SMC): ${fmt_price(tech['smc_res'])}\n"
            f"• Order Block Institucional: ${fmt_price(tech['order_block'])}\n"
            f"--- FIN DE DATOS EN VIVO ---"
        )
    elif final_price:
        tech_block = (
            f"--- DATOS EN VIVO ---\n"
            f"• Precio EXACTO en vivo: ${fmt_price(final_price)}\n"
            f"• Indicadores técnicos: No disponibles (mercado cerrado o sin historial)\n"
            f"--- FIN DE DATOS EN VIVO ---"
        )
    else:
        tech_block = "--- DATOS EN VIVO: No disponibles en este momento ---"

    # PASO 2: Noticias recientes del activo
    news_str = "No hay noticias recientes disponibles."
    try:
        raw_news = yf.Ticker(tk).news or []
        if raw_news:
            news_str = "\n".join([f"- {n.get('title', '')}" for n in raw_news[:5]])
    except: pass

    # PASO 3: Prompt blindado anti-alucinación hiper-detallado para GPT-4o
    price_str = f"${fmt_price(final_price)}" if final_price else "N/A"
    prompt = (
        f"Actúa como GÉNESIS, un analista financiero institucional senior (modelo GPT-4o).\n\n"
        f"ACTIVO: {display_name} ({tk})\n\n"
        f"{tech_block}\n\n"
        f"NOTICIAS RECIENTES:\n{news_str}\n\n"
        f"REGLAS INQUEBRANTABLES:\n"
        f"1. El precio REAL Y VERIFICADO de {display_name} en este momento es {price_str} (proveedor: FMP). Tienes PROHIBIDO inventar, adivinar o usar otro precio.\n"
        f"2. Basa tu análisis EXCLUSIVAMENTE en los datos numéricos proporcionados arriba.\n"
        f"3. Realiza una fusión de perspectivas: cruza los niveles mecánicos de 'Smart Money Concepts' (Bloques de órdenes y vacíos de liquidez) con el indicador de Tendencia SMC.\n"
        f"4. Evalúa exhaustivamente si el precio actual sugiere que los algoritmos institucionales están acumulando en zona de demanda o distribuyendo en zona de oferta.\n"
        f"5. Combina el pulso macro de las noticias y detalla de qué manera afectan los niveles técnicos.\n\n"
        f"FORMATO DE RESPUESTA EN GITHUB MARKDOWN:\n"
        f"📊 **Análisis Smart Money (SMC):** [Profundiza sobre liquidez, imbalances y el order block actual]\n"
        f"📰 **Contexto Macro / Institucional:** [Tu lectura de cómo el flujo de impacto altera la técnica]\n"
        f"🎯 **VEREDICTO FINAL:** [COMPRAR / VENDER / MANTENER] + [Justificación institucional en 2 líneas]\n\n"
        f"RESPONDE ESTRICTAMENTE EN ESPAÑOL."
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
        return ("---\n💎 *ESTADO GLOBAL DE TU WALLET* 💎\n---\n"
                "💹 <b>Capital Operativo Activo:</b> $0.00\n"
                "💰 <b>Ganancia Mensual Acumulada:</b> $0.00 USD\n"
                "📈 <b>Rendimiento M/M:</b> [0.00%]\n"
                "📊 <b>Estatus:</b> [⚪ SIN OPERACIONES]\n"
                "🎯 <b>Meta del Mes (10%):</b> [░░░░░░░░░░] 0%\n---")

    if not investments and realized_pnl != 0:
        return ("---\n💎 *ESTADO GLOBAL DE TU WALLET* 💎\n---\n"
                "💹 <b>Capital Operativo Activo:</b> $0.00\n"
                f"💵 <b>Ganancia Mensual (Acumulado Ventas):</b> {'+' if realized_pnl>=0 else ''}${realized_pnl:,.2f} USD\n"
                "📈 <b>Rendimiento M/M:</b> [0.00%]\n"
                "📊 <b>Estatus:</b> [⚪ SIN POSICIONES ABIERTAS]\n"
                "🎯 <b>Meta del Mes (10%):</b> [░░░░░░░░░░] 0%\n---")

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
            details.append(f"• {display_name}: {sign}{roi_percent*100:.2f}% (${fmt_price(live_price)})")
        else:
            # Mercado cerrado - mostrar activo sin ocultar
            total_invested += init_amount
            total_current += init_amount
            details.append(f"• {display_name}: ⏳ Mercado cerrado (entrada: ${fmt_price(entry_p)})")



    total_roi = (total_current - total_invested) / total_invested if total_invested > 0 else 0
    sign_roi = "+" if total_roi >= 0 else ""
    status_icon = "🟢 EN GANANCIAS" if total_roi >= 0 else "🔴 EN PÉRDIDAS"

    goal = 0.10
    progress_ratio = max(0, min(1, total_roi / goal))

    filled_blocks = int(progress_ratio * 10)
    empty_blocks = 10 - filled_blocks
    bar = "▓" * filled_blocks + "░" * empty_blocks
    progress_text = f"{int(progress_ratio*100)}% completado"

    report = []
    report.append("---")
    report.append("💎 <b>ESTADO GLOBAL DE TU WALLET</b> 💎")
    report.append("---")
    report.append(f"💹 <b>Rendimiento M/M (Activo):</b> [{sign_roi}{total_roi*100:.2f}%]")
    report.append(f"📊 <b>Estatus:</b> [{status_icon}]")
    report.append(f"🎯 <b>Meta del Mes (10%):</b> [{bar}] {progress_text}")
    if realized_pnl != 0:
        report.append(f"💵 <b>Acumulado en Ventas (Mes):</b> {'+' if realized_pnl>=0 else ''}${realized_pnl:,.2f} USD")
    report.append("---")
    if details:
        report.append("<i>(Detalle por activo)</i>")
        report.extend(details)
    return "\n".join(report)

# ----------------- CONTROLADORES TELEBOT (NLP & ACCIONES DIRECTAS) -----------------

@bot.message_handler(commands=['check_db'])
def test_db(message):
    print("⏳ Intentando conectar con Supabase (Timeout de 5 segundos)...")
    try:
        conn = get_db_connection()
        if not conn:
             print("❌ ERROR DE RED O AUTENTICACIÓN: conn es None")
             return
        c = conn.cursor()
        c.execute('SELECT version();')
        v = c.fetchone()[0]
        print(f"✅ CONEXIÓN ESTABLECIDA\nPostgreSQL OK. Base de Datos en línea y funcional.\n\nDetalle: {v}")
    except Exception as e:
        print(f"❌ ERROR DE RED O AUTENTICACIÓN\nSupabase ha rechazado la conexión.\n\nLog Técnico: {e}")

@bot.message_handler(commands=['clear_all'])
def command_clear_all(message):
    if str(message.chat.id) != str(CHAT_ID): return
    conn = get_db_connection()
    if not conn:
        bot.reply_to(message, "🚨 Error: No hay conexión a Supabase.")
        return
    try:
        c = conn.cursor()
        c.execute('TRUNCATE TABLE wallet')
        conn.commit()
        bot.reply_to(message, "⚠️ ATENCIÓN: DB Supabase (Tabla: wallet) vacuada por completo.")
    except Exception as e:
        bot.reply_to(message, f"❌ Fallo al limpiar DB: {e}")
    finally:
        pass # conn.close() delegado a pooling global

@bot.message_handler(commands=['start'])
def cmd_start(message):
    if str(message.chat.id) != str(CHAT_ID): return
    # Auto-restaurar estado al arrancar
    restore_state_from_telegram()
    tkrs = get_tracked_tickers()
    markup = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.row(KeyboardButton("🌎 Geopolítica"), KeyboardButton("🐳 Radar Ballenas"))
    markup.row(KeyboardButton("📉 SMC / Mi Cartera"), KeyboardButton("💰 Mi Wallet / Estado"))
    bot.reply_to(message, f"---\n🧠 *GÉNESIS 1.0 — TRADING INSTITUCIONAL* 🧠\n---\n✅ Bot iniciado correctamente.\n📊 {len(tkrs)} activos cargados en tu radar.\n🛡️ Persistencia activa. Tu cartera está segura.", reply_markup=markup, parse_mode="HTML")

@bot.message_handler(commands=['reset_pnl'])
def cmd_reset_pnl(message):
    """Comando oculto para resetear la ganancia mensual a $0.00"""
    if str(message.chat.id) != str(CHAT_ID): return
    reset_realized_pnl()
    bot.reply_to(message, "🔄 <b>PnL Mensual Reseteado</b>\n\n✅ Ganancia Mensual Acumulada: <b>$0.00 USD</b>\n✅ Contabilidad limpia desde este momento.", parse_mode="HTML")

@bot.message_handler(commands=['reset_total'])
def cmd_reset_total(message):
    """RESET RADICAL: borra todo el historial contable"""
    if str(message.chat.id) != str(CHAT_ID): return
    reset_total_db()
    bot.reply_to(message, (
        "⚠️ <b>SISTEMA REINICIADO</b>\n\n"
        "🗑️ Todo el historial contable ha sido eliminado.\n"
        "💹 Capital Operativo: <b>$0.00</b>\n"
        "💰 Ganancia Mensual: <b>$0.00 USD</b>\n"
        "📈 Rendimiento: <b>0.00%</b>\n\n"
        "✅ Wallet limpia. Los activos en tu radar siguen activos para monitoreo SMC."
    ), parse_mode="HTML")


@bot.message_handler(commands=['recover'])
def cmd_recover(message):
    """Herramienta de Carga Crítica de Respaldo por Base64"""
    if str(message.chat.id) != str(CHAT_ID): return
    try:
        command_parts = message.text.split(' ', 1)
        if len(command_parts) < 2:
            bot.reply_to(message, "⚠️ Restauración Crítica.\nUso: `/recover [STRING_BASE64_DEL_LOG]`", parse_mode="Markdown")
            return

        b64_str = command_parts[1].strip()
        _restore_from_b64(b64_str)
        save_state_to_telegram()  # Guardar inmediatamente en Telegram

        tkrs = get_tracked_tickers()
        bot.reply_to(message, f"✅ **¡RECUPERACIÓN EXITOSA!**\nSe restauraron {len(tkrs)} activos.\nEl backup ya fue guardado en Telegram.", parse_mode="Markdown")

        for tk in tkrs:
            val = fetch_and_analyze_stock(tk)
            if val: update_smc_memory(tk, val)

    except Exception as e:
        bot.reply_to(message, f"❌ Error en recuperación: `{e}`", parse_mode="Markdown")

@bot.message_handler(commands=['backup'])
def cmd_backup(message):
    """Forzar un backup manual visible"""
    if str(message.chat.id) != str(CHAT_ID): return
    save_state_to_telegram()
    tkrs = get_tracked_tickers()
    bot.reply_to(message, f"✅ Backup forzado completado.\n📊 {len(tkrs)} activos guardados en Telegram Cloud.")

from openai import OpenAI

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    if str(message.chat.id) != str(CHAT_ID): return
    msg = bot.reply_to(message, "👁️ Analizando gráfica con GÉNESIS Vision (GPT-4o OpenAI)...")
    try:
        if not OPENAI_API_KEY:
            bot.edit_message_text("⚠️ Error de configuración de modelo: OPENAI_API_KEY no detectada.", chat_id=message.chat.id, message_id=msg.message_id)
            return

        file_info = bot.get_file(message.photo[-1].file_id)
        image_bytes = bot.download_file(file_info.file_path)
        base_img = base64.b64encode(image_bytes).decode('utf-8')
        
        client = OpenAI(api_key=OPENAI_API_KEY)
        
        prompt = (
            "Actúa como una herramienta educativa de análisis técnico. Este análisis es puramente para fines de estudio y simulación, no es asesoría financiera. Analiza la siguiente imagen de manera objetiva.\n\n"
            "Analiza bajo conceptos Smart Money Concepts (SMC). PROHIBIDO explicar qué significa SMC, Order Blocks, BOS o CHoCH. CERO TEORÍA.\n\n"
            "Tu respuesta DEBE seguir ESTRICTAMENTE este formato, sin agregar introducciones ni despedidas. Tono frío, analítico y directo a los datos duros:\n\n"
            "📊 CONTEXTO TÉCNICO: [1 o 2 líneas sobre la tendencia actual y la acción del precio evaluando liquidez y estructura].\n"
            "🎯 NIVELES CLAVE: [Soportes, Resistencias u Order Blocks con PRECIOS EXACTOS según la gráfica].\n"
            "⚠️ RIESGO DE INVERSIÓN: [Bajo / Medio / Alto] - [Razón técnica directa].\n"
            "⚖️ SESGO DIRECCIONAL: [Fuerte Alcista / Fuerte Bajista / Neutral / Esperar Confirmación] - [Justificación descriptiva en una línea, ej. 'Alta probabilidad de rebote en FVG en $150']."
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
        
        bot.edit_message_text(f"---\n📊 *REPORTE VISUAL GÉNESIS*\n---\n{res.choices[0].message.content.strip()}", chat_id=message.chat.id, message_id=msg.message_id, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Error de visión OpenAI: {e}")
        bot.edit_message_text("⚠️ Error de configuración de modelo", chat_id=message.chat.id, message_id=msg.message_id)

@bot.message_handler(func=lambda message: True, content_types=['text'])
def handle_text(message):
    if str(message.chat.id) != str(CHAT_ID): return
    text = message.text.strip()

    # Ignorar mensajes de backup del bot
    if text.startswith(BACKUP_PREFIX): return

    # === BOTONES MENÚ RÁPIDO ===
    if text == "💰 Mi Wallet / Estado" or "CÓMO VOY" in text.upper() or "RESUMEN" in text.upper():
        bot.reply_to(message, "💰 Extrayendo datos robustos y valuando métricas live...")
        bot.send_message(message.chat.id, build_wallet_dashboard(), parse_mode="HTML")
        return

    if text == "🐳 Radar Ballenas":
        bot.reply_to(message, "🐳 Analizando inteligencia integrada Ballenas + Geopolítica...")
        try:
            # Reporte unificado: ballenas + contexto geopolítico
            lines = ["🐳 <b>RADAR BALLENAS + INTELIGENCIA GÉNESIS</b> 🐳"]
            lines.append("\u2500" * 28)

            if not WHALE_MEMORY:
                lines.append("")
                lines.append("🌊 Océano tranquilo. Sin anomalías institucionales detectadas.")
            else:
                lines.append("")
                lines.append("🚨 <b>ÚLTIMOS MOVIMIENTOS INSTITUCIONALES:</b>")
                lines.append("")
                for w in list(WHALE_MEMORY)[::-1]:
                    is_crypto = '-USD' in w['ticker']
                    vol_str = f"${w['vol_approx']:,} USD" if is_crypto else f"{w['vol_approx']:,} unidades"
                    minutes_ago = int((datetime.now() - w['timestamp']).total_seconds() / 60)
                    # Cruzar con contexto geopolítico
                    risk_tag = ""
                    if w['ticker'] in GENESIS_RISK_CONTEXT.get('high_risk_tickers', []):
                        risk_tag = "\n    ⚠️ <b>[ENTORNO DE INESTABILIDAD DETECTADO]</b>"
                    lines.append(f"🐋 <b>{get_display_name(w['ticker'])}</b>")
                    lines.append(f"    Vol: {vol_str} | {w['type']} | {minutes_ago}min ago{risk_tag}")
                    lines.append("")

            # Contexto geopolítico resumido
            lines.append("\u2500" * 28)
            ctx = GENESIS_RISK_CONTEXT
            if ctx.get('last_update'):
                global_s = _classify_sentiment(ctx['sentiment_global'])
                lines.append(f"🌍 <b>Contexto Macro:</b> {global_s['icon']} {global_s['label']} — {global_s['bull_pct']}% Alcista / {global_s['bear_pct']}% Bajista")
                if ctx.get('high_risk_tickers'):
                    risk_names = ', '.join([get_display_name(tk) for tk in ctx['high_risk_tickers'][:5]])
                    lines.append(f"🔴 <b>Activos en zona de riesgo:</b> {risk_names}")
            else:
                lines.append("🌍 <b>Contexto Macro:</b> Pendiente de actualización")

            lines.append("\u2500" * 28)
            bot.send_message(message.chat.id, "\n".join(lines), parse_mode="HTML")
        except Exception as e:
            logging.error(f"Error en Radar Ballenas unificado: {e}")
            bot.send_message(message.chat.id, "☕ Sin eventos de riesgo detectados en este momento. Vigilancia activa.", parse_mode="HTML")
        return

    if text == "🌎 Geopolítica":
        bot.reply_to(message, "🌍 Generando Reporte Estratégico Unificado GÉNESIS...")
        try:
            report = generar_reporte_macro_manual()
            if report:
                bot.send_message(message.chat.id, report, parse_mode="HTML")
            else:
                bot.send_message(message.chat.id, "☕ Sin eventos de riesgo detectados en este momento. Vigilancia activa.", parse_mode="HTML")
        except Exception as e:
            logging.error(f"Error en Geopolítica: {e}")
            bot.send_message(message.chat.id, "☕ Sin eventos de riesgo detectados en este momento. Vigilancia activa.", parse_mode="HTML")
        return

    if text == "📉 SMC / Mi Cartera":
        bot.reply_to(message, "📉 Limpiando caché y forzando datos frescos de exchange...")
        report_lines = ["---", "🦅 *GÉNESIS: SMC / NIVELES CRÍTICOS*", "---"]
        tkrs = get_tracked_tickers()

        if not tkrs:
             bot.send_message(message.chat.id, "Tu cartera está vacía.", parse_mode="HTML")
             return

        # REFRESH FORZADO: limpiar caché de precios para obligar consulta fresca
        for raw_tk in tkrs:
            tk = remap_ticker(raw_tk)
            LAST_KNOWN_PRICES.pop(tk, None)

        for raw_tk in tkrs:
            tk = remap_ticker(raw_tk)
            analysis = fetch_and_analyze_stock(tk)
            d_name = get_display_name(tk)

            if analysis:
                report_lines.extend([f"🏦 <b>{d_name}</b> - ${fmt_price(analysis['price'])}", f"• Tendencia SMC: {analysis['smc_trend']}", f"• Buy-side Liquidity: ${fmt_price(analysis['smc_sup'])}", f"• Sell-side Liquidity: ${fmt_price(analysis['smc_res'])}", f"• Order Block Institucional: ${fmt_price(analysis['order_block'])}", "---"])
            else:
                report_lines.extend([f"🏦 <b>{d_name}</b>", f"• ⏳ Sin datos disponibles en este momento desde los exchanges", "---"])

        bot.send_message(message.chat.id, "\n".join(report_lines), parse_mode="HTML")
        return

    # === EXPRESIONES REGULARES INTELIGENTES NLP ===
    if re.search(r'(?i)\bANALIZA\b\s+([A-Za-z0-9\-]+)', text):
        match = re.search(r'(?i)\bANALIZA\b\s+([A-Za-z0-9\-]+)', text)
        if match:
            tk = remap_ticker(match.group(1))
            display_name = get_display_name(tk)
            bot.reply_to(message, f"🔍 Análisis Profundo Institucional en {display_name}...")
            bot.send_message(message.chat.id, f"---\n🏦 *RESEARCH: {display_name}*\n---\n{perform_deep_analysis(tk)}", parse_mode="HTML")
        return

    if re.search(r'(?i)\b(?:ELIMINA|BORRA|BORRAR|ELIMINAR)\b\s+([A-Za-z0-9\-]+)', text):
        match = re.search(r'(?i)\b(?:ELIMINA|BORRA|BORRAR|ELIMINAR)\b\s+([A-Za-z0-9\-]+)', text)
        if match:
             raw_input = match.group(1)
             tk = remap_ticker(raw_input)
             display_name = get_display_name(tk)
             if remove_ticker(tk):
                 bot.reply_to(message, f"---\n✅ *GESTIÓN DE CARTERA*\n---\n✅ [ {display_name} ] ha sido borrado del radar.\n\n✅ Guardado en Base de Datos Blindada. Esta información no se borrará aunque el bot se reinicie.", parse_mode="HTML")
             else:
                 bot.reply_to(message, f"⚠️ El activo {display_name} no residía en tu radar.")
        return

    if re.search(r'(?i)\b(?:AGREGA|AÑADE|AGREGAR)\b\s+([A-Za-z0-9\-]+)', text):
        match = re.search(r'(?i)\b(?:AGREGA|AÑADE|AGREGAR)\b\s+([A-Za-z0-9\-]+)', text)
        if match:
             raw_input = match.group(1).upper()
             tk = remap_ticker(raw_input)
             display_name = get_display_name(tk)

             validation = get_safe_ticker_price(tk)
             if validation is None:
                 bot.reply_to(message, "⚠️ Activo no encontrado en FMP. No se agregó.")
                 return
             res = add_ticker(tk)
             if res == "DB_ERROR":
                 bot.reply_to(message, f"🚨 ERROR DE BASE DE DATOS: No se pudo conectar a Supabase. Revisa tu DATABASE_URL.")
             elif res == True:
                 bot.reply_to(message, f"---\n✅ *GESTIÓN DE CARTERA*\n---\n✅ [ {display_name} ] añadido al radar SMC.\n\n✅ Guardado directamente en Supabase (Sin cachés).", parse_mode="HTML")
             else:
                 bot.reply_to(message, f"⚠️ El activo {display_name} ya existe en tu DB centralizada (Supabase).")
        return

    if re.search(r'(?i)\bCOMPR[EÉ]\b', text):
        match = re.search(r'(?i)\bCOMPR[EÉ]\b\s+(?:DE\s+)?\$?(\d+(?:\.\d+)?)\s+(?:EN\s+|DE\s+|ACCIONES\s+DE\s+)?([A-Za-z0-9\-]+)', text)
        if match:
            amt = match.group(1)
            tk = remap_ticker(match.group(2))
            display_name = get_display_name(tk)

            bot.reply_to(message, f"💸 Consultando precio de fijación para {display_name}...")
            intra = fetch_intraday_data(tk)
            if intra:
                add_investment(tk, amt, intra['latest_price'])
                bot.send_message(message.chat.id, f"---\n✅ *CAPITAL REGISTRADO*\n---\n• Activo: {display_name}\n• Capital Invertido: ${float(amt):,.2f} USD\n• Entrada: ${fmt_price(intra['latest_price'])}\n\n✅ Guardado en Base de Datos Blindada. Esta información no se borrará aunque el bot se reinicie.", parse_mode="HTML")
            else:
                bot.reply_to(message, f"❌ No pude fijar el precio real de {display_name} ahora. Mercado cerrado temporalmente.")
        return

    if re.search(r'(?i)\bVEND[IÍ]\b', text):
        match = re.search(r'(?i)\bVEND[IÍ]\b\s+(?:TODO\s+)?(?:DE\s+)?\$?(?:\d+(?:\.\d+)?\s+(?:EN\s+|DE\s+|ACCIONES\s+DE\s+)?)?([A-Za-z0-9\-]+)', text)
        if match:
            tk = remap_ticker(match.group(1))
            display_name = get_display_name(tk)

            investments = get_investments()
            if tk in investments:
                bot.reply_to(message, f"💸 Procesando cierre institucional para {display_name}...")
                entry = investments[tk]['entry_price']
                amt = investments[tk]['amount_usd']

                intra = fetch_intraday_data(tk)
                if intra:
                    live_price = intra['latest_price']
                    roi = (live_price - entry) / entry if entry > 0 else 0
                    prof = amt * roi
                    sign = "+" if prof >= 0 else ""
                    icon = "🟢" if prof >= 0 else "🔴"
                    final_usd = amt + prof

                    close_investment(tk)
                    add_realized_pnl(prof)

                    ans_str = (
                        f"---\n✅ *GESTIÓN DE CARTERA: CIERRE*\n---\n"
                        f"✅ [ {display_name} ] liquidado al precio de ${fmt_price(live_price)}\n"
                        f"💰 <b>Capital Retirado:</b> ${final_usd:,.2f} USD\n"
                        f"{icon} <b>Ganancia Mensual Sumada:</b> {sign}${prof:,.2f} USD ({sign}{roi*100:.2f}%)\n\n"
                        f"✅ Guardado en Base de Datos Blindada. Esta información no se borrará aunque el bot se reinicie."
                    )
                    bot.send_message(message.chat.id, ans_str, parse_mode="HTML")
                else:
                    bot.reply_to(message, f"❌ No pude contactar al mercado para saldar la liquidación de {display_name}.")
            else:
                 bot.reply_to(message, f"⚠️ No tienes capital invertido en {display_name}. Usa 'Elimina {display_name}' para detener rastreo.")
        return


# ----------------- MODO CENTINELA: VIGILANCIA DE NOTICIAS POR ACTIVO -----------------
_SENTINEL_TICK_INTERVAL = 40  # Cada 40 ticks de 30s = ~20 minutos

def verificar_noticias_cartera():
    """Vigila noticias específicas de los activos en la cartera de Eduardo"""
    tkrs = get_tracked_tickers()
    if not tkrs:
        return

    for raw_tk in tkrs:
        tk = remap_ticker(raw_tk)
        display_name = get_display_name(tk)

        try:
            ticker_obj = yf.Ticker(tk)
            news_list = ticker_obj.news or []
        except Exception:
            continue

        for article in news_list[:3]:  # Solo las 3 más recientes
            title = article.get('title', '')
            if not title:
                continue

            # Deduplicar con hash: no alertar la misma noticia dos veces
            news_hash = f"SENTINEL_{tk}_{hash(title) % 100000}"
            if check_and_add_seen_event(news_hash):
                continue  # Ya la vimos

            # Pasar por GPT para análisis de riesgo
            if not GEMINI_API_KEY:
                continue

            try:
                client = genai.Client(api_key=GEMINI_API_KEY)
                prompt = (
                    f"Actúa como GÉNESIS, un gestor de riesgos senior de un fondo institucional (con base en Gemini 3.1 Pro).\n"
                    f"Analiza esta noticia del activo {display_name} ({tk}):\n"
                    f"Titular: \"{title}\"\n\n"
                    f"REGLAS ESTRICTAS:\n"
                    f"- Si la noticia es NEUTRAL, de relleno, o sin impacto real en el precio local, responde EXACTAMENTE: 'NEUTRAL'\n"
                    f"- Si la noticia tiene impacto REAL (positivo o negativo), predice el impacto en las zonas de oferta/demanda y genera una alerta con este formato:\n"
                    f"  📰 Suceso: [Resumen de 1 línea]\n"
                    f"  💡 Sugerencia Institucional: [Vender / Vigilar / Hold / Comprar]\n"
                    f"  ⚡ Impacto Estimado: [Alto / Medio] en la liquidez\n"
                    f"RESPONDE EN ESPAÑOL."
                )

                res = client.models.generate_content(
                    model="gemini-1.5-pro",
                    contents=prompt,
                ).text.strip()

                # Filtro de ruido: si GPT dice NEUTRAL, silencio total
                if "NEUTRAL" in res.upper() and len(res) < 30:
                    continue

                # Alerta que SÍ amerita atención
                alert_msg = (
                    f"---\n🚨 *CENTINELA GÉNESIS: ALERTA DE ACTIVO* 🚨\n---\n"
                    f"📈 Activo: <b>{display_name}</b>\n"
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
    """SISTEMA GÉNESIS: Monitor de protección de activos en la wallet.
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

        # Calcular variación porcentual
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

        # Determinar dirección
        direction = "📉 CAÍDA" if pct_change < 0 else "📈 SUBIDA"
        emoji = "🔴" if pct_change < 0 else "🟢"

        # Obtener contexto SMC si está disponible
        smc_context = ""
        smc = SMC_LEVELS_MEMORY.get(tk)
        if smc:
            if current_price < smc.get('sup', 0):
                smc_context = f"\n⚠️ Precio POR DEBAJO del Soporte SMC (${fmt_price(smc['sup'])}). Zona de riesgo."
            elif current_price > smc.get('res', 0):
                smc_context = f"\n✅ Precio POR ENCIMA de Resistencia SMC (${fmt_price(smc['res'])}). Posible breakout."
            else:
                smc_context = f"\n📊 Rango SMC: Soporte ${fmt_price(smc['sup'])} | Resistencia ${fmt_price(smc['res'])}"

        # Generar veredicto con IA si está disponible
        veredicto = ""
        if GEMINI_API_KEY:
            try:
                client = genai.Client(api_key=GEMINI_API_KEY)
                prompt = (
                    f"Eres GÉNESIS (Gemini 3.1 Pro), un sistema de protección de activos enfocado en la prevención de riesgos y la estrategia Smart Money.\n"
                    f"Activo protegido: {display_name} ({tk})\n"
                    f"Precio real actual de FMP: ${fmt_price(current_price)}\n"
                    f"Desviación anómala detectada: {pct_change:+.2f}% en las últimas horas\n"
                    f"Contexto SMC en vivo:\n{smc_context}\n\n"
                    f"Analiza profunda pero rápidamente esta desviación en relación a la liquidez del Order Block.\n"
                    f"Da un VEREDICTO en 2 líneas: ¿Mantener, vender parcial, o reforzar posición institucional? Justifica mecánicamente.\n"
                    f"ESPAÑOL ESTRICTO."
                )
                res = client.models.generate_content(
                    model="gemini-1.5-pro",
                    contents=prompt,
                ).text.strip()
                veredicto = f"\n\n🧠 <b>VEREDICTO GÉNESIS:</b>\n{res}"
            except Exception as e:
                logging.debug(f"Protection GPT error: {e}")

        # Construir y enviar alerta
        entry_price = inv_data.get('entry_price', 0)
        entry_info = f"\n🎯 Precio de entrada: ${fmt_price(entry_price)}" if entry_price > 0 else ""

        alert_msg = (
            f"---\n🚨 <b>SISTEMA GÉNESIS — PROTECCIÓN DE ACTIVOS</b> 🚨\n---\n\n"
            f"{emoji} <b>{direction} DETECTADA</b>\n\n"
            f"💰 Activo: <b>{display_name}</b>\n"
            f"📉 Movimiento: <b>{pct_change:+.2f}%</b>\n"
            f"💵 Precio FMP: <b>${fmt_price(current_price)}</b>{entry_info}"
            f"{smc_context}"
            f"{veredicto}\n\n---"
        )

        try:
            bot.send_message(CHAT_ID, alert_msg, parse_mode="HTML")
        except Exception as e:
            logging.error(f"Error enviando alerta de protección para {tk}: {e}")


# ----------------- BUCLE CENTINELA HFT PRECISIÓN QUIRÚRGICA -----------------
def boot_smc_levels_once():
    logging.info("Arrancando Centinela Quirúrgico (30s)...")

    # PASO CRÍTICO: Restaurar datos ANTES de hacer cualquier otra cosa
    restore_state_from_telegram()

    tkrs = get_tracked_tickers()
    logging.info(f"Activos cargados en radar: {len(tkrs)} → {tkrs}")

    for tk in tkrs:
        val = fetch_and_analyze_stock(tk)
        if val: update_smc_memory(tk, val)

def background_loop_proactivo():
    """BUCLE DE ALTA LATENCIA CON DOBLE VERIFICACIÓN Y ANTI-SPAM (TTL 7 DÍAS)"""
    boot_smc_levels_once()
    sentinel_tick_counter = 0  # Contador para noticias de cartera cada ~20 min
    protection_tick_counter = 0  # Contador para monitor de protección cada ~5 min
    _PROTECTION_INTERVAL = 10  # ~5 minutos (10 ticks * 30s)
    while True:
        try:
            time.sleep(30)
            now = datetime.now()
            purge_old_events()
            sentinel_tick_counter += 1
            protection_tick_counter += 1

            raw_news = check_geopolitical_news()
            unique_news = []
            for n_title in raw_news:
                nws_id = f"NWS_{n_title}"
                if not check_and_add_seen_event(nws_id):
                    unique_news.append(n_title)

            if unique_news:
                ai_threat_evaluation = gpt_advanced_geopolitics(unique_news, manual=False)
                if ai_threat_evaluation:
                     bot.send_message(CHAT_ID, f"---\n🚨 *VIGILANCIA GLOBAL ALTO RIESGO*\n---\n{ai_threat_evaluation}", parse_mode="HTML")

            # === MODO CENTINELA: verificar noticias de activos cada ~20 minutos ===
            if sentinel_tick_counter >= _SENTINEL_TICK_INTERVAL:
                sentinel_tick_counter = 0
                try:
                    verificar_noticias_cartera()
                except Exception as e:
                    logging.error(f"Error en Centinela de Noticias: {e}")

            # === MONITOR DE PROTECCIÓN DE ACTIVOS: cada ~5 minutos ===
            if protection_tick_counter >= _PROTECTION_INTERVAL:
                protection_tick_counter = 0
                try:
                    monitor_proteccion_activos()
                except Exception as e:
                    logging.error(f"Error en Monitor de Protección: {e}")

            for tk in get_tracked_tickers():
                intra = fetch_intraday_data(tk)
                if not intra: continue
                cur_price = intra['latest_price']
                display_name = get_display_name(tk)

                # === GUARDIA DE COHERENCIA: bloquear alertas si el precio es ilógico ===
                price_is_reliable = True
                if tk in LAST_KNOWN_PRICES:
                    last_p = LAST_KNOWN_PRICES[tk]['price']
                    if last_p > 0 and abs(cur_price - last_p) / last_p > 0.50:
                        logging.warning(f"🚫 ALERTA BLOQUEADA para {tk}: ${cur_price:.2f} vs último ${last_p:.2f} (>50% de desviación). Error de API probable.")
                        price_is_reliable = False

                # Rupturas Doble Verificadas (YFinance 1 Minuto) — SOLO si el precio es confiable
                topol = SMC_LEVELS_MEMORY.get(tk)
                if topol and price_is_reliable:
                    if cur_price > topol['res']:
                        rt = verify_1m_realtime_data(tk)
                        if rt and rt['price'] > topol['res'] and _sanity_check_price(tk, rt['price']):
                           hash_brk = f"BRK_UP_{tk}_{topol['res']}"
                           if not check_and_add_seen_event(hash_brk):
                               adv = analyze_breakout_gpt(tk, "Resistencia", rt['price'])
                               bot.send_message(CHAT_ID, f"---\n🚨 *ALERTA DE RUPTURA INMINENTE*\n---\n<b>{display_name}</b> cruzó quirúrgicamente Resistencia en <b>${fmt_price(rt['price'])}</b>.\n\n🤖 *DECISIÓN IA:*\n{adv}", parse_mode="HTML")

                    elif cur_price < topol['sup']:
                        rt = verify_1m_realtime_data(tk)
                        if rt and rt['price'] < topol['sup'] and _sanity_check_price(tk, rt['price']):
                           hash_drp = f"BRK_DWN_{tk}_{topol['sup']}"
                           if not check_and_add_seen_event(hash_drp):
                               adv = analyze_breakout_gpt(tk, "Soporte", rt['price'])
                               bot.send_message(CHAT_ID, f"---\n🚨 *ALERTA DE RUPTURA (DUMP)*\n---\n<b>{display_name}</b> cruzó quirúrgicamente Soporte en <b>${fmt_price(rt['price'])}</b>.\n\n🤖 *DECISIÓN IA:*\n{adv}", parse_mode="HTML")

                # Ballenas Doble Verificadas — con cruce geopolítico GENESIS
                if intra['avg_vol'] > 0 and price_is_reliable:
                    is_crypto = '-USD' in tk
                    # Crypto: umbral más alto (5x) porque yfinance reporta volumen en USD acumulado 24h
                    whale_threshold = 5.0 if is_crypto else 2.5
                    spike = intra['latest_vol'] / intra['avg_vol']
                    if spike >= whale_threshold:
                        rt = verify_1m_realtime_data(tk)
                        valid_vol = int(rt['vol']) if rt else int(intra['latest_vol'])
                        whale_hash_id = f"WHL_{tk}_{valid_vol}"

                        if not check_and_add_seen_event(whale_hash_id):
                            note = "\n<i>[Confirmando volumen institucional...]</i>" if not rt or rt['vol'] < intra['latest_vol'] else ""
                            WHALE_MEMORY.append({"ticker": tk, "vol_approx": valid_vol, "type": intra['vol_type'], "timestamp": now})
                            # Formato inteligente: cripto en USD, acciones en unidades
                            if is_crypto:
                                vol_display = f"${valid_vol:,} USD"
                            else:
                                vol_display = f"{valid_vol:,} unidades"

                            # === ALERTA ROJA: Ballena + Sentimiento negativo ===
                            is_high_risk = tk in GENESIS_RISK_CONTEXT.get('high_risk_tickers', [])
                            neg_sentiment = GENESIS_RISK_CONTEXT.get('sentiment_global', 0) < -0.3
                            is_massive = valid_vol > 100_000_000 if is_crypto else valid_vol > 5_000_000

                            if is_high_risk or (neg_sentiment and is_massive):
                                # ALERTA ROJA: whale en entorno de inestabilidad
                                alert_msg = (
                                    f"\u2500" * 28 + "\n"
                                    f"\ud83d\udea8 <b>ALERTA ROJA: BALLENA EN ENTORNO DE INESTABILIDAD</b> \ud83d\udea8\n"
                                    f"\u2500" * 28 + "\n"
                                    f"\ud83d\udc0b Bloque masivo en <b>{display_name}</b>: {vol_display}\n"
                                    f"\ud83d\udcca Presi\u00f3n: {intra['vol_type']}\n"
                                    f"\ud83c\udf0d Contexto: Sentimiento del mercado <b>NEGATIVO</b>\n"
                                    f"\u26a0\ufe0f Movimiento de Ballena detectado en entorno de inestabilidad\n"
                                    f"\ud83d\udca1 <b>Recomendaci\u00f3n:</b> Aumentar vigilancia sobre {display_name}{note}\n"
                                    f"\u2500" * 28
                                )
                                bot.send_message(CHAT_ID, alert_msg, parse_mode="HTML")
                            else:
                                bot.send_message(CHAT_ID, f"---\n\u26a0\ufe0f *ALERTA DE BALLENA HFT*\n---\nBloque masivo cruzado en <b>{display_name}</b>: {vol_display}.\nPresi\u00f3n Institucional: {intra['vol_type']}{note}", parse_mode="HTML")

        except Exception as e:
            logging.error(f"Error HFT: {e}")

# ----------------- MAIN -----------------
def main():
    logging.info("Iniciando Génesis 1.0 — Persistencia: Telegram Cloud + SQLite local + Base64 logs")
    t = threading.Thread(target=background_loop_proactivo, daemon=True)
    t.start()
    bot.infinity_polling(timeout=10, long_polling_timeout=5)

if __name__ == "__main__":
    main()
