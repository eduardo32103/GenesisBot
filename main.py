import logging
import base64
import requests
import re
import xml.etree.ElementTree as ET
import pandas as pd
import yfinance as yf
import threading
import time
from openai import OpenAI
import os
import telebot
import json
import sqlite3
from collections import deque
from telebot.types import ReplyKeyboardMarkup, KeyboardButton
from datetime import datetime, timedelta

# Configuración extendida de logs para Railway
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
PREMIUM_API_KEY = (os.environ.get('PREMIUM_API_KEY') or '').strip()
# Canal privado donde el bot fija el backup (puede ser el mismo CHAT_ID o un canal dedicado)
BACKUP_CHAT_ID = os.environ.get('BACKUP_CHAT_ID', CHAT_ID)

if not TELEGRAM_TOKEN or not CHAT_ID:
    logging.critical("Falta TELEGRAM_TOKEN o CHAT_ID. Saliendo.")
    exit()

if not PREMIUM_API_KEY:
    logging.warning("⚠️ PREMIUM_API_KEY no configurada. El motor de precios FMP no funcionará.")
else:
    logging.info(f"✅ PREMIUM_API_KEY cargada correctamente ({len(PREMIUM_API_KEY)} caracteres).")

bot = telebot.TeleBot(TELEGRAM_TOKEN)

# --- BASE DE DATOS LOCAL (SQLite como cache de runtime) ---
DATA_DIR = os.environ.get('DATA_DIR', '.')
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, 'genesis_data.db')

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS portfolio (ticker TEXT PRIMARY KEY, is_investment INTEGER, amount_usd REAL, entry_price REAL, timestamp TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS global_stats (key TEXT PRIMARY KEY, value REAL)''')
        c.execute('''CREATE TABLE IF NOT EXISTS seen_events (hash_id TEXT PRIMARY KEY, timestamp TEXT)''')
        conn.commit()

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

def save_state_to_telegram():
    """Guarda el estado completo como mensaje SILENCIOSO en Telegram.
    Si BACKUP_CHAT_ID == CHAT_ID, borra el mensaje anterior para no ensuciar el chat."""
    global _last_backup_msg_id

    # SIEMPRE guardar en disco local primero (ultra-rápido, nunca falla)
    _save_local_portfolio_json()

    try:
        payload = _build_backup_payload()
        json_str = json.dumps(payload, ensure_ascii=False)
        b64 = base64.b64encode(json_str.encode('utf-8')).decode('utf-8')

        backup_text = f"{BACKUP_PREFIX}\n{b64}"

        # Log interno de Railway (nunca visible para Eduardo)
        logging.info(f"BACKUP_DB_LOG: estado guardado ({len(b64)} bytes)")

        # Si tenemos un mensaje anterior, editarlo en vez de crear uno nuevo
        if _last_backup_msg_id:
            try:
                bot.edit_message_text(
                    backup_text,
                    chat_id=BACKUP_CHAT_ID,
                    message_id=_last_backup_msg_id
                )
                logging.info(f"✅ Backup actualizado silenciosamente (msg_id: {_last_backup_msg_id})")
                return
            except Exception:
                # Si falla editar (msg borrado, etc), intentar borrar y enviar nuevo
                try:
                    bot.delete_message(BACKUP_CHAT_ID, _last_backup_msg_id)
                except Exception:
                    pass

        # Enviar mensaje nuevo SILENCIOSO (sin notificación al usuario)
        msg = bot.send_message(BACKUP_CHAT_ID, backup_text, disable_notification=True)
        _last_backup_msg_id = msg.message_id
        logging.info(f"✅ Backup silencioso enviado (msg_id: {_last_backup_msg_id})")

        # Si el backup va al chat principal, borrarlo después de 2s para que no se vea
        if str(BACKUP_CHAT_ID) == str(CHAT_ID):
            try:
                import time as _time
                _time.sleep(1)
                bot.delete_message(CHAT_ID, msg.message_id)
                logging.info(f"Backup eliminado del chat principal (era visible).")
                # NO resetear _last_backup_msg_id: ya no sirve para editar, se crea nuevo la próxima vez
                _last_backup_msg_id = None
            except Exception:
                pass  # Si no puede borrar, al menos es silencioso

    except Exception as e:
        logging.error(f"Error guardando backup en Telegram: {e}")

def restore_state_from_telegram():
    """Recupera el estado de la cartera con múltiples fuentes, en orden de prioridad"""
    global _last_backup_msg_id

    # Verificar si la DB local ya tiene datos con inversiones REALES
    existing = get_tracked_tickers()
    investments = get_investments()
    if existing and investments:
        logging.info(f"DB local tiene {len(existing)} activos y {len(investments)} inversiones. No se necesita restaurar.")
        return True

    logging.info("DB local vacía o sin inversiones. Buscando backup...")

    # FUENTE 1: Buscar backup en updates recientes de Telegram
    try:
        updates = bot.get_updates(limit=100, timeout=5)
        for update in reversed(updates):
            msg = update.message or update.edited_message
            if msg and msg.text and msg.text.startswith(BACKUP_PREFIX):
                b64_data = msg.text.replace(BACKUP_PREFIX, "").strip()
                if b64_data:
                    _restore_from_b64(b64_data)
                    _last_backup_msg_id = msg.message_id
                    logging.info(f"✅ RESTAURACIÓN desde Telegram updates exitosa (msg_id: {msg.message_id})")
                    return True
    except Exception as e:
        logging.debug(f"No se encontró backup en updates: {e}")

    # FUENTE 2: Mensaje fijado en el chat de backup
    try:
        chat_info = bot.get_chat(BACKUP_CHAT_ID)
        pinned = chat_info.pinned_message
        if pinned and pinned.text and pinned.text.startswith(BACKUP_PREFIX):
            b64_data = pinned.text.replace(BACKUP_PREFIX, "").strip()
            if b64_data:
                _restore_from_b64(b64_data)
                _last_backup_msg_id = pinned.message_id
                logging.info("✅ RESTAURACIÓN desde mensaje fijado exitosa!")
                return True
    except Exception:
        pass

    # FUENTE 3: portfolio.json del repositorio
    restored = _restore_from_repo_json()
    if restored:
        # Guardar inmediatamente en Telegram para futuras restauraciones
        try:
            save_state_to_telegram()
        except Exception:
            pass
        return True

    logging.warning("⚠️ No se encontró ningún respaldo. Cartera inicia vacía.")
    return False

def _restore_from_b64(b64_data):
    """Restaura la base de datos desde un string Base64"""
    json_str = base64.b64decode(b64_data).decode('utf-8')
    payload = json.loads(json_str)

    portfolio = payload.get("portfolio", {})
    stats = payload.get("global_stats", {})

    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        for tk, info in portfolio.items():
            c.execute('INSERT OR REPLACE INTO portfolio (ticker, is_investment, amount_usd, entry_price, timestamp) VALUES (?, ?, ?, ?, ?)',
                (tk,
                 int(info.get("is_investment", 0)),
                 float(info.get("amount_usd", 0)),
                 float(info.get("entry_price", 0)),
                 info.get("timestamp", datetime.now().isoformat())))

        rpnl = stats.get("realized_pnl", 0)
        if rpnl:
            c.execute('INSERT OR REPLACE INTO global_stats (key, value) VALUES ("realized_pnl", ?)', (float(rpnl),))
        conn.commit()

    logging.info(f"Restaurados {len(portfolio)} activos desde backup Base64.")

def _restore_from_repo_json():
    """Último recurso: lee portfolio.json que está en el repositorio Git"""
    # Intentar varias rutas posibles
    script_dir = os.path.dirname(os.path.abspath(__file__))
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

                with sqlite3.connect(DB_PATH) as conn:
                    c = conn.cursor()
                    for tk, val in legacy.items():
                        if isinstance(val, (int, float)):
                            # Formato antiguo: {"IXC": 927.55} donde el valor es el precio de entrada
                            c.execute('INSERT OR IGNORE INTO portfolio (ticker, is_investment, amount_usd, entry_price, timestamp) VALUES (?, 1, 1000.0, ?, ?)',
                                (tk, float(val), datetime.now().isoformat()))
                        elif isinstance(val, dict):
                            c.execute('INSERT OR IGNORE INTO portfolio (ticker, is_investment, amount_usd, entry_price, timestamp) VALUES (?, ?, ?, ?, ?)',
                                (tk,
                                 int(val.get('is_investment', 1)),
                                 float(val.get('amount_usd', 1000.0)),
                                 float(val.get('entry_price', 0.0)),
                                 datetime.now().isoformat()))
                    conn.commit()

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

# --- CONTROLADORES DE BASE DE DATOS (SQLite local como cache) ---
def check_and_add_seen_event(event_hash):
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute('SELECT 1 FROM seen_events WHERE hash_id = ?', (event_hash,))
        if c.fetchone(): return True
        c.execute('INSERT INTO seen_events (hash_id, timestamp) VALUES (?, ?)', (event_hash, datetime.now().isoformat()))
        conn.commit()
    return False

def purge_old_events():
    cutoff_date = (datetime.now() - timedelta(days=7)).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute('DELETE FROM seen_events WHERE timestamp < ?', (cutoff_date,))
        conn.commit()

def get_tracked_tickers():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute('SELECT ticker FROM portfolio')
        return [row[0] for row in c.fetchall()]

def get_all_portfolio_data():
    pf = {}
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute('SELECT * FROM portfolio')
        for row in c.fetchall():
            pf[row[0]] = {"is_investment": bool(row[1]), "amount_usd": row[2], "entry_price": row[3], "timestamp": row[4]}
    return pf

def add_ticker(ticker):
    ticker = remap_ticker(ticker)
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute('SELECT 1 FROM portfolio WHERE ticker = ?', (ticker,))
        if not c.fetchone():
            c.execute('INSERT INTO portfolio (ticker, is_investment, amount_usd, entry_price, timestamp) VALUES (?, 0, 0, 0, ?)', (ticker, datetime.now().isoformat()))
            conn.commit()
            save_state_to_telegram()  # ← PERSISTENCIA EN TELEGRAM
            val = fetch_and_analyze_stock(ticker)
            if val: update_smc_memory(ticker, val)
            return True
    return False

def remove_ticker(ticker):
    ticker = remap_ticker(ticker)
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute('SELECT 1 FROM portfolio WHERE ticker = ?', (ticker,))
        if c.fetchone():
            c.execute('DELETE FROM portfolio WHERE ticker = ?', (ticker,))
            conn.commit()
            save_state_to_telegram()  # ← PERSISTENCIA EN TELEGRAM
            if ticker in SMC_LEVELS_MEMORY: del SMC_LEVELS_MEMORY[ticker]
            return True
    return False

def add_investment(ticker, amount_usd, entry_price):
    ticker = remap_ticker(ticker)
    timestamp = datetime.now().isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute('SELECT 1 FROM portfolio WHERE ticker = ?', (ticker,))
        if c.fetchone():
            c.execute('UPDATE portfolio SET is_investment = 1, amount_usd = ?, entry_price = ?, timestamp = ? WHERE ticker = ?', (amount_usd, entry_price, timestamp, ticker))
        else:
            c.execute('INSERT INTO portfolio (ticker, is_investment, amount_usd, entry_price, timestamp) VALUES (?, 1, ?, ?, ?)', (ticker, amount_usd, entry_price, timestamp))
        conn.commit()
    save_state_to_telegram()  # ← PERSISTENCIA EN TELEGRAM
    val = fetch_and_analyze_stock(ticker)
    if val: update_smc_memory(ticker, val)

def close_investment(ticker):
    ticker = remap_ticker(ticker)
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute('UPDATE portfolio SET is_investment = 0, amount_usd = 0, entry_price = 0 WHERE ticker = ?', (ticker,))
        conn.commit()
    save_state_to_telegram()  # ← PERSISTENCIA EN TELEGRAM

def get_investments():
    invs = {}
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute('SELECT ticker, amount_usd, entry_price FROM portfolio WHERE is_investment = 1')
        for row in c.fetchall():
            invs[row[0]] = {'amount_usd': row[1], 'entry_price': row[2]}
    return invs

def add_realized_pnl(prof_usd):
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute('SELECT value FROM global_stats WHERE key = "realized_pnl"')
        res = c.fetchone()
        cur_pnl = res[0] if res else 0.0
        new_val = cur_pnl + float(prof_usd)
        if res: c.execute('UPDATE global_stats SET value = ? WHERE key = "realized_pnl"', (new_val,))
        else: c.execute('INSERT INTO global_stats (key, value) VALUES ("realized_pnl", ?)', (new_val,))
        conn.commit()
    save_state_to_telegram()  # ← PERSISTENCIA EN TELEGRAM

def get_realized_pnl():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute('SELECT value FROM global_stats WHERE key = "realized_pnl"')
        res = c.fetchone()
        return res[0] if res else 0.0

def reset_realized_pnl():
    """Resetea la ganancia mensual acumulada a $0.00"""
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute('INSERT OR REPLACE INTO global_stats (key, value) VALUES ("realized_pnl", 0.0)')
        conn.commit()
    save_state_to_telegram()
    logging.info("🔄 PnL mensual reseteado a $0.00")

def reset_total_db():
    """RESET RADICAL: borra TODAS las inversiones, PnL y contabilidad"""
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        # Limpiar inversiones (poner is_investment=0, amount=0, entry=0)
        c.execute('UPDATE portfolio SET is_investment = 0, amount_usd = 0, entry_price = 0')
        # Resetear PnL acumulado
        c.execute('INSERT OR REPLACE INTO global_stats (key, value) VALUES ("realized_pnl", 0.0)')
        conn.commit()
    # NO llamar a save_state_to_telegram() aquí para evitar que el código Base64
    # aparezca en el chat. El backup se hará automáticamente en el próximo ciclo.
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
# Endpoint: https://financialmodelingprep.com/api/v3/quote/{SYMBOL}?apikey={KEY}

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
    """Consulta precio en vivo desde FMP — endpoints STABLE (post-agosto 2025)"""
    global _FMP_LAST_ERROR
    if not PREMIUM_API_KEY:
        _FMP_LAST_ERROR[tk] = "PREMIUM_API_KEY no detectada en Railway."
        logging.error("FMP: PREMIUM_API_KEY no configurada.")
        return None

    fmp_symbol = _get_fmp_symbol(tk)

    # Construir lista de símbolos a probar
    symbols_to_try = [fmp_symbol]
    if _is_crypto_ticker(tk):
        base = tk.replace('-USD', '')
        symbols_to_try = [f"{base}USD", tk, base]
    elif tk == "BZ=F":
        symbols_to_try = ["BZUSD", "BCOUSD"]
    elif tk == "GC=F":
        symbols_to_try = ["GCUSD", "XAUUSD"]

    last_status = 0
    last_raw = ""

    # === INTENTO 1: /stable/quote (ENDPOINT MODERNO — obligatorio post-agosto 2025) ===
    for symbol in symbols_to_try:
        try:
            url = f"https://financialmodelingprep.com/stable/quote?symbol={symbol}&apikey={PREMIUM_API_KEY}"
            resp = requests.get(url, timeout=8)
            last_status = resp.status_code
            last_raw = resp.text[:500] if resp.text else "(vacío)"
            logging.info(f"LOG FMP [stable/quote]: Status {resp.status_code} para {symbol}")

            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and len(data) > 0:
                    quote = data[0]
                    price = float(quote.get('price', 0) or quote.get('previousClose', 0) or 0)
                    volume = float(quote.get('volume', 0) or 0)
                    if price > 0:
                        logging.info(f"FMP ✅ {tk} ({symbol}): ${fmt_price(price)}")
                        return {'price': price, 'vol': volume}
                elif isinstance(data, dict) and data:
                    price = float(data.get('price', 0) or data.get('previousClose', 0) or 0)
                    if price > 0:
                        logging.info(f"FMP ✅ {tk} ({symbol}): ${fmt_price(price)}")
                        return {'price': price, 'vol': float(data.get('volume', 0) or 0)}
                logging.info(f"FMP stable: respuesta vacía para {symbol}")

            elif resp.status_code == 401:
                _FMP_LAST_ERROR[tk] = "401 Unauthorized. Key rechazada."
                logging.error("FMP: 401 Key inválida.")
                return None
            elif resp.status_code == 403:
                logging.warning(f"FMP stable: 403 para {symbol}")
        except Exception as e:
            logging.warning(f"FMP stable error {symbol}: {e}")
            last_raw = str(e)

    # === INTENTO 2: /stable/quote-short (endpoint ligero) ===
    for symbol in symbols_to_try:
        try:
            url = f"https://financialmodelingprep.com/stable/quote-short?symbol={symbol}&apikey={PREMIUM_API_KEY}"
            resp = requests.get(url, timeout=5)
            logging.info(f"LOG FMP [stable/quote-short]: Status {resp.status_code} para {symbol}")
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and len(data) > 0:
                    price = float(data[0].get('price', 0))
                    if price > 0:
                        logging.info(f"FMP (short) ✅ {tk} ({symbol}): ${fmt_price(price)}")
                        return {'price': price, 'vol': 0}
                elif isinstance(data, dict) and data:
                    price = float(data.get('price', 0))
                    if price > 0:
                        logging.info(f"FMP (short) ✅ {tk} ({symbol}): ${fmt_price(price)}")
                        return {'price': price, 'vol': 0}
        except Exception:
            pass

    # === INTENTO 3: Batch crypto quotes (solo crypto) ===
    if _is_crypto_ticker(tk):
        try:
            url = f"https://financialmodelingprep.com/stable/batch-crypto-quotes?apikey={PREMIUM_API_KEY}"
            resp = requests.get(url, timeout=10)
            logging.info(f"LOG FMP [batch-crypto]: Status {resp.status_code}")
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    target = _get_fmp_symbol(tk).upper()
                    for item in data:
                        sym = (item.get('symbol', '') or '').upper()
                        if sym == target or sym == tk.replace('-', '').upper():
                            price = float(item.get('price', 0) or 0)
                            if price > 0:
                                logging.info(f"FMP (batch-crypto) ✅ {tk}: ${fmt_price(price)}")
                                return {'price': price, 'vol': float(item.get('volume', 0) or 0)}
                    logging.warning(f"FMP: {tk} no en batch-crypto ({len(data)} activos).")
        except Exception as e:
            logging.warning(f"FMP batch-crypto error: {e}")

    # === FALLBACK LEGACY (por si el plan sí soporta v3) ===
    for symbol in symbols_to_try[:1]:
        try:
            url = f"https://financialmodelingprep.com/api/v3/quote/{symbol}?apikey={PREMIUM_API_KEY}"
            resp = requests.get(url, timeout=5)
            logging.info(f"LOG FMP [legacy v3]: Status {resp.status_code} para {symbol}")
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and len(data) > 0:
                    price = float(data[0].get('price', 0))
                    if price > 0:
                        logging.info(f"FMP (legacy) ✅ {tk} ({symbol}): ${fmt_price(price)}")
                        return {'price': price, 'vol': float(data[0].get('volume', 0) or 0)}
        except Exception:
            pass

    # Guardar diagnóstico
    _FMP_LAST_ERROR[tk] = f"Status {last_status} | Símbolos: {symbols_to_try} | Respuesta: {last_raw[:200]}"
    logging.error(f"FMP FALLÓ para {tk}: {_FMP_LAST_ERROR[tk]}")
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
    """Extrae noticias premium del mercado financiero via FMP /stable/news"""
    if not PREMIUM_API_KEY:
        return []

    # Intento 1: endpoint stable (cuentas post-agosto 2025)
    try:
        url = f"https://financialmodelingprep.com/stable/news?limit={limit}&apikey={PREMIUM_API_KEY}"
        resp = requests.get(url, timeout=8)
        logging.info(f"LOG FMP [stable/news]: Status {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                return data
    except Exception as e:
        logging.debug(f"FMP stable/news error: {e}")

    # Intento 2: endpoint legacy v3
    try:
        url = f"https://financialmodelingprep.com/api/v3/stock_news?limit={limit}&apikey={PREMIUM_API_KEY}"
        resp = requests.get(url, timeout=8)
        logging.info(f"LOG FMP [v3/stock_news]: Status {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                return data
    except Exception as e:
        logging.debug(f"FMP v3/stock_news error: {e}")

    # Intento 3: general news
    try:
        url = f"https://financialmodelingprep.com/api/v3/fmp/articles?page=0&size={limit}&apikey={PREMIUM_API_KEY}"
        resp = requests.get(url, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, dict) and 'content' in data:
                return data['content'][:limit]
            elif isinstance(data, list):
                return data[:limit]
    except Exception:
        pass

    return []


def check_geopolitical_news():
    """Monitor automático: extrae noticias FMP y filtra por alto impacto"""
    HIGH_IMPACT_KEYWORDS = ["war", "attack", "strike", "escalation", "missile", "sanction",
                            "embargo", "explosion", "guerra", "ataque", "tensión", "misil",
                            "sanciones", "rates", "fed", "trump", "powell", "crash", "recession",
                            "tariff", "default", "crisis"]
    news_alerts = []

    # Fuente principal: FMP Premium
    fmp_news = _fetch_fmp_news(15)
    if fmp_news:
        for article in fmp_news:
            title = article.get('title', '') or article.get('text', '') or ''
            if title and any(re.search(rf"\b{kw}\b", title, re.IGNORECASE) for kw in HIGH_IMPACT_KEYWORDS):
                news_alerts.append(title)
                if len(news_alerts) >= 5: break

    # Fallback: Google News RSS si FMP no devuelve nada
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
        except: pass

    return news_alerts


def gpt_advanced_geopolitics(news_list, manual=False):
    if not news_list or not OPENAI_API_KEY: return None
    client = OpenAI(api_key=OPENAI_API_KEY)
    news_text = "\n".join([f"- {n}" for n in news_list])
    if manual:
        prompt = f"Titulares globales:\n{news_text}\nHaz un resumen y dime qué movería el mercado hoy. RESPONDE ESTRICTAMENTE EN ESPAÑOL."
    else:
        prompt = (f"Titulares recientes:\n{news_text}\nAnaliza si hay algo de nivel 'Alto Impacto' (>2%). Si no lo hay, responde 'TRANQUILIDAD'.\nSi lo hay: '⚠️ ALERTA URGENTE: [Resumen] - Impacto en [Acción/Sector]'\nRESPONDE ESTRICTA Y ÚNICAMENTE EN ESPAÑOL.")
    try:
        res = client.chat.completions.create(model="gpt-4o", messages=[{"role": "user", "content": prompt}], max_tokens=300).choices[0].message.content.strip()
        if not manual and ("TRANQUILIDAD" in res.upper() and len(res) < 20): return None
        return res
    except: return None


def generar_reporte_macro_manual():
    """Reporte macro PREMIUM: FMP News + GPT Impact Assessment (GÉNESIS Intelligence)"""

    # Paso 1: Extraer noticias premium de FMP
    fmp_news = _fetch_fmp_news(10)

    if not fmp_news:
        return ("⚠️ <b>Feed de noticias premium temporalmente fuera de línea.</b>\n\n"
                "No se pudieron extraer titulares de FMP en este momento.\n"
                "Intenta en unos minutos.")

    # Paso 2: Preparar los titulares para GPT
    headlines = []
    for article in fmp_news[:6]:
        title = article.get('title', '') or ''
        symbol = article.get('symbol', '') or article.get('tickers', '') or ''
        source = article.get('site', '') or article.get('source', '') or ''
        if title:
            entry = title
            if symbol:
                entry += f" [{symbol}]"
            if source:
                entry += f" (Fuente: {source})"
            headlines.append(entry)

    if not headlines:
        return ("⚠️ <b>Feed de noticias premium temporalmente fuera de línea.</b>\n\n"
                "Los titulares no contienen datos procesables.")

    # Paso 3: GPT Impact Assessment (GÉNESIS Intelligence)
    if not OPENAI_API_KEY:
        # Sin IA: mostrar titulares crudos
        bullets = "\n".join([f"• {h}" for h in headlines[:5]])
        return f"---\n🌎 <b>REPORTE MACROECONÓMICO GLOBAL</b> 🌎\n---\n{bullets}\n---\n📊 Sentimiento: <b>Pendiente (sin IA)</b>"

    news_text = "\n".join([f"{i+1}. {h}" for i, h in enumerate(headlines)])
    prompt = (
        f"Eres GÉNESIS, un sistema de inteligencia de mercados de grado institucional.\n\n"
        f"TITULARES EN VIVO (fuente: Financial Modeling Prep):\n{news_text}\n\n"
        f"INSTRUCCIONES ESTRICTAS:\n"
        f"1. Selecciona las 3 noticias de MAYOR IMPACTO para los mercados globales.\n"
        f"2. Para CADA una, genera EXACTAMENTE este formato:\n\n"
        f"📰 [Titular traducido al español - conciso]\n"
        f"🔥 Nivel de Impacto: [Bajo / Medio / Alto]\n"
        f"🎯 Activos Afectados: [Lista: BTC, NVDA, Oro, Petróleo, etc.]\n"
        f"🧠 [Comentario de 1 línea sobre cómo cambia la dirección del mercado]\n\n"
        f"3. Al final, dictamina:\n"
        f"📊 Sentimiento General del Mercado: [Alcista 🟢 / Bajista 🔴 / Neutral ⚪ / Tenso ⚠️]\n\n"
        f"REGLAS: No inventes noticias. Solo usa los titulares dados. ESPAÑOL ESTRICTO."
    )

    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        res = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=600
        ).choices[0].message.content.strip()
        return f"---\n🌎 <b>REPORTE MACRO — GÉNESIS INTELLIGENCE</b> 🌎\n---\n\n{res}"
    except Exception as e:
        logging.error(f"GPT macro error: {e}")
        bullets = "\n".join([f"• {h}" for h in headlines[:5]])
        return f"---\n🌎 <b>REPORTE MACROECONÓMICO GLOBAL</b> 🌎\n---\n{bullets}\n---\n📊 Sentimiento: <b>Sin determinar</b>"

def fetch_intraday_data(ticker):
    tk = remap_ticker(ticker)
    try:
        safe_check = get_safe_ticker_price(tk)
        if not safe_check: return None

        # Usar Ticker.history() para evitar bugs de MultiIndex
        ticker_obj = yf.Ticker(tk)
        data = ticker_obj.history(period="5d", interval="5m")
        if data.empty: return None

        close_prices = data['Close']; open_prices = data['Open']; volumes = data['Volume']

        vol_type = "Compra 🟢" if float(close_prices.iloc[-1]) >= float(open_prices.iloc[-1]) else "Venta 🔴"
        return {'ticker': tk, 'latest_vol': float(volumes.iloc[-1]), 'avg_vol': float(volumes.mean()), 'vol_type': vol_type, 'latest_price': safe_check['price']}
    except: return None

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
    client = OpenAI(api_key=OPENAI_API_KEY)
    prompt = f"El activo {display_name} rompió su {level_type} en ${fmt_price(price)}. Consejo corto de 1 párrafo: ¿Qué hacer ahora? (Elige y resalta COMPRAR, VENDER o MANTENER) y por qué. ESPAÑOL ESTRICTO."
    try: return client.chat.completions.create(model="gpt-4o", messages=[{"role": "user", "content": prompt}], max_tokens=200).choices[0].message.content
    except: return "¿Qué hacer? Esperar al cierre del día."

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
        return (f"⚠️ <b>Error de conexión con FMP</b>\n\n"
                f"No se pudo obtener el precio de {display_name} "
                f"(símbolo: {fmp_sym}).\n\n"
                f"🔍 <b>Diagnóstico:</b>\n<code>{diag}</code>\n\n"
                f"🔑 Key cargada: {'Sí' if PREMIUM_API_KEY else 'NO'} "
                f"({len(PREMIUM_API_KEY)} chars)\n"
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

    # PASO 3: Prompt blindado anti-alucinación
    price_str = f"${fmt_price(final_price)}" if final_price else "N/A"
    prompt = (
        f"Actúa como un analista financiero institucional senior.\n\n"
        f"ACTIVO: {display_name} ({tk})\n\n"
        f"{tech_block}\n\n"
        f"NOTICIAS RECIENTES:\n{news_str}\n\n"
        f"REGLAS INQUEBRANTABLES:\n"
        f"1. El precio REAL Y VERIFICADO de {display_name} en este momento es {price_str}. Este dato viene DIRECTO del exchange. Tienes PROHIBIDO inventar, adivinar o usar otro precio.\n"
        f"2. Basa tu análisis EXCLUSIVAMENTE en los datos numéricos proporcionados arriba.\n"
        f"3. Si el RSI > 70, indica sobrecompra. Si RSI < 30, indica sobreventa.\n"
        f"4. Analiza la posición del precio respecto a los niveles SMC (Soporte/Resistencia/Order Block).\n"
        f"5. Combina análisis técnico + noticias para dar un VEREDICTO FINAL.\n\n"
        f"FORMATO DE RESPUESTA:\n"
        f"📊 Análisis Técnico: [Tu análisis basado en RSI, MACD, SMC]\n"
        f"📰 Impacto Noticias: [Tu lectura de las noticias]\n"
        f"🎯 VEREDICTO FINAL: [COMPRAR / VENDER / MANTENER] + justificación en 1 línea.\n\n"
        f"RESPONDE ESTRICTAMENTE EN ESPAÑOL."
    )

    if not OPENAI_API_KEY: return "Error: API KEY no configurada."
    try:
        return OpenAI(api_key=OPENAI_API_KEY).chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=600
        ).choices[0].message.content
    except Exception as e: return f"Fallo al analizar: {e}"


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

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    if str(message.chat.id) != str(CHAT_ID): return
    msg = bot.reply_to(message, "👁️ Analizando estructura visual...")
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        base_img = base64.b64encode(bot.download_file(file_info.file_path)).decode('utf-8')
        res = OpenAI(api_key=OPENAI_API_KEY).chat.completions.create(
            model="gpt-4o", messages=[{"role": "user", "content": [{"type": "text", "text": "Analiza gráfica SMC con rigor. Veredicto: 'Bullish' o 'Bearish'. ESPAÑOL ESTRICTO."}, {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base_img}"}}]}], max_tokens=800)
        bot.edit_message_text(f"---\n📊 *REPORTE VISUAL*\n---\n{res.choices[0].message.content}", chat_id=message.chat.id, message_id=msg.message_id)
    except Exception as e: bot.edit_message_text(f"❌ Falló visión: {e}", chat_id=message.chat.id, message_id=msg.message_id)

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
        bot.reply_to(message, "🐳 Memoria HFT Institucional invocada...")
        if not WHALE_MEMORY:
            bot.send_message(message.chat.id, "---\n🐋 *RADAR BALLENAS*\n---\nEl océano está quieto. Sin anomalías detectadas hoy.", parse_mode="HTML")
            return
        lines = ["---", "🐋 *ÚLTIMAS 5 BALLENAS*", "---"]
        for w in list(WHALE_MEMORY)[::-1]:
            is_crypto = '-USD' in w['ticker']
            vol_str = f"${w['vol_approx']:,} USD" if is_crypto else f"{w['vol_approx']:,} unidades"
            lines.append(f"• <b>{get_display_name(w['ticker'])}</b> | Vol: {vol_str} | Tipo: {w['type']} | {int((datetime.now() - w['timestamp']).total_seconds() / 60)} mins ago")
        bot.send_message(message.chat.id, "\n".join(lines), parse_mode="HTML")
        return

    if text == "🌎 Geopolítica":
        bot.reply_to(message, "🌎 Generando Reporte Macro Institucional...")
        report = generar_reporte_macro_manual()
        bot.send_message(message.chat.id, report, parse_mode="HTML")
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
                update_smc_memory(tk, analysis)
                report_lines.extend([f"🏦 <b>{d_name}</b> - ${fmt_price(analysis['price'])}", f"• Tendencia SMC: {analysis['smc_trend']}", f"• Buy-side Liquidity: ${fmt_price(analysis['smc_sup'])}", f"• Sell-side Liquidity: ${fmt_price(analysis['smc_res'])}", f"• Order Block Institucional: ${fmt_price(analysis['order_block'])}", "---"])
            elif tk in LAST_KNOWN_ANALYSIS:
                cached = LAST_KNOWN_ANALYSIS[tk]
                report_lines.extend([f"🏦 <b>{d_name}</b> - ${fmt_price(cached['price'])} <i>(último cierre)</i>", f"• Tendencia SMC: {cached['smc_trend']}", f"• Buy-side Liquidity: ${fmt_price(cached['smc_sup'])}", f"• Sell-side Liquidity: ${fmt_price(cached['smc_res'])}", f"• Order Block Institucional: ${fmt_price(cached['order_block'])}", "---"])
            elif tk in LAST_KNOWN_PRICES:
                report_lines.extend([f"🏦 <b>{d_name}</b> - ${fmt_price(LAST_KNOWN_PRICES[tk]['price'])} <i>(último cierre)</i>", f"• ⏳ Niveles SMC pendientes de cálculo", "---"])
            else:
                report_lines.extend([f"🏦 <b>{d_name}</b>", f"• ⏳ Sin datos disponibles en este momento", "---"])

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

             if add_ticker(tk):
                 bot.reply_to(message, f"---\n✅ *GESTIÓN DE CARTERA*\n---\n✅ [ {display_name} ] añadido al radar SMC.\n\n✅ Guardado en Base de Datos Blindada. Esta información no se borrará aunque el bot se reinicie.", parse_mode="HTML")
             else:
                 if tk == "BZ=F" and raw_input in ["LCO", "BRENT", "PETROLEO"]:
                     bot.reply_to(message, f"✅ LCO (Brent) ya está en tu radar y actualizado con el precio real de $BZ=F.")
                 else:
                     bot.reply_to(message, f"⚠️ El activo {display_name} ya existía en tu radar SMC.")
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
            if not OPENAI_API_KEY:
                continue

            try:
                client = OpenAI(api_key=OPENAI_API_KEY)
                prompt = (
                    f"Actúa como un gestor de riesgos senior de un fondo institucional.\n"
                    f"Analiza esta noticia del activo {display_name} ({tk}):\n"
                    f"Titular: \"{title}\"\n\n"
                    f"REGLAS ESTRICTAS:\n"
                    f"- Si la noticia es NEUTRAL, de relleno, o sin impacto real en el precio, responde EXACTAMENTE: 'NEUTRAL'\n"
                    f"- Si la noticia tiene impacto REAL (positivo o negativo), genera una alerta con este formato:\n"
                    f"  📰 Suceso: [Resumen de 1 línea]\n"
                    f"  💡 Sugerencia: [Vender / Vigilar / Hold / Comprar]\n"
                    f"  ⚡ Impacto: [Alto / Medio]\n"
                    f"RESPONDE EN ESPAÑOL."
                )

                res = client.chat.completions.create(
                    model="gpt-4o",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=200
                ).choices[0].message.content.strip()

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
        if OPENAI_API_KEY:
            try:
                client = OpenAI(api_key=OPENAI_API_KEY)
                prompt = (
                    f"Eres GÉNESIS, un sistema de protección de activos institucional.\n"
                    f"Activo: {display_name} ({tk})\n"
                    f"Precio actual: ${fmt_price(current_price)}\n"
                    f"Movimiento: {pct_change:+.2f}% en las últimas horas\n"
                    f"{smc_context}\n\n"
                    f"Da un VEREDICTO en 1-2 líneas: ¿Mantener, vender parcial, o reforzar posición?\n"
                    f"ESPAÑOL ESTRICTO. Sé directo y profesional."
                )
                res = client.chat.completions.create(
                    model="gpt-4o",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=150
                ).choices[0].message.content.strip()
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

                # Ballenas Doble Verificadas — también protegidas por coherencia
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
                            bot.send_message(CHAT_ID, f"---\n⚠️ *ALERTA DE BALLENA HFT*\n---\nBloque masivo cruzado en <b>{display_name}</b>: {vol_display}.\nPresión Institucional: {intra['vol_type']}{note}", parse_mode="HTML")

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
