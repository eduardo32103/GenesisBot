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
# Canal privado donde el bot fija el backup (puede ser el mismo CHAT_ID o un canal dedicado)
BACKUP_CHAT_ID = os.environ.get('BACKUP_CHAT_ID', CHAT_ID)

if not TELEGRAM_TOKEN or not CHAT_ID:
    logging.critical("Falta TELEGRAM_TOKEN o CHAT_ID. Saliendo.")
    exit()

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

def save_state_to_telegram():
    """Guarda el estado completo como mensaje SILENCIOSO en Telegram"""
    global _last_backup_msg_id
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
                pass  # Si falla editar (msg borrado, etc), enviar uno nuevo

        # Enviar mensaje nuevo SILENCIOSO (sin notificación al usuario)
        msg = bot.send_message(BACKUP_CHAT_ID, backup_text, disable_notification=True)
        _last_backup_msg_id = msg.message_id
        logging.info(f"✅ Backup silencioso enviado (msg_id: {_last_backup_msg_id})")

    except Exception as e:
        logging.error(f"Error guardando backup en Telegram: {e}")

def restore_state_from_telegram():
    """Intenta recuperar el estado desde mensajes recientes de Telegram"""
    global _last_backup_msg_id
    try:
        # Verificar si la DB local ya tiene datos
        existing = get_tracked_tickers()
        if existing:
            logging.info(f"DB local tiene {len(existing)} activos. No se necesita restaurar.")
            return True

        logging.info("DB local vacía. Buscando backup en Telegram...")

        # Buscar en las últimas actualizaciones del bot
        # Método: usar getUpdates para buscar mensajes con el prefijo
        updates = bot.get_updates(limit=100, timeout=5)
        for update in reversed(updates):  # Del más reciente al más antiguo
            msg = update.message or update.edited_message
            if msg and msg.text and msg.text.startswith(BACKUP_PREFIX):
                b64_data = msg.text.replace(BACKUP_PREFIX, "").strip()
                _restore_from_b64(b64_data)
                _last_backup_msg_id = msg.message_id
                logging.info(f"✅ RESTAURACIÓN AUTOMÁTICA EXITOSA desde Telegram (msg_id: {msg.message_id})")
                return True

        # Si no encontramos en updates, buscar con el método de Telegram
        # Intentar leer mensajes fijados del chat
        try:
            chat_info = bot.get_chat(BACKUP_CHAT_ID)
            pinned = chat_info.pinned_message
            if pinned and pinned.text and pinned.text.startswith(BACKUP_PREFIX):
                b64_data = pinned.text.replace(BACKUP_PREFIX, "").strip()
                _restore_from_b64(b64_data)
                _last_backup_msg_id = pinned.message_id
                logging.info("✅ RESTAURACIÓN desde mensaje fijado exitosa!")
                return True
        except Exception:
            pass

        # Último recurso: cargar desde portfolio.json del repositorio
        return _restore_from_repo_json()

    except Exception as e:
        logging.error(f"Error en restauración desde Telegram: {e}")
        return _restore_from_repo_json()

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
    return tk

def get_display_name(ticker_key):
    mapping = {
        "BZ=F": "LCO (Petróleo Brent)",
        "GC=F": "Oro (Gold)",
        "BTC-USD": "BTC (Bitcoin)"
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
    save_state_to_telegram()
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
    # Hasta 6 decimales de precisión, eliminar ceros sobrantes
    s = f"{val:.6f}".rstrip('0')
    # Asegurar mínimo 2 decimales para legibilidad
    parts = s.split('.')
    if len(parts) == 2 and len(parts[1]) < 2:
        s = f"{val:.2f}"
    elif s.endswith('.'):
        s = f"{val:.2f}"
    return s

def _try_ticker_history(symbol, period="1d", interval="1m"):
    """Obtiene precio con precisión RAW del exchange. Prioriza info['regularMarketPrice']"""
    try:
        ticker_obj = yf.Ticker(symbol)

        # PRIORIDAD 1: regularMarketPrice (precio exacto del exchange, sin redondeo)
        try:
            info = ticker_obj.info
            raw_price = info.get('regularMarketPrice') or info.get('currentPrice')
            raw_vol = info.get('regularMarketVolume') or info.get('volume') or 0
            if raw_price and float(raw_price) > 0:
                return {'price': float(raw_price), 'vol': float(raw_vol)}
        except Exception:
            pass

        # PRIORIDAD 2: history() como fallback (datos históricos)
        data = ticker_obj.history(period=period, interval=interval)
        if data.empty: return None

        price = float(data['Close'].iloc[-1])
        vol = float(data['Volume'].iloc[-1])

        if price <= 0: return None
        return {'price': price, 'vol': vol}
    except Exception as e:
        logging.debug(f"_try_ticker_history({symbol}): {e}")
        return None

def _sanity_check_price(tk, new_price):
    """Verifica que el precio no sea basura (desviación >50% vs último conocido = error de API)"""
    if tk in LAST_KNOWN_PRICES:
        last_price = LAST_KNOWN_PRICES[tk]['price']
        if last_price > 0:
            change_pct = abs(new_price - last_price) / last_price
            if change_pct > 0.50:  # Desviación de más del 50% = imposible en un tick de 30s
                logging.warning(f"⚠️ SANITY CHECK FALLIDO para {tk}: ${new_price:.2f} vs último ${last_price:.2f} (desviación: {change_pct*100:.1f}%). Posible error de API.")
                return False
    # Si el precio es absurdamente bajo para activos conocidos
    if new_price < 0.50:
        logging.warning(f"⚠️ SANITY CHECK: {tk} precio ${new_price:.4f} demasiado bajo. Rechazado.")
        return False
    return True

# Sufijos de mercado para reintento cuando Yahoo falla con el ticker base
MARKET_SUFFIX_RETRIES = {
    "IXC": ["IXC"],       # NYSE Arca - usar sin sufijo, ya es correcto
    "BNO": ["BNO"],       # NYSE Arca
    "IAU": ["IAU"],       # NYSE Arca
    "NVDA": ["NVDA"],     # NASDAQ
}

def get_safe_ticker_price(ticker, force_validation=False):
    """Descarga precio con validación anti-basura, reintentos y fallback para commodities"""
    tk = remap_ticker(ticker)

    # --- CASO ESPECIAL: BRENT (cadena de fallback BZ=F → CO=F → BNO) ---
    if tk == "BZ=F":
        for fallback_symbol in BRENT_FALLBACK_CHAIN:
            result = _try_ticker_history(fallback_symbol)
            if result and result['price'] >= BRENT_MIN_VALID_PRICE:
                logging.info(f"Brent OK vía {fallback_symbol}: ${result['price']:.2f}")
                LAST_KNOWN_PRICES[tk] = result
                return result
            elif result:
                logging.warning(f"Brent rechazado vía {fallback_symbol}: ${result['price']:.2f} (< ${BRENT_MIN_VALID_PRICE})")

        for fallback_symbol in BRENT_FALLBACK_CHAIN:
            result = _try_ticker_history(fallback_symbol, period="5d", interval="1d")
            if result and result['price'] >= BRENT_MIN_VALID_PRICE:
                logging.info(f"Brent (cierre diario) OK vía {fallback_symbol}: ${result['price']:.2f}")
                LAST_KNOWN_PRICES[tk] = result
                return result

        if tk in LAST_KNOWN_PRICES:
            logging.warning(f"Brent: usando último precio conocido del cache: ${LAST_KNOWN_PRICES[tk]['price']:.2f}")
            return LAST_KNOWN_PRICES[tk]

        logging.error("Brent: TODAS las fuentes fallaron.")
        return None

    # --- CASO GENERAL: cualquier otro ticker ---
    # Intento 1: Ticker.history() con intervalo 1m
    result = _try_ticker_history(tk)
    if result and _sanity_check_price(tk, result['price']):
        LAST_KNOWN_PRICES[tk] = result
        return result
    elif result:
        logging.warning(f"{tk}: precio 1m falló sanity check (${result['price']:.4f}).")

    # Intento 2: datos diarios (más estables, mercado cerrado)
    result = _try_ticker_history(tk, period="5d", interval="1d")
    if result and _sanity_check_price(tk, result['price']):
        LAST_KNOWN_PRICES[tk] = result
        return result

    # Intento 3: reintentar con sufijos de mercado alternativos
    for alt_sym in MARKET_SUFFIX_RETRIES.get(tk, []):
        if alt_sym != tk:
            result = _try_ticker_history(alt_sym, period="5d", interval="1d")
            if result and _sanity_check_price(tk, result['price']):
                logging.info(f"{tk}: precio corregido vía {alt_sym}: ${result['price']:.2f}")
                LAST_KNOWN_PRICES[tk] = result
                return result

    # Devolver cache si existe (precio conocido y confiable)
    if tk in LAST_KNOWN_PRICES:
        logging.warning(f"{tk}: usando último precio conocido: ${LAST_KNOWN_PRICES[tk]['price']:.2f}")
        return LAST_KNOWN_PRICES[tk]

    # Primera carga: aceptar el resultado si pasó > $0.50
    if result and result['price'] > 0.50:
        LAST_KNOWN_PRICES[tk] = result
        return result

    return None

def verify_1m_realtime_data(ticker):
    return get_safe_ticker_price(ticker)


def check_geopolitical_news():
    search_url = "https://news.google.com/rss/search?q=geopolitics+OR+Trump+OR+rates+OR+war+OR+economy"
    HIGH_IMPACT_KEYWORDS = ["war", "attack", "strike", "escalation", "missile", "sanction", "embargo", "explosion", "guerra", "ataque", "tensión", "misil", "sanciones", "rates", "fed", "trump", "powell"]
    news_alerts = []
    try:
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
    """Reporte macro dedicado para el BOTÓN MANUAL. Siempre devuelve contenido útil."""
    # Paso 1: Recoger noticias amplias (sin filtro de keywords para no perder contexto)
    all_news = []
    try:
        # Fuente 1: Google News RSS amplio (mercados + geopolítica)
        for query in ["stock+market+today", "geopolitics+economy+2026", "oil+gold+bitcoin+market"]:
            url = f"https://news.google.com/rss/search?q={query}&hl=es"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                root = ET.fromstring(resp.text)
                for item in root.findall('.//item')[:3]:
                    title = item.find('title').text
                    if title and title not in all_news:
                        all_news.append(title)
    except: pass

    # Fuente 2: noticias de activos del usuario vía yfinance
    try:
        for tk in get_tracked_tickers()[:3]:
            ticker_obj = yf.Ticker(remap_ticker(tk))
            for n in (ticker_obj.news or [])[:2]:
                title = n.get('title', '')
                if title and title not in all_news:
                    all_news.append(title)
    except: pass

    # Fallback: usar las noticias filtradas originales
    if not all_news:
        all_news = check_geopolitical_news()

    if not all_news:
        return ("---\n🌎 <b>REPORTE MACROECONÓMICO GLOBAL</b> 🌎\n---\n"
                "• No se detectaron titulares relevantes en este momento.\n"
                "• Los mercados parecen operar sin catalizadores nuevos.\n"
                "• Recomendación: Mantener posiciones actuales.\n"
                "---\n📊 Sentimiento General: <b>Neutral</b>")

    # Paso 2: Prompt estructurado a GPT
    if not OPENAI_API_KEY:
        bullets = "\n".join([f"• {n}" for n in all_news[:5]])
        return f"---\n🌎 <b>REPORTE MACROECONÓMICO GLOBAL</b> 🌎\n---\n{bullets}\n---\n📊 Sentimiento General: <b>Pendiente</b>"

    news_text = "\n".join([f"- {n}" for n in all_news[:8]])
    prompt = (f"Eres un analista macro institucional. Basado en estos titulares recientes:\n{news_text}\n\n"
              f"Genera un REPORTE MACRO estructurado con EXACTAMENTE este formato:\n"
              f"• [Análisis de la noticia más relevante y su impacto en mercados]\n"
              f"• [Segunda noticia relevante y sectores afectados]\n"
              f"• [Tercera noticia o tendencia macro global]\n\n"
              f"Al final, dictamina el SENTIMIENTO GENERAL del mercado: Alcista, Bajista, Neutral o Tenso.\n"
              f"RESPONDE ESTRICTAMENTE EN ESPAÑOL. Sé conciso y directo.")
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        res = client.chat.completions.create(model="gpt-4o", messages=[{"role": "user", "content": prompt}], max_tokens=400).choices[0].message.content.strip()
        return f"---\n🌎 <b>REPORTE MACROECONÓMICO GLOBAL</b> 🌎\n---\n{res}"
    except:
        bullets = "\n".join([f"• {n}" for n in all_news[:5]])
        return f"---\n🌎 <b>REPORTE MACROECONÓMICO GLOBAL</b> 🌎\n---\n{bullets}\n---\n📊 Sentimiento General: <b>Sin determinar</b>"

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
    tech_info = f"Información técnica no disponible para {display_name}."
    tech = fetch_and_analyze_stock(tk)
    if tech: tech_info = f"Precio: ${fmt_price(tech['price'])}\nRSI: {tech['rsi']:.2f}\nSMC Trend: {tech['smc_trend']}"

    news_str = ""
    try:
        news_str = "\n".join([f"- {n.get('title', '')}" for n in yf.Ticker(tk).news[:3]])
    except: pass

    prompt = (f"Analiza profundamente '{display_name}'.\nTécnicos:\n{tech_info}\n\nNoticias:\n{news_str}\n" "Combina enfoques. Dictamina un VEREDICTO FINAL resaltado: 'COMPRAR', 'VENDER' o 'MANTENER/ESPERAR'. ESPAÑOL ESTRICTO.")
    if not OPENAI_API_KEY: return "Error: API KEY INCORRECTA."
    try: return OpenAI(api_key=OPENAI_API_KEY).chat.completions.create(model="gpt-4o", messages=[{"role": "user", "content": prompt}], max_tokens=600).choices[0].message.content
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
    while True:
        try:
            time.sleep(30)
            now = datetime.now()
            purge_old_events()
            sentinel_tick_counter += 1

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
