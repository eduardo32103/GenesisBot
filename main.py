import os, pg8000.dbapi, telebot
import html
import ssl
import urllib.parse
import logging
import base64
import requests
import re
import xml.etree.ElementTree as ET
import pandas as pd
from google import genai
# yfinance ELIMINADO â€” Todo via FMP Pro
import threading
import time
import json
import hashlib
import unicodedata
import math
from collections import deque
from telebot.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from datetime import datetime, timedelta
from PIL import Image, ImageDraw, ImageFont

# Configuración extendida de logs para Railway
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY') # Volvemos a requerir OpenAI para visión
# Canal privado donde el bot fija el backup (puede ser el mismo CHAT_ID o un canal dedicado)
BACKUP_CHAT_ID = os.environ.get('BACKUP_CHAT_ID', CHAT_ID)
FMP_API_KEY = "".join(c for c in os.environ.get('FMP_API_KEY', '') if ord(c) < 128).strip()

if not TELEGRAM_TOKEN or not CHAT_ID:
    logging.critical("Falta TELEGRAM_TOKEN o CHAT_ID. Saliendo.")
    exit()

if not os.environ.get('FMP_API_KEY'):
    logging.warning("⚠️ FMP_API_KEY no configurada. El motor de precios FMP no funcionará.")
else:
    logging.info(f"✅ FMP_API_KEY cargada correctamente ({len(os.environ.get('FMP_API_KEY'))} caracteres).")

bot = telebot.TeleBot(TELEGRAM_TOKEN)

MENU_GEOPOLITICS = "🌍 Geopolítica"
MENU_WHALES = "🐋 Radar de Ballenas"
MENU_SMC = "🦅 Niveles SMC"
MENU_WALLET = "💼 Mi Cartera"

_MOJIBAKE_PATTERN = re.compile(r'(?:[ÃÂâðÅï]|[\u0080-\u00ff\u0152-\u0178\u2013-\u203a\u20ac\u2122]){2,}')
_MOJIBAKE_REPLACEMENTS = {
    "\xf0\u0178\u2018\x81\xef\xb8\x8f": "👁️",
    "\xf0\u0178\x8f\xa6": "🏦",
    "\xf0\u0178\xa7\xa0": "🧠",
    "\xf0\u0178\x90\u2039": "🐋",
    "\xf0\u0178\u0178\xa1": "🟡",
    "\xe2\u20ac\xa2": "•",
    "\xe2\u20ac\u201d": "—",
    "\U0001f512\u201e": "🔒",
    "\U0001f512\x8d": "🔍",
    "\u26a0\ufe0f\x8f": "⚠️",
    "\U0001f525\xb8": "🔥",
    "\U0001f525\xb5": "💰",
    "\U0001f534\xb4": "🔴",
    "\U0001f310\u0160": "🌐",
}


def _decode_mojibake_segment(segment):
    raw_bytes = bytearray()
    for ch in segment:
        codepoint = ord(ch)
        if codepoint <= 255:
            raw_bytes.append(codepoint)
            continue
        try:
            raw_bytes.extend(ch.encode('cp1252'))
        except Exception:
            return segment

    try:
        return raw_bytes.decode('utf-8')
    except Exception:
        return segment


def _clean_outgoing_text(text):
    if not isinstance(text, str) or not text:
        return text

    backup_prefix = globals().get("BACKUP_PREFIX")
    if backup_prefix and text.startswith(backup_prefix):
        return text

    cleaned = text
    for _ in range(2):
        updated = _MOJIBAKE_PATTERN.sub(lambda match: _decode_mojibake_segment(match.group(0)), cleaned)
        if updated == cleaned:
            break
        cleaned = updated

    for bad, good in _MOJIBAKE_REPLACEMENTS.items():
        cleaned = cleaned.replace(bad, good)

    for control_char in ("\u008f", "\u0090", "\u0081", "\u009d"):
        cleaned = cleaned.replace(control_char, "")

    return cleaned


def _wrap_bot_text_methods():
    original_send_message = bot.send_message
    original_reply_to = bot.reply_to
    original_edit_message_text = bot.edit_message_text
    original_answer_callback_query = bot.answer_callback_query

    def send_message_wrapper(*args, **kwargs):
        args = list(args)
        if len(args) >= 2:
            args[1] = _clean_outgoing_text(args[1])
        elif 'text' in kwargs:
            kwargs['text'] = _clean_outgoing_text(kwargs['text'])
        return original_send_message(*args, **kwargs)

    def reply_to_wrapper(*args, **kwargs):
        args = list(args)
        if len(args) >= 2:
            args[1] = _clean_outgoing_text(args[1])
        elif 'text' in kwargs:
            kwargs['text'] = _clean_outgoing_text(kwargs['text'])
        return original_reply_to(*args, **kwargs)

    def edit_message_text_wrapper(*args, **kwargs):
        args = list(args)
        if args:
            args[0] = _clean_outgoing_text(args[0])
        elif 'text' in kwargs:
            kwargs['text'] = _clean_outgoing_text(kwargs['text'])
        return original_edit_message_text(*args, **kwargs)

    def answer_callback_query_wrapper(*args, **kwargs):
        args = list(args)
        if len(args) >= 2:
            args[1] = _clean_outgoing_text(args[1])
        elif 'text' in kwargs and kwargs['text'] is not None:
            kwargs['text'] = _clean_outgoing_text(kwargs['text'])
        return original_answer_callback_query(*args, **kwargs)

    bot.send_message = send_message_wrapper
    bot.reply_to = reply_to_wrapper
    bot.edit_message_text = edit_message_text_wrapper
    bot.answer_callback_query = answer_callback_query_wrapper


_wrap_bot_text_methods()

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
    # Si ya hay conexión activa, verificarla
    if _global_db_conn is not None:
        try:
            c = _global_db_conn.cursor()
            c.execute("SELECT 1")
            c.fetchone()
            return _global_db_conn
        except:
            print("DEBUG DB: Conexión existente caída, reconectando...")
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
            print(f"✅ Conexión exitosa a Supabase (intento {attempt}/3)")

            # Crear tablas si no existen
            cr = _global_db_conn.cursor()
            cr.execute("CREATE TABLE IF NOT EXISTS wallet (user_id BIGINT, ticker TEXT, is_investment INTEGER DEFAULT 0, amount_usd REAL DEFAULT 0.0, entry_price REAL DEFAULT 0.0, timestamp TEXT, PRIMARY KEY (user_id, ticker))")
            cr.execute("CREATE TABLE IF NOT EXISTS global_stats (key TEXT PRIMARY KEY, value REAL)")
            cr.execute("CREATE TABLE IF NOT EXISTS seen_events (hash_id TEXT PRIMARY KEY, timestamp TEXT)")
            _global_db_conn.commit()

            return _global_db_conn

        except Exception as e:
            print(f"âŒ Error de conexión a Supabase (intento {attempt}/3): {e}")
            _global_db_conn = None
            if attempt < 3:
                wait = attempt * 2  # 2s, 4s
                print(f"DEBUG DB: Reintentando en {wait}s...")
                time.sleep(wait)

    print("âŒ FATAL: No se pudo conectar a Supabase después de 3 intentos")
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

def gpt_advanced_geopolitics_v2(news_list, manual=False):
    if not news_list or not OPENAI_API_KEY:
        return None

    from openai import OpenAI

    client = OpenAI(api_key=OPENAI_API_KEY)
    news_text = "\n".join([f"- {n}" for n in news_list])
    wallet_tickers = ", ".join(get_display_name(tk) for tk in get_tracked_tickers()) or "Sin activos en radar"

    try:
        if manual:
            prompt = (
                "Eres GÉNESIS, un analista macro y geopolítico enfocado en mercados financieros.\n\n"
                f"Cartera vigilada: {wallet_tickers}\n\n"
                f"Titulares recientes:\n{news_text}\n\n"
                "Responde en ESPAÑOL con este formato exacto:\n"
                "🌍 RESUMEN: [2-3 líneas con lo que realmente importa hoy].\n"
                "🎯 IMPACTO EN CARTERA: [qué activos o sectores de la cartera podrían verse afectados y por qué].\n"
                "⚠️ RIESGO PRIORITARIO: [evento dominante + dirección probable del impacto].\n"
                "🧭 ACCIÓN TÁCTICA: [Mantener / Vigilar de cerca / Reducir exposición / Aprovechar oportunidad] - [razón breve]."
            )
        else:
            prompt = (
                "Eres GÉNESIS, un centinela de riesgo macro para una cartera accionaria.\n\n"
                f"Cartera vigilada: {wallet_tickers}\n\n"
                f"Titulares recientes:\n{news_text}\n\n"
                "Tu tarea es decidir si alguno de estos titulares amerita una alerta push inmediata.\n"
                "Si NO hay un evento con impacto operativo alto, responde EXACTAMENTE: TRANQUILIDAD\n"
                "Si SÍ lo hay, responde EXACTAMENTE en una sola línea y en español:\n"
                "⚠️ ALERTA GEOPOLÍTICA: [evento clave]. Impacto probable: [sector/mercado]. Cartera afectada: [tickers o sectores de la cartera]. Acción sugerida: [Vigilar/Reducir/Aprovechar]."
            )

        prompt = (
            "Actúa como un analista técnico institucional. Este análisis es educativo y no es asesoría financiera.\n\n"
            "Debes analizar la gráfica con enfoque Smart Money Concepts y, SI Y SOLO SI SON VISIBLES, incorporar estos indicadores: RSI, MACD, volumen, EMA 50, EMA 200, SMA 50, SMA 200, retrocesos de Fibonacci, golden pocket, bandas de Bollinger, canales de Donchian y OBV.\n"
            "Si un indicador no se ve o no puede leerse con claridad, escribe exactamente: No visible. No inventes datos.\n"
            "No expliques teoría; entrega lectura operativa.\n\n"
            "Responde exactamente en este formato:\n"
            "📊 CONTEXTO TÉCNICO: [tendencia, estructura, liquidez, BOS/CHoCH/FVG/OB si son visibles].\n"
            "📐 INDICADORES: RSI [lectura o No visible]; MACD [lectura o No visible]; Volumen [lectura o No visible]; EMA50/EMA200 [lectura o No visible]; SMA50/SMA200 [lectura o No visible]; Fibonacci/golden pocket [lectura o No visible]; Bollinger [lectura o No visible]; Donchian [lectura o No visible]; OBV [lectura o No visible].\n"
            "🎯 NIVELES CLAVE: [soportes, resistencias, order blocks, golden pocket y niveles exactos si se pueden leer].\n"
            "⚠️ RIESGO DE INVERSIÓN: [Bajo / Medio / Alto] - [razón técnica directa].\n"
            "⚖️ SESGO DIRECCIONAL: [Fuerte Alcista / Alcista / Neutral / Bajista / Fuerte Bajista / Esperar Confirmación] - [justificación breve y operable]."
        )

        res = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=600
        ).choices[0].message.content.strip()

        if not manual and res.strip().upper() == "TRANQUILIDAD":
            return None

        return res
    except Exception as e:
        logging.debug(f"gpt_advanced_geopolitics_v2 error: {e}")
        return None


def _format_geopolitics_news_for_ai(news_items, wallet_tickers):
    lines = []
    for idx, item in enumerate(news_items, start=1):
        if isinstance(item, dict):
            title = item.get("title_es") or item.get("title") or ""
            source = item.get("source") or item.get("site") or "Fuente no disponible"
            sentiment = (item.get("sentiment") or {}).get("label", "Neutral")
            affected = item.get("affected_tickers") or []
            affected_names = ", ".join(get_display_name(tk) for tk in affected) if affected else "Sin cruce directo"
            lines.append(f"{idx}. {title} | Fuente: {source} | Sentimiento: {sentiment} | Cartera: {affected_names}")
        else:
            title = str(item or "").strip()
            if not title:
                continue
            affected = _extract_mentioned_tickers_plus(title, wallet_tickers)
            affected_names = ", ".join(get_display_name(tk) for tk in affected) if affected else "Sin cruce directo"
            lines.append(f"{idx}. {title} | Cartera: {affected_names}")
    return "\n".join(lines)


def gpt_advanced_geopolitics_v3(news_items, manual=False):
    if not news_items or not OPENAI_API_KEY:
        return None

    from openai import OpenAI

    client = OpenAI(api_key=OPENAI_API_KEY)
    wallet_tickers = get_tracked_tickers()
    wallet_names = ", ".join(get_display_name(tk) for tk in wallet_tickers) or "Sin activos en radar"
    news_text = _format_geopolitics_news_for_ai(news_items, wallet_tickers)

    try:
        if manual:
            prompt = (
                "Eres GÉNESIS, un analista macro-geopolítico de mercados que piensa como mesa institucional.\n\n"
                f"Cartera vigilada: {wallet_names}\n\n"
                f"Noticias más influyentes ahora:\n{news_text}\n\n"
                "Analiza únicamente con fundamento y sin relleno. Debes conectar cada noticia con sectores, liquidez, tasas, commodities, defensa, energía, semiconductores, cripto o growth si aplica.\n"
                "Responde EXACTAMENTE en este formato:\n"
                "🌍 CATALIZADORES CLAVE:\n"
                "1. [noticia más influyente + por qué importa al mercado]\n"
                "2. [segunda noticia + por qué importa]\n"
                "3. [tercera noticia + por qué importa]\n"
                "🎯 IMPACTO EN MI CARTERA:\n"
                "• [ticker/sector] -> [alcista/bajista/mixto] | probabilidad [X]% | [mecánica del impacto]\n"
                "• [ticker/sector] -> [alcista/bajista/mixto] | probabilidad [X]% | [mecánica del impacto]\n"
                "🛡️ PROTECCIÓN GÉNESIS:\n"
                "• [riesgo principal a vigilar]\n"
                "• [qué confirmación invalidaría el escenario]\n"
                "• [acción táctica sugerida para proteger capital]\n"
                "⚖️ VEREDICTO FINAL: [Mantener / Vigilar de cerca / Reducir exposición / Aprovechar oportunidad] | Confianza [X]% | [tesis final en 2 líneas]."
            )
        else:
            prompt = (
                "Eres GÉNESIS, un centinela de riesgo macro para una cartera accionaria.\n\n"
                f"Cartera vigilada: {wallet_names}\n\n"
                f"Noticias recientes:\n{news_text}\n\n"
                "Debes decidir si existe una alerta geopolítica realmente operable para la cartera.\n"
                "Si NO hay catalizador con impacto alto y accionable, responde EXACTAMENTE: TRANQUILIDAD\n"
                "Si SÍ lo hay, responde EXACTAMENTE en una sola línea y en español:\n"
                "⚠️ ALERTA GEOPOLÍTICA: [evento clave]. Cartera afectada: [tickers/sectores]. Sesgo: [alcista/bajista/mixto]. Probabilidad: [X]%. Acción sugerida: [Vigilar/Reducir/Aprovechar]."
            )

        res = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=700
        ).choices[0].message.content.strip()

        if not manual and res.strip().upper() == "TRANQUILIDAD":
            return None

        return res
    except Exception as e:
        logging.debug(f"gpt_advanced_geopolitics_v3 error: {e}")
        return None


# =====================================================================
# PERSISTENCIA REAL: TELEGRAM COMO BASE DE DATOS
# El bot guarda el estado completo de la cartera como un mensaje
# en Telegram. Railway no puede borrar mensajes de Telegram.
# =====================================================================
BACKUP_PREFIX = "🔒GENESIS_BACKUP_V2🔒"
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
            logging.info("ðŸ“Œ Backup fijado en el chat.")
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

    logging.info("🔒„ DB local vacía o sin inversiones. Buscando backup...")

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

    logging.info("Restauración remota segura agotada. Se omite get_updates para evitar conflictos 409 con polling.")
    return _restore_from_repo_json()

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
    logging.warning("No se encontró respaldo remoto. Intentando restauración local desde portfolio.json.")
    return _restore_from_repo_json()

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
                    INSERT INTO wallet (user_id, ticker, is_investment, amount_usd, entry_price, timestamp) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id, ticker) DO UPDATE SET 
                    is_investment = EXCLUDED.is_investment, amount_usd = EXCLUDED.amount_usd, 
                    entry_price = EXCLUDED.entry_price, timestamp = EXCLUDED.timestamp
                ''', (int(CHAT_ID), tk, int(info.get("is_investment", 0)), float(info.get("amount_usd", 0)), float(info.get("entry_price", 0)), info.get("timestamp", datetime.now().isoformat())))

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

    logging.warning("⚠️ No se encontró ningún respaldo. Cartera inicia vacía.")
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
    if not conn:
        print("DEBUG WALLET: Sin conexión a DB, retornando lista vacía")
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
    logging.info("🔒„ PnL mensual reseteado a $0.00")

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
    save_state_to_telegram()
    logging.info("⚠️ RESET TOTAL ejecutado: inversiones y PnL eliminados")


WHALE_MEMORY = deque(maxlen=5)
SMC_LEVELS_MEMORY = {}
LAST_KNOWN_PRICES = {}  # Cache de último precio válido por ticker
LAST_KNOWN_ANALYSIS = {}  # Cache de último análisis SMC completo por ticker
last_whale_alert = {} # Memoria Anti-Spam para Ballenas
WHALE_HISTORY_DB = {} # Acumulador de flujos 24H

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

# === MOTOR FMP (Financial Modeling Prep) â€” FUENTE ÚNICA DE PRECIOS ===
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
                    changes_pct = float(quote.get('changesPercentage', 0) or 0)
                    pe = float(quote.get('pe', 0) or 0)
                    market_cap = float(quote.get('marketCap', 0) or 0)
                    name = quote.get('name') or quote.get('companyName') or ''
                    if price > 0:
                        logging.info(f"FMP OK {tk} ({symbol}): ${fmt_price(price)}")
                        return {
                            'price': price,
                            'vol': volume,
                            'volume': volume,
                            'avgVolume': avg_volume,
                            'change': change,
                            'changesPercentage': changes_pct,
                            'pe': pe,
                            'marketCap': market_cap,
                            'name': name,
                        }
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


def _parse_fmp_historical_payload(raw):
    hist = []
    if isinstance(raw, list):
        hist = raw
    elif isinstance(raw, dict):
        for key in ("historical", "data", "results", "prices"):
            if isinstance(raw.get(key), list):
                hist = raw[key]
                break

    cleaned = []
    for row in hist:
        if not isinstance(row, dict):
            continue
        if row.get("date") or row.get("label"):
            cleaned.append(row)

    cleaned.sort(key=lambda item: str(item.get("date") or item.get("label") or ""), reverse=True)
    return cleaned


def _fetch_fmp_historical_eod(ticker, limit=None):
    tk = remap_ticker(ticker)
    if not FMP_API_KEY:
        _FMP_LAST_ERROR[tk] = "FMP_API_KEY no detectada."
        return None

    symbol = _get_fmp_symbol(tk)
    if _is_crypto_ticker(tk):
        symbol = tk.replace("-USD", "") + "USD"

    safe_symbol = urllib.parse.quote(str(symbol).strip().upper())
    endpoints = [
        ("stable", f"https://financialmodelingprep.com/stable/historical-price-eod/full?symbol={safe_symbol}&apikey={FMP_API_KEY}"),
        ("legacy", f"https://financialmodelingprep.com/api/v3/historical-price-full/{safe_symbol}?apikey={FMP_API_KEY}"),
    ]

    last_status = None
    for endpoint_name, url in endpoints:
        try:
            resp = requests.get(url, timeout=10)
            last_status = resp.status_code
            logging.debug(f"FMP historical {endpoint_name} status {resp.status_code} para {tk}")
            if resp.status_code != 200:
                continue

            hist = _parse_fmp_historical_payload(resp.json())
            if hist:
                return hist[:int(limit)] if limit else hist
        except Exception as e:
            logging.debug(f"FMP historical {endpoint_name} error para {tk}: {e}")

    if last_status in (401, 403):
        _FMP_LAST_ERROR[tk] = f"Histórico FMP no disponible para {tk}: HTTP {last_status}"
        logging.warning(f"FMP histórico restringido para {tk} (HTTP {last_status}). Se usará fallback local.")
    else:
        _FMP_LAST_ERROR[tk] = f"Histórico FMP no disponible para {tk}"
    return None


def _sanity_check_price(tk, new_price):
    """Verifica que el precio no sea basura (desviación >50% vs último conocido)"""
    if tk in LAST_KNOWN_PRICES:
        last_price = LAST_KNOWN_PRICES[tk]['price']
        if last_price > 0:
            change_pct = abs(new_price - last_price) / last_price
            if change_pct > 0.50:
                logging.warning(f"⚠️ SANITY CHECK FALLIDO para {tk}: ${new_price:.2f} vs último ${last_price:.2f} ({change_pct*100:.1f}%)")
                return False
    if not _is_crypto_ticker(tk) and new_price < 0.50:
        logging.warning(f"⚠️ SANITY CHECK: {tk} precio ${new_price:.4f} demasiado bajo.")
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
    if not FMP_API_KEY:
        return []

    all_news = []

    tracked = [remap_ticker(tk) for tk in get_tracked_tickers()]
    tracked_symbols = []
    for tk in tracked:
        if _is_crypto_ticker(tk):
            tracked_symbols.append(tk.replace("-USD", "") + "USD")
        elif tk not in {"BZ=F", "GC=F"}:
            tracked_symbols.append(_get_fmp_symbol(tk))

    default_tickers = ["SPY", "QQQ", "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META", "BTCUSD"]
    symbols = []
    seen = set()
    for ticker in tracked_symbols + default_tickers:
        safe_ticker = str(ticker or "").strip().upper()
        if not safe_ticker or safe_ticker in seen:
            continue
        seen.add(safe_ticker)
        symbols.append(safe_ticker)

    for ticker in symbols:
        try:
            url = f"https://financialmodelingprep.com/stable/stock-news?symbol={ticker}&limit=2&apikey={FMP_API_KEY}"
            resp = requests.get(url, timeout=5)
            logging.info(f"LOG FMP [stable/stock-news {ticker}]: Status {resp.status_code}")
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    all_news.extend(data)
            if len(all_news) >= limit:
                break
        except Exception as e:
            logging.debug(f"FMP stock-news fallback error for {ticker}: {e}")

    unique = []
    seen = set()
    for article in all_news:
        title = (article.get('title') or '').strip().lower()
        if not title or title in seen:
            continue
        seen.add(title)
        unique.append(article)

    return unique[:limit]
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
    """Monitor automático unificado: FMP news + sentiment + wallet cross-reference"""
    try:
        HIGH_IMPACT_KEYWORDS = ["war", "attack", "strike", "escalation", "missile", "sanction",
                                "embargo", "explosion", "guerra", "ataque", "tensión", "misil",
                                "sanciones", "rates", "fed", "trump", "powell", "crash", "recession",
                                "tariff", "default", "crisis"]
        HIGH_IMPACT_KEYWORDS.extend(["tariffs", "inflation", "cpi", "ppi", "yield", "yields",
                                     "china", "taiwan", "iran", "israel", "opec", "oil",
                                     "export control", "chips", "jobs", "treasury"])
        news_alerts = []
        wallet_tickers = get_tracked_tickers()

        fmp_news = _fetch_fmp_news_with_sentiment(18)
        if fmp_news:
            ranked_news = []
            for article in fmp_news:
                title = article.get('title', '') or article.get('text', '') or ''
                if not title:
                    continue

                title_upper = title.upper()
                keyword_hit = any(kw.upper() in title_upper for kw in HIGH_IMPACT_KEYWORDS)
                wallet_hit = bool(_extract_mentioned_tickers_plus(title, wallet_tickers))
                magnitude = abs((article.get('sentiment') or {}).get('raw', 0.0))
                score = magnitude + (1.5 if keyword_hit else 0) + (1.5 if wallet_hit else 0)
                if score <= 0:
                    continue
                ranked_news.append((score, title))

            for _, title in sorted(ranked_news, key=lambda item: item[0], reverse=True)[:5]:
                news_alerts.append(title)

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
    wallet_tickers = ", ".join(get_display_name(tk) for tk in get_tracked_tickers()) or "Sin activos en radar"
    if manual:
        prompt = f"Titulares globales:\n{news_text}\nHaz un resumen y dime qué movería el mercado hoy. RESPONDE ESTRICTAMENTE EN ESPAÑOL."
    else:
        prompt = (f"Titulares recientes:\n{news_text}\nAnaliza si hay algo de nivel 'Alto Impacto' (>2%). Si no lo hay, responde 'TRANQUILIDAD'.\nSi lo hay: '⚠️ ALERTA URGENTE: [Resumen] - Impacto en [Acción/Sector]'\nRESPONDE ESTRICTA Y ÚNICAMENTE EN ESPAÑOL.")
    try:
        prompt = (
            "Actúa como un analista técnico institucional. Este análisis es educativo y no es asesoría financiera.\n\n"
            "Debes evaluar la gráfica usando, SI Y SOLO SI SON VISIBLES en la imagen: RSI, MACD, volumen, EMA 50, EMA 200, SMA 50, SMA 200, retrocesos de Fibonacci, golden pocket, bandas de Bollinger, canales de Donchian y OBV.\n"
            "Si un indicador no es visible o no se puede leer con suficiente claridad, debes decir 'No visible' y NO inventarlo.\n"
            "Mantén enfoque Smart Money Concepts y comportamiento institucional. No expliques teoría.\n\n"
            "Responde exactamente en este formato:\n"
            "📊 CONTEXTO TÉCNICO: [Tendencia, estructura, liquidez, BOS/CHoCH/FVG/OB si son visibles].\n"
            "📐 INDICADORES: [RSI / MACD / Volumen / EMA-SMA / Fibonacci-golden pocket / Bollinger / Donchian / OBV. Para cada uno: lectura concreta o 'No visible'].\n"
            "🎯 NIVELES CLAVE: [Soportes, resistencias, order blocks, golden pocket y niveles relevantes con precios exactos si pueden leerse].\n"
            "⚠️ RIESGO DE INVERSIÓN: [Bajo / Medio / Alto] - [razón directa].\n"
            "⚖️ SESGO DIRECCIONAL: [Fuerte Alcista / Alcista / Neutral / Bajista / Fuerte Bajista / Esperar Confirmación] - [justificación breve y operable]."
        )

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
        return {'label': 'Neutral', 'icon': 'ðŸŸ¡', 'bull_pct': 50, 'bear_pct': 50, 'raw': 0.0}

    if s > 0.3:
        bull = min(95, int(50 + s * 50))
        return {'label': 'Alcista', 'icon': '🟢', 'bull_pct': bull, 'bear_pct': 100 - bull, 'raw': s}
    elif s < -0.3:
        bear = min(95, int(50 + abs(s) * 50))
        return {'label': 'Bajista', 'icon': '🔒´', 'bull_pct': 100 - bear, 'bear_pct': bear, 'raw': s}
    else:
        return {'label': 'Neutral', 'icon': 'ðŸŸ¡', 'bull_pct': 50, 'bear_pct': 50, 'raw': s}


_PROFILE_ALIAS_CACHE = {}


def _get_ticker_aliases(ticker):
    tk = remap_ticker(ticker)
    cached = _PROFILE_ALIAS_CACHE.get(tk)
    if cached:
        return cached

    clean_tk = tk.replace('-USD', '').replace('=F', '').upper()
    aliases = {clean_tk, get_display_name(tk).upper()}

    manual_aliases = {
        'BTC': ['BITCOIN', 'BTC', 'CRYPTO', 'CRYPTOCURRENCY'],
        'ETH': ['ETHEREUM', 'ETH'],
        'SOL': ['SOLANA'],
        'NVDA': ['NVIDIA', 'GPU', 'AI CHIP', 'SEMICONDUCTOR', 'SEMICONDUCTORS'],
        'AMD': ['ADVANCED MICRO DEVICES', 'CHIP', 'SEMICONDUCTOR', 'SEMICONDUCTORS'],
        'TSM': ['TSMC', 'TAIWAN SEMICONDUCTOR', 'SEMICONDUCTOR', 'SEMICONDUCTORS'],
        'MARA': ['MARATHON', 'BITCOIN MINER', 'MINER'],
        'COIN': ['COINBASE', 'CRYPTO EXCHANGE'],
        'PLTR': ['PALANTIR', 'DEFENSE SOFTWARE'],
        'IONQ': ['IONQ', 'QUANTUM', 'QUANTUM COMPUTING'],
        'GC': ['GOLD', 'ORO', 'BULLION'],
        'BZ': ['BRENT', 'OIL', 'PETROLEO', 'PETRÓLEO', 'CRUDE'],
        'XRP': ['RIPPLE'],
    }
    for key, items in manual_aliases.items():
        if key in clean_tk:
            aliases.update(items)

    profile = _fetch_fmp_profile(tk) or {}
    for field in [profile.get("companyName", ""), profile.get("sector", ""), profile.get("industry", "")]:
        upper_field = str(field or "").upper()
        if upper_field:
            aliases.add(upper_field)
        for token in re.split(r"[^A-Z0-9]+", upper_field):
            if len(token) >= 4 and token not in {"INC", "CORP", "LTD", "PLC", "HOLDINGS", "GROUP", "CLASS", "COMMON"}:
                aliases.add(token)

    sector_text = f"{profile.get('sector', '')} {profile.get('industry', '')}".upper()
    if "TECH" in sector_text or "SOFTWARE" in sector_text:
        aliases.update(["AI", "CLOUD", "SOFTWARE", "BIG TECH"])
    if "SEMICON" in sector_text:
        aliases.update(["CHIPS", "CHIP", "SEMIS", "EXPORT CONTROLS"])
    if "ENERGY" in sector_text or "OIL" in sector_text:
        aliases.update(["OIL", "CRUDE", "GAS", "OPEC"])
    if "BANK" in sector_text or "FINANCIAL" in sector_text:
        aliases.update(["BANKS", "YIELDS", "RATES", "CREDIT"])

    clean_aliases = sorted({alias.strip().upper() for alias in aliases if str(alias).strip()})
    _PROFILE_ALIAS_CACHE[tk] = clean_aliases
    return clean_aliases


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


def _extract_mentioned_tickers_plus(text, wallet_tickers):
    """Versión ampliada: detecta tickers por nombre, sector, industria y aliases."""
    mentioned = []
    text_upper = (text or "").upper()
    for tk in wallet_tickers:
        for alias in _get_ticker_aliases(tk):
            if alias and alias in text_upper:
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


# === SISTEMA DE TRADUCCIÓN AUTOMÃTICA AL ESPAÑOL ===

# Diccionario de términos financieros inglés â†’ español
_FINANCIAL_DICT = {
    'bull market': 'mercado alcista', 'bear market': 'mercado bajista',
    'interest rates': 'tasas de interés', 'interest rate': 'tasa de interés',
    'rate hike': 'alza de tasas', 'rate cut': 'recorte de tasas',
    'earnings': 'ganancias', 'revenue': 'ingresos', 'profit': 'beneficio',
    'loss': 'pérdida', 'losses': 'pérdidas',
    'surge': 'alza fuerte', 'surges': 'sube fuertemente',
    'plunge': 'desplome', 'plunges': 'se desploma',
    'rally': 'rally alcista', 'rallies': 'repunta',
    'crash': 'desplome', 'crashes': 'se desploma',
    'drop': 'caída', 'drops': 'cae',
    'rise': 'alza', 'rises': 'sube',
    'gain': 'ganancia', 'gains': 'ganancias',
    'fall': 'caída', 'falls': 'cae',
    'soar': 'se dispara', 'soars': 'se dispara',
    'decline': 'descenso', 'declines': 'desciende',
    'volatility': 'volatilidad', 'volatile': 'volátil',
    'downturn': 'recesión', 'recession': 'recesión',
    'inflation': 'inflación', 'deflation': 'deflación',
    'tariff': 'arancel', 'tariffs': 'aranceles',
    'sanction': 'sanción', 'sanctions': 'sanciones',
    'trade war': 'guerra comercial', 'trade deal': 'acuerdo comercial',
    'federal reserve': 'Reserva Federal', 'the fed': 'la Fed',
    'treasury': 'Tesoro', 'bond': 'bono', 'bonds': 'bonos',
    'yield': 'rendimiento', 'yields': 'rendimientos',
    'stock': 'acción', 'stocks': 'acciones',
    'shares': 'acciones', 'share': 'acción',
    'market cap': 'capitalización de mercado',
    'all-time high': 'máximo histórico', 'record high': 'máximo histórico',
    'all-time low': 'mínimo histórico',
    'breakout': 'ruptura alcista', 'breakdown': 'ruptura bajista',
    'support': 'soporte', 'resistance': 'resistencia',
    'sell-off': 'venta masiva', 'selloff': 'venta masiva',
    'buyback': 'recompra de acciones',
    'dividend': 'dividendo', 'dividends': 'dividendos',
    'outperform': 'supera expectativas', 'underperform': 'por debajo de expectativas',
    'upgrade': 'mejora de calificación', 'downgrade': 'rebaja de calificación',
    'bullish': 'alcista', 'bearish': 'bajista',
    'outlook': 'perspectiva', 'forecast': 'pronóstico',
    'growth': 'crecimiento', 'expansion': 'expansión',
    'layoffs': 'despidos', 'hiring': 'contrataciones',
    'bankruptcy': 'bancarrota', 'default': 'impago',
    'crisis': 'crisis', 'recovery': 'recuperación',
    'quarter': 'trimestre', 'quarterly': 'trimestral',
    'annual': 'anual', 'yearly': 'anual',
    'report': 'reporte', 'reports': 'reportes',
    'warns': 'advierte', 'warning': 'advertencia',
    'announces': 'anuncia', 'announcement': 'anuncio',
    'launch': 'lanzamiento', 'launches': 'lanza',
    'deal': 'acuerdo', 'merger': 'fusión', 'acquisition': 'adquisición',
    'investor': 'inversionista', 'investors': 'inversionistas',
    'traders': 'operadores', 'analyst': 'analista', 'analysts': 'analistas',
    'ahead of': 'antes de', 'amid': 'en medio de',
    'despite': 'a pesar de', 'due to': 'debido a',
    'according to': 'según', 'following': 'tras',
    'higher': 'más alto', 'lower': 'más bajo',
    'strong': 'fuerte', 'weak': 'débil',
    'bitcoin': 'Bitcoin', 'ethereum': 'Ethereum',
    'cryptocurrency': 'criptomoneda', 'crypto': 'cripto',
}


def _quick_translate_financial(text):
    """Traducción rápida por diccionario â€” reemplaza términos financieros comunes"""
    result = text
    # Ordenar por longitud descendente para evitar reemplazos parciales
    sorted_terms = sorted(_FINANCIAL_DICT.items(), key=lambda x: len(x[0]), reverse=True)
    for eng, esp in sorted_terms:
        # Reemplazo case-insensitive preservando capitalización del contexto
        pattern = re.compile(re.escape(eng), re.IGNORECASE)
        result = pattern.sub(esp, result)
    return result


def _translate_titles_to_spanish(titles):
    """Traduce una lista de títulos al español usando OpenAI (batch).
    Si OpenAI no está disponible, usa traducción por diccionario."""
    if not titles:
        return titles

    # Si hay OpenAI, traducción por lotes (más natural)
    if OPENAI_API_KEY and len(titles) > 0:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=OPENAI_API_KEY)
            numbered = "\n".join([f"{i+1}. {t}" for i, t in enumerate(titles)])
            prompt = (
                f"Traduce estos titulares financieros al ESPAÑOL con vocabulario profesional de mercados.\n"
                f"Usa términos como: mercado alcista, tasas de interés, rendimiento, volatilidad, arancel, etc.\n"
                f"Mantén los nombres propios (empresas, personas, países) sin traducir.\n"
                f"Devuelve SOLO las traducciones numeradas, sin explicaciones.\n\n"
                f"{numbered}"
            )
            prompt = (
                "Eres GÉNESIS, un analista macro-geopolítico de mercados que piensa como mesa institucional y protege el capital.\n\n"
                f"Cartera vigilada: {wallet_str or 'Sin activos en radar'}\n"
                f"Sentimiento global actual: {global_risk['label']} ({avg_sentiment:.2f})\n\n"
                f"Noticias más influyentes ahora:\n{influential_str}\n\n"
                "Tu trabajo es razonar con fundamento, no resumir por resumir. Debes conectar los titulares con tasas, petróleo, defensa, cadenas de suministro, chips, growth, cripto, liquidez y rotación sectorial cuando aplique.\n"
                "Responde EXACTAMENTE en este formato:\n"
                "🌍 CATALIZADORES CLAVE:\n"
                "1. [evento dominante + por qué mueve al mercado]\n"
                "2. [segundo evento + por qué importa]\n"
                "3. [tercer evento + por qué importa]\n"
                "🎯 IMPACTO EN MI CARTERA:\n"
                "• [ticker/sector] -> [alcista/bajista/mixto] | probabilidad [X]% | [mecánica del impacto]\n"
                "• [ticker/sector] -> [alcista/bajista/mixto] | probabilidad [X]% | [mecánica del impacto]\n"
                "🛡️ PROTECCIÓN GÉNESIS:\n"
                "• [riesgo principal a vigilar]\n"
                "• [qué confirmación invalidaría el escenario]\n"
                "• [acción táctica sugerida para proteger capital]\n"
                "⚖️ VEREDICTO FINAL: [Mantener / Vigilar de cerca / Reducir exposición / Aprovechar oportunidad] | Confianza [X]% | [tesis final en 2 líneas]."
            )
            prompt = (
                "Eres GÉNESIS, un analista macro-geopolítico de mercados que piensa como mesa institucional y protege el capital.\n\n"
                f"Cartera vigilada: {wallet_str or 'Sin activos en radar'}\n"
                f"Sentimiento global actual: {global_risk['label']} ({avg_sentiment:.2f})\n\n"
                f"Noticias más influyentes ahora:\n{influential_str}\n\n"
                "Tu trabajo es razonar con fundamento, no resumir por resumir. Debes conectar los titulares con tasas, petróleo, defensa, cadenas de suministro, chips, growth, cripto, liquidez y rotación sectorial cuando aplique.\n"
                "Responde EXACTAMENTE en este formato:\n"
                "🌍 CATALIZADORES CLAVE:\n"
                "1. [evento dominante + por qué mueve al mercado]\n"
                "2. [segundo evento + por qué importa]\n"
                "3. [tercer evento + por qué importa]\n"
                "🎯 IMPACTO EN MI CARTERA:\n"
                "• [ticker/sector] -> [alcista/bajista/mixto] | probabilidad [X]% | [mecánica del impacto]\n"
                "• [ticker/sector] -> [alcista/bajista/mixto] | probabilidad [X]% | [mecánica del impacto]\n"
                "🛡️ PROTECCIÓN GÉNESIS:\n"
                "• [riesgo principal a vigilar]\n"
                "• [qué confirmación invalidaría el escenario]\n"
                "• [acción táctica sugerida para proteger capital]\n"
                "⚖️ VEREDICTO FINAL: [Mantener / Vigilar de cerca / Reducir exposición / Aprovechar oportunidad] | Confianza [X]% | [tesis final en 2 líneas]."
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
            logging.debug(f"Error en traducción batch OpenAI: {e}")

    # Fallback: traducción por diccionario
    return [_quick_translate_financial(t) for t in titles]


def _translate_titles_to_spanish_v2(titles):
    """Versión limpia y robusta para traducir titulares financieros al español."""
    if not titles:
        return titles

    if OPENAI_API_KEY:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=OPENAI_API_KEY)
            numbered = "\n".join([f"{i+1}. {t}" for i, t in enumerate(titles)])
            prompt = (
                "Traduce estos titulares financieros al ESPAÑOL con lenguaje natural y profesional de mercados.\n"
                "Mantén intactos nombres propios, empresas, países y tickers.\n"
                "Devuelve SOLO las traducciones numeradas, una por línea, sin comentarios adicionales.\n\n"
                f"{numbered}"
            )
            res = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1200
            ).choices[0].message.content.strip()

            translated = []
            for line in res.split('\n'):
                line = line.strip()
                if line and line[0].isdigit():
                    clean = re.sub(r'^\d+[\.\)]\s*', '', line)
                    if clean:
                        translated.append(clean)

            if len(translated) >= max(1, int(len(titles) * 0.6)):
                while len(translated) < len(titles):
                    translated.append(_quick_translate_financial(titles[len(translated)]))
                return translated
        except Exception as e:
            logging.debug(f"Error en traducción batch OpenAI v2: {e}")

    return [_quick_translate_financial(t) for t in titles]


_TEXT_TRANSLATION_CACHE = {}


def _translate_text_to_spanish(text, max_chars=420):
    """Traduce descripciones cortas al español con fallback local."""
    raw = re.sub(r"\s+", " ", str(text or "")).strip()
    if not raw:
        return ""

    raw = raw[:max_chars]
    cache_key = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    cached = _TEXT_TRANSLATION_CACHE.get(cache_key)
    if cached:
        return cached

    translated = _quick_translate_financial(raw)
    if OPENAI_API_KEY:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=OPENAI_API_KEY)
            prompt = (
                "Traduce este texto corporativo al español con tono profesional de mercados.\n"
                "Mantén intactos nombres propios, marcas, países y tickers.\n"
                "Devuelve solo la traducción final.\n\n"
                f"{raw}"
            )
            res = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=260
            ).choices[0].message.content.strip()
            if res:
                translated = res
        except Exception as e:
            logging.debug(f"Error traduciendo descripción al español: {e}")

    _TEXT_TRANSLATION_CACHE[cache_key] = translated
    return translated


def genesis_strategic_report(manual=True):
    """REPORTE ESTRATÉGICO UNIFICADO GÉNESIS
    Integra: FMP Sentiment + Wallet Cross-Reference + Whale Data + IA
    TODO el contenido se entrega en ESPAÑOL."""
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

    # === PASO 1.5: Traducir títulos al español ===
    titles_to_translate = [n['title'] for n in news_data]
    translated = _translate_titles_to_spanish_v2(titles_to_translate)
    for i, news in enumerate(news_data):
        if i < len(translated) and translated[i]:
            news['title_es'] = translated[i]
        else:
            news['title_es'] = _quick_translate_financial(news['title'])

    # === PASO 2: Cross-reference con wallet ===
    wallet_alerts = []    # Noticias que tocan activos de Eduardo
    general_news = []     # Noticias generales del mercado

    for news in news_data:
        mentioned = _extract_mentioned_tickers_plus(news['title'], wallet_tickers)
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
    lines.append("🌐 <b>REPORTE ESTRATÉGICO GÉNESIS</b> 🌐")
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
                    whale_note = f"\nðŸ‹ <b>Ballena:</b> {wctx['vol_str']} ({wctx['type']}) hace {wctx['minutes_ago']}min"
                    break

            lines.append(f"📰 <b>Noticia:</b> {news['title_es'][:140]}")
            source_name = _escape_html(news.get('source') or "Fuente")
            source_url = html.escape((news.get('url') or '').strip(), quote=True)
            if source_url:
                lines.append(f'🌍 <b>Fuente:</b> <a href="{source_url}">{source_name}</a>')
            else:
                lines.append(f"🌍 <b>Fuente:</b> {source_name}")
            lines.append(f"🎯 <b>Activos afectados:</b> {affected}")
            lines.append(f"{s['icon']} <b>Riesgo:</b> {s['bull_pct']}% Alcista / {s['bear_pct']}% Bajista ({s['label']})")
            if whale_note:
                lines.append(whale_note)
            lines.append(f"🔥¡ <b>Análisis:</b> Según el sentimiento del mercado, la probabilidad de impacto en tu cartera es <b>{s['bear_pct']}%</b>.")
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
        source_name = _escape_html(news.get('source') or "Fuente")
        source_url = html.escape((news.get('url') or '').strip(), quote=True)
        if source_url:
            lines.append(f'{s["icon"]} <a href="{source_url}">{_escape_html(news["title_es"][:120])}</a> ({source_name})')
        else:
            src = f" ({source_name})" if news.get('source') else ""
            lines.append(f"{s['icon']} {_escape_html(news['title_es'][:120])}{src}")
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
            # Cruzar con riesgo geopolítico
            risk_tag = ""
            if w['ticker'] in GENESIS_RISK_CONTEXT.get('high_risk_tickers', []):
                risk_tag = " ⚠️ <b>[ZONA DE RIESGO]</b>"
            lines.append(f"ðŸ‹ <b>{get_display_name(w['ticker'])}</b> | {vol_str} | {w['type']} | {minutes_ago}min{risk_tag}")
    else:
        lines.append("🌐Š Océano tranquilo. Sin anomalías.")

    # --- Sentimiento resumen ---
    lines.append("")
    lines.append("\u2500" * 28)
    lines.append(f"🎯 <b>SENTIMIENTO GLOBAL:</b> {global_risk['icon']} {global_risk['label']} â€” {global_risk['bull_pct']}% Alcista / {global_risk['bear_pct']}% Bajista")
    lines.append("\u2500" * 28)

    # === PASO 5: IA avanzada (si OpenAI está disponible) ===
    if manual and OPENAI_API_KEY and (wallet_alerts or top_general):
        try:
            all_titles = [n['title_es'] for n in (wallet_alerts + top_general)[:6]]
            wallet_str = ", ".join([get_display_name(tk) for tk in wallet_tickers])
            sentiments_str = "\n".join([f"- {n['title_es'][:60]} ({n['sentiment']['label']})" for n in news_data[:5]])
            influential_news = sorted(news_data, key=lambda x: abs(x['sentiment']['raw']), reverse=True)[:5]
            influential_str = _format_geopolitics_news_for_ai(influential_news, wallet_tickers)

            from openai import OpenAI
            client = OpenAI(api_key=OPENAI_API_KEY)
            prompt = (
                f"Eres GÉNESIS, un sistema de inteligencia estratégica de mercados financieros.\n\n"
                f"NOTICIAS DEL DÃA CON SENTIMIENTO:\n{sentiments_str}\n\n"
                f"CARTERA DE EDUARDO: {wallet_str}\n\n"
                f"SENTIMIENTO GLOBAL: {global_risk['label']} ({avg_sentiment:.2f})\n\n"
                f"INSTRUCCIONES OBLIGATORIAS:\n"
                f"1. Redacta TODO en ESPAÑOL con vocabulario financiero profesional.\n"
                f"2. Usa términos como: mercado alcista, tasas de interés, rendimiento, volatilidad, liquidez, soporte, resistencia, presión vendedora/compradora.\n"
                f"3. En 3-4 líneas, explica cómo estas noticias afectan DIRECTAMENTE los activos de Eduardo.\n"
                f"4. Da UNA recomendación estratégica clara: Mantener / Vigilar de cerca / Reducir exposición / Aprovechar oportunidad.\n"
                f"5. PROHIBIDO responder en inglés. Todo debe ser 100% en español.\n"
            )
            res = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=700
            ).choices[0].message.content.strip()
            res = gpt_advanced_geopolitics_v3(influential_news, manual=True) or res

            lines.append("")
            lines.append("ðŸ§  <b>ANÃLISIS IA GÉNESIS:</b>")
            lines.append(res)
        except Exception as e:
            logging.error(f"OpenAI strategic error: {e}")

    return "\n".join(lines)


def generar_reporte_macro_manual():
    """Wrapper para el botón Geopolítica â€” usa el motor unificado"""
    return genesis_strategic_report(manual=True)


def fetch_intraday_data(ticker):
    """Obtiene snapshot intradía desde FMP y reconstuye avg volume cuando FMP lo devuelve en cero."""
    tk = remap_ticker(ticker)

    if not FMP_API_KEY:
        return None

    fmp_data = _fetch_fmp_quote(tk)
    if not fmp_data:
        return None

    price = _safe_float(fmp_data.get('price'))
    if price <= 0:
        return None

    latest_vol = _safe_float(fmp_data.get('volume'))
    quote_avg_vol = _safe_float(fmp_data.get('avgVolume'))
    change = _safe_float(fmp_data.get('change'))
    vol_side = "buy" if change >= 0 else "sell"
    vol_type = "Compra 🟢" if vol_side == "buy" else "Venta 🔴"

    avg_vol = quote_avg_vol if quote_avg_vol > 0 else 0.0

    if avg_vol <= 0:
        hist = _fetch_fmp_historical_eod(tk, limit=20) or []
        recent_vols = []
        for day in hist[:20]:
            v = float(day.get('volume', 0) or 0)
            if v > 0:
                recent_vols.append(v)
        if recent_vols:
            avg_vol = sum(recent_vols) / len(recent_vols)

    if avg_vol <= 0 and latest_vol > 0:
        avg_vol = latest_vol

    if avg_vol <= 0 and latest_vol <= 0:
        print(f"DEBUG INTRADAY {tk}: Ignorando activo por avg_vol ({avg_vol}) y latest_vol ({latest_vol}).")
        return None

    if avg_vol > 0 and latest_vol > 0:
        print(f"DEBUG INTRADAY {tk}: spike={(latest_vol/avg_vol):.2f}x (vol: {latest_vol:,.0f} / avg_vol: {avg_vol:,.0f})")

    return {
        'ticker': tk,
        'latest_vol': latest_vol,
        'avg_vol': avg_vol,
        'vol_side': vol_side,
        'vol_type': vol_type,
        'latest_price': price
    }

    tk = remap_ticker(ticker)

    if not FMP_API_KEY:
        return None

    fmp_data = _fetch_fmp_quote(tk)
    if not fmp_data:
        return None

    price = fmp_data.get('price')
    if not price:
        return None

    latest_vol = float(fmp_data.get('volume', 0) or 0)
    quote_avg_vol = float(fmp_data.get('avgVolume', 0) or 0)
    change = float(fmp_data.get('change', 0) or 0)
    vol_side = "buy" if change >= 0 else "sell"
    vol_type = "Compra institucional" if vol_side == "buy" else "Venta institucional"

    avg_vol = 0.0
    if manual:
        prompt = (
            f"Titulares globales:\n{news_text}\n\n"
            f"Cartera vigilada: {wallet_tickers}\n\n"
            f"Haz un resumen macro accionable y explica qué titulares pueden afectar DIRECTAMENTE a esta cartera. "
            f"Si un titular pega a un activo o sector de la cartera, nómbralo de forma explícita. "
            f"RESPONDE ESTRICTAMENTE EN ESPAÑOL."
        )
    else:
        prompt = (
            f"Titulares recientes:\n{news_text}\n\n"
            f"Cartera vigilada: {wallet_tickers}\n\n"
            f"Analiza si hay algo de nivel 'Alto Impacto' (>2%) con efecto directo o indirecto en la cartera. "
            f"Si no lo hay, responde 'TRANQUILIDAD'.\n"
            f"Si lo hay, responde en una sola línea con este formato:\n"
            f"⚠️ ALERTA URGENTE: [Resumen] - Impacto en [Activo/Sector de la cartera] - Acción sugerida [Vigilar/Reducir/Aprovechar]\n"
            f"RESPONDE ESTRICTA Y ÚNICAMENTE EN ESPAÑOL."
        )

    try:
        fmp_sym = _get_fmp_symbol(tk)
        if _is_crypto_ticker(tk):
            fmp_sym = tk.replace('-USD', '') + 'USD'

        url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{urllib.parse.quote(fmp_sym)}?apikey={FMP_API_KEY}"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            raw = resp.json()
            hist = []
            if isinstance(raw, dict) and isinstance(raw.get('historical'), list):
                hist = raw['historical']
            elif isinstance(raw, list):
                hist = raw
            recent_vols = []
            for day in hist[:20]:
                v = float(day.get('volume', 0) or 0)
                if v > 0:
                    recent_vols.append(v)
            if recent_vols:
                avg_vol = sum(recent_vols) / len(recent_vols)
    except Exception as e:
        logging.debug(f"fetch_intraday_data historical avg_vol error para {tk}: {e}")

    if avg_vol <= 0 and quote_avg_vol > 0:
        avg_vol = quote_avg_vol

    if avg_vol <= 0 and latest_vol > 0:
        avg_vol = latest_vol

    if avg_vol < 1000 and latest_vol <= 0:
        print(f"DEBUG INTRADAY {tk}: Ignorando activo por avg_vol ({avg_vol}) y latest_vol ({latest_vol}).")
        return None

    if avg_vol > 0 and latest_vol > 0:
        print(f"DEBUG INTRADAY {tk}: spike={(latest_vol/avg_vol):.2f}x (vol: {latest_vol:,.0f} / avg_vol: {avg_vol:,.0f})")

    return {
        'ticker': tk,
        'latest_vol': latest_vol,
        'avg_vol': avg_vol,
        'vol_side': vol_side,
        'vol_type': vol_type,
        'latest_price': price
    }
    """Obtiene precio + volumen de FMP quote directo, ignorando endpoints Legacy."""
    tk = remap_ticker(ticker)

    if not FMP_API_KEY:
        return None

    fmp_data = _fetch_fmp_quote(tk)
    if not fmp_data:
        return None

    price = fmp_data.get('price')
    if not price:
        return None

    latest_vol = float(fmp_data.get('volume', 0) or 0)
    avg_vol = float(fmp_data.get('avgVolume', 0) or 0)
    change = float(fmp_data.get('change', 0) or 0)
    vol_side = "buy" if change >= 0 else "sell"
    
    vol_type = "Compra 🟢" if change >= 0 else "Venta 🔴"

    if avg_vol < 1000:
        print(f"DEBUG INTRADAY {tk}: Ignorando activo por avg_vol ({avg_vol}).")
        return None

    if avg_vol > 0 and latest_vol > 0:
        print(f"DEBUG INTRADAY {tk}: spike={(latest_vol/avg_vol):.2f}x (vol: {latest_vol:,.0f} / avg_vol: {avg_vol:,.0f})")

    return {
        'ticker': tk,
        'latest_vol': latest_vol,
        'avg_vol': avg_vol,
        'vol_side': vol_side,
        'vol_type': vol_type,
        'latest_price': price
    }


def _is_winner_whale_setup(price, intra, topol=None, analysis=None):
    """Filtro estricto: solo compras institucionales con estructura favorable."""
    topol = topol or {}
    analysis = analysis or {}
    side = (intra or {}).get('vol_side')

    if side != "buy":
        return False, "Solo se permiten compras institucionales."

    if 'sup' not in topol or 'res' not in topol:
        return False, "Sin niveles SMC válidos."

    support = float(topol.get('sup', 0) or 0)
    resistance = float(topol.get('res', 0) or 0)
    if support <= 0 or resistance <= 0 or price <= 0:
        return False, "Niveles SMC inválidos."

    if price > (support * 1.05):
        return False, "Compra demasiado lejos del soporte."

    rsi = float(analysis.get('rsi', 50) or 50)
    if rsi >= 68:
        return False, "RSI demasiado exigido para entrada."

    stop_ref = support * 0.98
    upside_pct = ((resistance - price) / price) * 100 if price > 0 else 0
    downside_pct = ((price - stop_ref) / price) * 100 if price > 0 else 0
    rr = (upside_pct / downside_pct) if downside_pct > 0 else 0

    if upside_pct <= 0 or downside_pct <= 0 or rr < 1.20:
        return False, "Relación beneficio/riesgo insuficiente."

    return True, "Compra institucional en zona táctica de soporte."
def fetch_and_analyze_stock(ticker):
    """Calcula RSI, MACD, SMC usando datos diarios de FMP."""
    clean_ticker = str(ticker).strip().upper()
    tk = remap_ticker(clean_ticker)
    print(f"DEBUG SMC: Consultando niveles para {tk}...")
    try:
        safe_check = get_safe_ticker_price(tk)
        if not safe_check:
            print(f"DEBUG SMC: get_safe_ticker_price falló para {tk}")
            return "\u26a0\ufe0f Error de conexi\u00f3n con FMP"
            
        def _get_fallback_smc():
            latest_price = safe_check['price']
            pe = safe_check.get('pe', 0.0)
            return {
                'ticker': tk, 'price': latest_price, 'rsi': 50.0, 'macd_line': 0.0, 'macd_signal': 0.0, 
                'smc_sup': latest_price * 0.95, 'smc_res': latest_price * 1.05, 'smc_trend': "Alcista (\u26a0\ufe0f)", 
                'order_block': latest_price, 'take_profit': latest_price * 1.05, 'stop_loss': latest_price * 0.95 * 0.98,
                'rvol': 1.0, 'pe': pe,
                'sma50': latest_price, 'sma200': latest_price, 'ema50': latest_price, 'ema200': latest_price,
                'bb_upper': latest_price * 1.03, 'bb_lower': latest_price * 0.97, 'bb_basis': latest_price,
                'donchian_upper': latest_price * 1.03, 'donchian_lower': latest_price * 0.97, 'donchian_mid': latest_price,
                'obv': 0.0, 'obv_trend': "Neutral",
                'fib_high': latest_price * 1.05, 'fib_low': latest_price * 0.95,
                'fib_382': latest_price * 1.031, 'fib_500': latest_price, 'fib_618': latest_price * 0.969,
                'golden_pocket_low': latest_price * 0.965, 'golden_pocket_high': latest_price * 0.972,
            }

        hist = _fetch_fmp_historical_eod(tk, limit=260) or []
        print(f"DEBUG SMC: Histórico recibido para {tk}: {len(hist)} velas")

        if not hist or not isinstance(hist, list) or len(hist) < 5:
            logging.warning(f"SMC fallback para {tk}: histórico FMP insuficiente o no disponible.")
            return _get_fallback_smc()

        # FMP viene en orden reciente-primero, revertir para cálculos
        hist = list(reversed(hist[:260]))  # Hasta 260 días para cálculos más institucionales (SMA/EMA 200, Fibonacci, Bollinger, Donchian)

        closes = pd.Series([float(d.get('close', 0)) for d in hist])
        volumes = pd.Series([float(d.get('volume', 0) or 0) for d in hist])
        highs = pd.Series([float(d.get('high', 0) or 0) for d in hist])
        lows = pd.Series([float(d.get('low', 0) or 0) for d in hist])

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
        latest_rsi = float(rsi_series.iloc[-1]) if not rsi_series.empty else 50.0

        # MACD
        macd_line = closes.ewm(span=12, adjust=False).mean() - closes.ewm(span=26, adjust=False).mean()
        macd_signal = macd_line.ewm(span=9, adjust=False).mean()

        sma50 = float(closes.rolling(window=50, min_periods=1).mean().iloc[-1])
        sma200 = float(closes.rolling(window=200, min_periods=1).mean().iloc[-1])
        ema50 = float(closes.ewm(span=50, adjust=False).mean().iloc[-1])
        ema200 = float(closes.ewm(span=200, adjust=False).mean().iloc[-1])

        bb_basis_series = closes.rolling(window=20, min_periods=1).mean()
        bb_std_series = closes.rolling(window=20, min_periods=1).std().fillna(0)
        bb_basis = float(bb_basis_series.iloc[-1])
        bb_upper = float((bb_basis_series + (bb_std_series * 2)).iloc[-1])
        bb_lower = float((bb_basis_series - (bb_std_series * 2)).iloc[-1])

        donchian_upper = float(highs.rolling(window=20, min_periods=1).max().iloc[-1])
        donchian_lower = float(lows.rolling(window=20, min_periods=1).min().iloc[-1])
        donchian_mid = (donchian_upper + donchian_lower) / 2

        obv_step = closes.diff().fillna(0).apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        obv_series = (obv_step * volumes).cumsum()
        obv_value = float(obv_series.iloc[-1]) if not obv_series.empty else 0.0
        obv_reference = float(obv_series.iloc[-5]) if len(obv_series) >= 5 else 0.0
        if obv_value > obv_reference:
            obv_trend = "Ascendente"
        elif obv_value < obv_reference:
            obv_trend = "Descendente"
        else:
            obv_trend = "Neutral"

        # Extracci\u00f3n directa del array reverso (de m\u00e1s viejo a m\u00e1s nuevo)
        recent_month_data = hist[-20:] # los \u00faltimos 20 d\u00edas de la lista invertida (los m\u00e1s recientes cronol\u00f3gicamente)
        
        smc_res = float(max([float(d.get('high', 0)) for d in recent_month_data])) if recent_month_data else latest_price
        smc_sup = float(min([float(d.get('low', float('inf'))) for d in recent_month_data])) if recent_month_data else latest_price
        latest_price = float(recent_month_data[-1].get('close', latest_price)) if recent_month_data else latest_price

        fib_window = min(len(hist), 120)
        fib_high = float(highs.iloc[-fib_window:].max()) if fib_window > 0 else latest_price
        fib_low = float(lows.iloc[-fib_window:].min()) if fib_window > 0 else latest_price
        fib_range = max(fib_high - fib_low, 0.0001)
        fib_382 = fib_high - (fib_range * 0.382)
        fib_500 = fib_high - (fib_range * 0.500)
        fib_618 = fib_high - (fib_range * 0.618)
        fib_650 = fib_high - (fib_range * 0.650)
        golden_pocket_low = min(fib_650, fib_618)
        golden_pocket_high = max(fib_650, fib_618)
        
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
            'pe': pe,
            'sma50': sma50,
            'sma200': sma200,
            'ema50': ema50,
            'ema200': ema200,
            'bb_upper': bb_upper,
            'bb_lower': bb_lower,
            'bb_basis': bb_basis,
            'donchian_upper': donchian_upper,
            'donchian_lower': donchian_lower,
            'donchian_mid': donchian_mid,
            'obv': obv_value,
            'obv_trend': obv_trend,
            'fib_high': fib_high,
            'fib_low': fib_low,
            'fib_382': fib_382,
            'fib_500': fib_500,
            'fib_618': fib_618,
            'golden_pocket_low': golden_pocket_low,
            'golden_pocket_high': golden_pocket_high,
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
    if not OPENAI_API_KEY: return "Â¿Qué hacer? Mantener cautela."
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)

    # === Recopilar contexto unificado GÉNESIS ===
    # Contexto geopolítico
    geo_context = "Sin datos geopolíticos recientes."
    risk_ctx = GENESIS_RISK_CONTEXT
    if risk_ctx.get('last_update'):
        global_s = _classify_sentiment(risk_ctx['sentiment_global'])
        geo_context = f"Sentimiento global del mercado: {global_s['label']} ({global_s['bull_pct']}% Alcista / {global_s['bear_pct']}% Bajista)."
        if risk_ctx.get('news_digest'):
            top_news = [n.get('title_es', n.get('title', ''))[:60] for n in risk_ctx['news_digest'][:3]]
            geo_context += f"\nNoticias clave: {'; '.join(top_news)}"
        if tk in risk_ctx.get('high_risk_tickers', []):
            geo_context += f"\n⚠️ {display_name} está en ZONA DE RIESGO GEOPOLÃTICO."

    # Contexto de ballenas
    whale_context = "Sin movimientos de ballena recientes en este activo."
    wctx = _get_whale_context_for_ticker(tk)
    if wctx:
        whale_context = f"Ballena detectada: {wctx['vol_str']} ({wctx['type']}) hace {wctx['minutes_ago']} minutos."

    prompt = (f"Eres GÉNESIS, analista institucional senior de un fondo de cobertura.\n\n"
              f"EVENTO: El activo {display_name} acaba de romper su nivel de {level_type} (Smart Money Concept) en ${fmt_price(price)} verificado vía FMP.\n\n"
              f"CONTEXTO GEOPOLÃTICO:\n{geo_context}\n\n"
              f"CONTEXTO BALLENAS:\n{whale_context}\n\n"
              f"INSTRUCCIONES OBLIGATORIAS:\n"
              f"1. Evalúa esta ruptura cruzando: dirección del precio, sentimiento geopolítico, y movimientos de ballenas.\n"
              f"2. Da un consejo claro: Â¿COMPRAR, VENDER o MANTENER? Resalta tu elección en negrita.\n"
              f"3. Asigna un PORCENTAJE DE CONFIANZA (ejemplo: 75%, 85%, 92%) basado en cuántas señales convergen:\n"
              f"   - Si ruptura + ballenas + sentimiento apuntan en la misma dirección = 85-95%\n"
              f"   - Si hay señales mixtas = 60-75%\n"
              f"   - Si hay contradicción fuerte = 50-65%\n"
              f"4. Formato: 1 párrafo de máximo 4 líneas. ESPAÑOL ESTRICTO con vocabulario financiero profesional.\n"
              f"5. Termina con: '🎯 Confianza: [X]%'\n")
    try:
        return client.chat.completions.create(model="gpt-4o", messages=[{"role": "user", "content": prompt}], max_tokens=400).choices[0].message.content.strip()
    except Exception as e:
        logging.error(f"Fallo OpenAI breakout: {e}")
        return "Â¿Qué hacer? Esperar confirmación de volumen en la siguiente hora. 🎯 Confianza: 50%"

def _safe_float(value, default=0.0):
    try:
        if value in (None, "", "None"):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _format_compact_money(value):
    amount = _safe_float(value, 0.0)
    if amount <= 0:
        return "N/D"
    abs_amount = abs(amount)
    if abs_amount >= 1_000_000_000_000:
        return f"${amount / 1_000_000_000_000:.2f}T"
    if abs_amount >= 1_000_000_000:
        return f"${amount / 1_000_000_000:.2f}B"
    if abs_amount >= 1_000_000:
        return f"${amount / 1_000_000:.2f}M"
    if abs_amount >= 1_000:
        return f"${amount / 1_000:.2f}K"
    return f"${amount:,.0f}"


def _escape_html(text):
    return html.escape(str(text or ""), quote=False)


def _truncate_text(text, limit=180):
    clean = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(clean) <= limit:
        return clean
    clipped = clean[:limit].rsplit(" ", 1)[0].strip()
    return f"{clipped}..."


def _fetch_fmp_profile(ticker):
    if not FMP_API_KEY:
        return None

    tk = remap_ticker(ticker)
    if _is_crypto_ticker(tk) or tk in {"BZ=F", "GC=F"}:
        return None

    safe_symbol = urllib.parse.quote(_get_fmp_symbol(tk))
    urls = [
        f"https://financialmodelingprep.com/stable/profile?symbol={safe_symbol}&apikey={FMP_API_KEY}",
        f"https://financialmodelingprep.com/api/v3/profile/{safe_symbol}?apikey={FMP_API_KEY}",
        f"https://financialmodelingprep.com/stable/sec-profile?symbol={safe_symbol}&apikey={FMP_API_KEY}",
    ]

    for url in urls:
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                continue
            data = resp.json()
            if isinstance(data, list) and data:
                row = data[0]
            elif isinstance(data, dict):
                row = data
            else:
                continue

            if row:
                return row
        except Exception as e:
            logging.debug(f"FMP profile error for {tk} en {url}: {e}")

    return None


def _fetch_fmp_ticker_news(ticker, limit=3):
    if not FMP_API_KEY:
        return []

    tk = remap_ticker(ticker)
    if tk in {"BZ=F", "GC=F"}:
        return []

    if _is_crypto_ticker(tk):
        symbol = tk.replace("-USD", "") + "USD"
    else:
        symbol = _get_fmp_symbol(tk)

    url = f"https://financialmodelingprep.com/stable/stock-news?symbol={urllib.parse.quote(symbol)}&limit={int(limit)}&apikey={FMP_API_KEY}"

    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return []
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        logging.debug(f"FMP ticker news error for {tk}: {e}")
        return []


def _evaluate_news_materiality(title, body=""):
    text = f"{title or ''} {body or ''}".lower()

    positive_weights = {
        "merger": 4, "merge": 4, "merges": 4, "acquisition": 4, "acquire": 4, "partnership": 3, "partner": 3,
        "joint venture": 3, "business combination": 4, "contract": 3, "deal": 3, "approval": 3, "approved": 3,
        "fda": 3, "phase 3": 3, "phase iii": 3, "license": 2, "licensing": 2,
        "wins": 2, "award": 2, "buyback": 3, "beats": 3, "beat": 3, "guidance raised": 4,
        "raises guidance": 4, "investment": 2, "funding": 2, "expands": 2, "launch": 1,
        "surge": 2, "record": 2, "breakout": 2, "secures": 3, "signs": 2, "selected": 2,
        "strategic": 1, "commercial": 2, "exclusive": 2,
    }
    negative_weights = {
        "offering": 4, "dilution": 4, "dilutive": 4, "lawsuit": 3, "investigation": 4,
        "probe": 3, "downgrade": 3, "misses": 3, "miss": 3, "guidance cut": 4,
        "cuts guidance": 4, "bankruptcy": 5, "fraud": 5, "delay": 2, "recall": 2,
        "antitrust": 3, "hack": 3, "breach": 3, "default": 4, "layoffs": 2, "slump": 2,
        "sanction": 3, "tariff": 3, "crash": 4, "plunge": 3, "falls": 2, "terminated": 4,
        "delisting": 5, "restatement": 4, "resigns": 3, "resignation": 3, "chapter 11": 5,
    }
    neutral_noise = ["conference", "interview", "watch", "price prediction", "recap", "live updates"]

    if any(noise in text for noise in neutral_noise):
        return {"material": False, "direction": "neutral", "score": 0, "impact": "Bajo", "reason": "Titular de bajo valor operativo."}

    pos_score = sum(weight for key, weight in positive_weights.items() if key in text)
    neg_score = sum(weight for key, weight in negative_weights.items() if key in text)
    net_score = pos_score - neg_score
    abs_score = abs(net_score)

    if abs_score < 2:
        return {"material": False, "direction": "neutral", "score": net_score, "impact": "Bajo", "reason": "Sin catalizador claro todavía."}

    direction = "bullish" if net_score > 0 else "bearish"
    impact = "Alto" if abs_score >= 4 else "Medio"
    reason = "Catalizador corporativo relevante." if impact == "Alto" else "Noticia potencialmente operable."
    return {"material": True, "direction": direction, "score": net_score, "impact": impact, "reason": reason}


def _build_tracked_news_alert(tk, article):
    title = (article.get('title') or '').strip()
    body = (article.get('text') or article.get('content') or '').strip()
    site = (article.get('site') or article.get('source') or 'Fuente no disponible').strip()
    published = (article.get('publishedDate') or article.get('date') or '').strip()
    article_url = (article.get('url') or article.get('link') or '').strip()
    signal = _evaluate_news_materiality(title, body)
    if not signal.get("material"):
        return None

    display_name = get_display_name(tk)
    analysis = LAST_KNOWN_ANALYSIS.get(tk)
    if not analysis or not isinstance(analysis, dict):
        analysis = fetch_and_analyze_stock(tk)
        if analysis and isinstance(analysis, dict):
            LAST_KNOWN_ANALYSIS[tk] = analysis
            update_smc_memory(tk, analysis)

    lines = [
        f"📰 <b>Activo:</b> {display_name}",
        f"• <b>Titular:</b> {_escape_html(_truncate_text(title, 180))}",
        f"• <b>Lectura:</b> {'Alcista' if signal['direction'] == 'bullish' else 'Bajista'} | Impacto {signal['impact']}",
    ]

    if analysis and isinstance(analysis, dict):
        price = analysis.get('price', 0)
        support = analysis.get('smc_sup', 0)
        resistance = analysis.get('smc_res', 0)
        rsi = analysis.get('rsi', 50)
        lines.append(f"• <b>Precio:</b> ${fmt_price(price)} | RSI {rsi:.1f}")
        lines.append(f"• <b>Soporte:</b> ${fmt_price(support)} | <b>Resistencia:</b> ${fmt_price(resistance)}")

    if analysis and isinstance(analysis, dict):
        macd_line = _safe_float(analysis.get('macd_line'))
        macd_signal = _safe_float(analysis.get('macd_signal'))
        macd_bias = "alcista" if macd_line >= macd_signal else "bajista"
        gp_low = _safe_float(analysis.get('golden_pocket_low'))
        gp_high = _safe_float(analysis.get('golden_pocket_high'))
        lines.append(f"• <b>MACD:</b> {macd_bias}")
        if gp_low > 0 and gp_high > 0:
            lines.append(f"• <b>Golden pocket:</b> ${fmt_price(gp_low)} - ${fmt_price(gp_high)}")

    if signal['direction'] == 'bullish':
        suggestion = "Vigilar para compra o aumento de exposición si confirma estructura."
    else:
        suggestion = "Vigilar riesgo y proteger ganancias si rompe soporte."

    source_line = f"â€¢ <b>Fuente:</b> {_escape_html(site)}"
    if article_url:
        safe_url = html.escape(article_url, quote=True)
        source_line = f'â€¢ <b>Fuente:</b> <a href="{safe_url}">{_escape_html(site)}</a>'
    if published:
        source_line += f" | {_escape_html(published)}"
    source_line = source_line.replace("Ã¢â‚¬Â¢ ", "• ")
    lines.append(source_line)

    lines.extend([
        f"• <b>Racional:</b> {signal['reason']}",
        f"• <b>Acción sugerida:</b> {suggestion}",
        f"• <b>Fuente:</b> {_escape_html(site)}" + (f" | {_escape_html(published)}" if published else ""),
    ])

    return _make_card("CENTINELA GÉNESIS", lines, icon="🚨")


def _iter_whale_history_entries(hours=24):
    cutoff = datetime.now() - timedelta(hours=hours)
    entries = []
    for tk, rows in WHALE_HISTORY_DB.items():
        for row in rows:
            ts = row.get("timestamp")
            if isinstance(ts, datetime) and ts >= cutoff:
                enriched = dict(row)
                enriched["ticker"] = tk
                entries.append(enriched)
    return sorted(entries, key=lambda item: item.get("timestamp"), reverse=True)


def _count_whale_alerts_today():
    today = datetime.now().date()
    total = 0
    for entry in _iter_whale_history_entries(hours=48):
        ts = entry.get("timestamp")
        if isinstance(ts, datetime) and ts.date() == today and entry.get("alert_sent"):
            total += 1
    return total


def _get_chart_font(size=16, bold=False):
    candidates = [
        "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf",
        "Arial Bold.ttf" if bold else "Arial.ttf",
        "arialbd.ttf" if bold else "arial.ttf",
    ]
    for font_name in candidates:
        try:
            return ImageFont.truetype(font_name, size=size)
        except Exception:
            continue
    return ImageFont.load_default()


def _find_recent_pivots(values, mode="low", window=3, lookback=55, min_gap=6):
    arr = [float(v) for v in list(values)]
    if len(arr) < (window * 2 + 5):
        return []

    start = max(window, len(arr) - lookback)
    pivots = []
    for idx in range(start, len(arr) - window):
        segment = arr[idx - window: idx + window + 1]
        current = arr[idx]
        condition = current <= min(segment) if mode == "low" else current >= max(segment)
        if not condition:
            continue

        if not pivots:
            pivots.append(idx)
            continue

        if idx - pivots[-1] < min_gap:
            prev_idx = pivots[-1]
            replace = current < arr[prev_idx] if mode == "low" else current > arr[prev_idx]
            if replace:
                pivots[-1] = idx
        else:
            pivots.append(idx)

    return pivots[-2:]


def _detect_divergence_signal(pack):
    closes = pack.get("closes_full", [])
    rsi = pack.get("rsi_full", [])
    macd_hist = pack.get("macd_hist_full", [])
    obv = pack.get("obv_full", [])
    dates = pack.get("dates_full", [])
    support = _safe_float(pack.get("support"))
    resistance = _safe_float(pack.get("resistance"))
    current_price = _safe_float(pack.get("price"))

    result = {
        "active": False,
        "kind": None,
        "confidence": 0,
        "summary": "Sin divergencia operable de alta calidad.",
        "signals": [],
        "pivot_index": None,
        "pivot_date": None,
    }

    if len(closes) < 30 or len(rsi) != len(closes) or len(macd_hist) != len(closes) or len(obv) != len(closes):
        return result

    def _zone_bonus(kind):
        if kind == "bullish" and support > 0 and current_price <= support * 1.03:
            return 8
        if kind == "bearish" and resistance > 0 and current_price >= resistance * 0.97:
            return 8
        return 0

    low_pivots = _find_recent_pivots(closes, mode="low")
    if len(low_pivots) == 2:
        i1, i2 = low_pivots
        if closes[i2] < closes[i1] * 0.995:
            signals = []
            if rsi[i2] > rsi[i1] + 2.5:
                signals.append("RSI")
            if macd_hist[i2] > macd_hist[i1]:
                signals.append("MACD")
            if obv[i2] > obv[i1]:
                signals.append("OBV")
            if len(signals) >= 2:
                confidence = min(92, 68 + len(signals) * 7 + _zone_bonus("bullish"))
                result = {
                    "active": True,
                    "kind": "bullish",
                    "confidence": confidence,
                    "summary": f"Divergencia alcista confirmada por {', '.join(signals)}.",
                    "signals": signals,
                    "pivot_index": i2,
                    "pivot_date": dates[i2] if i2 < len(dates) else None,
                }

    high_pivots = _find_recent_pivots(closes, mode="high")
    if len(high_pivots) == 2:
        i1, i2 = high_pivots
        if closes[i2] > closes[i1] * 1.005:
            signals = []
            if rsi[i2] < rsi[i1] - 2.5:
                signals.append("RSI")
            if macd_hist[i2] < macd_hist[i1]:
                signals.append("MACD")
            if obv[i2] < obv[i1]:
                signals.append("OBV")
            if len(signals) >= 2:
                confidence = min(92, 68 + len(signals) * 7 + _zone_bonus("bearish"))
                bearish_result = {
                    "active": True,
                    "kind": "bearish",
                    "confidence": confidence,
                    "summary": f"Divergencia bajista confirmada por {', '.join(signals)}.",
                    "signals": signals,
                    "pivot_index": i2,
                    "pivot_date": dates[i2] if i2 < len(dates) else None,
                }
                if bearish_result["confidence"] > result.get("confidence", 0):
                    result = bearish_result

    return result


def _build_projection_series(pack):
    closes = pack.get("closes", [])
    if len(closes) < 8:
        return []

    current = float(closes[-1])
    support = _safe_float(pack.get("support"), current * 0.96)
    resistance = _safe_float(pack.get("resistance"), current * 1.04)
    trend_score = 0
    if _safe_float(pack.get("ema50")) >= _safe_float(pack.get("ema200")):
        trend_score += 1
    else:
        trend_score -= 1
    if _safe_float(pack.get("macd_line")) >= _safe_float(pack.get("macd_signal")):
        trend_score += 1
    else:
        trend_score -= 1
    if _safe_float(pack.get("rsi")) >= 55:
        trend_score += 1
    elif _safe_float(pack.get("rsi")) <= 45:
        trend_score -= 1

    divergence = pack.get("divergence") or {}
    if divergence.get("active"):
        trend_score += 1 if divergence.get("kind") == "bullish" else -1

    recent_slope = (closes[-1] - closes[-6]) / 5 if len(closes) >= 6 else 0.0
    if trend_score >= 2:
        target = max(current + recent_slope * 7, min(resistance, current * 1.06))
    elif trend_score <= -2:
        target = min(current + recent_slope * 7, max(support, current * 0.94))
    else:
        mid = (support + resistance) / 2 if support > 0 and resistance > 0 else current
        target = (current * 0.65) + (mid * 0.35)

    projection = []
    steps = 12
    for step in range(1, steps + 1):
        t = step / steps
        eased = 1 - ((1 - t) ** 2)
        curvature = math.sin(t * math.pi) * recent_slope * 1.2
        projection.append(current + ((target - current) * eased) + curvature)
    return projection


def _build_chart_pack(ticker, candles=110):
    tk = remap_ticker(ticker)
    hist = _fetch_fmp_historical_eod(tk, limit=max(260, candles)) or []
    if len(hist) < 60:
        return None

    hist = list(reversed(hist[:max(260, candles)]))
    closes_full = [float(row.get("close", 0) or 0) for row in hist]
    highs_full = [float(row.get("high", 0) or 0) for row in hist]
    lows_full = [float(row.get("low", 0) or 0) for row in hist]
    volumes_full = [float(row.get("volume", 0) or 0) for row in hist]
    dates_full = [str(row.get("date") or row.get("label") or "") for row in hist]

    closes = pd.Series(closes_full)
    highs = pd.Series(highs_full)
    lows = pd.Series(lows_full)
    volumes = pd.Series(volumes_full)

    delta = closes.diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ema_up = up.ewm(com=13, adjust=False).mean()
    ema_down = down.ewm(com=13, adjust=False).mean()
    rs = ema_up / ema_down.replace(0, pd.NA)
    rsi_series = (100 - (100 / (1 + rs))).fillna(50)

    macd_line = closes.ewm(span=12, adjust=False).mean() - closes.ewm(span=26, adjust=False).mean()
    macd_signal = macd_line.ewm(span=9, adjust=False).mean()
    macd_hist = (macd_line - macd_signal).fillna(0)

    ema50 = closes.ewm(span=50, adjust=False).mean()
    ema200 = closes.ewm(span=200, adjust=False).mean()
    sma50 = closes.rolling(window=50, min_periods=1).mean()
    sma200 = closes.rolling(window=200, min_periods=1).mean()

    bb_basis = closes.rolling(window=20, min_periods=1).mean()
    bb_std = closes.rolling(window=20, min_periods=1).std().fillna(0)
    bb_upper = bb_basis + (bb_std * 2)
    bb_lower = bb_basis - (bb_std * 2)

    obv_step = closes.diff().fillna(0).apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    obv_series = (obv_step * volumes).cumsum()

    recent_window = min(len(hist), 20)
    support = float(lows.iloc[-recent_window:].min())
    resistance = float(highs.iloc[-recent_window:].max())
    fib_window = min(len(hist), 120)
    fib_high = float(highs.iloc[-fib_window:].max())
    fib_low = float(lows.iloc[-fib_window:].min())
    fib_range = max(fib_high - fib_low, 0.0001)
    fib_618 = fib_high - (fib_range * 0.618)
    fib_650 = fib_high - (fib_range * 0.650)
    golden_pocket_low = min(fib_650, fib_618)
    golden_pocket_high = max(fib_650, fib_618)

    pack = {
        "ticker": tk,
        "dates_full": dates_full,
        "closes_full": closes_full,
        "highs_full": highs_full,
        "lows_full": lows_full,
        "volumes_full": volumes_full,
        "rsi_full": [float(v) for v in rsi_series.fillna(50).tolist()],
        "macd_line_full": [float(v) for v in macd_line.fillna(0).tolist()],
        "macd_signal_full": [float(v) for v in macd_signal.fillna(0).tolist()],
        "macd_hist_full": [float(v) for v in macd_hist.fillna(0).tolist()],
        "obv_full": [float(v) for v in obv_series.fillna(0).tolist()],
        "ema50_full": [float(v) for v in ema50.bfill().fillna(0).tolist()],
        "ema200_full": [float(v) for v in ema200.bfill().fillna(0).tolist()],
        "bb_upper_full": [float(v) for v in bb_upper.fillna(0).tolist()],
        "bb_lower_full": [float(v) for v in bb_lower.fillna(0).tolist()],
        "bb_basis_full": [float(v) for v in bb_basis.fillna(0).tolist()],
        "sma50": float(sma50.iloc[-1]),
        "sma200": float(sma200.iloc[-1]),
        "ema50": float(ema50.iloc[-1]),
        "ema200": float(ema200.iloc[-1]),
        "macd_line": float(macd_line.iloc[-1]),
        "macd_signal": float(macd_signal.iloc[-1]),
        "rsi": float(rsi_series.iloc[-1]),
        "support": support,
        "resistance": resistance,
        "price": float(closes.iloc[-1]),
        "golden_pocket_low": golden_pocket_low,
        "golden_pocket_high": golden_pocket_high,
        "fib_618": fib_618,
    }

    pack["divergence"] = _detect_divergence_signal(pack)
    pack["projection"] = _build_projection_series(pack)

    tail = candles
    for key in ("dates_full", "closes_full", "highs_full", "lows_full", "volumes_full", "rsi_full",
                "macd_line_full", "macd_signal_full", "macd_hist_full", "obv_full", "ema50_full",
                "ema200_full", "bb_upper_full", "bb_lower_full", "bb_basis_full"):
        trimmed_key = key.replace("_full", "")
        pack[trimmed_key] = pack[key][-tail:]

    return pack


def _render_stock_analysis_chart(ticker, analysis=None):
    pack = _build_chart_pack(ticker, candles=110)
    if not pack:
        return None, None

    tk = remap_ticker(ticker)
    display_name = get_display_name(tk)
    analysis = analysis or (LAST_KNOWN_ANALYSIS.get(tk) if isinstance(LAST_KNOWN_ANALYSIS.get(tk), dict) else {}) or {}
    divergence = pack.get("divergence") or {}

    img = Image.new("RGBA", (1460, 860), "#F4F1EA")
    draw = ImageDraw.Draw(img, "RGBA")
    font_title = _get_chart_font(30, bold=True)
    font_sub = _get_chart_font(18, bold=False)
    font_label = _get_chart_font(16, bold=False)
    font_small = _get_chart_font(14, bold=False)
    font_bold = _get_chart_font(19, bold=True)

    main_panel = (50, 105, 1045, 790)
    side_panel = (1080, 105, 1410, 790)
    for panel in (main_panel, side_panel):
        draw.rounded_rectangle(panel, radius=28, fill=(255, 255, 255, 242), outline="#D8D1C2", width=2)

    draw.text((58, 28), f"Ruta táctica de {display_name}", fill="#10233E", font=font_title)
    draw.text((60, 64), "Tramo real del precio y trayectoria proyectada a partir de la lectura técnica interna.", fill="#5A677D", font=font_sub)

    def _map_series(values, panel, vmin=None, vmax=None, extra_right=0):
        x1, y1, x2, y2 = panel
        usable_x1, usable_x2 = x1 + 26, x2 - 26 - extra_right
        usable_y1, usable_y2 = y1 + 28, y2 - 30
        if vmin is None:
            vmin = min(values)
        if vmax is None:
            vmax = max(values)
        if vmax <= vmin:
            vmax = vmin + 1
        pts = []
        total = max(len(values) - 1, 1)
        for idx, value in enumerate(values):
            x = usable_x1 + ((usable_x2 - usable_x1) * idx / total)
            y = usable_y2 - (((float(value) - vmin) / (vmax - vmin)) * (usable_y2 - usable_y1))
            pts.append((x, y))
        return pts, usable_x2

    x1, y1, x2, y2 = main_panel
    for idx in range(6):
        y = y1 + ((y2 - y1) * idx / 5)
        draw.line((x1 + 18, y, x2 - 18, y), fill="#ECE7DC", width=1)

    closes = pack["closes"]
    projection = pack["projection"]
    combined = closes + projection if projection else closes
    price_min = min(combined) * 0.97
    price_max = max(combined) * 1.03

    close_pts, price_panel_end = _map_series(closes, main_panel, price_min, price_max, extra_right=110)
    if len(close_pts) > 1:
        draw.line(close_pts, fill="#132B45", width=5, joint="curve")

    last_price = closes[-1]
    proj_color = "#1B9C5A" if projection and projection[-1] >= last_price else "#C94D3F"
    if close_pts:
        px, py = close_pts[-1]
        draw.ellipse((px - 6, py - 6, px + 6, py + 6), fill="#132B45")

    if close_pts and projection:
        projection_pts = [close_pts[-1]]
        for idx, value in enumerate(projection, start=1):
            x = price_panel_end + (idx * 10)
            y = _map_series([value], main_panel, price_min, price_max, extra_right=110)[0][0][1]
            projection_pts.append((x, y))
        for idx in range(len(projection_pts) - 1):
            if idx % 2 == 0:
                draw.line((projection_pts[idx], projection_pts[idx + 1]), fill=proj_color, width=5)
        lx, ly = projection_pts[-1]
        draw.ellipse((lx - 7, ly - 7, lx + 7, ly + 7), fill=proj_color)
        draw.text((lx - 72, ly - 32), "Trayectoria", fill=proj_color, font=font_small)

    split_x = close_pts[-1][0] if close_pts else (x1 + x2) / 2
    draw.line((split_x, y1 + 20, split_x, y2 - 20), fill="#B9C1D0", width=2)
    draw.text((x1 + 24, y2 - 24), "Precio real", fill="#10233E", font=font_bold)
    draw.text((split_x + 16, y2 - 24), "Escenario proyectado", fill=proj_color, font=font_bold)

    support = _safe_float(pack.get("support"))
    resistance = _safe_float(pack.get("resistance"))
    for level, color in ((support, "#1B9C5A"), (resistance, "#C94D3F")):
        if level > 0:
            pts, _ = _map_series([level] * len(closes), main_panel, price_min, price_max, extra_right=110)
            if len(pts) > 1:
                draw.line(pts, fill=color, width=2)

    summary_lines = [
        f"Activo: {display_name}",
        f"Precio actual: ${fmt_price(pack['price'])}",
        f"Sesgo: {'alcista' if proj_color == '#1B9C5A' else 'bajista'}",
        f"Soporte clave: ${fmt_price(support)}",
        f"Resistencia clave: ${fmt_price(resistance)}",
    ]
    if divergence.get("active"):
        summary_lines.extend([
            "",
            f"Divergencia: {'alcista' if divergence['kind'] == 'bullish' else 'bajista'}",
            f"Fuerza: {divergence.get('confidence', 0)}%",
        ])
    else:
        summary_lines.extend(["", "Divergencia: sin señal fuerte"])

    summary_lines.extend([
        "",
        f"RSI backend: {pack['rsi']:.1f}",
        f"MACD backend: {'alcista' if pack['macd_line'] >= pack['macd_signal'] else 'bajista'}",
        f"EMA50/EMA200 backend: {'positiva' if pack['ema50'] >= pack['ema200'] else 'presión bajista'}",
    ])

    draw.text((side_panel[0] + 22, side_panel[1] + 20), "Lectura condensada", fill="#10233E", font=font_bold)
    y_cursor = side_panel[1] + 64
    for line in summary_lines:
        draw.text((side_panel[0] + 22, y_cursor), line, fill="#31425B", font=font_label)
        y_cursor += 31 if line else 18

    chart_dir = os.path.join(DATA_DIR, "charts")
    os.makedirs(chart_dir, exist_ok=True)
    chart_path = os.path.join(chart_dir, f"{tk}_{int(time.time())}.png")
    img.convert("RGB").save(chart_path, format="PNG", optimize=True)

    projection_hint = "alcista" if projection and projection[-1] >= closes[-1] else "bajista"
    caption = _make_card(
        f"GRÁFICO TÁCTICO | {display_name}",
        [
            f"• Tramo real: muestra el recorrido confirmado hasta ahora.",
            f"• Trayectoria proyectada: sesgo {projection_hint} calculado con la lectura interna de indicadores.",
            f"• Divergencia: {divergence['summary'] if divergence.get('active') else 'Sin divergencia operable fuerte por ahora.'}",
        ],
        icon="🖼️",
        footer="Visual limpio: precio real + escenario probable."
    )
    return chart_path, caption

    img = Image.new("RGBA", (1480, 980), "#F4F1EA")
    draw = ImageDraw.Draw(img, "RGBA")
    font_title = _get_chart_font(30, bold=True)
    font_sub = _get_chart_font(18, bold=False)
    font_label = _get_chart_font(16, bold=False)
    font_small = _get_chart_font(14, bold=False)
    font_bold = _get_chart_font(18, bold=True)

    panel_fill = (255, 255, 255, 240)
    panel_border = "#D8D1C2"
    main_panel = (50, 90, 960, 545)
    volume_panel = (50, 560, 960, 690)
    rsi_panel = (50, 710, 960, 820)
    macd_panel = (50, 840, 960, 930)
    side_panel = (1000, 90, 1430, 930)

    for panel in (main_panel, volume_panel, rsi_panel, macd_panel, side_panel):
        draw.rounded_rectangle(panel, radius=26, fill=panel_fill, outline=panel_border, width=2)

    draw.text((58, 28), f"Análisis visual de {display_name}", fill="#10233E", font=font_title)
    draw.text((60, 62), "Gráfico limpio con tendencia, indicadores, zonas tácticas y proyección educativa.", fill="#5A677D", font=font_sub)

    def _draw_grid(panel, rows=4):
        x1, y1, x2, y2 = panel
        for idx in range(rows + 1):
            y = y1 + ((y2 - y1) * idx / rows)
            draw.line((x1 + 16, y, x2 - 16, y), fill="#ECE7DC", width=1)

    for panel in (main_panel, volume_panel, rsi_panel, macd_panel):
        _draw_grid(panel, rows=4)

    def _map_series(values, panel, vmin=None, vmax=None, extra_right=0):
        x1, y1, x2, y2 = panel
        usable_x1, usable_x2 = x1 + 20, x2 - 20 - extra_right
        usable_y1, usable_y2 = y1 + 16, y2 - 18
        if vmin is None:
            vmin = min(values)
        if vmax is None:
            vmax = max(values)
        if vmax <= vmin:
            vmax = vmin + 1
        pts = []
        total = max(len(values) - 1, 1)
        for idx, value in enumerate(values):
            x = usable_x1 + ((usable_x2 - usable_x1) * idx / total)
            y = usable_y2 - (((float(value) - vmin) / (vmax - vmin)) * (usable_y2 - usable_y1))
            pts.append((x, y))
        return pts, usable_x2

    closes = pack["closes"]
    price_values = closes + pack["projection"]
    price_min = min(min(pack["bb_lower"]), min(closes), pack["support"], pack["golden_pocket_low"]) * 0.985
    price_max = max(max(pack["bb_upper"]), max(closes), pack["resistance"], pack["golden_pocket_high"]) * 1.015

    close_pts, price_panel_end = _map_series(closes, main_panel, price_min, price_max, extra_right=120)
    ema50_pts, _ = _map_series(pack["ema50"], main_panel, price_min, price_max, extra_right=120)
    ema200_pts, _ = _map_series(pack["ema200"], main_panel, price_min, price_max, extra_right=120)
    bb_upper_pts, _ = _map_series(pack["bb_upper"], main_panel, price_min, price_max, extra_right=120)
    bb_lower_pts, _ = _map_series(pack["bb_lower"], main_panel, price_min, price_max, extra_right=120)

    gp_top = _map_series([pack["golden_pocket_high"]] * len(closes), main_panel, price_min, price_max, extra_right=120)[0]
    gp_bottom = _map_series([pack["golden_pocket_low"]] * len(closes), main_panel, price_min, price_max, extra_right=120)[0]
    if gp_top and gp_bottom:
        polygon = gp_top + list(reversed(gp_bottom))
        draw.polygon(polygon, fill=(240, 196, 92, 55))

    if len(bb_upper_pts) > 1 and len(bb_lower_pts) > 1:
        band_polygon = bb_upper_pts + list(reversed(bb_lower_pts))
        draw.polygon(band_polygon, fill=(125, 134, 158, 30))

    for points, color, width in (
        (bb_upper_pts, "#B7BDC9", 2),
        (bb_lower_pts, "#B7BDC9", 2),
        (ema200_pts, "#2A7E8C", 3),
        (ema50_pts, "#E18D2B", 3),
        (close_pts, "#132B45", 4),
    ):
        if len(points) > 1:
            draw.line(points, fill=color, width=width, joint="curve")

    proj_start = close_pts[-1] if close_pts else None
    if proj_start and pack["projection"]:
        projection_pts = [proj_start]
        for idx, value in enumerate(pack["projection"], start=1):
            x = price_panel_end + (idx * 8)
            _, y_points = None, None
            y = _map_series([value], (main_panel[0], main_panel[1], main_panel[2], main_panel[3]), price_min, price_max, extra_right=120)[0][0][1]
            projection_pts.append((x, y))
        proj_color = "#1B9C5A" if pack["projection"][-1] >= closes[-1] else "#C94D3F"
        for idx in range(len(projection_pts) - 1):
            if idx % 2 == 0:
                draw.line((projection_pts[idx], projection_pts[idx + 1]), fill=proj_color, width=4)
        draw.text((projection_pts[-1][0] - 16, projection_pts[-1][1] - 26), "Proyección", fill=proj_color, font=font_small)

    for level, color, label in (
        (pack["support"], "#1B9C5A", "Soporte"),
        (pack["resistance"], "#C94D3F", "Resistencia"),
    ):
        level_pts, _ = _map_series([level] * len(closes), main_panel, price_min, price_max, extra_right=120)
        if len(level_pts) > 1:
            draw.line(level_pts, fill=color, width=2)
            draw.text((main_panel[2] - 145, level_pts[-1][1] - 16), f"{label} ${fmt_price(level)}", fill=color, font=font_small)

    if divergence.get("active") and divergence.get("pivot_index") is not None:
        pivot_idx = int(divergence["pivot_index"])
        tail_offset = len(pack["closes_full"]) - len(closes)
        local_idx = pivot_idx - tail_offset
        if 0 <= local_idx < len(close_pts):
            px, py = close_pts[local_idx]
            badge_color = "#1B9C5A" if divergence["kind"] == "bullish" else "#C94D3F"
            draw.ellipse((px - 8, py - 8, px + 8, py + 8), fill=badge_color, outline="white", width=2)
            draw.text((px + 14, py - 26), "Divergencia", fill=badge_color, font=font_small)

    volume_max = max(max(pack["volumes"]), 1)
    vol_x1, vol_y1, vol_x2, vol_y2 = volume_panel
    usable_width = (vol_x2 - vol_x1) - 40
    bar_width = max(3, int(usable_width / max(len(pack["volumes"]), 1)))
    for idx, vol in enumerate(pack["volumes"]):
        x = vol_x1 + 20 + idx * bar_width
        h = ((vol / volume_max) * ((vol_y2 - vol_y1) - 28))
        y = vol_y2 - 14 - h
        color = "#1B9C5A" if idx == 0 or pack["closes"][idx] >= pack["closes"][max(idx - 1, 0)] else "#C94D3F"
        draw.rectangle((x, y, x + max(bar_width - 1, 2), vol_y2 - 14), fill=color)

    rsi_pts, _ = _map_series(pack["rsi"], rsi_panel, 0, 100)
    if len(rsi_pts) > 1:
        draw.line(rsi_pts, fill="#7B4B94", width=3)
    for level, color in ((70, "#C94D3F"), (30, "#1B9C5A"), (50, "#8B95A7")):
        level_pts, _ = _map_series([level] * len(pack["rsi"]), rsi_panel, 0, 100)
        draw.line(level_pts, fill=color, width=1)

    macd_vals = pack["macd_hist"]
    macd_min = min(min(macd_vals), min(pack["macd_line"]), min(pack["macd_signal"]), 0)
    macd_max = max(max(macd_vals), max(pack["macd_line"]), max(pack["macd_signal"]), 0)
    macd_line_pts, _ = _map_series(pack["macd_line"], macd_panel, macd_min, macd_max)
    macd_signal_pts, _ = _map_series(pack["macd_signal"], macd_panel, macd_min, macd_max)
    zero_pts, _ = _map_series([0] * len(pack["macd_hist"]), macd_panel, macd_min, macd_max)
    draw.line(zero_pts, fill="#8B95A7", width=1)
    if len(macd_line_pts) > 1:
        draw.line(macd_line_pts, fill="#1B5E8A", width=3)
    if len(macd_signal_pts) > 1:
        draw.line(macd_signal_pts, fill="#D9822B", width=3)
    macd_x1, macd_y1, macd_x2, macd_y2 = macd_panel
    macd_bar_w = max(3, int(((macd_x2 - macd_x1) - 40) / max(len(pack["macd_hist"]), 1)))
    for idx, val in enumerate(pack["macd_hist"]):
        x = macd_x1 + 20 + idx * macd_bar_w
        zero_y = zero_pts[idx][1]
        y = _map_series([val], macd_panel, macd_min, macd_max)[0][0][1]
        draw.rectangle((x, min(y, zero_y), x + max(macd_bar_w - 1, 2), max(y, zero_y)), fill="#5A8F6A" if val >= 0 else "#C57266")

    draw.text((main_panel[0] + 18, main_panel[1] + 12), "Precio + EMA + Bollinger + proyección", fill="#10233E", font=font_bold)
    draw.text((volume_panel[0] + 18, volume_panel[1] + 10), "Volumen", fill="#10233E", font=font_bold)
    draw.text((rsi_panel[0] + 18, rsi_panel[1] + 10), "RSI", fill="#10233E", font=font_bold)
    draw.text((macd_panel[0] + 18, macd_panel[1] + 10), "MACD", fill="#10233E", font=font_bold)
    draw.text((side_panel[0] + 24, side_panel[1] + 18), "Resumen visual", fill="#10233E", font=font_bold)

    summary_lines = [
        f"Activo: {display_name}",
        f"Precio: ${fmt_price(pack['price'])}",
        f"RSI: {pack['rsi']:.1f}",
        f"MACD: {'alcista' if pack['macd_line'] >= pack['macd_signal'] else 'bajista'}",
        f"EMA50 / EMA200: ${fmt_price(pack['ema50'])} / ${fmt_price(pack['ema200'])}",
        f"Soporte / Resistencia: ${fmt_price(pack['support'])} / ${fmt_price(pack['resistance'])}",
        f"Golden pocket: ${fmt_price(pack['golden_pocket_low'])} - ${fmt_price(pack['golden_pocket_high'])}",
    ]
    if divergence.get("active"):
        summary_lines.extend([
            "",
            f"Divergencia: {'alcista' if divergence['kind'] == 'bullish' else 'bajista'}",
            f"Fuerza: {divergence['confidence']}%",
            f"Señales: {', '.join(divergence.get('signals', []))}",
        ])
    else:
        summary_lines.extend(["", "Divergencia: sin señal operable fuerte"])

    projection_hint = "alcista" if pack["projection"] and pack["projection"][-1] >= closes[-1] else "bajista"
    summary_lines.extend([
        "",
        f"Proyección táctica: sesgo {projection_hint}",
        "La línea punteada es un escenario probable, no una garantía.",
    ])

    y_cursor = side_panel[1] + 60
    for line in summary_lines:
        draw.text((side_panel[0] + 24, y_cursor), line, fill="#31425B", font=font_label)
        y_cursor += 30 if line else 18

    chart_dir = os.path.join(DATA_DIR, "charts")
    os.makedirs(chart_dir, exist_ok=True)
    chart_path = os.path.join(chart_dir, f"{tk}_{int(time.time())}.png")
    img.convert("RGB").save(chart_path, format="PNG", optimize=True)

    caption = _make_card(
        f"GRÁFICO TÁCTICO | {display_name}",
        [
            f"• Sesgo visual: {'Alcista' if projection_hint == 'alcista' else 'Bajista'}",
            f"• Divergencia: {divergence['summary'] if divergence.get('active') else 'Sin divergencia operable fuerte por ahora.'}",
            "• La proyección punteada muestra un escenario probable con base en tendencia, estructura e indicadores.",
        ],
        icon="🖼️",
        footer="Lectura institucional, clara y sin contaminación visual."
    )
    return chart_path, caption


def _send_stock_analysis_with_chart(chat_id, ticker):
    tk = remap_ticker(ticker)
    analysis_text = perform_deep_analysis(tk)
    bot.send_message(chat_id, analysis_text, parse_mode="HTML")

    chart_path = None
    try:
        chart_path, chart_caption = _render_stock_analysis_chart(tk, LAST_KNOWN_ANALYSIS.get(tk))
        if chart_path and os.path.exists(chart_path):
            with open(chart_path, "rb") as chart_file:
                bot.send_photo(chat_id, chart_file, caption=chart_caption, parse_mode="HTML")
    except Exception as e:
        logging.error(f"Error generando gráfico táctico para {tk}: {e}")
        bot.send_message(chat_id, _make_card("GRÁFICO TÁCTICO", ["No pude renderizar el gráfico visual en este momento, pero el análisis textual sí quedó listo."], icon="🖼️"), parse_mode="HTML")
    finally:
        if chart_path and os.path.exists(chart_path):
            try:
                os.remove(chart_path)
            except Exception:
                pass


def _monitor_quality_divergences(tracked):
    candidates = []
    for raw_tk in tracked:
        tk = remap_ticker(raw_tk)
        pack = _build_chart_pack(tk, candles=110)
        if not pack:
            continue

        divergence = pack.get("divergence") or {}
        if not divergence.get("active") or divergence.get("confidence", 0) < 78:
            continue

        candidates.append((int(divergence.get("confidence", 0)), tk, pack, divergence))

    alerts_sent = 0
    for _, tk, pack, divergence in sorted(candidates, key=lambda item: item[0], reverse=True):
        if alerts_sent >= 2:
            break

        pivot_date = divergence.get("pivot_date") or datetime.now().strftime("%Y-%m-%d")
        alert_hash = _stable_event_id("DIV", tk, divergence.get("kind"), pivot_date)
        if check_and_add_seen_event(alert_hash):
            continue

        display_name = get_display_name(tk)
        action = "vigilar rebote y confirmación" if divergence.get("kind") == "bullish" else "vigilar distribución y protección"
        msg = _make_card(
            "ALERTA DE DIVERGENCIA",
            [
                f"• Activo: <b>{display_name}</b>",
                f"• Tipo: <b>{'Divergencia alcista' if divergence.get('kind') == 'bullish' else 'Divergencia bajista'}</b>",
                f"• Confirmación: {', '.join(divergence.get('signals', []))}",
                f"• Probabilidad táctica: <b>{divergence.get('confidence', 0)}%</b>",
                f"• Zona clave: soporte ${fmt_price(pack.get('support', 0))} | resistencia ${fmt_price(pack.get('resistance', 0))}",
                f"• Acción sugerida: {action}.",
            ],
            icon="⚡",
            footer="Solo se envían divergencias de mayor calidad para evitar spam."
        )
        bot.send_message(CHAT_ID, msg, parse_mode="HTML")
        alerts_sent += 1


def _perform_deep_analysis_fmp(ticker):
    tk = remap_ticker(ticker)
    display_name = get_display_name(tk)

    quote = _fetch_fmp_quote(tk) or get_safe_ticker_price(tk) or {}
    tech = fetch_and_analyze_stock(tk)
    if not isinstance(tech, dict):
        tech = LAST_KNOWN_ANALYSIS.get(tk) if isinstance(LAST_KNOWN_ANALYSIS.get(tk), dict) else None

    if isinstance(tech, dict) and quote.get("price"):
        tech["price"] = quote["price"]

    price = _safe_float(quote.get("price") or ((tech or {}).get("price")))
    if price <= 0:
        fmp_sym = _get_fmp_symbol(tk)
        diag = _escape_html(_FMP_LAST_ERROR.get(tk, "Sin información de error"))
        key_len = len(FMP_API_KEY) if FMP_API_KEY else 0
        return _make_card(
            f"ANÁLISIS FMP | {display_name}",
            [
                "⚠️ No pude obtener un precio válido desde FMP.",
                f"• Símbolo consultado: <code>{_escape_html(fmp_sym)}</code>",
                f"• Diagnóstico: <code>{diag}</code>",
                f"• API key detectada: {'Sí' if FMP_API_KEY else 'No'} ({key_len} chars)",
                "🛡️ El análisis se bloqueó para evitar inventar datos.",
            ],
            icon="📉",
            footer="Revisa FMP_API_KEY o el símbolo del activo."
        )

    profile = _fetch_fmp_profile(tk) or {}
    news_items = _fetch_fmp_ticker_news(tk, limit=5)
    chart_pack = _build_chart_pack(tk, candles=110) or {}
    divergence = chart_pack.get("divergence") or {}
    projection = chart_pack.get("projection") or []

    company_name = profile.get("companyName") or profile.get("companyNameLong") or quote.get("name") or display_name
    sector = (
        profile.get("sector")
        or profile.get("sectorTitle")
        or profile.get("sicSector")
        or profile.get("exchangeSector")
        or "No disponible"
    )
    industry = (
        profile.get("industry")
        or profile.get("industryTitle")
        or profile.get("sicIndustry")
        or profile.get("exchangeIndustry")
        or "No disponible"
    )
    description_raw = (
        profile.get("description")
        or profile.get("companyDescription")
        or profile.get("businessAddressDescription")
        or ""
    )
    description = _truncate_text(_translate_text_to_spanish(description_raw, max_chars=420), 190)

    change_abs = _safe_float(quote.get("change"))
    change_pct = _safe_float(quote.get("changesPercentage"))
    if change_pct == 0 and price and (price - change_abs) > 0:
        change_pct = (change_abs / (price - change_abs)) * 100

    volume = _safe_float(quote.get("volume"))
    avg_volume = _safe_float(quote.get("avgVolume"))
    pe = _safe_float(quote.get("pe") or profile.get("pe") or profile.get("priceEarningsRatio"))
    beta = _safe_float(profile.get("beta") or quote.get("beta") or profile.get("betaValue"))
    market_cap = _safe_float(profile.get("mktCap") or quote.get("marketCap"))

    support = _safe_float((tech or {}).get("smc_sup"), price * 0.97)
    resistance = _safe_float((tech or {}).get("smc_res"), price * 1.03)
    order_block = _safe_float((tech or {}).get("order_block"), price)
    take_profit = _safe_float((tech or {}).get("take_profit"), resistance)
    stop_loss = _safe_float((tech or {}).get("stop_loss"), support * 0.98)
    rsi = _safe_float((tech or {}).get("rsi"), 50.0)
    macd_line = _safe_float((tech or {}).get("macd_line"))
    macd_signal = _safe_float((tech or {}).get("macd_signal"))
    rvol = _safe_float((tech or {}).get("rvol"), (volume / avg_volume) if avg_volume > 0 else 1.0)
    smc_trend = str((tech or {}).get("smc_trend", "Neutral"))
    sma50 = _safe_float((tech or {}).get("sma50"), price)
    sma200 = _safe_float((tech or {}).get("sma200"), price)
    ema50 = _safe_float((tech or {}).get("ema50"), price)
    ema200 = _safe_float((tech or {}).get("ema200"), price)
    bb_upper = _safe_float((tech or {}).get("bb_upper"), price * 1.03)
    bb_lower = _safe_float((tech or {}).get("bb_lower"), price * 0.97)
    bb_basis = _safe_float((tech or {}).get("bb_basis"), price)
    donchian_upper = _safe_float((tech or {}).get("donchian_upper"), resistance)
    donchian_lower = _safe_float((tech or {}).get("donchian_lower"), support)
    donchian_mid = _safe_float((tech or {}).get("donchian_mid"), price)
    obv_trend = str((tech or {}).get("obv_trend", "Neutral"))
    fib_382 = _safe_float((tech or {}).get("fib_382"), resistance)
    fib_500 = _safe_float((tech or {}).get("fib_500"), price)
    fib_618 = _safe_float((tech or {}).get("fib_618"), support)
    golden_pocket_low = _safe_float((tech or {}).get("golden_pocket_low"), fib_618)
    golden_pocket_high = _safe_float((tech or {}).get("golden_pocket_high"), fib_618)
    projection_target = float(projection[-1]) if projection else price
    if projection and projection_target >= price * 1.005:
        projection_bias = "alcista"
    elif projection and projection_target <= price * 0.995:
        projection_bias = "bajista"
    else:
        projection_bias = "neutral"

    reward_pct = ((take_profit - price) / price * 100) if price > 0 else 0.0
    risk_pct = ((price - stop_loss) / price * 100) if price > 0 else 0.0
    risk_reward = (reward_pct / risk_pct) if risk_pct > 0 else 0.0

    bullish_reasons = []
    bearish_reasons = []
    score = 0

    if price >= ema50 and ema50 >= ema200:
        score += 1
        bullish_reasons.append("precio sostiene EMA50/EMA200")
    elif price <= ema50 and ema50 <= ema200:
        score -= 1
        bearish_reasons.append("precio bajo EMA50/EMA200")

    if sma50 >= sma200:
        score += 1
        bullish_reasons.append("SMA50 por encima de SMA200")
    else:
        score -= 1
        bearish_reasons.append("SMA50 por debajo de SMA200")

    if "ALCISTA" in smc_trend.upper():
        score += 2
        bullish_reasons.append("tendencia SMC alcista")
    elif "BAJISTA" in smc_trend.upper():
        score -= 2
        bearish_reasons.append("tendencia SMC bajista")

    if macd_line >= macd_signal:
        score += 1
        bullish_reasons.append("MACD por encima de su señal")
    else:
        score -= 1
        bearish_reasons.append("MACD por debajo de su señal")

    if rsi <= 35:
        score += 1
        bullish_reasons.append("RSI en zona de rebote")
    elif rsi >= 70:
        score -= 2
        bearish_reasons.append("RSI en sobrecompra")
    elif rsi >= 60:
        score -= 1
        bearish_reasons.append("RSI algo exigido")
    else:
        bullish_reasons.append("RSI aún no está estirado")

    if price <= support * 1.02:
        score += 2
        bullish_reasons.append("precio cerca de soporte institucional")
    elif price >= resistance * 0.98:
        score -= 2
        bearish_reasons.append("precio demasiado cerca de resistencia")
    elif price <= order_block * 1.02:
        score += 1
        bullish_reasons.append("cotiza sobre el order block")

    if golden_pocket_low <= price <= golden_pocket_high:
        score += 1
        bullish_reasons.append("precio dentro del golden pocket")

    if price <= bb_lower:
        score += 1
        bullish_reasons.append("precio en banda baja de Bollinger")
    elif price >= bb_upper:
        score -= 1
        bearish_reasons.append("precio en banda alta de Bollinger")

    if price <= donchian_lower * 1.01:
        score += 1
        bullish_reasons.append("precio cerca del piso Donchian")
    elif price >= donchian_upper * 0.99:
        score -= 1
        bearish_reasons.append("precio cerca del techo Donchian")

    if "ASC" in obv_trend.upper():
        score += 1
        bullish_reasons.append("OBV con acumulación")
    elif "DESC" in obv_trend.upper():
        score -= 1
        bearish_reasons.append("OBV con distribución")

    if divergence.get("active"):
        divergence_signals = ", ".join(divergence.get("signals", [])) or "señales internas"
        if divergence.get("kind") == "bullish":
            score += 1
            bullish_reasons.append(f"divergencia alcista validada por {divergence_signals}")
        else:
            score -= 1
            bearish_reasons.append(f"divergencia bajista validada por {divergence_signals}")

    if rvol >= 1.5 and change_pct >= 0:
        score += 1
        bullish_reasons.append(f"volumen acompaña ({rvol:.1f}x)")
    elif rvol >= 1.5 and change_pct < 0:
        score -= 1
        bearish_reasons.append(f"presión vendedora con volumen ({rvol:.1f}x)")

    if pe > 0 and pe <= 35:
        score += 1
        bullish_reasons.append(f"P/E razonable ({pe:.1f})")
    elif pe >= 60:
        score -= 1
        bearish_reasons.append(f"valoración exigente (P/E {pe:.1f})")

    if beta >= 1.7:
        bearish_reasons.append(f"beta alta ({beta:.2f})")

    translated_titles = []
    raw_titles = [item.get("title", "").strip() for item in news_items if item.get("title")]
    if raw_titles:
        translated_titles = _translate_titles_to_spanish_v2(raw_titles)
        news_sentiment = sum(_infer_sentiment_from_title(title) for title in raw_titles) / len(raw_titles)
    else:
        news_sentiment = 0.0

    news_signal = _classify_sentiment(news_sentiment)
    if news_sentiment >= 0.25:
        score += 1
        bullish_reasons.append("titulares recientes con sesgo favorable")
    elif news_sentiment <= -0.25:
        score -= 1
        bearish_reasons.append("titulares recientes con sesgo adverso")

    if price <= stop_loss:
        verdict = "VENTA DEFENSIVA"
        thesis = "El precio ya perforó la zona táctica de defensa y ahora prima proteger capital."
    elif score >= 4 and reward_pct >= 4 and risk_reward >= 1.3:
        verdict = "COMPRA FACTIBLE"
        thesis = "La estructura acompaña y el precio sigue en una zona donde la relación beneficio/riesgo todavía es razonable."
    elif score <= -3 or price >= resistance * 0.99 or rsi >= 70:
        verdict = "VENTA / REDUCIR"
        thesis = "El activo se ve exigido o demasiado cerca de resistencia; tiene más sentido asegurar y esperar mejor reentrada."
    elif score >= 2:
        verdict = "MANTENER CON SESGO A COMPRA"
        thesis = "La lectura es favorable, pero conviene exigir confirmación adicional antes de perseguir el precio."
    else:
        verdict = "ESPERAR MEJOR ENTRADA"
        thesis = "Hay señales mixtas; lo más sano es no forzar una entrada hasta que el activo limpie estructura."

    confidence = int(max(55, min(92, 56 + abs(score) * 7 + (4 if risk_reward >= 1.5 else 0))))

    rr_text = f"{risk_reward:.2f}x" if risk_reward > 0 else "N/D"
    pe_text = f"{pe:.1f}" if pe > 0 else "No disponible"
    beta_text = f"{beta:.2f}" if beta > 0 else "No disponible"

    lines = [
        "🧾 <b>Resumen ejecutivo</b>",
        f"• Empresa: <b>{_escape_html(company_name)}</b>",
        f"• Sector: {_escape_html(sector)} | Industria: {_escape_html(industry)}",
        f"• Precio actual: <b>${fmt_price(price)}</b> ({change_pct:+.2f}% hoy)",
        f"• Capitalización: {_format_compact_money(market_cap)} | P/E: {pe_text} | Beta: {beta_text}",
    ]

    if description:
        lines.append(f"• Negocio: {_escape_html(description)}")

    lines.extend([
        "",
        "📊 <b>Lectura técnica FMP</b>",
        f"• Tendencia SMC: <b>{_escape_html(smc_trend)}</b>",
        f"• RSI: {rsi:.1f} | MACD: {macd_line:.3f} vs señal {macd_signal:.3f}",
        f"• Soporte: ${fmt_price(support)} | Resistencia: ${fmt_price(resistance)}",
        f"• Order block: ${fmt_price(order_block)} | Volumen relativo: {rvol:.2f}x",
        f"• Objetivo táctico: ${fmt_price(take_profit)} | Stop táctico: ${fmt_price(stop_loss)}",
    ])

    lines.append(f"• EMA50/EMA200: ${fmt_price(ema50)} / ${fmt_price(ema200)}")
    lines.append(f"• SMA50/SMA200: ${fmt_price(sma50)} / ${fmt_price(sma200)}")
    lines.append(f"• Fibonacci 0.382/0.5/0.618: ${fmt_price(fib_382)} / ${fmt_price(fib_500)} / ${fmt_price(fib_618)}")
    lines.append(f"• Golden pocket: ${fmt_price(golden_pocket_low)} - ${fmt_price(golden_pocket_high)}")
    lines.append(f"• Bollinger: baja ${fmt_price(bb_lower)} | media ${fmt_price(bb_basis)} | alta ${fmt_price(bb_upper)}")
    lines.append(f"• Donchian: piso ${fmt_price(donchian_lower)} | medio ${fmt_price(donchian_mid)} | techo ${fmt_price(donchian_upper)}")
    lines.append(f"• OBV: <b>{_escape_html(obv_trend)}</b>")
    if divergence.get("active"):
        lines.append(f"• Divergencia: {_escape_html(divergence.get('summary', 'Señal confirmada'))} <b>({int(divergence.get('confidence', 0))}%)</b>")
    else:
        lines.append("• Divergencia: sin señal de alta calidad por ahora")
    lines.append(f"• Proyección táctica: sesgo <b>{projection_bias}</b> hacia ${fmt_price(projection_target)}")

    if not tech:
        lines.append("• Nota: FMP no devolvió histórico suficiente; esta lectura pesa más precio, volumen y noticias.")

    lines.extend([
        "",
        "📰 <b>Contexto reciente</b>",
        f"• Sesgo de noticias: {news_signal['icon']} {_escape_html(news_signal['label'])}",
    ])

    if translated_titles:
        for title in translated_titles[:3]:
            lines.append(f"• {_escape_html(_truncate_text(title, 105))}")
    else:
        lines.append("• Sin titulares recientes relevantes para este activo en FMP.")

    lines.extend([
        "",
        "🎯 <b>Veredicto operativo</b>",
        f"• Acción sugerida: <b>{verdict}</b>",
        f"• Confianza del setup: <b>{confidence}%</b>",
        f"• A favor: {_escape_html('; '.join(bullish_reasons[:3]) if bullish_reasons else 'Sin señales fuertes a favor')}",
        f"• En contra: {_escape_html('; '.join(bearish_reasons[:3]) if bearish_reasons else 'Sin señales fuertes en contra')}",
        f"• Relación beneficio/riesgo estimada: <b>{rr_text}</b>",
        f"• Lectura final: {_escape_html(thesis)}",
    ])

    return _make_card(
        f"ANÁLISIS FMP | {display_name}",
        lines,
        icon="📈",
        footer="Análisis institucional con datos FMP."
    )


def perform_deep_analysis(ticker):
    return _perform_deep_analysis_fmp(ticker)

    # Bloque legacy conservado debajo por compatibilidad temporal.
    tk = remap_ticker(ticker)
    display_name = get_display_name(tk)

    # PASO 0: Obtener precio VERIFICADO de FMP ANTES de todo
    # Este precio es SAGRADO â€” viene directo del exchange via FMP
    verified_price = None
    fmp_data = _fetch_fmp_quote(tk)
    if fmp_data:
        verified_price = fmp_data['price']
        logging.info(f"ANÃLISIS {tk}: precio FMP verificado = ${fmt_price(verified_price)}")

    # PASO 1: Obtener indicadores técnicos (RSI, MACD, SMC) via FMP historical
    tech = fetch_and_analyze_stock(tk)

    # SIEMPRE imponer el precio FMP quote sobre el precio del historial
    if tech and verified_price:
        tech['price'] = verified_price

    # Si no hay tech pero sí tenemos precio verificado
    if not tech and not verified_price:
        live = get_safe_ticker_price(tk)
        if live:
            verified_price = live['price']

    # Precio final a inyectar en el prompt (INNEGOCIABLE)
    final_price = verified_price or (tech['price'] if tech else None)

    # === HARD-STOP: SIN PRECIO VERIFICADO = SIN ANÃLISIS ===
    if not final_price:
        fmp_sym = _get_fmp_symbol(tk)
        diag = _FMP_LAST_ERROR.get(tk, 'Sin información de error')
        _key_len = len(FMP_API_KEY) if FMP_API_KEY else 0
        return (f"⚠️ <b>Error de conexión con FMP</b>\n\n"
                f"No se pudo obtener el precio de {display_name} "
                f"(símbolo: {fmp_sym}).\n\n"
                f"🔒 <b>Diagnóstico:</b>\n<code>{diag}</code>\n\n"
                f"🔒‘ Key cargada: {'Sí' if FMP_API_KEY else 'NO'} "
                f"({_key_len} chars)\n"
                f"ðŸ›‘ Análisis BLOQUEADO para evitar datos inventados.")

    if tech:
        tech_block = (
            f"--- DATOS EN VIVO (calculados por el sistema, NO los inventes) ---\n"
            f"â€¢ Precio EXACTO en vivo: ${fmt_price(final_price)}\n"
            f"â€¢ RSI (14 períodos): {tech['rsi']:.2f}\n"
            f"â€¢ MACD Línea: {tech['macd_line']:.4f}\n"
            f"â€¢ MACD Señal: {tech['macd_signal']:.4f}\n"
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
            f"â€¢ Indicadores técnicos: No disponibles (mercado cerrado o sin historial)\n"
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
        f"FORMATO DE RESPUESTA EN HTML SIMPLE PARA TELEGRAM:\n"
        f"<b>📊 Análisis Smart Money (SMC):</b> [Profundiza sobre liquidez, imbalances y el order block actual]\n"
        f"<b>📰 Contexto Macro / Institucional:</b> [Tu lectura de cómo el flujo de impacto altera la técnica]\n"
        f"<b>🎯 Veredicto Final:</b> [COMPRAR / VENDER / MANTENER] + [Justificación institucional en 2 líneas]\n\n"
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

    total_invested = 0.0
    total_current = 0.0
    details = []

    for tk, dt in investments.items():
        init_amount = dt['amount_usd']
        entry_p = dt['entry_price']
        intra = fetch_intraday_data(tk)
        safe_price = get_safe_ticker_price(tk) if not intra else None

        display_name = get_display_name(tk)

        if intra or safe_price:
            live_price = intra['latest_price'] if intra else safe_price['price']
            roi_percent = (live_price - entry_p) / entry_p if entry_p > 0 else 0
            curr_val = init_amount * (1 + roi_percent)

            total_invested += init_amount
            total_current += curr_val

            sign = "+" if roi_percent >= 0 else ""
            icon = "🟢" if roi_percent >= 0 else "🔴"
            details.append(f"{icon} <b>{display_name}</b> | {sign}{roi_percent*100:.2f}% | ${fmt_price(live_price)}")
        else:
            total_invested += init_amount
            total_current += init_amount
            details.append(f"⏳ <b>{display_name}</b> | Sin precio en vivo | Entrada ${fmt_price(entry_p)}")

    total_roi = (total_current - total_invested) / total_invested if total_invested > 0 else 0
    sign_roi = "+" if total_roi >= 0 else ""
    status_icon = "🟢 EN GANANCIA" if total_roi >= 0 else "🔴 EN PÉRDIDA"

    goal = 0.10
    progress_ratio = max(0, min(1, total_roi / goal))

    filled_blocks = int(progress_ratio * 10)
    empty_blocks = 10 - filled_blocks
    bar = "█" * filled_blocks + "░" * empty_blocks
    progress_text = f"{int(progress_ratio*100)}%"
    total_current_display = total_current if investments else 0.0
    total_invested_display = total_invested if investments else 0.0

    overview = [
        f"💼 <b>Capital activo:</b> ${total_current_display:,.2f}",
        f"🧾 <b>Capital invertido:</b> ${total_invested_display:,.2f}",
        f"📈 <b>Rendimiento activo:</b> {sign_roi}{total_roi*100:.2f}%",
        f"📊 <b>Estado:</b> {status_icon}",
        f"🎯 <b>Meta mensual:</b> <code>{bar}</code> {progress_text}",
        f"🔥 <b>Realizado del mes:</b> {'+' if realized_pnl>=0 else ''}${realized_pnl:,.2f} USD",
    ]

    if details:
        overview.append("")
        overview.append("🧠 <b>Posiciones abiertas</b>")
        overview.extend(details)
    else:
        overview.append("")
        overview.append("⚪ <i>Sin posiciones abiertas en este momento.</i>")

    return _make_card("CARTERA GÉNESIS", overview, icon="💎")


# ----------------- CONTROLADORES TELEBOT (NLP & ACCIONES DIRECTAS) -----------------

@bot.message_handler(commands=['check_db'])
def test_db(message):
    print("â ³ Intentando conectar con Supabase (Timeout de 5 segundos)...")
    try:
        conn = get_db_connection()
        if not conn:
             print("â Œ ERROR DE RED O AUTENTICACIÓN: conn es None")
             return
        c = conn.cursor()
        c.execute('SELECT version();')
        v = c.fetchone()[0]
        print(f"✅ CONEXIÓN ESTABLECIDA\nPostgreSQL OK. Base de Datos en línea y funcional.\n\nDetalle: {v}")
    except Exception as e:
        print(f"â Œ ERROR DE RED O AUTENTICACIÓN\nSupabase ha rechazado la conexión.\n\nLog Técnico: {e}")

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
    except Exception as e:
        bot.reply_to(message, f"âŒ Fallo al limpiar DB: {e}")
    finally:
        pass # conn.close() delegado a pooling global

@bot.message_handler(commands=['start'])
def cmd_start(message):
    if str(message.chat.id) != str(CHAT_ID): return
    restore_state_from_telegram()
    tkrs = get_tracked_tickers()
    
    # 1. INLINE KEYBOARD (Flotante)
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton(text="🌍 Geopolítica", callback_data="geopolitics"),
        InlineKeyboardButton(text="🐋 Radar de Ballenas", callback_data="super_radar_24h")
    )
    markup.add(
        InlineKeyboardButton(text="🦅 Niveles SMC", callback_data="smc_levels"),
        InlineKeyboardButton(text="💼 Mi Cartera", callback_data="wallet_status")
    )

    # 2. REPLY KEYBOARD (Botones fijos abajo)
    from telebot.types import ReplyKeyboardMarkup, KeyboardButton
    reply_kbd = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    reply_kbd.add(
        KeyboardButton("🌍 Geopolítica"),
        KeyboardButton("🐋 Radar de Ballenas")
    )
    reply_kbd.add(
        KeyboardButton("🦅 Niveles SMC"),
        KeyboardButton("💼 Mi Cartera")
    )
    
    bot.send_message(message.chat.id, "🔄 Inicializando Base de Operaciones...", reply_markup=reply_kbd)

    reply_text = _make_card(
        "GÉNESIS 1.0",
        [
            "✅ Bot iniciado correctamente.",
            f"📊 <b>Radar activo:</b> {len(tkrs)} activos",
            "🛡️ <b>Persistencia:</b> cartera protegida y lista para operar",
            "🎛️ Usa los botones de abajo o el panel flotante para navegar",
        ],
        icon="🧠"
    )
    bot.reply_to(message, reply_text, reply_markup=markup, parse_mode="HTML")
@bot.message_handler(commands=['reset_pnl'])
def cmd_reset_pnl(message):
    """Comando oculto para resetear la ganancia mensual a $0.00"""
    if str(message.chat.id) != str(CHAT_ID): return
    reset_realized_pnl()
    bot.reply_to(message, "🔒„ <b>PnL Mensual Reseteado</b>\n\n✅ Ganancia Mensual Acumulada: <b>$0.00 USD</b>\n✅ Contabilidad limpia desde este momento.", parse_mode="HTML")

@bot.message_handler(commands=['reset_total'])
def cmd_reset_total(message):
    """RESET RADICAL: borra todo el historial contable"""
    if str(message.chat.id) != str(CHAT_ID): return
    reset_total_db()
    bot.reply_to(message, (
        "⚠️ <b>SISTEMA REINICIADO</b>\n\n"
        "ðŸ—‘ï¸ Todo el historial contable ha sido eliminado.\n"
        "💧 Capital Operativo: <b>$0.00</b>\n"
        "💰 Ganancia Mensual: <b>$0.00 USD</b>\n"
        "📈 Rendimiento: <b>0.00%</b>\n\n"
        "✅ Cartera limpia. Los activos en tu radar siguen activos para monitoreo SMC."
    ), parse_mode="HTML")


@bot.message_handler(commands=['recover'])
def cmd_recover(message):
    """Herramienta de Carga Crítica de Respaldo por Base64"""
    if str(message.chat.id) != str(CHAT_ID): return
    try:
        command_parts = message.text.split(' ', 1)
        if len(command_parts) < 2:
            bot.reply_to(message, "⚠️ Restauración Crítica.\nUso: `/recover [STRING_BASE64_DEL_LOG]`", parse_mode="Markdown")
            return

        b64_str = command_parts[1].strip()
        _restore_from_b64(b64_str)
        save_state_to_telegram()  # Guardar inmediatamente en Telegram

        tkrs = get_tracked_tickers()
        bot.reply_to(message, f"✅ **Â¡RECUPERACIÓN EXITOSA!**\nSe restauraron {len(tkrs)} activos.\nEl backup ya fue guardado en Telegram.", parse_mode="Markdown")

        for tk in tkrs:
            val = fetch_and_analyze_stock(tk)
            if val: update_smc_memory(tk, val)

    except Exception as e:
        bot.reply_to(message, f"âŒ Error en recuperación: `{e}`", parse_mode="Markdown")

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
    msg = bot.reply_to(message, "👁️ Analizando gráfica con Visión GÉNESIS y lectura técnica avanzada...")
    try:
        if not OPENAI_API_KEY:
            bot.edit_message_text("⚠️ Error de configuración del modelo: OPENAI_API_KEY no detectada.", chat_id=message.chat.id, message_id=msg.message_id)
            return

        file_info = bot.get_file(message.photo[-1].file_id)
        image_bytes = bot.download_file(file_info.file_path)
        base_img = base64.b64encode(image_bytes).decode('utf-8')

        client = OpenAI(api_key=OPENAI_API_KEY)
        prompt = (
            "Eres Visión GÉNESIS, un analista técnico institucional.\n\n"
            "Analiza SOLO lo que sea realmente visible en la imagen. No inventes datos, niveles ni indicadores.\n"
            "Mantén un tono profesional, claro y fácil de entender para una persona no experta, pero con criterio serio de mesa institucional.\n"
            "Debes evaluar, si se ven en la imagen: estructura SMC, liquidez, BOS, CHoCH, order blocks, RSI, MACD, volumen, EMA 50, EMA 200, SMA 50, SMA 200, retrocesos de Fibonacci, golden pocket, bandas de Bollinger, canales de Donchian, OBV y divergencias.\n"
            "No expliques teoría. Entrega lectura operativa.\n\n"
            "Responde EXACTAMENTE en ESPAÑOL con este formato:\n"
            "📊 CONTEXTO TÉCNICO: [tendencia actual, estructura y liquidez en 2 o 3 líneas].\n"
            "📐 INDICADORES CONFIRMADOS: [menciona SOLO los indicadores que realmente logres leer o inferir con suficiente claridad en la imagen. Ejemplo de estilo: RSI 61 con momentum alcista; MACD cruzado al alza; volumen creciente; EMA50 sobre EMA200. Si la imagen no permite una lectura seria de indicadores, escribe: Sin indicadores confiables para esta imagen.]\n"
            "⚡ DIVERGENCIAS: [solo si detectas una divergencia con suficiente claridad. Si no hay una lectura confiable, escribe: Sin divergencias claras.]\n"
            "🎯 NIVELES CLAVE: [soportes, resistencias, order blocks y zonas tácticas con precios si se alcanzan a leer].\n"
            "⚠️ RIESGO DE INVERSIÓN: [Bajo / Medio / Alto] - [motivo técnico directo].\n"
            "⚖️ SESGO DIRECCIONAL: [Fuerte Alcista / Alcista / Neutral / Bajista / Fuerte Bajista / Esperar Confirmación] - [justificación breve].\n"
            "🧭 PLAN TÁCTICO: [qué confirmación esperar, dónde protegerse y qué invalidaría la idea]."
        )

        res = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "user", "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base_img}"}}
                ]}
            ],
            max_tokens=950
        )

        vision_report = res.choices[0].message.content.strip()
        report_lines = []
        for raw_line in vision_report.splitlines():
            cleaned_line = raw_line.strip()
            report_lines.append(_escape_html(cleaned_line) if cleaned_line else "")

        bot.edit_message_text(
            _make_card(
                "REPORTE VISUAL GÉNESIS",
                report_lines,
                icon="👁️",
                footer="Lectura institucional basada en lo visible en la imagen."
            ),
            chat_id=message.chat.id,
            message_id=msg.message_id,
            parse_mode="HTML"
        )
        return
    except Exception as e:
        logging.error(f"Error de visión OpenAI: {e}")
        bot.edit_message_text("⚠️ No pude completar el análisis visual en este momento.", chat_id=message.chat.id, message_id=msg.message_id)
        return
    msg = bot.reply_to(message, "ðŸ‘ï¸ Analizando gráfica con GÉNESIS Vision (GPT-4o OpenAI)...")
    try:
        if not OPENAI_API_KEY:
            bot.edit_message_text("⚠️ Error de configuración de modelo: OPENAI_API_KEY no detectada.", chat_id=message.chat.id, message_id=msg.message_id)
            return

        file_info = bot.get_file(message.photo[-1].file_id)
        image_bytes = bot.download_file(file_info.file_path)
        base_img = base64.b64encode(image_bytes).decode('utf-8')
        
        client = OpenAI(api_key=OPENAI_API_KEY)
        
        prompt = (
            "Actúa como una herramienta educativa de análisis técnico. Este análisis es puramente para fines de estudio y simulación, no es asesoría financiera. Analiza la siguiente imagen de manera objetiva.\n\n"
            "Analiza bajo conceptos Smart Money Concepts (SMC). PROHIBIDO explicar qué significa SMC, Order Blocks, BOS o CHoCH. CERO TEORÃA.\n\n"
            "Tu respuesta DEBE seguir ESTRICTAMENTE este formato, sin agregar introducciones ni despedidas. Tono frío, analítico y directo a los datos duros:\n\n"
            "📊 CONTEXTO TÉCNICO: [1 o 2 líneas sobre la tendencia actual y la acción del precio evaluando liquidez y estructura].\n"
            "🎯 NIVELES CLAVE: [Soportes, Resistencias u Order Blocks con PRECIOS EXACTOS según la gráfica].\n"
            "⚠️ RIESGO DE INVERSIÓN: [Bajo / Medio / Alto] - [Razón técnica directa].\n"
            "⚖️ SESGO DIRECCIONAL: [Fuerte Alcista / Fuerte Bajista / Neutral / Esperar Confirmación] - [Justificación descriptiva en una línea, ej. 'Alta probabilidad de rebote en FVG en $150']."
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
        bot.edit_message_text("\u26a0\ufe0f Error de configuraci\u00f3n de modelo", chat_id=message.chat.id, message_id=msg.message_id)

def _normalize_menu_text(text):
    cleaned = _clean_outgoing_text((text or "").replace("\ufe0f", ""))
    normalized = unicodedata.normalize("NFKD", cleaned)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = re.sub(r"[^a-zA-Z0-9\s]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip().lower()


def _extract_analysis_ticker(intent_text):
    patterns = [
        r'\bANALIZA\b\s+(?:LA\s+ACCION\s+|EL\s+ACTIVO\s+|ACCIONES\s+DE\s+|DE\s+)?([A-Z0-9\-=]+)',
        r'\bANALIZAR\b\s+(?:LA\s+ACCION\s+|EL\s+ACTIVO\s+|ACCIONES\s+DE\s+|DE\s+)?([A-Z0-9\-=]+)',
        r'\bANALISIS\s+DE\s+([A-Z0-9\-=]+)',
        r'\bREVISA\b\s+(?:LA\s+ACCION\s+|EL\s+ACTIVO\s+|DE\s+)?([A-Z0-9\-=]+)',
        r'\bOPINAS\s+DE\s+([A-Z0-9\-=]+)',
    ]
    for pattern in patterns:
        match = re.search(pattern, intent_text)
        if match:
            return match.group(1)
    return None


def _make_card(title, lines, icon="🧠", footer=None):
    message_lines = [f"{icon} <b>{title}</b>", "━━━━━━━━━━━━━━━━━━━━"]
    message_lines.extend(lines)
    if footer:
        message_lines.extend(["━━━━━━━━━━━━━━━━━━━━", footer])
    return "\n".join(line for line in message_lines if line is not None)


def _stable_event_id(prefix, *parts):
    payload = "||".join(str(part) for part in parts)
    digest = hashlib.sha1(payload.encode('utf-8')).hexdigest()[:16]
    return f"{prefix}_{digest}"


def _refresh_smc_snapshot(tickers, force=False):
    refreshed = 0
    for raw_tk in tickers:
        tk = remap_ticker(raw_tk)
        cached = SMC_LEVELS_MEMORY.get(tk)
        is_stale = True

        if cached and isinstance(cached.get('update_date'), datetime):
            is_stale = (datetime.now() - cached['update_date']).total_seconds() >= 3600

        if not force and cached and not is_stale:
            continue

        analysis = fetch_and_analyze_stock(tk)
        if analysis and isinstance(analysis, dict):
            update_smc_memory(tk, analysis)
            LAST_KNOWN_ANALYSIS[tk] = analysis
            refreshed += 1

    return refreshed


def _send_super_radar_report(chat_id):
    try:
        tkrs = get_tracked_tickers()
        if not tkrs:
            bot.send_message(chat_id, _make_card("RADAR DE BALLENAS", ["✅ Tu radar está vacío."], icon="🐋"), parse_mode="HTML")
            return

        history_24h = _iter_whale_history_entries(hours=24)
        buys_24h = [item for item in history_24h if item.get("direction") == "buy"]
        sells_24h = [item for item in history_24h if item.get("direction") == "sell"]
        alerted_premium = [item for item in history_24h if item.get("alert_sent")]
        buy_cap = sum(float(item.get("vol_usd", 0) or 0) for item in buys_24h)
        sell_cap = sum(float(item.get("vol_usd", 0) or 0) for item in sells_24h)

        lines = [
            f"🛰️ <b>Activos rastreados:</b> {len(tkrs)}",
            f"📥 <b>Entradas 24h:</b> {len(buys_24h)} | Capital: {_format_compact_money(buy_cap)}",
            f"📤 <b>Salidas 24h:</b> {len(sells_24h)} | Capital: {_format_compact_money(sell_cap)}",
            "🎯 <b>Alertas:</b> se envían todas las ballenas élite/ganadoras que pasen filtro",
            "🧠 <b>Radar 24h:</b> muestra entradas y salidas detectadas aunque no hayan sido enviadas como alerta",
        ]

        if buys_24h:
            lines.extend(["", "🐳 <b>Entradas institucionales 24h</b>"])
            for item in buys_24h[:4]:
                display_name = get_display_name(item['ticker'])
                minutes_ago = int((datetime.now() - item['timestamp']).total_seconds() / 60)
                quality = "élite" if item.get("winner_only") else "flujo detectado"
                lines.append(f"• {display_name} | {_format_compact_money(item.get('vol_usd', 0))} | {quality} | hace {minutes_ago} min")

        if sells_24h:
            lines.extend(["", "🧨 <b>Salidas institucionales 24h</b>"])
            for item in sells_24h[:4]:
                display_name = get_display_name(item['ticker'])
                minutes_ago = int((datetime.now() - item['timestamp']).total_seconds() / 60)
                lines.append(f"• {display_name} | {_format_compact_money(item.get('vol_usd', 0))} | distribución | hace {minutes_ago} min")

        if alerted_premium:
            lines.extend(["", "🔥 <b>Alertas élite enviadas</b>"])
            for item in alerted_premium[:3]:
                display_name = get_display_name(item['ticker'])
                minutes_ago = int((datetime.now() - item['timestamp']).total_seconds() / 60)
                lines.append(f"• {display_name} | {_format_compact_money(item.get('vol_usd', 0))} | hace {minutes_ago} min")

        if not buys_24h and not sells_24h:
            lines.extend(["", "🌊 Mercado en calma.", "Sin flujos institucionales relevantes guardados en las últimas 24 horas."])

        bot.send_message(chat_id, _make_card("RADAR DE BALLENAS", lines, icon="🐋"), parse_mode="HTML")
        return
    except Exception as e:
        print(f"ERROR RADAR: {e}")
        try:
            bot.send_message(chat_id, _make_card("RADAR DE BALLENAS", [f"⚠️ Error interno en radar: {e}"], icon="🐋"), parse_mode="HTML")
        except Exception:
            pass
        return

    try:
        tkrs = get_tracked_tickers()
        if not tkrs:
            bot.send_message(chat_id, _make_card("RADAR DE BALLENAS", ["✅ Tu radar está vacío."], icon="🐋"), parse_mode="HTML")
            return

        history_24h = _iter_whale_history_entries(hours=24)
        buys_24h = [item for item in history_24h if item.get("direction") == "buy"]
        sells_24h = [item for item in history_24h if item.get("direction") == "sell"]
        buy_cap = sum(float(item.get("vol_usd", 0) or 0) for item in buys_24h)
        sell_cap = sum(float(item.get("vol_usd", 0) or 0) for item in sells_24h)

        lines = [
            f"🛰️ <b>Activos rastreados:</b> {len(tkrs)}",
            f"📥 <b>Entradas 24h:</b> {len(buys_24h)} | Capital: {_format_compact_money(buy_cap)}",
            f"📤 <b>Salidas 24h:</b> {len(sells_24h)} | Capital: {_format_compact_money(sell_cap)}",
            f"🎯 <b>Modo de alertas:</b> se envían todas las ballenas élite/ganadoras que pasen filtro",
        ]

        if buys_24h:
            lines.extend(["", "🐳 <b>Entradas institucionales 24h</b>"])
            for item in buys_24h[:4]:
                display_name = get_display_name(item['ticker'])
                minutes_ago = int((datetime.now() - item['timestamp']).total_seconds() / 60)
                quality = "élite" if item.get("winner_only") else "flujo detectado"
                lines.append(f"• {display_name} | {_format_compact_money(item.get('vol_usd', 0))} | {quality} | hace {minutes_ago} min")

        if sells_24h:
            lines.extend(["", "🧨 <b>Salidas institucionales 24h</b>"])
            for item in sells_24h[:4]:
                display_name = get_display_name(item['ticker'])
                minutes_ago = int((datetime.now() - item['timestamp']).total_seconds() / 60)
                lines.append(f"• {display_name} | {_format_compact_money(item.get('vol_usd', 0))} | distribución | hace {minutes_ago} min")

        premium_recent = [item for item in history_24h if item.get("winner_only")]
        if premium_recent:
            lines.extend(["", "🔥 <b>Últimas élite detectadas</b>"])
            for item in premium_recent[:3]:
                display_name = get_display_name(item['ticker'])
                minutes_ago = int((datetime.now() - item['timestamp']).total_seconds() / 60)
                lines.append(f"• {display_name} | {item.get('type', 'Compra')} | {_format_compact_money(item.get('vol_usd', 0))} | hace {minutes_ago} min")

        if sells_24h:
            lines.extend(["", "🧨 <b>Presión vendedora 24h</b>"])
            for item in sells_24h[:3]:
                display_name = get_display_name(item['ticker'])
                minutes_ago = int((datetime.now() - item['timestamp']).total_seconds() / 60)
                lines.append(f"• {display_name} | {item.get('type', 'Venta')} | {_format_compact_money(item.get('vol_usd', 0))} | hace {minutes_ago} min")

        alerted_premium = [item for item in history_24h if item.get("alert_sent")]
        if alerted_premium:
            lines.extend(["", "🔥 <b>Alertas élite enviadas</b>"])
            for item in alerted_premium[:3]:
                display_name = get_display_name(item['ticker'])
                minutes_ago = int((datetime.now() - item['timestamp']).total_seconds() / 60)
                lines.append(f"• {display_name} | {_format_compact_money(item.get('vol_usd', 0))} | hace {minutes_ago} min")

        if not premium_recent and not sells_24h:
            lines.extend(["", "🌊 Mercado en calma.", "Sin flujos institucionales relevantes guardados en las últimas 24 horas."])

        bot.send_message(chat_id, _make_card("RADAR DE BALLENAS", lines, icon="🐋"), parse_mode="HTML")
        return
    except Exception as e:
        print(f"ERROR RADAR: {e}")
        try:
            bot.send_message(chat_id, _make_card("RADAR DE BALLENAS", [f"⚠️ Error interno en radar: {e}"], icon="🐋"), parse_mode="HTML")
        except Exception:
            pass
        return
    try:
        tkrs = get_tracked_tickers()
        if not tkrs:
            bot.send_message(chat_id, _make_card("RADAR DE BALLENAS", ["✅ Tu radar está vacío."], icon="🐋"), parse_mode="HTML")
            return

        api_key = os.environ.get("FMP_API_KEY")
        symbol_map = {}
        for raw_tk in tkrs:
            tk = remap_ticker(raw_tk)
            symbol_map[_get_fmp_symbol(tk)] = tk

        syms = ",".join(symbol_map.keys())
        url = f"https://financialmodelingprep.com/api/v3/quote/{syms}?apikey={api_key}"

        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code != 200:
                raise ValueError(f"HTTP {resp.status_code}")
            data = resp.json()
        except Exception:
            data = []

        report = [
            "🛰️ <b>Radar institucional ganador</b>",
            f"• Activos rastreados: {len(tkrs)}",
            "• Filtro: solo compras institucionales, volumen >2x, soporte cercano y relación beneficio/riesgo favorable",
            "",
        ]
        ballenas_count = 0

        if isinstance(data, list):
            for q in data:
                tk = symbol_map.get(q.get("symbol", "UNKNOWN"), q.get("symbol", "UNKNOWN"))
                display_name = get_display_name(tk)
                vol = float(q.get("volume", 0) or 0)
                avg_vol = float(q.get("avgVolume", 0) or 0)
                price = float(q.get("price", 0) or 0)
                change = float(q.get("changesPercentage", 0) or 0)

                intra_snapshot = {
                    "vol_side": "buy" if change >= 0 else "sell",
                    "latest_vol": vol,
                    "avg_vol": avg_vol,
                }
                analysis = LAST_KNOWN_ANALYSIS.get(tk)
                if not analysis or not isinstance(analysis, dict):
                    analysis = fetch_and_analyze_stock(tk)
                    if analysis and isinstance(analysis, dict):
                        LAST_KNOWN_ANALYSIS[tk] = analysis
                        update_smc_memory(tk, analysis)
                topol = SMC_LEVELS_MEMORY.get(tk, {})
                is_winner_setup, winner_reason = _is_winner_whale_setup(
                    price,
                    intra_snapshot,
                    topol,
                    analysis if isinstance(analysis, dict) else {}
                )

                if avg_vol > 0 and vol > (avg_vol * 2) and is_winner_setup:
                    ballenas_count += 1
                    report.append(f"🪙 <b>{display_name}</b> | 🟢 COMPRA GANADORA")
                    report.append(f"• Precio: ${fmt_price(price)} ({change:+.2f}%)")
                    report.append(f"• Volumen: {vol:,.0f} vs promedio {avg_vol:,.0f} ({(vol / avg_vol):.1f}x)")
                    if topol.get('sup') and topol.get('res'):
                        report.append(f"• Soporte SMC: ${fmt_price(topol['sup'])} | Resistencia: ${fmt_price(topol['res'])}")
                    report.append(f"• Lectura: {winner_reason}")
                    report.append("")

        if ballenas_count == 0:
            calm_report = [
                "🌊 Mercado en calma.",
                "No hay compras institucionales ganadoras activas en este momento.",
            ]
            whale_memory_filtered = [w for w in list(WHALE_MEMORY)[::-1] if w.get('winner_only', False)]
            if whale_memory_filtered:
                calm_report.extend(["", "🫧 <b>Últimas detecciones en memoria</b>"])
                for whale in whale_memory_filtered[:3]:
                    display_name = get_display_name(whale['ticker'])
                    minutes_ago = int((datetime.now() - whale['timestamp']).total_seconds() / 60)
                    calm_report.append(f"• {display_name} | {whale['type']} | hace {minutes_ago} min")

            bot.send_message(chat_id, _make_card("RADAR DE BALLENAS", calm_report, icon="🐋"), parse_mode="HTML")
            return

        report.insert(3, f"🔥 <b>Señales activas:</b> {ballenas_count}")
        bot.send_message(chat_id, _make_card("RADAR DE BALLENAS", report, icon="🐋"), parse_mode="HTML")
        return

    except Exception as e:
        print(f"ERROR RADAR: {e}")
        try:
            bot.send_message(chat_id, _make_card("RADAR DE BALLENAS", [f"⚠️ Error interno en radar: {e}"], icon="🐋"), parse_mode="HTML")
        except Exception:
            pass
        return
    try:
        tkrs = get_tracked_tickers()
        if not tkrs:
            bot.send_message(chat_id, _make_card("RADAR DE BALLENAS", ["✅ Tu radar está vacío."], icon="🐋"), parse_mode="HTML")
            return

        api_key = os.environ.get("FMP_API_KEY")
        symbol_map = {}
        for raw_tk in tkrs:
            tk = remap_ticker(raw_tk)
            symbol_map[_get_fmp_symbol(tk)] = tk

        syms = ",".join(symbol_map.keys())
        url = f"https://financialmodelingprep.com/api/v3/quote/{syms}?apikey={api_key}"

        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code != 200:
                raise ValueError(f"HTTP {resp.status_code}")
            data = resp.json()
        except Exception:
            data = []

        report = [
            f"🛰️ <b>Activos rastreados:</b> {len(tkrs)}",
            "📡 <b>Criterio:</b> volumen actual superior a 2x su promedio",
            "",
        ]
        ballenas_count = 0

        if isinstance(data, list):
            for q in data:
                tk = symbol_map.get(q.get("symbol", "UNKNOWN"), q.get("symbol", "UNKNOWN"))
                display_name = get_display_name(tk)
                vol = float(q.get("volume", 0) or 0)
                avg_vol = float(q.get("avgVolume", 0) or 0)
                price = float(q.get("price", 0) or 0)
                change = float(q.get("changesPercentage", 0) or 0)

                if avg_vol > 0 and vol > (avg_vol * 2):
                    ballenas_count += 1
                    estado = "🟢 COMPRA MASIVA" if change > 0 else "🔴 VENTA MASIVA"
                    report.append(f"🪙 <b>{display_name}</b> | {estado}")
                    report.append(f"• Precio: ${fmt_price(price)} ({change:+.2f}%)")
                    report.append(f"• Volumen: {vol:,.0f} vs promedio {avg_vol:,.0f} ({(vol / avg_vol):.1f}x)")
                    report.append("")

        if ballenas_count == 0:
            calm_report = [
                "🌊 Mercado en calma.",
                "No hay movimientos institucionales de alto valor en este momento.",
            ]
            if WHALE_MEMORY:
                calm_report.extend(["", "🫧 <b>Últimas detecciones en memoria</b>"])
                for whale in list(WHALE_MEMORY)[::-1][:3]:
                    display_name = get_display_name(whale['ticker'])
                    minutes_ago = int((datetime.now() - whale['timestamp']).total_seconds() / 60)
                    calm_report.append(f"• {display_name} | {whale['type']} | hace {minutes_ago} min")

            bot.send_message(chat_id, _make_card("RADAR DE BALLENAS", calm_report, icon="🐋"), parse_mode="HTML")
            return

        report.insert(2, f"🔥 <b>Señales activas:</b> {ballenas_count}")
        bot.send_message(chat_id, _make_card("RADAR DE BALLENAS", report, icon="🐋"), parse_mode="HTML")

    except Exception as e:
        print(f"ERROR RADAR: {e}")
        try:
            bot.send_message(chat_id, _make_card("RADAR DE BALLENAS", [f"⚠️ Error interno en radar: {e}"], icon="🐋"), parse_mode="HTML")
        except Exception:
            pass


def _send_geopolitics_report(chat_id):
    try:
        report = generar_reporte_macro_manual()
        if report:
            bot.send_message(chat_id, report, parse_mode="HTML")
        else:
            bot.send_message(chat_id, "☕ Sin eventos de riesgo detectados en este momento. Vigilancia activa.", parse_mode="HTML")
    except Exception as e:
        logging.error(f"Error en Geopolítica: {e}")
        bot.send_message(chat_id, "☕ Sin eventos de riesgo detectados en este momento. Vigilancia activa.", parse_mode="HTML")


def _send_smc_levels_report(chat_id):
    tkrs = get_tracked_tickers()

    if not tkrs:
        bot.send_message(chat_id, _make_card("NIVELES SMC", ["Tu radar está vacío."], icon="🦅"), parse_mode="HTML")
        return

    _refresh_smc_snapshot(tkrs, force=True)
    report_lines = [
        f"🛰️ <b>Activos analizados:</b> {len(tkrs)}",
        "📘 <b>Lectura:</b> precio, soporte, resistencia y niveles tácticos actualizados",
    ]

    for raw_tk in tkrs:
        tk = remap_ticker(raw_tk)
        analysis = LAST_KNOWN_ANALYSIS.get(tk)
        if not analysis:
            analysis = fetch_and_analyze_stock(tk)
        d_name = get_display_name(tk)

        if analysis and isinstance(analysis, dict):
            precio = analysis['price']
            soporte = analysis['smc_sup']
            resistencia = analysis['smc_res']

            if precio < soporte:
                veredicto = "COMPRA 🟢"
            elif precio > resistencia:
                veredicto = "VENTA 🔴"
            else:
                veredicto = "MANTENER ⚠️"

            report_lines.extend([
                "",
                f"🏦 <b>{d_name}</b>",
                f"• Precio: ${fmt_price(precio)}",
                f"• Soporte: ${fmt_price(soporte)}",
                f"• Resistencia: ${fmt_price(resistencia)}",
                "",
                "🎯 <b>Niveles tácticos</b>",
                f"• TP: ${fmt_price(analysis.get('take_profit', 0))}",
                f"• SL: ${fmt_price(analysis.get('stop_loss', 0))}",
                f"⚖️ <b>Veredicto:</b> <b>{veredicto}</b>",
            ])
        elif isinstance(analysis, str):
            report_lines.extend(["", f"🏦 <b>{d_name}</b>", f"• {analysis}"])
        else:
            report_lines.extend(["", f"🏦 <b>{d_name}</b>", "• ⚠️ Niveles SMC no disponibles en este momento"])

    bot.send_message(chat_id, _make_card("NIVELES SMC", report_lines, icon="🦅"), parse_mode="HTML")


def _send_wallet_status(chat_id):
    bot.send_message(chat_id, build_wallet_dashboard(), parse_mode="HTML")


@bot.message_handler(func=lambda message: True, content_types=['text'])
def handle_text(message):
    if str(message.chat.id) != str(CHAT_ID): return
    text = message.text.strip()
    normalized_text = _normalize_menu_text(text)
    intent_text = normalized_text.upper()

    # Ignorar mensajes de backup del bot
    if text.startswith(BACKUP_PREFIX): return

    if normalized_text in {"geopolitica", "geopolitics"}:
        bot.reply_to(message, "🌍 Generando reporte estratégico GÉNESIS...")
        _send_geopolitics_report(message.chat.id)
        return

    if normalized_text == "radar de ballenas":
        bot.reply_to(message, "🐋 Activando Radar de Ballenas...")
        _send_super_radar_report(message.chat.id)
        return

    if normalized_text == "niveles smc":
        bot.reply_to(message, "🦅 Forzando datos frescos y analizando niveles SMC...")
        _send_smc_levels_report(message.chat.id)
        return

    if normalized_text in {"mi cartera", "mi wallet"}:
        bot.reply_to(message, "💼 Extrayendo datos robustos y valuando métricas en vivo...")
        _send_wallet_status(message.chat.id)
        return

    analysis_target = _extract_analysis_ticker(intent_text)
    if analysis_target:
        tk = remap_ticker(analysis_target)
        display_name = get_display_name(tk)
        bot.reply_to(message, f"📈 Consultando FMP y construyendo un análisis integral en español para {display_name}...")
        _send_stock_analysis_with_chart(message.chat.id, tk)
        return

    # === BOTONES MENÚ RÃPIDO ===
    if normalized_text in {"🌍 geopolítica", "🛡 geopolítica", "geopolítica"}:
        bot.reply_to(message, "🌍 Generando Reporte Estratégico GÉNESIS...")
        _send_geopolitics_report(message.chat.id)
        return

    if normalized_text in {"🐋 radar de ballenas", "radar de ballenas"}:
        bot.reply_to(message, "🐋 Activando Radar de Ballenas...")
        _send_super_radar_report(message.chat.id)
        return

    if normalized_text in {"🦅 niveles smc", "niveles smc"}:
        bot.reply_to(message, "🦅 Forzando datos frescos y analizando niveles SMC...")
        _send_smc_levels_report(message.chat.id)
        return

    if normalized_text in {"💼 mi cartera", "💼 mi wallet", "💰 mi cartera", "💰 mi wallet", "mi cartera", "mi wallet"}:
        bot.reply_to(message, "💼 Extrayendo datos robustos y valuando métricas en vivo...")
        _send_wallet_status(message.chat.id)
        return

    # === EXPRESIONES REGULARES INTELIGENTES NLP ===
    if re.search(r'\bANALIZA\b\s+([A-Z0-9\-]+)', intent_text):
        match = re.search(r'\bANALIZA\b\s+([A-Z0-9\-]+)', intent_text)
        if match:
            tk = remap_ticker(match.group(1))
            display_name = get_display_name(tk)
            bot.reply_to(message, f"🔍 Análisis profundo institucional en {display_name} con gráfico táctico...")
            _send_stock_analysis_with_chart(message.chat.id, tk)
        return

    if re.search(r'\b(?:ELIMINA|BORRA|BORRAR|ELIMINAR)\b\s+([A-Z0-9\-]+)', intent_text):
        match = re.search(r'\b(?:ELIMINA|BORRA|BORRAR|ELIMINAR)\b\s+([A-Z0-9\-]+)', intent_text)
        if match:
             raw_input = match.group(1)
             tk = remap_ticker(raw_input)
             display_name = get_display_name(tk)
             if remove_ticker(tk):
                 bot.reply_to(message, _make_card("GESTIÓN DE CARTERA", [f"✅ <b>{display_name}</b> ha sido borrado del radar.", "🛡️ El cambio quedó guardado en la persistencia blindada."], icon="🗂️"), parse_mode="HTML")
             else:
                 bot.reply_to(message, _make_card("GESTIÓN DE CARTERA", [f"⚠️ El activo <b>{display_name}</b> no estaba en tu radar."], icon="🗂️"), parse_mode="HTML")
        return

    if re.search(r'\b(?:AGREGA|ANADE|AGREGAR)\b\s+([A-Z0-9\-]+)', intent_text):
        match = re.search(r'\b(?:AGREGA|ANADE|AGREGAR)\b\s+([A-Z0-9\-]+)', intent_text)
        if match:
             raw_input = match.group(1).upper()
             tk = remap_ticker(raw_input)
             display_name = get_display_name(tk)

             validation = get_safe_ticker_price(tk)
             if validation is None:
                 bot.reply_to(message, _make_card("GESTIÓN DE CARTERA", ["⚠️ Activo no encontrado en FMP. No se agregó."], icon="🗂️"), parse_mode="HTML")
                 return
             res = add_ticker(tk)
             if res == "DB_ERROR":
                 bot.reply_to(message, _make_card("ERROR DE BASE DE DATOS", ["No se pudo conectar a Supabase.", "Revisa tu DATABASE_URL."], icon="🚨"), parse_mode="HTML")
             elif res == True:
                 bot.reply_to(message, _make_card("GESTIÓN DE CARTERA", [f"✅ <b>{display_name}</b> añadido al radar SMC.", "🛡️ Guardado directamente en Supabase."], icon="🗂️"), parse_mode="HTML")
             else:
                 bot.reply_to(message, _make_card("GESTIÓN DE CARTERA", [f"⚠️ <b>{display_name}</b> ya existe en tu base centralizada."], icon="🗂️"), parse_mode="HTML")
        return

    if re.search(r'\bCOMPRE\b', intent_text):
        match = re.search(r'\bCOMPRE\b\s+(?:DE\s+)?\$?(\d+(?:\.\d+)?)\s+(?:EN\s+|DE\s+|ACCIONES\s+DE\s+)?([A-Z0-9\-]+)', intent_text)
        if match:
            amt = match.group(1)
            tk = remap_ticker(match.group(2))
            display_name = get_display_name(tk)

            bot.reply_to(message, f"🔥 Consultando precio de fijación para {display_name}...")
            intra = fetch_intraday_data(tk)
            if intra:
                add_investment(tk, amt, intra['latest_price'])
                bot.send_message(
                    message.chat.id,
                    _make_card(
                        "CAPITAL REGISTRADO",
                        [
                            f"• Activo: <b>{display_name}</b>",
                            f"• Capital invertido: ${float(amt):,.2f} USD",
                            f"• Precio de entrada: ${fmt_price(intra['latest_price'])}",
                            "",
                            "🛡️ Guardado en Base de Datos Blindada.",
                        ],
                        icon="💰"
                    ),
                    parse_mode="HTML"
                )
            else:
                bot.reply_to(message, _make_card("CAPITAL REGISTRADO", [f"❌ No pude fijar el precio real de {display_name} ahora mismo."], icon="💰"), parse_mode="HTML")
        return

    if re.search(r'\bVENDI\b', intent_text):
        match = re.search(r'\bVENDI\b\s+(?:TODO\s+)?(?:DE\s+)?\$?(?:\d+(?:\.\d+)?\s+(?:EN\s+|DE\s+|ACCIONES\s+DE\s+)?)?([A-Z0-9\-]+)', intent_text)
        if match:
            tk = remap_ticker(match.group(1))
            display_name = get_display_name(tk)

            investments = get_investments()
            if tk in investments:
                bot.reply_to(message, f"🔥 Procesando cierre institucional para {display_name}...")
                entry = investments[tk]['entry_price']
                amt = investments[tk]['amount_usd']

                intra = fetch_intraday_data(tk)
                if intra:
                    live_price = intra['latest_price']
                    roi = (live_price - entry) / entry if entry > 0 else 0
                    prof = amt * roi
                    sign = "+" if prof >= 0 else ""
                    icon = "🟢" if prof >= 0 else "🔒´"
                    final_usd = amt + prof

                    close_investment(tk)
                    add_realized_pnl(prof)

                    ans_str = _make_card(
                        "CIERRE DE POSICIÓN",
                        [
                            f"✅ <b>{display_name}</b> liquidado a ${fmt_price(live_price)}",
                            f"💰 <b>Capital retirado:</b> ${final_usd:,.2f} USD",
                            f"{icon} <b>Resultado:</b> {sign}${prof:,.2f} USD ({sign}{roi*100:.2f}%)",
                            "",
                            "🛡️ El cierre quedó guardado en la persistencia blindada.",
                        ],
                        icon="📕"
                    )
                    bot.send_message(message.chat.id, ans_str, parse_mode="HTML")
                else:
                    bot.reply_to(message, _make_card("CIERRE DE POSICIÓN", [f"❌ No pude contactar al mercado para liquidar {display_name}."], icon="📕"), parse_mode="HTML")
            else:
                 bot.reply_to(message, _make_card("CIERRE DE POSICIÓN", [f"⚠️ No tienes capital invertido en {display_name}.", f"Usa 'Elimina {display_name}' si quieres detener el rastreo."], icon="📕"), parse_mode="HTML")
        return


# ----------------- MODO CENTINELA: VIGILANCIA DE NOTICIAS POR ACTIVO -----------------
_SENTINEL_TICK_INTERVAL = 4  # Cada 4 ticks de 30s = ~2 minutos


def verificar_noticias_cartera_v2():
    tkrs = get_tracked_tickers()
    if not tkrs or not FMP_API_KEY:
        return

    for raw_tk in tkrs:
        tk = remap_ticker(raw_tk)
        try:
            news_list = _fetch_fmp_ticker_news(tk, limit=6)
        except Exception as e:
            logging.debug(f"Sentinel fetch error for {tk}: {e}")
            continue

        for article in news_list[:6]:
            title = (article.get('title') or '').strip()
            if not title:
                continue

            published = (article.get('publishedDate') or article.get('date') or '').strip()
            news_hash = _stable_event_id("SENTINEL", tk, title, published[:16])
            if check_and_add_seen_event(news_hash):
                continue

            alert_msg = _build_tracked_news_alert(tk, article)
            if not alert_msg:
                continue

            try:
                bot.send_message(CHAT_ID, alert_msg, parse_mode="HTML")
            except Exception as e:
                logging.debug(f"Sentinel alert send error for {tk}: {e}")

def verificar_noticias_cartera():
    """Vigila noticias específicas de los activos en la cartera de Eduardo"""
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
            url = f"https://financialmodelingprep.com/stable/stock-news?symbol={fmp_sym}&limit=5&apikey={FMP_API_KEY}"
            resp = requests.get(url, timeout=10)
            news_list = resp.json() if resp.status_code == 200 else []
            if not isinstance(news_list, list):
                news_list = []
        except Exception:
            continue

        for article in news_list[:5]:
            title = article.get('title', '')
            if not title:
                continue

            # Deduplicar con hash: no alertar la misma noticia dos veces
            news_hash = _stable_event_id("SENTINEL", tk, title)
            if check_and_add_seen_event(news_hash):
                continue  # Ya la vimos

            alert_msg = _build_tracked_news_alert(tk, article)
            if not alert_msg:
                continue

            try:
                bot.send_message(CHAT_ID, alert_msg, parse_mode="HTML")
            except Exception as e:
                logging.debug(f"Sentinel alert send error for {tk}: {e}")
            continue

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
                    f"  🔥¡ Sugerencia Institucional: [Vender / Vigilar / Hold / Comprar]\n"
                    f"  âš¡ Impacto Estimado: [Alto / Medio] en la liquidez\n"
                    f"RESPONDE EN ESPAÑOL."
                )

                res = client.models.generate_content(
                    model="gemini-1.5-pro",
                    contents=prompt,
                ).text.strip()

                # Filtro de ruido: si GPT dice NEUTRAL, silencio total
                if "NEUTRAL" in res.upper() and len(res) < 30:
                    continue

                # Alerta que SÃ amerita atención
                alert_msg = _make_card(
                    "CENTINELA GÉNESIS",
                    [
                        f"📈 <b>Activo:</b> {display_name}",
                        res,
                    ],
                    icon="🚨"
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
        direction = "📉 CAÃDA" if pct_change < 0 else "📈 SUBIDA"
        emoji = "🔒´" if pct_change < 0 else "🟢"

        # Obtener contexto SMC si está disponible
        smc_context = ""
        smc = SMC_LEVELS_MEMORY.get(tk)
        if not smc:
            _refresh_smc_snapshot([tk], force=True)
            smc = SMC_LEVELS_MEMORY.get(tk)
        if smc:
            if current_price < smc.get('sup', 0):
                smc_context = f"\n⚠️ Precio POR DEBAJO del Soporte SMC (${fmt_price(smc['sup'])}). Zona de riesgo."
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
                    f"Da un VEREDICTO en 2 líneas: Â¿Mantener, vender parcial, o reforzar posición institucional? Justifica mecánicamente.\n"
                    f"ESPAÑOL ESTRICTO."
                )
                res = client.models.generate_content(
                    model="gemini-1.5-pro",
                    contents=prompt,
                ).text.strip()
                veredicto = f"\n\nðŸ§  <b>VEREDICTO GÉNESIS:</b>\n{res}"
            except Exception as e:
                logging.debug(f"Protection GPT error: {e}")

        # Construir y enviar alerta
        entry_price = inv_data.get('entry_price', 0)
        entry_info = f"\n🎯 Precio de entrada: ${fmt_price(entry_price)}" if entry_price > 0 else ""

        alert_msg = (
            f"---\n🚨 <b>SISTEMA GÉNESIS â€” PROTECCIÓN DE ACTIVOS</b> 🚨\n---\n\n"
            f"{emoji} <b>{direction} DETECTADA</b>\n\n"
            f"💰 Activo: <b>{display_name}</b>\n"
            f"📉 Movimiento: <b>{pct_change:+.2f}%</b>\n"
            f"🔥µ Precio FMP: <b>${fmt_price(current_price)}</b>{entry_info}"
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

    # PASO CRÃTICO: Restaurar datos ANTES de hacer cualquier otra cosa
    restore_state_from_telegram()

    tkrs = get_tracked_tickers()
    logging.info(f"Activos cargados en radar: {len(tkrs)} â†’ {tkrs}")

    for tk in tkrs:
        val = fetch_and_analyze_stock(tk)
        if val: update_smc_memory(tk, val)

    # PASO INICIAL: Poblar contexto geopolítico al arrancar
    try:
        print("DEBUG BOOT: Inicializando contexto geopolitico...")
        genesis_strategic_report(manual=False)
        print(f"DEBUG BOOT: Contexto listo. Sentimiento: {GENESIS_RISK_CONTEXT.get('sentiment_global', 'N/A')} | High risk: {GENESIS_RISK_CONTEXT.get('high_risk_tickers', [])}")
    except Exception as e:
        print(f"DEBUG BOOT: Error inicializando contexto geo: {e}")

def background_loop_proactivo():
    """BUCLE DE ALTA LATENCIA CON DOBLE VERIFICACIÓN Y ANTI-SPAM (TTL 7 DÃAS)"""
    boot_smc_levels_once()
    sentinel_tick_counter = 0  # Contador para noticias de cartera cada ~20 min
    protection_tick_counter = 0  # Contador para monitor de protección cada ~5 min
    geo_refresh_counter = 0  # Contador para refrescar contexto geopolítico
    smc_refresh_counter = 0  # Refresco periódico de niveles SMC
    _PROTECTION_INTERVAL = 10  # ~5 minutos (10 ticks * 30s)
    _GEO_REFRESH_INTERVAL = 20  # ~10 minutos (20 ticks * 30s)
    _SMC_REFRESH_INTERVAL = 120  # ~60 minutos (120 ticks * 30s)
    divergence_tick_counter = 0  # Divergencias de alta calidad cada ~20 min
    _DIVERGENCE_INTERVAL = 40  # ~20 minutos (40 ticks * 30s)
    loop_counter = 0  # Contador total de ciclos para heartbeat
    while True:
        try:
            time.sleep(30)
            now = datetime.now()
            purge_old_events()
            sentinel_tick_counter += 1
            protection_tick_counter += 1
            geo_refresh_counter += 1
            smc_refresh_counter += 1
            divergence_tick_counter += 1
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
                ai_threat_evaluation = gpt_advanced_geopolitics_v3(unique_news, manual=False)
                if ai_threat_evaluation:
                     bot.send_message(
                         CHAT_ID,
                         _make_card("VIGILANCIA GLOBAL", [ai_threat_evaluation], icon="🚨"),
                         parse_mode="HTML"
                     )

            # === REFRESCAR CONTEXTO GEOPOLÃTICO: cada ~10 minutos ===
            if geo_refresh_counter >= _GEO_REFRESH_INTERVAL:
                geo_refresh_counter = 0
                try:
                    print(f"DEBUG GEO REFRESH: Actualizando contexto geopolitico...")
                    genesis_strategic_report(manual=False)  # Actualiza GENESIS_RISK_CONTEXT sin enviar
                    print(f"DEBUG GEO REFRESH: Contexto actualizado. Sentimiento: {GENESIS_RISK_CONTEXT.get('sentiment_global', 'N/A')} | High risk: {GENESIS_RISK_CONTEXT.get('high_risk_tickers', [])}")
                except Exception as e:
                    print(f"DEBUG GEO REFRESH ERROR: {e}")

            if smc_refresh_counter >= _SMC_REFRESH_INTERVAL:
                smc_refresh_counter = 0
                try:
                    refreshed = _refresh_smc_snapshot(tracked, force=True)
                    print(f"DEBUG SMC REFRESH: {refreshed} niveles actualizados")
                except Exception as e:
                    logging.error(f"Error refrescando niveles SMC: {e}")

            # === MODO CENTINELA: verificar noticias de activos cada ~20 minutos ===
            if sentinel_tick_counter >= _SENTINEL_TICK_INTERVAL:
                sentinel_tick_counter = 0
                try:
                    verificar_noticias_cartera_v2()
                except Exception as e:
                    logging.error(f"Error en Centinela de Noticias: {e}")

            # === MONITOR DE PROTECCIÓN DE ACTIVOS: cada ~5 minutos ===
            if protection_tick_counter >= _PROTECTION_INTERVAL:
                protection_tick_counter = 0
                try:
                    monitor_proteccion_activos()
                except Exception as e:
                    logging.error(f"Error en Monitor de Protección: {e}")

            if divergence_tick_counter >= _DIVERGENCE_INTERVAL:
                divergence_tick_counter = 0
                try:
                    _monitor_quality_divergences(tracked)
                except Exception as e:
                    logging.error(f"Error en Monitor de Divergencias: {e}")

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

                    # === GUARDIA DE COHERENCIA: bloquear alertas si el precio es ilógico ===
                    price_is_reliable = True
                    if tk in LAST_KNOWN_PRICES:
                        last_p = LAST_KNOWN_PRICES[tk]['price']
                        if last_p > 0 and abs(cur_price - last_p) / last_p > 0.50:
                            logging.warning(f"ðŸš« ALERTA BLOQUEADA para {tk}: ${cur_price:.2f} vs último ${last_p:.2f} (>50% de desviación). Error de API probable.")
                            price_is_reliable = False

                    # Rupturas Doble Verificadas â€” SOLO si el precio es confiable
                    # Rupturas Doble Verificadas â€” SOLO si el precio es confiable
                    topol = SMC_LEVELS_MEMORY.get(tk)
                    analysis = LAST_KNOWN_ANALYSIS.get(tk)
                    if price_is_reliable and (not topol or not analysis):
                        fresh_analysis = fetch_and_analyze_stock(tk)
                        if fresh_analysis and isinstance(fresh_analysis, dict):
                            update_smc_memory(tk, fresh_analysis)
                            LAST_KNOWN_ANALYSIS[tk] = fresh_analysis
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
                                msg = _make_card(
                                    "RUPTURA DE RESISTENCIA",
                                    [
                                        f"📈 <b>Activo:</b> {display_name}",
                                        f"💰 <b>Precio detectado:</b> ${fmt_price(cur_price)}",
                                        f"🧠 {reason}",
                                        "⚖️ <b>Veredicto:</b> COMPRAR / MANTENER",
                                    ],
                                    icon="🚀"
                                )
                                bot.send_message(CHAT_ID, msg, parse_mode="HTML")

                        # L\u00f3gica 2: Ruptura Descendente
                        elif cur_price < topol['sup']:
                            hash_drp = f"BRK_DWN_{tk}_{topol['sup']}"
                            if not check_and_add_seen_event(hash_drp):
                                msg = _make_card(
                                    "RUPTURA DE SOPORTE",
                                    [
                                        f"📉 <b>Activo:</b> {display_name}",
                                        f"💰 <b>Soporte perdido:</b> ${fmt_price(topol['sup'])} → ${fmt_price(cur_price)}",
                                        f"🧠 {reason}",
                                        "⚖️ <b>Veredicto:</b> VENDER / CORTAR PÉRDIDAS",
                                    ],
                                    icon="⚠️"
                                )
                                bot.send_message(CHAT_ID, msg, parse_mode="HTML")
                            
                        # L\u00f3gica 3: Zona de Acumulaci\u00f3n (Cerca del Soporte)
                        elif topol['sup'] <= cur_price <= (topol['sup'] * 1.015):
                            hash_acc = f"ACCUM_{tk}_{topol['sup']}"
                            if not check_and_add_seen_event(hash_acc):
                                msg = _make_card(
                                    "ZONA DE ACUMULACIÓN",
                                    [
                                        f"💎 <b>Activo:</b> {display_name}",
                                        f"💰 <b>Order block:</b> ${fmt_price(topol['sup'])}",
                                        f"🧠 {reason}",
                                        "⚖️ <b>Veredicto:</b> OPORTUNIDAD DE COMPRA",
                                    ],
                                    icon="💠"
                                )
                                bot.send_message(CHAT_ID, msg, parse_mode="HTML")

                    # Ballenas â€” con cruce geopolítico GENESIS
                    # UMBRAL TEMPORAL REDUCIDO PARA TESTING (original: crypto=5.0, stocks=2.5)
                    if intra['avg_vol'] > 0 and price_is_reliable:
                        is_crypto = '-USD' in tk
                        whale_threshold = 2.0 if is_crypto else 1.5
                        spike = intra['latest_vol'] / intra['avg_vol']

                        # DEBUG: logear ratios de volumen significativos
                        if spike > 1.0:
                            print(f"DEBUG WHALE SCAN {tk}: latest_vol={intra['latest_vol']:,.0f} | avg_vol={intra['avg_vol']:,.0f} | spike={spike:.2f}x | threshold={whale_threshold}x | {'WHALE!' if spike >= whale_threshold else 'no trigger'}")

                        if spike >= whale_threshold:
                            current_time = time.time()
                            if tk in last_whale_alert and (current_time - last_whale_alert[tk]) < 7200:
                                continue # Cooldown de 2 horas activo
                        
                            rt = verify_1m_realtime_data(tk)
                            valid_vol = int(rt['vol']) if rt else int(intra['latest_vol'])
                        
                            is_crypto = '-USD' in tk
                            vol_usd = valid_vol if is_crypto else (valid_vol * cur_price)
                        
                            min_elite_vol = 1_000_000
                            if vol_usd < min_elite_vol:
                                continue # Filtro \u00c9lite Institucional ($1M USD Minimo)
                            whale_direction = intra.get('vol_side', 'buy')
                            flow_hash_id = _stable_event_id(
                                "WHL_FLOW",
                                tk,
                                whale_direction,
                                int(vol_usd / 50000),
                                now.strftime("%Y-%m-%d-%H")
                            )
                            flow_event = None
                            if not check_and_add_seen_event(flow_hash_id):
                                if tk not in WHALE_HISTORY_DB:
                                    WHALE_HISTORY_DB[tk] = []
                                flow_event = {
                                    "type": intra['vol_type'],
                                    "direction": whale_direction,
                                    "winner_only": False,
                                    "alert_sent": False,
                                    "vol_usd": float(vol_usd),
                                    "timestamp": now,
                                }
                                WHALE_HISTORY_DB[tk].append(flow_event)
                            # === INICIO DE SMART MONEY FILTER ===
                            topol_whale = SMC_LEVELS_MEMORY.get(tk, {})
                            analysis_whale = LAST_KNOWN_ANALYSIS.get(tk, {})
                            rsi_w = analysis_whale.get('rsi', 50) if analysis_whale else 50
                            smart_msg = None
                            is_winner_setup, whale_reason = _is_winner_whale_setup(cur_price, intra, topol_whale, analysis_whale)
                            if not is_winner_setup:
                                continue
                            try:
                                # Solo si precio est\u00e1 en zonas SMC
                                if 'sup' in topol_whale and 'res' in topol_whale:
                                    w_sup = topol_whale['sup']
                                    w_res = topol_whale['res']
                                    
                                    if intra.get('vol_side') == 'buy' and cur_price > (w_sup * 1.05): continue # Ignora compras caras
                                    if intra.get('vol_side') == 'sell': continue # Modo ganador: bloquear ventas
                                    
                                    if intra.get('vol_side') == 'buy':
                                        smart_msg = "\ud83d\udd25 <b>BALLENA GANADORA DETECTADA:</b>\nCompra institucional en zona de soporte t\u00e9cnico.\nProbabilidad de \u00e9xito: ALTA."
                                    else:
                                        continue
                                else:
                                    continue # Si no tiene niveles validos, bloquear
                            except:
                                pass # Si no hay datos SMC, que no rompa el c\u00f3digo
                                
                            # Si pasa y no hay smart_msg, es porque pas\u00f3 el except pero se filtr\u00f3 mal, mejor asegurar
                            if smart_msg is None:
                                continue
                            # === FIN DE SMART MONEY FILTER ===
                            
                            whale_hash_id = f"WHL_SMART_{tk}_{valid_vol}"

                            if not check_and_add_seen_event(whale_hash_id):
                                last_whale_alert[tk] = current_time # Registrar el env\u00edo s\u00f3lo si es nuevo
                                whale_detected_count += 1
                                note = "\n<i>[Confirmando volumen institucional...]</i>" if not rt or rt['vol'] < intra['latest_vol'] else ""
                                WHALE_MEMORY.append({"ticker": tk, "vol_approx": valid_vol, "type": intra['vol_type'], "timestamp": now, "winner_only": True, "reason": whale_reason})
                                if flow_event is not None:
                                    flow_event["winner_only"] = True
                                    flow_event["alert_sent"] = True
                             
                                if is_crypto:
                                    vol_display = f"${valid_vol:,} USD"
                                else:
                                    vol_display = f"{valid_vol:,} unidades"

                                print(f"DEBUG WHALE SMART DETECTADA: {display_name} vol={vol_display} tipo={intra['vol_type']} spike={spike:.2f}x")

                                bot_msg = f"---\n{smart_msg}\n---\n<b>{display_name} ({tk})</b>\n\ud83d\udcb0 Capital transferido: <b>${vol_usd:,.0f} USD</b>\n\ud83d\udcca Riesgo T\u00e9cnico: RSI {rsi_w:.1f} | Precio: ${fmt_price(cur_price)}\n\ud83e\udde0 Filtro ganador: {whale_reason}{note}"
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

@bot.callback_query_handler(func=lambda call: call.data == "super_radar_24h")
def callback_super_radar(call):
    try:
        bot.answer_callback_query(call.id, text="🚀 Iniciando Radar Institucional...")
    except Exception:
        pass
    _send_super_radar_report(call.message.chat.id)

@bot.callback_query_handler(func=lambda call: call.data == "geopolitics")
def callback_geopolitics(call):
    try:
        bot.answer_callback_query(call.id, text="🌍 Generando Reporte Estratégico GÉNESIS...")
    except Exception:
        pass
    _send_geopolitics_report(call.message.chat.id)
    return
    bot.answer_callback_query(call.id, "🌍 Generando Reporte Estratégico GÉNESIS...")
    _send_geopolitics_report(call.message.chat.id)

@bot.callback_query_handler(func=lambda call: call.data == "smc_levels")
def callback_smc(call):
    bot.answer_callback_query(call.id, "🦅 Forzando datos frescos y analizando niveles SMC...")
    _send_smc_levels_report(call.message.chat.id)

@bot.callback_query_handler(func=lambda call: call.data == "wallet_status")
def callback_wallet(call):
    bot.answer_callback_query(call.id, "💰 Extrayendo datos robustos y valuando métricas live...")
    _send_wallet_status(call.message.chat.id)

def _acquire_bot_leader_lock():
    """Evita que más de una instancia del bot consuma getUpdates al mismo tiempo."""
    try:
        conn = get_db_connection()
        if not conn:
            logging.warning("No se pudo verificar el candado de líder; continuaré sin lock.")
            return True

        cursor = conn.cursor()
        cursor.execute("SELECT pg_try_advisory_lock(%s)", (987654321,))
        row = cursor.fetchone()
        acquired = bool(row and row[0])
        if acquired:
            logging.info("Candado de líder adquirido: esta instancia controlará Telegram.")
        else:
            logging.warning("Otra instancia de GÉNESIS ya está controlando Telegram; esta quedará en espera.")
        return acquired
    except Exception as e:
        logging.warning(f"No pude adquirir el candado de líder: {e}")
        return True

# ----------------- MAIN -----------------
def main():
    logging.info("Iniciando Génesis 1.0 â€” Persistencia: Telegram Cloud + SQLite local + Base64 logs")
    if not _acquire_bot_leader_lock():
        while True:
            time.sleep(60)

    t = threading.Thread(target=background_loop_proactivo, daemon=True)
    t.start()
    
    # 1. FORZAR CIERRE DE CONEXIÓN: Elimina conflicto getUpdates
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
            wait_seconds = 30 if "409" in str(e) else 5
            if "409" in str(e):
                try:
                    bot.delete_webhook(drop_pending_updates=False)
                except Exception:
                    pass
            print(f"DEBUG: Reconectando en {wait_seconds} segundos...")
            import time
            time.sleep(wait_seconds)
            print("DEBUG: Reintentando polling...")

if __name__ == "__main__":
    main()

