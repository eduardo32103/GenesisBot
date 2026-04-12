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
import pymongo
from collections import deque
from telebot.types import ReplyKeyboardMarkup, KeyboardButton
from datetime import datetime, timedelta

# Configuración extendida de logs para Railway
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')

if not TELEGRAM_TOKEN or not CHAT_ID:
    logging.critical("Falta TELEGRAM_TOKEN o CHAT_ID. Saliendo sin saturar.")
    exit()

bot = telebot.TeleBot(TELEGRAM_TOKEN)

# --- CONFIGURACIÓN DE BASE DE DATOS HÍBRIDA (NUBE MONGO / LOCAL SQLITE) ---
MONGO_URI = os.environ.get('MONGO_URI')
DATA_DIR = os.environ.get('DATA_DIR', '.')

# Variables de colecciones Mongo
mongo_client = None
db_cloud = None
col_portfolio = None
col_stats = None
col_events = None

if MONGO_URI:
    try:
        mongo_client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        mongo_client.server_info() # Validate connection
        db_cloud = mongo_client.genesis_db
        col_portfolio = db_cloud.portfolio
        col_stats = db_cloud.global_stats
        col_events = db_cloud.seen_events
        logging.info("✅ MONGODB ATLAS: Conectado a la Nube exitosamente.")
    except Exception as e:
        logging.error(f"❌ MONGODB ERROR: Falló conexión. Usando SQLite local. Error: {e}")
        MONGO_URI = None

# Fallback Local DB
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, 'genesis_data.db')

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS portfolio (ticker TEXT PRIMARY KEY, is_investment INTEGER, amount_usd REAL, entry_price REAL, timestamp TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS global_stats (key TEXT PRIMARY KEY, value REAL)''')
        c.execute('''CREATE TABLE IF NOT EXISTS seen_events (hash_id TEXT PRIMARY KEY, timestamp TEXT)''')
        conn.commit()

if not MONGO_URI:
    init_db()

def log_base64_backup():
    """Genera una huella criptográfica a texto plano constante a prueba de Wipeos de Log"""
    try:
        portfolio = get_all_portfolio_data()
        stats = get_realized_pnl()
        payload = {"portfolio": portfolio, "global_stats": {"realized_pnl": stats}}
        json_str = json.dumps(payload)
        b64 = base64.b64encode(json_str.encode('utf-8')).decode('utf-8')
        logging.info(f"BACKUP_DB_LOG (SAFE BASE64): {b64}")
    except Exception as e:
        logging.error(f"Error generando encriptador Base64: {e}")

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

# --- CONTROLADORES DE BASE DE DATOS ---
def check_and_add_seen_event(event_hash):
    if MONGO_URI:
        if col_events.find_one({"_id": event_hash}): return True
        col_events.insert_one({"_id": event_hash, "timestamp": datetime.now().isoformat()})
        return False
    else:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute('SELECT 1 FROM seen_events WHERE hash_id = ?', (event_hash,))
            if c.fetchone(): return True 
            c.execute('INSERT INTO seen_events (hash_id, timestamp) VALUES (?, ?)', (event_hash, datetime.now().isoformat()))
            conn.commit()
        return False

def purge_old_events():
    now = datetime.now()
    cutoff_date = (now - timedelta(days=7)).isoformat()
    if MONGO_URI:
        col_events.delete_many({"timestamp": {"$lt": cutoff_date}})
    else:
        with sqlite3.connect(DB_PATH) as conn:
             c = conn.cursor()
             c.execute('DELETE FROM seen_events WHERE timestamp < ?', (cutoff_date,))
             conn.commit()

def get_tracked_tickers():
    if MONGO_URI:
        return [doc["_id"] for doc in col_portfolio.find()]
    else:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute('SELECT ticker FROM portfolio')
            return [row[0] for row in c.fetchall()]

def get_all_portfolio_data():
    pf = {}
    if MONGO_URI:
        for doc in col_portfolio.find():
            pf[doc["_id"]] = {"is_investment": doc.get("is_investment"), "amount_usd": doc.get("amount_usd"), "entry_price": doc.get("entry_price"), "timestamp": doc.get("timestamp")}
    else:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM portfolio')
            for row in c.fetchall():
                pf[row[0]] = {"is_investment": bool(row[1]), "amount_usd": row[2], "entry_price": row[3], "timestamp": row[4]}
    return pf

def add_ticker(ticker):
    ticker = remap_ticker(ticker)
    if MONGO_URI:
        if not col_portfolio.find_one({"_id": ticker}):
            col_portfolio.insert_one({"_id": ticker, "is_investment": False, "amount_usd": 0, "entry_price": 0, "timestamp": datetime.now().isoformat()})
            log_base64_backup()
            val = fetch_and_analyze_stock(ticker)
            if val: update_smc_memory(ticker, val)
            return True
        return False
    else:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute('SELECT 1 FROM portfolio WHERE ticker = ?', (ticker,))
            if not c.fetchone():
                c.execute('INSERT INTO portfolio (ticker, is_investment, amount_usd, entry_price, timestamp) VALUES (?, 0, 0, 0, ?)', (ticker, datetime.now().isoformat()))
                conn.commit()
                log_base64_backup()
                
                val = fetch_and_analyze_stock(ticker)
                if val: update_smc_memory(ticker, val)
                return True
        return False

def remove_ticker(ticker):
    ticker = remap_ticker(ticker)
    if MONGO_URI:
        res = col_portfolio.delete_one({"_id": ticker})
        if res.deleted_count > 0:
            log_base64_backup()
            if ticker in SMC_LEVELS_MEMORY: del SMC_LEVELS_MEMORY[ticker]
            return True
        return False
    else:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute('SELECT 1 FROM portfolio WHERE ticker = ?', (ticker,))
            if c.fetchone():
                c.execute('DELETE FROM portfolio WHERE ticker = ?', (ticker,))
                conn.commit()
                log_base64_backup()
                
                if ticker in SMC_LEVELS_MEMORY: del SMC_LEVELS_MEMORY[ticker]
                return True
        return False

def add_investment(ticker, amount_usd, entry_price):
    ticker = remap_ticker(ticker)
    timestamp = datetime.now().isoformat()
    if MONGO_URI:
        col_portfolio.update_one({"_id": ticker}, {"$set": {"is_investment": True, "amount_usd": float(amount_usd), "entry_price": float(entry_price), "timestamp": timestamp}}, upsert=True)
    else:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute('SELECT 1 FROM portfolio WHERE ticker = ?', (ticker,))
            if c.fetchone():
                 c.execute('UPDATE portfolio SET is_investment = 1, amount_usd = ?, entry_price = ?, timestamp = ? WHERE ticker = ?', (amount_usd, entry_price, timestamp, ticker))
            else:
                 c.execute('INSERT INTO portfolio (ticker, is_investment, amount_usd, entry_price, timestamp) VALUES (?, 1, ?, ?, ?)', (ticker, amount_usd, entry_price, timestamp))
            conn.commit()
            
    log_base64_backup()
    val = fetch_and_analyze_stock(ticker)
    if val: update_smc_memory(ticker, val)

def close_investment(ticker):
    ticker = remap_ticker(ticker)
    if MONGO_URI:
        col_portfolio.update_one({"_id": ticker}, {"$set": {"is_investment": False, "amount_usd": 0, "entry_price": 0}})
    else:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute('UPDATE portfolio SET is_investment = 0, amount_usd = 0, entry_price = 0 WHERE ticker = ?', (ticker,))
            conn.commit()
    log_base64_backup()

def get_investments():
    invs = {}
    if MONGO_URI:
        for doc in col_portfolio.find({"is_investment": True}):
            invs[doc["_id"]] = {'amount_usd': doc["amount_usd"], 'entry_price': doc["entry_price"]}
        return invs
    else:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute('SELECT ticker, amount_usd, entry_price FROM portfolio WHERE is_investment = 1')
            for row in c.fetchall():
                invs[row[0]] = {'amount_usd': row[1], 'entry_price': row[2]}
        return invs

def add_realized_pnl(prof_usd):
    if MONGO_URI:
        col_stats.update_one({"_id": "realized_pnl"}, {"$inc": {"value": float(prof_usd)}}, upsert=True)
    else:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute('SELECT value FROM global_stats WHERE key = "realized_pnl"')
            res = c.fetchone()
            cur_pnl = res[0] if res else 0.0
            new_val = cur_pnl + float(prof_usd)
            
            if res: c.execute('UPDATE global_stats SET value = ? WHERE key = "realized_pnl"', (new_val,))
            else: c.execute('INSERT INTO global_stats (key, value) VALUES ("realized_pnl", ?)', (new_val,))
            conn.commit()
    log_base64_backup()

def get_realized_pnl():
    if MONGO_URI:
        doc = col_stats.find_one({"_id": "realized_pnl"})
        return doc["value"] if doc else 0.0
    else:
        with sqlite3.connect(DB_PATH) as conn:
             c = conn.cursor()
             c.execute('SELECT value FROM global_stats WHERE key = "realized_pnl"')
             res = c.fetchone()
             return res[0] if res else 0.0


WHALE_MEMORY = deque(maxlen=5) 
SMC_LEVELS_MEMORY = {} 

# ----------------- NÚCLEO DE MERCADO E INTELIGENCIA -----------------
def get_safe_ticker_price(ticker, force_validation=False):
    """Descarga de datos sanitizando engaños de Yahoo en activos duros (Ej: LCO)"""
    tk = remap_ticker(ticker)
    try:
        data = yf.download(tk, period="1d", interval="1m", progress=False)
        if data.empty: return None
        if isinstance(data.columns, pd.MultiIndex):
             data = data.copy(); data.columns = data.columns.get_level_values(0)
             
        close_prices = data['Close']; volumes = data['Volume']
        if isinstance(close_prices, pd.DataFrame): 
             close_prices = close_prices.iloc[:, 0]; volumes = volumes.iloc[:, 0]
        
        price = float(close_prices.iloc[-1])
        # Validación Defensiva Exigida (LCO/BZ=F nunca debajo de $70 en este contexto temporal)
        if tk == "BZ=F" and price < 60:
             logging.warning(f"Posible error residual en Yahoo para {tk} (${price}).")
             return None
             
        return {'price': price, 'vol': float(volumes.iloc[-1])}
    except: return None

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

def fetch_intraday_data(ticker):
    tk = remap_ticker(ticker)
    try:
        safe_check = get_safe_ticker_price(tk)
        if not safe_check: return None
        
        data = yf.download(tk, period="5d", interval="5m", progress=False)
        if data.empty: return None
        if isinstance(data.columns, pd.MultiIndex):
            data = data.copy(); data.columns = data.columns.get_level_values(0)
            
        close_prices = data['Close']; open_prices = data['Open']; volumes = data['Volume']
        if isinstance(close_prices, pd.DataFrame): 
             close_prices = close_prices.iloc[:, 0]; open_prices = open_prices.iloc[:, 0]; volumes = volumes.iloc[:, 0]
             
        vol_type = "Compra 🟢" if float(close_prices.iloc[-1]) >= float(open_prices.iloc[-1]) else "Venta 🔴"
        return {'ticker': tk, 'latest_vol': float(volumes.iloc[-1]), 'avg_vol': float(volumes.mean()), 'vol_type': vol_type, 'latest_price': safe_check['price']} # Retornamos validado
    except: return None

def fetch_and_analyze_stock(ticker):
    tk = remap_ticker(ticker)
    try:
        safe_check = get_safe_ticker_price(tk)
        if not safe_check: return None
        
        data = yf.download(tk, period="6mo", interval="1d", progress=False)
        if data.empty: return None
        if isinstance(data.columns, pd.MultiIndex):
            data = data.copy(); data.columns = data.columns.get_level_values(0)
            
        close_prices = data['Close']; volume = data['Volume']
        if isinstance(close_prices, pd.DataFrame): 
             close_prices = close_prices.iloc[:, 0]; volume = volume.iloc[:, 0]
             
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
        
        return {'ticker': tk, 'price': latest_price, 'rsi': latest_rsi, 'macd_line': float(macd_line.iloc[-1]), 'macd_signal': float(macd_signal.iloc[-1]), 'smc_sup': smc_sup, 'smc_res': smc_res, 'smc_trend': smc_trend, 'order_block': order_block_price}
    except: return None

def update_smc_memory(ticker, analysis):
    tk = remap_ticker(ticker)
    SMC_LEVELS_MEMORY[tk] = {'sup': analysis['smc_sup'], 'res': analysis['smc_res'], 'update_date': datetime.now()}

def analyze_breakout_gpt(ticker, level_type, price):
    tk = remap_ticker(ticker)
    display_name = get_display_name(tk)
    if not OPENAI_API_KEY: return "¿Qué hacer? Mantener cautela."
    client = OpenAI(api_key=OPENAI_API_KEY)
    prompt = f"El activo {display_name} rompió su {level_type} en ${price:.2f}. Consejo corto de 1 párrafo: ¿Qué hacer ahora? (Elige y resalta COMPRAR, VENDER o MANTENER) y por qué. ESPAÑOL ESTRICTO."
    try: return client.chat.completions.create(model="gpt-4o", messages=[{"role": "user", "content": prompt}], max_tokens=200).choices[0].message.content
    except: return "¿Qué hacer? Esperar al cierre del día."

def perform_deep_analysis(ticker):
    tk = remap_ticker(ticker)
    display_name = get_display_name(tk)
    tech_info = f"Información técnica no disponible para {display_name}."
    tech = fetch_and_analyze_stock(tk)
    if tech: tech_info = f"Precio: ${tech['price']:.2f}\nRSI: {tech['rsi']:.2f}\nSMC Trend: {tech['smc_trend']}"
        
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
        return "---\n💎 *ESTADO GLOBAL DE TU WALLET* 💎\n---\n⚠️ Portafolio Vacío. No hay liquidez invertida."
        
    total_invested = 0.0
    total_current = 0.0
    details = []

    for tk, dt in investments.items():
        init_amount = dt['amount_usd']
        entry_p = dt['entry_price']
        intra = fetch_intraday_data(tk)
        
        if intra:
            live_price = intra['latest_price']
            roi_percent = (live_price - entry_p) / entry_p
            curr_val = init_amount * (1 + roi_percent)
            
            total_invested += init_amount
            total_current += curr_val
            
            sign = "+" if roi_percent >= 0 else ""
            display_name = get_display_name(tk)
            details.append(f"• {display_name}: {sign}{roi_percent*100:.2f}%")
            
    if total_invested == 0 and realized_pnl != 0:
         return (f"---\n💎 *ESTADO GLOBAL DE TU WALLET* 💎\n---\n"
                 f"💹 <b>Capital Operativo Activo:</b> $0.00\n"
                 f"💵 <b>Ganancia Mensual (Acumulado Ventas):</b> {'+' if realized_pnl>=0 else ''}${realized_pnl:,.2f} USD\n---")
        
    total_roi = (total_current - total_invested) / total_invested
    sign_roi = "+" if total_roi >= 0 else ""
    status_icon = "🟢 EN GANANCIAS" if total_roi >= 0 else "🔴 EN PÉRDIDAS"
    
    goal = 0.10
    progress_ratio = total_roi / goal
    if progress_ratio < 0: progress_ratio = 0
    if progress_ratio > 1: progress_ratio = 1
    
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

def load_mongo_state():
    """Repoblación Automática al Iniciar"""
    tkrs = get_tracked_tickers()
    if tkrs:
        logging.info(f"¡INFO DB RECUPERADA EXITOSAMENTE! Activos en radar Nube: {len(tkrs)}")
    else:
        logging.warning("Cartera detectada vacía al Inicio.")

@bot.message_handler(commands=['start'])
def cmd_start(message):
    if str(message.chat.id) != str(CHAT_ID): return
    load_mongo_state()
    markup = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.row(KeyboardButton("🌎 Geopolítica"), KeyboardButton("🐳 Radar Ballenas"))
    markup.row(KeyboardButton("📉 SMC / Mi Cartera"), KeyboardButton("💰 Mi Wallet / Estado"))
    cloud_status = "NUBE MONGO ATLAS" if MONGO_URI else "SQLITE FALLBACK + CONFIG BASE64"
    bot.reply_to(message, f"¡Génesis Dashboard Patrimonial Online!\nProtección Anticolapso Activa ({cloud_status}). Botonera lista:", reply_markup=markup)

@bot.message_handler(commands=['recover'])
def cmd_recover(message):
    """Herramienta de Carga Crítica de Respaldo por Base64 dictada en los Requerimientos"""
    if str(message.chat.id) != str(CHAT_ID): return
    try:
        command_parts = message.text.split(' ', 1)
        if len(command_parts) < 2:
            bot.reply_to(message, "⚠️ Has invocado la Restauración Crítica.\nUso: `/recover [STRING_BASE64_DEL_LOG]`", parse_mode="Markdown")
            return
            
        b64_str = command_parts[1].strip()
        json_str = base64.b64decode(b64_str).decode('utf-8')
        payload = json.loads(json_str)
        
        portfolio = payload.get("portfolio", {})
        stats = payload.get("global_stats", {})
        
        # Recuperación adaptativa (SQL o Mongo)
        for tk, info in portfolio.items():
            amount = info.get("amount_usd", 0)
            if info.get("is_investment"): add_investment(tk, amount, info.get("entry_price", 0))
            else: add_ticker(tk)
            
        realized = stats.get("realized_pnl", 0)
        if realized > 0: add_realized_pnl(realized)
            
        bot.reply_to(message, f"✅ **¡RECUPERACIÓN CRÍTICA EXITOSA!**\nLa Nube ha sido parchada.\nSe restauraron {len(portfolio)} activos en Base de Datos y PnL histórico desde la cápsula Base64.", parse_mode="Markdown")
        
        for tk in portfolio.keys():
            val = fetch_and_analyze_stock(tk)
            if val: update_smc_memory(tk, val)
            
    except Exception as e:
        bot.reply_to(message, f"❌ Falló la inyección de recuperación: `{e}`", parse_mode="Markdown")

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
            lines.append(f"• <b>{get_display_name(w['ticker'])}</b> | Vol: {w['vol_approx']:,} | Tipo: {w['type']} | {int((datetime.now() - w['timestamp']).total_seconds() / 60)} mins ago")
        bot.send_message(message.chat.id, "\n".join(lines), parse_mode="HTML")
        return
        
    if text == "🌎 Geopolítica":
        bot.reply_to(message, "🌎 Procesando macro Geopolítica Manual...")
        ai_res = gpt_advanced_geopolitics(check_geopolitical_news(), manual=True)
        bot.send_message(message.chat.id, f"---\n🌍 *INSIGHT GLOBAL*\n---\n{ai_res}" if ai_res else "✅ Radar limpio.", parse_mode="HTML")
        return
            
    if text == "📉 SMC / Mi Cartera":
        bot.reply_to(message, "📉 Computando Mapas SMC Instritucionales...")
        report_lines = ["---", "🦅 *GÉNESIS: SMC / NIVELES CRÍTICOS*", "---"]
        for tk in get_tracked_tickers():
            analysis = fetch_and_analyze_stock(tk)
            if analysis:
                update_smc_memory(tk, analysis)
                d_name = get_display_name(analysis['ticker'])
                report_lines.extend([f"🏦 <b>{d_name}</b> - ${analysis['price']:.2f}", f"• Tendencia SMC: {analysis['smc_trend']}", f"• Buy-side Liquidity: ${analysis['smc_sup']:.2f}", f"• Sell-side Liquidity: ${analysis['smc_res']:.2f}", f"• Order Block Institucional: ${analysis['order_block']:.2f}", "---"])
        bot.send_message(message.chat.id, "\n".join(report_lines) if len(report_lines)>3 else "Tu cartera está vacía.", parse_mode="HTML")
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
                     bot.reply_to(message, f"✅ LCO ( Brent ) ya está en tu radar y actualizado con el precio real de $BZ=F.")
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
                bot.send_message(message.chat.id, f"---\n✅ *CAPITAL REGISTRADO*\n---\n• Activo: {display_name}\n• Capital Invertido: ${float(amt):,.2f} USD\n• Entrada: ${intra['latest_price']:.2f}\n\n✅ Guardado en Base de Datos Blindada. Esta información no se borrará aunque el bot se reinicie.", parse_mode="HTML")
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
                    roi = (live_price - entry) / entry
                    prof = amt * roi
                    sign = "+" if prof >= 0 else ""
                    icon = "🟢" if prof >= 0 else "🔴"
                    final_usd = amt + prof
                    
                    close_investment(tk)
                    add_realized_pnl(prof)

                    ans_str = (
                        f"---\n✅ *GESTIÓN DE CARTERA: CIERRE*\n---\n"
                        f"✅ [ {display_name} ] liquidado al precio de ${live_price:.2f}\n"
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


# ----------------- BUCLE CENTINELA HFT PRECISIÓN QUIRÚRGICA -----------------
def boot_smc_levels_once():
    logging.info("Arrancando Centinela Quirúrgico (30s) / Precisión Total / MOTOR ACTIVO...")
    load_mongo_state()
        
    for tk in get_tracked_tickers():
        val = fetch_and_analyze_stock(tk)
        if val: update_smc_memory(tk, val)

def background_loop_proactivo():
    """BUCLE DE ALTA LATENCIA CON DOBLE VERIFICACIÓN Y ANTI-SPAM DE DISCO (TTL 7 DÍAS)"""
    boot_smc_levels_once() 
    while True:
        try:
            time.sleep(30)
            now = datetime.now()
            purge_old_events() # TTL automático
            
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
                     
            for tk in get_tracked_tickers():
                intra = fetch_intraday_data(tk)
                if not intra: continue
                cur_price = intra['latest_price']
                display_name = get_display_name(tk)
                
                # Rupturas Doble Verificadas (YFinance 1 Minuto)
                topol = SMC_LEVELS_MEMORY.get(tk)
                if topol:
                    if cur_price > topol['res']:
                        rt = verify_1m_realtime_data(tk)
                        if rt and rt['price'] > topol['res']:
                           hash_brk = f"BRK_UP_{tk}_{topol['res']}"
                           if not check_and_add_seen_event(hash_brk):
                               adv = analyze_breakout_gpt(tk, "Resistencia", rt['price'])
                               bot.send_message(CHAT_ID, f"---\n🚨 *ALERTA DE RUPTURA INMINENTE*\n---\n<b>{display_name}</b> cruzó quirúrgicamente Resistencia en <b>${rt['price']:.2f}</b>.\n\n🤖 *DECISIÓN IA:*\n{adv}", parse_mode="HTML")
                    
                    elif cur_price < topol['sup']:
                        rt = verify_1m_realtime_data(tk)
                        if rt and rt['price'] < topol['sup']:
                           hash_drp = f"BRK_DWN_{tk}_{topol['sup']}"
                           if not check_and_add_seen_event(hash_drp):
                               adv = analyze_breakout_gpt(tk, "Soporte", rt['price'])
                               bot.send_message(CHAT_ID, f"---\n🚨 *ALERTA DE RUPTURA (DUMP)*\n---\n<b>{display_name}</b> cruzó quirúrgicamente Soporte en <b>${rt['price']:.2f}</b>.\n\n🤖 *DECISIÓN IA:*\n{adv}", parse_mode="HTML")
                
                # Ballenas Doble Verificadas
                if intra['avg_vol'] > 0:
                    spike = intra['latest_vol'] / intra['avg_vol']
                    if spike >= 2.5: 
                        rt = verify_1m_realtime_data(tk)
                        valid_vol = int(rt['vol']) if rt else int(intra['latest_vol'])
                        whale_hash_id = f"WHL_{tk}_{valid_vol}" 
                        
                        if not check_and_add_seen_event(whale_hash_id):
                            note = "\n<i>[Confirmando volumen institucional...]</i>" if not rt or rt['vol'] < intra['latest_vol'] else ""
                            WHALE_MEMORY.append({"ticker": tk, "vol_approx": valid_vol, "type": intra['vol_type'], "timestamp": now})
                            bot.send_message(CHAT_ID, f"---\n⚠️ *ALERTA DE BALLENA HFT*\n---\nBloque masivo cruzado en <b>{display_name}</b>: {valid_vol:,} unidades.\nPresión Institucional: {intra['vol_type']}{note}", parse_mode="HTML")
                        
        except Exception as e:
            logging.error(f"Error HFT: {e}")

# ----------------- MAIN -----------------
def main():
    print(f"Iniciando Módulo de Alta Frecuencia (30s) / Motores de Persistencia Nube/Local listos.")
    t = threading.Thread(target=background_loop_proactivo, daemon=True)
    t.start()
    bot.infinity_polling(timeout=10, long_polling_timeout=5)

if __name__ == "__main__":
    main()
