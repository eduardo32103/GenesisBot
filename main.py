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
from collections import deque
from telebot.types import ReplyKeyboardMarkup, KeyboardButton
from datetime import datetime, timedelta

# Configuración extendida de logs para capturar en la consola de Railway
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')

if not TELEGRAM_TOKEN or not CHAT_ID:
    logging.critical("Falta TELEGRAM_TOKEN o CHAT_ID. Saliendo sin saturar.")
    exit()

bot = telebot.TeleBot(TELEGRAM_TOKEN)

# --- CONFIGURACIÓN DE VOLÚMENES PERSISTENTES RAILWAY ---
# Railway borra los archivos en carpeta root si no se mapea un Volume.
# Aquí indicamos que si existe la variable DATA_DIR, la use. Si no, usa local (.)
DATA_DIR = os.environ.get('DATA_DIR', '.')
if not os.path.exists(DATA_DIR):
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
    except Exception as e:
        logging.error(f"Error creando DATA_DIR: {e}")

CARTERA_FILE = os.path.join(DATA_DIR, 'cartera.json')

WHALE_MEMORY = deque(maxlen=5) 
SMC_LEVELS_MEMORY = {} 

class Deduplicator:
    """Escudo Inteligente de Memoria RAM: Filtra duplicados (Spam) sin comer memoria infinita."""
    def __init__(self, max_size=200):
        self._cache = deque(maxlen=max_size)
    
    def add_and_check(self, item_hash):
        if item_hash in self._cache:
            return True
        self._cache.append(item_hash)
        return False

# Inicializar Escudos contra saturación HFT
CACHE_NEWS = Deduplicator(100) 
CACHE_WHALES = Deduplicator(100)

# --- NÚCLEO DE PERSISTENCIA SEGURA ---
def get_cartera_data():
    if os.path.exists(CARTERA_FILE):
        try:
            with open(CARTERA_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Falla de lectura JSON: {e}")
            return {}
    return {}

def save_cartera_data(data):
    try:
        with open(CARTERA_FILE, 'w') as f:
            json.dump(data, f, indent=4)
        # Log DE RESPALDO IMPENETRABLE PARA RECUPERACIÓN VÍA CONSOLA DE RAILWAY
        logging.info(f"BACKUP_DB_LOG (SAFE): {json.dumps(data)}")
    except Exception as e:
        logging.error(f"Error crítico guardando la base de datos: {e}")

def get_tracked_tickers():
    data = get_cartera_data()
    return [k for k in data.keys() if k != "_GLOBAL_"]

def add_ticker(ticker):
    ticker = ticker.upper()
    if ticker == "BTC": ticker = "BTC-USD"
    data = get_cartera_data()
    
    if ticker not in data:
        data[ticker] = {"is_investment": False}
        save_cartera_data(data) 
        val = fetch_and_analyze_stock(ticker)
        if val: update_smc_memory(ticker, val)
        return True
    return False

def remove_ticker(ticker):
    ticker = ticker.upper()
    if ticker == "BTC": ticker = "BTC-USD"
    data = get_cartera_data()
    
    if ticker in data:
        del data[ticker]
        save_cartera_data(data) 
        if ticker in SMC_LEVELS_MEMORY: 
            del SMC_LEVELS_MEMORY[ticker]
        return True
    return False

def add_investment(ticker, amount_usd, entry_price):
    ticker = ticker.upper()
    if ticker == "BTC": ticker = "BTC-USD"
    data = get_cartera_data()
    
    if ticker not in data:
        data[ticker] = {}
        
    data[ticker].update({
        "is_investment": True,
        "amount_usd": float(amount_usd),
        "entry_price": float(entry_price),
        "timestamp": datetime.now().isoformat()
    })
    save_cartera_data(data) 
    
    val = fetch_and_analyze_stock(ticker)
    if val: update_smc_memory(ticker, val)

def get_investments():
    data = get_cartera_data()
    investments = {}
    for tk, info in data.items():
        if tk != "_GLOBAL_" and info.get("is_investment", False):
            investments[tk] = info
    return investments

def add_realized_pnl(prof_usd):
    data = get_cartera_data()
    if "_GLOBAL_" not in data:
        data["_GLOBAL_"] = {"realized_pnl_usd": 0.0}
    cur_pnl = data["_GLOBAL_"].get("realized_pnl_usd", 0.0)
    data["_GLOBAL_"]["realized_pnl_usd"] = cur_pnl + float(prof_usd)
    save_cartera_data(data)

def get_realized_pnl():
    data = get_cartera_data()
    return data.get("_GLOBAL_", {}).get("realized_pnl_usd", 0.0)


# ----------------- NÚCLEO DE MERCADO E INTELIGENCIA -----------------
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
    try:
        data = yf.download(ticker, period="5d", interval="5m", progress=False)
        if data.empty: return None
        if isinstance(data.columns, pd.MultiIndex):
            data = data.copy(); data.columns = data.columns.get_level_values(0)
            
        close_prices = data['Close']; open_prices = data['Open']; volumes = data['Volume']
        if isinstance(close_prices, pd.DataFrame): 
             close_prices = close_prices.iloc[:, 0]; open_prices = open_prices.iloc[:, 0]; volumes = volumes.iloc[:, 0]
             
        vol_type = "Compra 🟢" if float(close_prices.iloc[-1]) >= float(open_prices.iloc[-1]) else "Venta 🔴"
        return {'ticker': ticker, 'latest_vol': float(volumes.iloc[-1]), 'avg_vol': float(volumes.mean()), 'vol_type': vol_type, 'latest_price': float(close_prices.iloc[-1])}
    except: return None

def fetch_and_analyze_stock(ticker):
    try:
        data = yf.download(ticker, period="6mo", interval="1d", progress=False)
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
        latest_price = float(close_prices.iloc[-1]); latest_rsi = float(rsi_series.iloc[-1])
        
        smc_trend = "Alcista 🟢" if latest_price > close_prices.ewm(span=20).mean().iloc[-1] else "Bajista 🔴"
        recent_month = close_prices.iloc[-22:]
        smc_sup = float(recent_month.min()); smc_res = float(recent_month.max())
        vol_month = volume.iloc[-22:]; order_block_price = float(close_prices.loc[vol_month.idxmax()])
        
        return {'ticker': ticker, 'price': latest_price, 'rsi': latest_rsi, 'macd_line': float(macd_line.iloc[-1]), 'macd_signal': float(macd_signal.iloc[-1]), 'smc_sup': smc_sup, 'smc_res': smc_res, 'smc_trend': smc_trend, 'order_block': order_block_price}
    except: return None

def update_smc_memory(ticker, analysis):
    now = datetime.now()
    if ticker not in SMC_LEVELS_MEMORY or (now - SMC_LEVELS_MEMORY[ticker]['update_date']).total_seconds() > 43200:
        SMC_LEVELS_MEMORY[ticker] = {'sup': analysis['smc_sup'], 'res': analysis['smc_res'], 'alert_sup': False, 'alert_res': False, 'update_date': now}

def analyze_breakout_gpt(ticker, level_type, price):
    if not OPENAI_API_KEY: return "¿Qué hacer? Mantener cautela."
    client = OpenAI(api_key=OPENAI_API_KEY)
    prompt = f"El activo {ticker} rompió su {level_type} en ${price:.2f}. Consejo corto de 1 párrafo: ¿Qué hacer ahora? (Elige y resalta COMPRAR, VENDER o MANTENER) y por qué. ESPAÑOL ESTRICTO."
    try: return client.chat.completions.create(model="gpt-4o", messages=[{"role": "user", "content": prompt}], max_tokens=200).choices[0].message.content
    except: return "¿Qué hacer? Esperar al cierre del día."

def perform_deep_analysis(ticker):
    ticker = ticker.upper()
    if ticker == "BTC": ticker = "BTC-USD"
    tech_info = f"Información técnica no disponible para {ticker}."
    tech = fetch_and_analyze_stock(ticker)
    if tech: tech_info = f"Precio: ${tech['price']:.2f}\nRSI: {tech['rsi']:.2f}\nSMC Trend: {tech['smc_trend']}"
        
    news_str = ""
    try:
        news_str = "\n".join([f"- {n.get('title', '')}" for n in yf.Ticker(ticker).news[:3]])
    except: pass
        
    prompt = (f"Analiza profundamente '{ticker}'.\nTécnicos:\n{tech_info}\n\nNoticias:\n{news_str}\n" "Combina enfoques. Dictamina un VEREDICTO FINAL resaltado: 'COMPRAR', 'VENDER' o 'MANTENER/ESPERAR'. ESPAÑOL ESTRICTO.")
    if not OPENAI_API_KEY: return "Error: API API KEY MISSING."
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
            details.append(f"• {tk}: {sign}{roi_percent*100:.2f}%")
            
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
@bot.message_handler(commands=['start'])
def cmd_start(message):
    if str(message.chat.id) != str(CHAT_ID): return
    markup = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.row(KeyboardButton("🌎 Geopolítica"), KeyboardButton("🐳 Radar Ballenas"))
    markup.row(KeyboardButton("📉 SMC / Mi Cartera"), KeyboardButton("💰 Mi Wallet / Estado"))
    bot.reply_to(message, "¡Génesis Dashboard Patrimonial Online!\nProtección Anticolapso Activa para Nube. Botonera lista:", reply_markup=markup)

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
        for w in list(WHALE_MEMORY)[::-1]: lines.append(f"• <b>{w['ticker']}</b> | Vol: {w['vol_approx']:,} | Tipo: {w['type']} | {int((datetime.now() - w['timestamp']).total_seconds() / 60)} mins ago")
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
                report_lines.extend([f"🏦 <b>{analysis['ticker']}</b> - ${analysis['price']:.2f}", f"• Tendencia SMC: {analysis['smc_trend']}", f"• Buy-side Liquidity: ${analysis['smc_sup']:.2f}", f"• Sell-side Liquidity: ${analysis['smc_res']:.2f}", f"• Order Block Institucional: ${analysis['order_block']:.2f}", "---"])
        bot.send_message(message.chat.id, "\n".join(report_lines) if len(report_lines)>3 else "Tu cartera está vacía.", parse_mode="HTML")
        return
        
    # === EXPRESIONES REGULARES INTELIGENTES NLP ===
    if re.search(r'(?i)\bANALIZA\b\s+([A-Za-z0-9\-]+)', text):
        match = re.search(r'(?i)\bANALIZA\b\s+([A-Za-z0-9\-]+)', text)
        if match:
            tk = match.group(1).upper()
            if tk == "BTC": tk = "BTC-USD"
            bot.reply_to(message, f"🔍 Análisis Profundo Institucional en {tk}...")
            bot.send_message(message.chat.id, f"---\n🏦 *RESEARCH: {tk}*\n---\n{perform_deep_analysis(tk)}", parse_mode="HTML")
        return
            
    if re.search(r'(?i)\b(?:ELIMINA|BORRA|BORRAR|ELIMINAR)\b\s+([A-Za-z0-9\-]+)', text):
        match = re.search(r'(?i)\b(?:ELIMINA|BORRA|BORRAR|ELIMINAR)\b\s+([A-Za-z0-9\-]+)', text)
        if match: 
             tk = match.group(1).upper()
             if remove_ticker(tk):
                 bot.reply_to(message, f"---\n✅ *GESTIÓN DE CARTERA*\n---\n✅ [ {tk} ] ha sido borrado del radar.\n\n✅ Datos guardados permanentemente en la nube.", parse_mode="HTML")
             else:
                 bot.reply_to(message, f"⚠️ El activo {tk} no residía en tu radar.")
        return

    if re.search(r'(?i)\b(?:AGREGA|AÑADE|AGREGAR)\b\s+([A-Za-z0-9\-]+)', text):
        match = re.search(r'(?i)\b(?:AGREGA|AÑADE|AGREGAR)\b\s+([A-Za-z0-9\-]+)', text)
        if match: 
             tk = match.group(1).upper()
             if add_ticker(tk):
                 bot.reply_to(message, f"---\n✅ *GESTIÓN DE CARTERA*\n---\n✅ [ {tk} ] añadido al radar SMC.\n\n✅ Datos guardados permanentemente en la nube.", parse_mode="HTML")
             else:
                 bot.reply_to(message, f"⚠️ El activo {tk} ya existía en tu radar SMC.")
        return

    if re.search(r'(?i)\bCOMPR[EÉ]\b', text):
        match = re.search(r'(?i)\bCOMPR[EÉ]\b\s+(?:DE\s+)?\$?(\d+(?:\.\d+)?)\s+(?:EN\s+|DE\s+|ACCIONES\s+DE\s+)?([A-Za-z0-9\-]+)', text)
        if match:
            amt = match.group(1)
            tk = match.group(2).upper()
            if tk == "BTC": tk = "BTC-USD"
            bot.reply_to(message, f"💸 Consultando precio de fijación para {tk}...")
            intra = fetch_intraday_data(tk)
            if intra:
                add_investment(tk, amt, intra['latest_price'])
                bot.send_message(message.chat.id, f"---\n✅ *CAPITAL REGISTRADO*\n---\n• Activo: {tk}\n• Capital Invertido: ${float(amt):,.2f} USD\n• Entrada: ${intra['latest_price']:.2f}\n\n✅ Datos guardados permanentemente en la nube.", parse_mode="HTML")
            else:
                bot.reply_to(message, f"❌ No pude fijar el precio real de {tk} ahora. Mercado cerrado temporalmente.")
        return

    if re.search(r'(?i)\bVEND[IÍ]\b', text):
        match = re.search(r'(?i)\bVEND[IÍ]\b\s+(?:TODO\s+)?(?:DE\s+)?\$?(?:\d+(?:\.\d+)?\s+(?:EN\s+|DE\s+|ACCIONES\s+DE\s+)?)?([A-Za-z0-9\-]+)', text)
        if match:
            tk = match.group(1).upper()
            if tk == "BTC": tk = "BTC-USD"
            
            investments = get_investments()
            if tk in investments:
                bot.reply_to(message, f"💸 Procesando cierre institucional para {tk}...")
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
                    
                    data = get_cartera_data()
                    data[tk]['is_investment'] = False
                    save_cartera_data(data)
                    
                    add_realized_pnl(prof)

                    ans_str = (
                        f"---\n✅ *GESTIÓN DE CARTERA: CIERRE*\n---\n"
                        f"✅ [ {tk} ] liquidado al precio de ${live_price:.2f}\n"
                        f"💰 <b>Capital Retirado:</b> ${final_usd:,.2f} USD\n"
                        f"{icon} <b>Ganancia Mensual Sumada:</b> {sign}${prof:,.2f} USD ({sign}{roi*100:.2f}%)\n\n"
                        f"✅ Datos guardados permanentemente en la nube."
                    )
                    bot.send_message(message.chat.id, ans_str, parse_mode="HTML")
                else:
                    bot.reply_to(message, f"❌ No pude contactar al mercado para saldar la liquidación de {tk}.")
            else:
                 bot.reply_to(message, f"⚠️ No tienes capital invertido en {tk}. Usa 'Elimina {tk}' para detener rastreo.")
        return


# ----------------- BUCLE CENTINELA HFT (TIEMPO REAL) -----------------
def boot_smc_levels_once():
    logging.info("Arrancando Centinela a Ultra-Alta Velocidad (30s) y cargando Cachés/Base DB...")
    # CHEQUEO INICIAL REQUERIDO POR EL USUARIO PARA AVISO DE DB AL ARRANCAR
    data = get_cartera_data()
    if data:
        logging.info(f"¡INFO DB RECUPERADA EXITOSAMENTE! Activos en radar: {list(data.keys())}")
    else:
        logging.warning("No se encontró base de datos previa o está vacía. Empezando limpia.")
        
    for tk in get_tracked_tickers():
        val = fetch_and_analyze_stock(tk)
        if val: update_smc_memory(tk, val)

def background_loop_proactivo():
    """BUCLE DE ALTA LATENCIA A 30 SEGUNDOS ESTRICTOS CON FILTRO ANTI-SPAM DE MEMORIA"""
    boot_smc_levels_once() 
    while True:
        try:
            time.sleep(30)
            now = datetime.now()
            
            raw_news = check_geopolitical_news()
            unique_news = []
            for n_title in raw_news:
                if not CACHE_NEWS.add_and_check(n_title): 
                    unique_news.append(n_title)
            
            if unique_news:
                ai_threat_evaluation = gpt_advanced_geopolitics(unique_news, manual=False)
                if ai_threat_evaluation:
                     bot.send_message(CHAT_ID, f"---\n🚨 *VIGILANCIA GLOBAL ALTO RIESGO*\n---\n{ai_threat_evaluation}", parse_mode="HTML")
                     
            for tk in get_tracked_tickers():
                intra = fetch_intraday_data(tk)
                if not intra: continue
                cur_price = intra['latest_price']
                
                topol = SMC_LEVELS_MEMORY.get(tk)
                if topol:
                    if cur_price > topol['res'] and not topol['alert_res']:
                        topol['alert_res'] = True
                        adv = analyze_breakout_gpt(tk, "Resistencia", cur_price)
                        bot.send_message(CHAT_ID, f"---\n🚨 *ALERTA DE RUPTURA INMINENTE*\n---\n<b>{tk}</b> rompió su Resistencia SMC logrando los <b>${cur_price:.2f}</b>.\n\n🤖 *DECISIÓN IA:*\n{adv}", parse_mode="HTML")
                    elif cur_price < topol['sup'] and not topol['alert_sup']:
                        topol['alert_sup'] = True
                        adv = analyze_breakout_gpt(tk, "Soporte", cur_price)
                        bot.send_message(CHAT_ID, f"---\n🚨 *ALERTA DE RUPTURA (DUMP)*\n---\n<b>{tk}</b> ha quebrado el Soporte SMC hundiéndose a <b>${cur_price:.2f}</b>.\n\n🤖 *DECISIÓN IA:*\n{adv}", parse_mode="HTML")
                
                if intra['avg_vol'] > 0:
                    spike = intra['latest_vol'] / intra['avg_vol']
                    if spike >= 2.5: 
                        clean_amount = int(intra['latest_vol'])
                        whale_hash_id = f"{tk}_{clean_amount}" 
                        
                        if not CACHE_WHALES.add_and_check(whale_hash_id):
                            WHALE_MEMORY.append({"ticker": tk, "vol_approx": clean_amount, "type": intra['vol_type'], "timestamp": now})
                            bot.send_message(CHAT_ID, f"---\n⚠️ *ALERTA DE BALLENA HFT*\n---\nBloque masivo en <b>{tk}</b> detectado AHORA: {clean_amount:,} unidades.\nPresión Institucional: {intra['vol_type']}", parse_mode="HTML")
                        
        except Exception as e:
            logging.error(f"Error HFT: {e}")

# ----------------- MAIN -----------------
def main():
    print(f"Iniciando Módulo de Alta Frecuencia (30s) / Persistencia Garantizada en Railway")
    t = threading.Thread(target=background_loop_proactivo, daemon=True)
    t.start()
    bot.infinity_polling(timeout=10, long_polling_timeout=5)

if __name__ == "__main__":
    main()
