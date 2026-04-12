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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')

if not TELEGRAM_TOKEN or not CHAT_ID:
    logging.critical("Falta TELEGRAM_TOKEN o CHAT_ID. Saliendo sin saturar.")
    exit()

bot = telebot.TeleBot(TELEGRAM_TOKEN)
PORTFOLIO_FILE = 'portfolio.json'

WHALE_MEMORY = deque(maxlen=5) 
SMC_LEVELS_MEMORY = {} 
# Estructura de SMC_LEVELS_MEMORY: 
# "TICKER": {'sup': float, 'res': float, 'alert_sup': False, 'alert_res': False, 'update_date': datetime}

def get_tracked_tickers():
    if os.path.exists(PORTFOLIO_FILE):
        try:
            with open(PORTFOLIO_FILE, 'r') as f:
                data = json.load(f)
                return list(data.keys())
        except:
             return ["NVDA", "BTC-USD"]
    return ["NVDA", "BTC-USD"]

def add_ticker(ticker):
    ticker = ticker.upper()
    if ticker == "BTC": ticker = "BTC-USD"
    data = {}
    if os.path.exists(PORTFOLIO_FILE):
        try:
            with open(PORTFOLIO_FILE, 'r') as f:
                data = json.load(f)
        except:
            pass
    if ticker not in data:
        data[ticker] = 0.0
        with open(PORTFOLIO_FILE, 'w') as f:
            json.dump(data, f, indent=4)
        
        # Inicializar memoria basal SMC inmediatemente
        val = fetch_and_analyze_stock(ticker)
        if val: update_smc_memory(ticker, val)
        return True
    return False

def remove_ticker(ticker):
    ticker = ticker.upper()
    if ticker == "BTC": ticker = "BTC-USD"
    if os.path.exists(PORTFOLIO_FILE):
        try:
            with open(PORTFOLIO_FILE, 'r') as f:
                data = json.load(f)
            if ticker in data:
                del data[ticker]
                with open(PORTFOLIO_FILE, 'w') as f:
                    json.dump(data, f, indent=4)
                if ticker in SMC_LEVELS_MEMORY:
                    del SMC_LEVELS_MEMORY[ticker]
                return True
        except:
            pass
    return False

# ----------------- NÚCLEO DE MERCADO -----------------
def check_geopolitical_news():
    logging.info("Monitoreando Radar Geopolítico...")
    search_url = "https://news.google.com/rss/search?q=geopolitics+OR+Trump+OR+rates+OR+war+OR+economy"
    HIGH_IMPACT_KEYWORDS = ["war", "attack", "strike", "escalation", "missile", "sanction", "embargo", 
                            "explosion", "guerra", "ataque", "tensión", "misil", "sanciones", "rates", "fed", "trump", "powell"]
    news_alerts = []
    try:
        response = requests.get(search_url)
        if response.status_code == 200:
            root = ET.fromstring(response.text)
            for item in root.findall('.//item'):
                title = item.find('title').text
                if any(re.search(rf"\b{kw}\b", title, re.IGNORECASE) for kw in HIGH_IMPACT_KEYWORDS):
                    news_alerts.append(title)
                    if len(news_alerts) >= 5: break
    except Exception as e:
        pass
    return news_alerts

def gpt_advanced_geopolitics(news_list, manual=False):
    if not news_list or not OPENAI_API_KEY: return None
    client = OpenAI(api_key=OPENAI_API_KEY)
    news_text = "\n".join([f"- {n}" for n in news_list])
    if manual:
        prompt = f"Titulares globales:\n{news_text}\nHaz un resumen y dime qué movería el mercado hoy. RESPONDE ESTRICTA Y ÚNICAMENTE EN ESPAÑOL."
    else:
        prompt = (f"Titulares recientes:\n{news_text}\n"
             "Analiza si hay algo de nivel 'Alto Impacto' (>2%). Si no lo hay, responde 'TRANQUILIDAD'.\n"
             "Si lo hay: '⚠️ ALERTA URGENTE: [Resumen] - Impacto en [Acción/Sector]'\n"
             "RESPONDE ESTRICTA Y ÚNICAMENTE EN ESPAÑOL.")
    try:
        response = client.chat.completions.create(model="gpt-4o", messages=[{"role": "user", "content": prompt}], max_tokens=300)
        res = response.choices[0].message.content.strip()
        if not manual and ("TRANQUILIDAD" in res.upper() and len(res) < 20): return None
        return res
    except:
        return None

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
        return {
            'ticker': ticker, 'latest_vol': float(volumes.iloc[-1]), 'avg_vol': float(volumes.mean()),
            'vol_type': vol_type, 'latest_price': float(close_prices.iloc[-1])
        }
    except Exception as e:
        return None

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
        
        res = {
            'ticker': ticker, 'price': latest_price, 'rsi': latest_rsi, 'macd_line': float(macd_line.iloc[-1]), 
            'macd_signal': float(macd_signal.iloc[-1]), 'smc_sup': smc_sup, 'smc_res': smc_res, 
            'smc_trend': smc_trend, 'order_block': order_block_price
        }
        return res
    except Exception as e:
        return None

def update_smc_memory(ticker, analysis):
    """Refresca la memoria de niveles críticos estables si el día cambió."""
    now = datetime.now()
    if ticker not in SMC_LEVELS_MEMORY or (now - SMC_LEVELS_MEMORY[ticker]['update_date']).total_seconds() > 43200:
        SMC_LEVELS_MEMORY[ticker] = {
            'sup': analysis['smc_sup'], 'res': analysis['smc_res'], 
            'alert_sup': False, 'alert_res': False, 'update_date': now
        }

def analyze_breakout_gpt(ticker, level_type, price):
    if not OPENAI_API_KEY: return "¿Qué hacer? Mantener cautela."
    client = OpenAI(api_key=OPENAI_API_KEY)
    prompt = f"El activo {ticker} acaba de romper agresivamente su nivel de {level_type} cotizando en exactos ${price:.2f}. Dale a nuestro operador institucional un consejo cortísimo de un párrafo:\n¿Qué hacer ahora? (Elige y resalta COMPRAR, VENDER o MANTENER) y por qué.\nRESPONDE ESTRICTA Y ÚNICAMENTE EN ESPAÑOL."
    try:
        res = client.chat.completions.create(model="gpt-4o", messages=[{"role": "user", "content": prompt}], max_tokens=200)
        return res.choices[0].message.content
    except:
         return "¿Qué hacer? Esperar confirmación de la siguiente candela direccional."

def perform_deep_analysis(ticker):
    ticker = ticker.upper()
    if ticker == "BTC": ticker = "BTC-USD"
    tech = fetch_and_analyze_stock(ticker)
    tech_info = f"Información técnica no disponible para {ticker}."
    if tech:
        tech_info = f"Precio: ${tech['price']:.2f}\nRSI: {tech['rsi']:.2f}\nMACD Line: {tech['macd_line']:.2f}\nSMC Trend: {tech['smc_trend']}"
        
    news_str = ""
    try:
        stock = yf.Ticker(ticker)
        news_str = "\n".join([f"- {n.get('title', '')}" for n in stock.news[:3]])
    except: pass
        
    prompt = (
        f"Analiza profundamente el activo '{ticker}'. Extrae información técnica y fundamental.\n"
        f"Datos Técnicos:\n{tech_info}\n\nNoticias YF:\n{news_str if news_str else 'Sin noticias.'}\n\n"
        "Comportate como un analista institucional. Combina enfoques. Debes dictaminar un VEREDICTO FINAL resaltado: 'COMPRAR', 'VENDER' o 'MANTENER/ESPERAR'.\nRESPONDE ESTRICTAMENTE EN ESPAÑOL."
    )
    if not OPENAI_API_KEY: return "Error: API API KEY MISSING."
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        return client.chat.completions.create(model="gpt-4o", messages=[{"role": "user", "content": prompt}], max_tokens=600).choices[0].message.content
    except Exception as e:
        return f"Fallo al analizar: {e}"

def build_full_report():
    report_lines = ["🦅 <b>Génesis: SMC / Mi Cartera</b> 🦅\n"]
    tickers = get_tracked_tickers()
    if not tickers: return "Tu cartera está vacía. Añade activos usando 'Agrega AAPL'."
    
    for tk in tickers:
        analysis = fetch_and_analyze_stock(tk)
        if analysis:
            update_smc_memory(tk, analysis) # Mantenemos en caché la topologia reportada
            report_lines.append(f"🏦 <b>{analysis['ticker']}</b> - Cotización: ${analysis['price']:.2f}")
            report_lines.append(f"• <b>Tendencia SMC:</b> {analysis['smc_trend']}")
            report_lines.append(f"• <b>Soporte (Buy-side Liquidity):</b> ${analysis['smc_sup']:.2f}")
            report_lines.append(f"• <b>Resistencia (Sell-side):</b> ${analysis['smc_res']:.2f}")
            report_lines.append(f"• <b>Order Block Institucional:</b> ${analysis['order_block']:.2f}")
            report_lines.append(f"• <b>RSI:</b> {analysis['rsi']:.2f}\n")
    return "\n".join(report_lines)

def format_time_ago(ts):
    diff = int((datetime.now() - ts).total_seconds() / 60)
    if diff == 0: return "Hace unos segundos"
    if diff == 1: return "Hace 1 min"
    return f"Hace {diff} mins"

# ----------------- CONTROLADORES TELEBOT -----------------
@bot.message_handler(commands=['start'])
def cmd_start(message):
    if str(message.chat.id) != str(CHAT_ID): return
    markup = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(KeyboardButton("🌎 Geopolítica"), KeyboardButton("🐳 Radar Ballenas"), KeyboardButton("📉 SMC / Mi Cartera"))
    bot.reply_to(message, "¡Génesis Niveles Críticos Online! Cero latencia. Botonera lista:", reply_markup=markup)

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    if str(message.chat.id) != str(CHAT_ID): return
    msg = bot.reply_to(message, "👁️ Analizando estructura visual...")
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        base64_image = base64.b64encode(bot.download_file(file_info.file_path)).decode('utf-8')
        client = OpenAI(api_key=OPENAI_API_KEY)
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": [
                    {"type": "text", "text": "Analiza gráfica SMC con rigor. Veredicto: 'Bullish' o 'Bearish'. ESPAÑOL ESTRICTO."},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
                ]}], max_tokens=800
        )
        bot.edit_message_text(f"📊 [REPORTE VISUAL]\n\n{response.choices[0].message.content}", chat_id=message.chat.id, message_id=msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Falló visión: {e}", chat_id=message.chat.id, message_id=msg.message_id)

@bot.message_handler(func=lambda message: True, content_types=['text'])
def handle_text(message):
    if str(message.chat.id) != str(CHAT_ID): return
    text = message.text.strip()
    
    if text == "🐳 Radar Ballenas":
        bot.reply_to(message, "🐳 Memoria HFT Institucional invocada...")
        if not WHALE_MEMORY:
            bot.send_message(message.chat.id, "🐋 <b>Radar Ballenas</b>\n\nEl océano está quieto. Sin anomalías detectadas hoy.", parse_mode="HTML")
            return
        lines = ["🐋 <b>ÚLTIMAS 5 BALLENAS:</b>\n"]
        for w in list(WHALE_MEMORY)[::-1]:
            lines.append(f"• <b>{w['ticker']}</b> | Vol: {w['vol_approx']:,} | Tipo: {w['type']} | {format_time_ago(w['timestamp'])}")
        bot.send_message(message.chat.id, "\n".join(lines), parse_mode="HTML")
        
    elif text == "🌎 Geopolítica":
        bot.reply_to(message, "🌎 Procesando macro Geopolítica Manual...")
        ai_res = gpt_advanced_geopolitics(check_geopolitical_news(), manual=True)
        bot.send_message(message.chat.id, f"🌍 <b>Insight Global:</b>\n\n{ai_res}" if ai_res else "✅ Radar limpio.", parse_mode="HTML")
            
    elif text == "📉 SMC / Mi Cartera":
        bot.reply_to(message, "📉 Computando Mapas Institucionales SMC (Actualizando Memorias Lógicas)...")
        bot.send_message(message.chat.id, build_full_report(), parse_mode="HTML")
        
    elif text.upper().startswith("ANALIZA "):
        match = re.search(r'ANALIZA\s+([A-Za-z0-9\-]+)', text.upper())
        if match:
            tk = match.group(1)
            bot.reply_to(message, f"🔍 Bajando data para Análisis Profundo Institucional en {tk}...")
            bot.send_message(message.chat.id, f"🏦 <b>RESEARCH: {tk}</b>\n\n{perform_deep_analysis(tk)}", parse_mode="HTML")
            
    elif "AGREGA" in text.upper():
        match = re.search(r'AGREGA\s+([A-Za-z0-9\-]+)', text.upper())
        if match:
            tk = match.group(1)
            bot.reply_to(message, f"✅ Activo <b>{tk}</b> blindado." if add_ticker(tk) else f"⚠️ {tk} ya existía.", parse_mode="HTML")
            
    elif "ELIMINA" in text.upper() or "BORRA" in text.upper():
        match = re.search(r'(?:ELIMINA|BORRA)\s+([A-Za-z0-9\-]+)', text.upper())
        if match:
            tk = match.group(1)
            bot.reply_to(message, f"🗑️ Activo <b>{tk}</b> destrozado de la memoria." if remove_ticker(tk) else f"⚠️ No residía en tu radar.", parse_mode="HTML")

# ----------------- BUCLE CENTINELA MAESTRO -----------------
def boot_smc_levels_once():
    """Ejecutado al iniciar el Bot para popular Niveles Críticos en memoria"""
    for tk in get_tracked_tickers():
        val = fetch_and_analyze_stock(tk)
        if val: update_smc_memory(tk, val)

def background_loop_proactivo():
    """TICK EXACTO: 5 minutos. Ejecuta Niveles Críticos y Espionaje Intradia"""
    tick_count = 0
    boot_smc_levels_once() 
    
    while True:
        try:
            time.sleep(300) # 5 MINUTOS EXACTOS REQUERIDOS
            tick_count += 1
            now = datetime.now()
            
            # --- TAREA 1: 5-MINUTOS RASTREO VOLUMEN BALLENAS Y SMC BREAKOUTS ---
            for tk in get_tracked_tickers():
                intra = fetch_intraday_data(tk)
                if not intra: continue
                
                cur_price = intra['latest_price']
                
                # Check de Rupturas de Nivel Critico (Breakout Sentinel)
                topol = SMC_LEVELS_MEMORY.get(tk)
                if topol:
                    # Breakout Resistencia
                    if cur_price > topol['res'] and not topol['alert_res']:
                        topol['alert_res'] = True
                        adv = analyze_breakout_gpt(tk, "Resistencia", cur_price)
                        bot.send_message(CHAT_ID, f"🚨 <b>ALERTA DE RUPTURA</b>:\n<b>{tk}</b> ha roto la <i>Resistencia Institucional</i> en <b>${cur_price:.2f}</b>.\n\n🤖 <b>DECISIÓN IA:</b>\n{adv}", parse_mode="HTML")
                        
                    # Breakdown Soporte
                    elif cur_price < topol['sup'] and not topol['alert_sup']:
                        topol['alert_sup'] = True
                        adv = analyze_breakout_gpt(tk, "Soporte", cur_price)
                        bot.send_message(CHAT_ID, f"🚨 <b>ALERTA DE RUPTURA (DUMP)</b>:\n<b>{tk}</b> ha quebrado el <i>Soporte Institucional</i> perdiendo el nivel a <b>${cur_price:.2f}</b>.\n\n🤖 <b>DECISIÓN IA:</b>\n{adv}", parse_mode="HTML")

                # Check de Ballenas (+2.5x Vol)
                if intra['avg_vol'] > 0:
                    spike = intra['latest_vol'] / intra['avg_vol']
                    if spike >= 2.5: 
                        clean_amount = int(intra['latest_vol'])
                        WHALE_MEMORY.append({"ticker": tk, "vol_approx": clean_amount, "type": intra['vol_type'], "timestamp": now})
                        bot.send_message(CHAT_ID, f"⚠️ <b>ALERTA DE BALLENA</b>:\nMovimiento masivo en <b>{tk}</b>.\nCantidad aprox: <b>{clean_amount:,}</b> ({intra['vol_type']})", parse_mode="HTML")

            # --- TAREA 2: ESCÁNER MACRO GEOPOLÍTICA (> 2% Impacto - Cada 3 Ticks = 15 Mins) ---
            if tick_count % 3 == 0:
                ai_threat_evaluation = gpt_advanced_geopolitics(check_geopolitical_news(), manual=False)
                if ai_threat_evaluation:
                     bot.send_message(CHAT_ID, f"🚨 <b>VIGILANCIA GLOBAL ALTO RIESGO:</b>\n\n{ai_threat_evaluation}", parse_mode="HTML")
                     
        except Exception as e:
            logging.error(f"Error en Centinela HFT 5min: {e}")

# ----------------- MAIN -----------------
def main():
    print(f"Iniciando Centinela. Cargando Memorias SMC y Red Neural...")
    t = threading.Thread(target=background_loop_proactivo, daemon=True)
    t.start()
    bot.infinity_polling(timeout=10, long_polling_timeout=5)

if __name__ == "__main__":
    main()
