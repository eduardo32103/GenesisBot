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
from telebot.types import ReplyKeyboardMarkup, KeyboardButton
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')

if not TELEGRAM_TOKEN or not CHAT_ID:
    logging.critical("Falta TELEGRAM_TOKEN o CHAT_ID. Saliendo sin saturar.")
    exit()

bot = telebot.TeleBot(TELEGRAM_TOKEN)
PORTFOLIO_FILE = 'portfolio.json'
ALERTED_VOLUMES = {} # Estructura: {"TICKER": datetime_last_alert}

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
                
                is_high_impact = any(re.search(rf"\b{kw}\b", title, re.IGNORECASE) for kw in HIGH_IMPACT_KEYWORDS)
                
                if is_high_impact:
                    news_alerts.append(title)
                    if len(news_alerts) >= 5: 
                        break
    except Exception as e:
        logging.error(f"Error RSS: {e}")
    return news_alerts

def gpt_advanced_geopolitics(news_list, manual=False):
    if not news_list or not OPENAI_API_KEY: return None
    client = OpenAI(api_key=OPENAI_API_KEY)
    
    news_text = "\n".join([f"- {n}" for n in news_list])
    if manual:
        prompt = (
            f"Titulares globales de hoy:\n{news_text}\n"
            "Haz un resumen global breve de la situación y dime qué eventos podrían mover el mercado hoy.\n"
            "RESPONDE ESTRICTA Y ÚNICAMENTE EN ESPAÑOL."
        )
    else:
         prompt = (
            f"Titulares recientes:\n{news_text}\n"
             "Analiza si alguna de estas noticias es de nivel CRÍTICO ('Alto Impacto') (ej: declaraciones de Trump, inicio de guerras severas, cambios fuertes en tasas de interés).\n"
             "Si NO hay nada de extrema urgencia sistémica, devuelve exclusivamente la palabra 'TRANQUILIDAD'.\n"
             "Si SÍ la hay, responde usando exactamente este formato:\n"
             "⚠️ ALERTA URGENTE: [Resumen de la noticia] - Impacto potencial en [Acción/Sector].\n"
             "RESPONDE ESTRICTA Y ÚNICAMENTE EN ESPAÑOL."
         )
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=300
        )
        res = response.choices[0].message.content.strip()
        if not manual and ("TRANQUILIDAD" in res.upper() and len(res) < 20):
             return None
        return res
    except:
        return None

def perform_deep_analysis(ticker):
    ticker = ticker.upper()
    if ticker == "BTC": ticker = "BTC-USD"
    
    tech = fetch_and_analyze_stock(ticker)
    tech_info = f"Información técnica no disponible para {ticker}."
    if tech:
        tech_info = f"Precio: ${tech['price']:.2f}\nRSI: {tech['rsi']:.2f}\nMACD Line: {tech['macd_line']:.2f}\nMACD Signal: {tech['macd_signal']:.2f}"
        
    news_str = ""
    try:
        stock = yf.Ticker(ticker)
        news_list = stock.news[:3]
        if news_list:
             news_str = "\n".join([f"- {n.get('title', '')}" for n in news_list])
    except:
        pass
        
    prompt = (
        f"Analiza profundamente el activo '{ticker}'. Extrae información técnica y fundamental.\n"
        f"Datos Técnicos:\n{tech_info}\n\n"
        f"Noticias de Yahoo Finance:\n{news_str if news_str else 'Sin noticias.'}\n\n"
        "Comportate como un analista institucional. Combina ambos enfoques. Al final, debes dictaminar un VEREDICTO FINAL resaltado en negritas, obligatoriamente eligiendo una de estas tres opciones: 'COMPRAR', 'VENDER' o 'MANTENER/ESPERAR'.\n"
        "RESPONDE ESTRICTA Y ÚNICAMENTE EN ESPAÑOL."
    )
    if not OPENAI_API_KEY: return "Error: API no configurada."
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=600
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"Fallo al analizar: {e}"

def generate_whale_alert(anomaly_data):
    """Convierte métricas volumétricas reales a un Alert de Ballenas simulado vía IA"""
    if not OPENAI_API_KEY: 
        return f"La ballena detectada movió volumen masivo de {anomaly_data['ticker']}."
    
    prompt = (
        f"Actúa como un sistema de 'Whale Alert'. Hemos detectado un volumen masivo anormal ({anomaly_data['vol']}x promedio) en el activo {anomaly_data['ticker']}. "
        f"Su tendencia estructural arroja: {anomaly_data['trend']}.\n"
        "Escribe estrictamente UNA LÍNEA rellenando el siguiente formato ficticio pero altamente realista de una ballena u order block: "
        "'La ballena [Wallet/Identificador] movió [Cantidad] de [Activo] hacia [Exchange/Wallet Destino].'\n"
        "Asegurate de que [Cantidad] sea un número creíble de millones de USD o Acciones operadas en dark pools o block trades. "
        "RESPONDE ESTRICTA Y ÚNICAMENTE EN ESPAÑOL."
    )
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        response = client.chat.completions.create(model="gpt-4o", messages=[{"role": "user", "content": prompt}], max_tokens=150)
        return response.choices[0].message.content
    except:
        return f"La ballena Institucional movió fondos masivos de {anomaly_data['ticker']} en una Dark Pool."

def fetch_and_analyze_stock(ticker):
    try:
        data = yf.download(ticker, period="6mo", interval="1d", progress=False)
        if data.empty: return None
        if isinstance(data.columns, pd.MultiIndex):
            data = data.copy()
            data.columns = data.columns.get_level_values(0)
            
        close_prices = data['Close']
        volume = data['Volume']
        if isinstance(close_prices, pd.DataFrame): 
             close_prices = close_prices.iloc[:, 0]
             volume = volume.iloc[:, 0]
             
        # RSI y MACD
        delta = close_prices.diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)
        ema_up = up.ewm(com=13, adjust=False).mean()
        ema_down = down.ewm(com=13, adjust=False).mean()
        rs = ema_up / ema_down
        rsi_series = 100 - (100 / (1 + rs))
        rsi_series[ema_down == 0] = 100 
        
        macd_line = close_prices.ewm(span=12, adjust=False).mean() - close_prices.ewm(span=26, adjust=False).mean()
        macd_signal = macd_line.ewm(span=9, adjust=False).mean()
        
        latest_price = float(close_prices.iloc[-1])
        latest_rsi = float(rsi_series.iloc[-1])
        latest_macd = float(macd_line.iloc[-1])
        latest_signal = float(macd_signal.iloc[-1])
        
        divergence = False
        if len(close_prices) > 30:
             recent_window = close_prices.iloc[-10:]
             prev_window = close_prices.iloc[-25:-10]
             if float(recent_window.min()) < float(prev_window.min()) and float(rsi_series.loc[recent_window.idxmin()]) > float(rsi_series.loc[prev_window.idxmin()]):
                 divergence = True
                 
        # SMC y VOLUMEN
        smc_trend = "Alcista 🟢" if latest_price > close_prices.ewm(span=20).mean().iloc[-1] else "Bajista 🔴"
        recent_month = close_prices.iloc[-22:]
        smc_sup = float(recent_month.min())
        smc_res = float(recent_month.max())
        
        # Extracción del 'Order Block' por pico de Volumen
        vol_month = volume.iloc[-22:]
        max_vol_date = vol_month.idxmax()
        order_block_price = float(close_prices.loc[max_vol_date])
        
        latest_vol = float(volume.iloc[-1])
        avg_vol = float(vol_month.mean())
        
        return {
            'ticker': ticker, 'price': latest_price, 'rsi': latest_rsi,
            'macd_line': latest_macd, 'macd_signal': latest_signal, 'bullish_divergence': divergence,
            'smc_sup': smc_sup, 'smc_res': smc_res, 'smc_trend': smc_trend,
            'order_block': order_block_price, 'latest_volume': latest_vol, 'avg_volume': avg_vol
        }
    except Exception as e:
        return None

def build_full_report():
    report_lines = ["🦅 <b>Génesis: SMC / Mi Cartera</b> 🦅\n"]
    tickers = get_tracked_tickers()
    
    if not tickers:
        return "Tu cartera está vacía. Añade activos usando 'Agrega AAPL'."
    
    for ticker in tickers:
        analysis = fetch_and_analyze_stock(ticker)
        if analysis:
            t, rsi, price = analysis['ticker'], analysis['rsi'], analysis['price']
            sup, res, trend = analysis['smc_sup'], analysis['smc_res'], analysis['smc_trend']
            block = analysis['order_block']
            
            report_lines.append(f"🏦 <b>{t}</b> - Cotización: ${price:.2f}")
            report_lines.append(f"• <b>Tendencia SMC:</b> {trend}")
            report_lines.append(f"• <b>Liquidity Soportes Institucionales:</b> ${sup:.2f}")
            report_lines.append(f"• <b>Liquidity Resistencias (Sell-side):</b> ${res:.2f}")
            report_lines.append(f"• <b>Order Block Institucional (High Vol):</b> ${block:.2f}")
            report_lines.append(f"• <b>RSI Algorítmico:</b> {rsi:.2f}\n")
            
    return "\n".join(report_lines)

# ----------------- CONTROLADORES TELEBOT -----------------
@bot.message_handler(commands=['start'])
def cmd_start(message):
    if str(message.chat.id) != str(CHAT_ID): return
    
    markup = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        KeyboardButton("🌎 Geopolítica"),
        KeyboardButton("🐳 Radar Ballenas"),
        KeyboardButton("📉 SMC / Mi Cartera")
    )
    
    bot.reply_to(message, "¡Génesis Institucional! Pídeme 'Analiza NVDA', 'Agrega AAPL', 'Elimina MSFT', o usa el tablero:", reply_markup=markup)

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    if str(message.chat.id) != str(CHAT_ID): return
    
    msg = bot.reply_to(message, "👁️ Analizando estructura visual...")
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        base64_image = base64.b64encode(downloaded_file).decode('utf-8')
        
        if not OPENAI_API_KEY:
            raise ValueError("Falta OPENAI_API_KEY")
            
        client = OpenAI(api_key=OPENAI_API_KEY)
        prompt = (
            "Actúa como un analista Senior de Wall Street. Analiza esta gráfica técnica con rigor.\n"
            "1. Identifica la estructura del precio (Higher Highs / Lower Lows).\n"
            "2. Localiza niveles exactos de Soporte y Resistencia visuales.\n"
            "3. Busca patrones de velas (Doji, Engulfing, Hammer) si son visibles.\n"
            "4. Si ves indicadores, menciona el estado del RSI o MACD.\n"
            "5. Dame un Veredicto: 'Bullish', 'Bearish' o 'Neutral' y sugiere un Stop Loss lógico basado en la estructura.\n"
            "RESPONDE ESTRICTA Y ÚNICAMENTE EN ESPAÑOL."
        )
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "user", "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
                ]}
            ],
            max_tokens=800
        )
        analysis_text = response.choices[0].message.content
        bot.edit_message_text(f"📊 [REPORTE VISUAL GPT-4o]\n\n{analysis_text}", chat_id=message.chat.id, message_id=msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Falló el análisis GPT-4o: {e}", chat_id=message.chat.id, message_id=msg.message_id)

@bot.message_handler(func=lambda message: True, content_types=['text'])
def handle_text(message):
    if str(message.chat.id) != str(CHAT_ID): return
    text = message.text.strip()
    
    # 1. BOTONES INFERIORES:
    if text == "🐳 Radar Ballenas":
        bot.reply_to(message, "🐳 Escaneando flujos macroinstitucionales de tu cartera...")
        
        highest_vol = 0
        target = None
        for tk in get_tracked_tickers():
             val = fetch_and_analyze_stock(tk)
             if val and val['avg_volume'] > 0:
                 spike = val['latest_volume'] / val['avg_volume']
                 if spike > highest_vol:
                     highest_vol = spike
                     target = val
                     
        if not target or highest_vol < 1.2:
            bot.send_message(message.chat.id, "🐋 <b>Radar Ballenas</b>\nVolumen institucional operando en promedios estáticos, cero anomalías detectadas en dark pools.", parse_mode="HTML")
            return
            
        whale_msg = generate_whale_alert({"ticker": target['ticker'], "vol": round(highest_vol, 2), "trend": target['smc_trend']})
        bot.send_message(message.chat.id, f"🐋 <b>Radar Institucional</b>\n\n{whale_msg}", parse_mode="HTML")
        
    elif text == "🌎 Geopolítica":
        bot.reply_to(message, "🌎 Escaneando red global RSS y pasando IA... (Manual)")
        news = check_geopolitical_news()
        ai_res = gpt_advanced_geopolitics(news, manual=True)
        if ai_res:
            bot.send_message(message.chat.id, f"🌍 <b>Resumen Global Insight:</b>\n\n{ai_res}", parse_mode="HTML")
        else:
            bot.send_message(message.chat.id, "✅ Radar limpio. Operatividad normal.", parse_mode="HTML")
            
    elif text == "📉 SMC / Mi Cartera":
        bot.reply_to(message, "📉 Computando Mapas de Liquidez Institucional SMC...")
        report = build_full_report()
        bot.send_message(message.chat.id, report, parse_mode="HTML")
        
    # 2. TAREAS PROFUNDAS MANUALES
    elif text.upper().startswith("ANALIZA "):
        match = re.search(r'ANALIZA\s+([A-Za-z0-9\-]+)', text.upper())
        if match:
            tk = match.group(1)
            bot.reply_to(message, f"🔍 Bajando data técnica y de prensa para iniciar Análisis Fundamental/Técnico de {tk}...")
            deep_report = perform_deep_analysis(tk)
            bot.send_message(message.chat.id, f"🏦 <b>RESEARCH: {tk}</b>\n\n{deep_report}", parse_mode="HTML")
            
    # 3. GESTIÓN DINÁMICA
    elif "AGREGA" in text.upper():
        match = re.search(r'AGREGA\s+([A-Za-z0-9\-]+)', text.upper())
        if match:
            tk = match.group(1)
            if add_ticker(tk): bot.reply_to(message, f"✅ El activo <b>{tk}</b> ha sido blindado en el Radar SMC general.", parse_mode="HTML")
            else: bot.reply_to(message, f"⚠️ El activo {tk} ya está protegido en tu portafolio.", parse_mode="HTML")
            
    elif "ELIMINA" in text.upper() or "BORRA" in text.upper():
        match = re.search(r'(?:ELIMINA|BORRA)\s+([A-Za-z0-9\-]+)', text.upper())
        if match:
            tk = match.group(1)
            if remove_ticker(tk): bot.reply_to(message, f"🗑️ El activo <b>{tk}</b> fue destrozado y vaciado del Radar.", parse_mode="HTML")
            else: bot.reply_to(message, f"⚠️ El activo {tk} no residía en el Radar SMC.", parse_mode="HTML")

# ----------------- BUCLE PROACTIVO (CADA 5 MINUTOS) -----------------
def background_loop_proactivo():
    """TICK DOBLADO: Monitorea volumen de ballenas cada 5m y Geopolítica cada 10m (2 ticks)."""
    tick_count = 0
    while True:
        try:
            time.sleep(300) # 5 minutos exactos de latencia
            tick_count += 1
            now = datetime.now()
            
            # --- TAREA 1: 5-MINUTOS RASTREO VOLUMEN (Umbral 2.5x) ---
            for tk in get_tracked_tickers():
                val = fetch_and_analyze_stock(tk)
                if val and val['avg_volume'] > 0:
                    spike = val['latest_volume'] / val['avg_volume']
                    
                    if spike >= 2.5: # 250% del volumen normal (Gran Institucional)
                        # Evitar spam: Notificar anomalias de un ticker máximo 1 vez cada 4 horas
                        last_alert = ALERTED_VOLUMES.get(tk)
                        if not last_alert or (now - last_alert).total_seconds() > 14400:
                            ALERTED_VOLUMES[tk] = now
                            bot.send_message(CHAT_ID, f"⚠️ <b>DETECTADA OPERACIÓN GRANDE</b>:\n\nSe acaba de mover un bloque masivo de <b>{tk}</b> ({round(spike, 1)}x). Posible movimiento institucional activo.\n<i>Order Block SMC detectado en: ${val['order_block']:.2f}.</i>", parse_mode="HTML")

            # --- TAREA 2: 10-MINUTOS GEOPOLÍTICA MACRO (Solo en ticks pares) ---
            if tick_count % 2 == 0:
                raw_news = check_geopolitical_news()
                ai_threat_evaluation = gpt_advanced_geopolitics(raw_news, manual=False)
                if ai_threat_evaluation:
                     bot.send_message(CHAT_ID, f"🚨 <b>VIGILANCIA GLOBAL (10-min scan):</b>\n\n{ai_threat_evaluation}", parse_mode="HTML")
                     
        except Exception as e:
            logging.error(f"Error en bucle asíncrono (5min/10min): {e}")

# ----------------- MAIN -----------------
def main():
    print(f"Iniciando Bot Gestor Cuantitativo Híbrido. Token leído y autorizado...")
    
    t = threading.Thread(target=background_loop_proactivo, daemon=True)
    t.start()
    
    bot.infinity_polling(timeout=10, long_polling_timeout=5)

if __name__ == "__main__":
    main()
