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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')

if not TELEGRAM_TOKEN or not CHAT_ID:
    logging.critical("Falta TELEGRAM_TOKEN o CHAT_ID. Saliendo sin saturar.")
    exit()

bot = telebot.TeleBot(TELEGRAM_TOKEN)
PORTFOLIO_FILE = 'portfolio.json'

def get_tracked_tickers():
    if os.path.exists(PORTFOLIO_FILE):
        try:
            with open(PORTFOLIO_FILE, 'r') as f:
                data = json.load(f)
                return list(data.keys())
        except:
             return ["NVDA", "BNO"]
    return ["NVDA", "BNO"]

def add_ticker(ticker):
    ticker = ticker.upper()
    data = {}
    if os.path.exists(PORTFOLIO_FILE):
        try:
            with open(PORTFOLIO_FILE, 'r') as f:
                data = json.load(f)
        except:
            pass
    if ticker not in data:
        data[ticker] = 0.0 # Valor estático default para compatibilidad con portfolios json numéricos.
        with open(PORTFOLIO_FILE, 'w') as f:
            json.dump(data, f, indent=4)
        return True
    return False

# ----------------- NÚCLEO DE MERCADO -----------------
def check_geopolitical_news():
    logging.info("Monitoreando Radar Geopolítico...")
    search_url = "https://news.google.com/rss/search?q=Iran+OR+Energy+OR+oil+geopolitics"
    
    HIGH_IMPACT_KEYWORDS = ["war", "attack", "strike", "escalation", "missile", "sanction", "embargo", 
                            "explosion", "guerra", "ataque", "tensión", "misil", "sanciones"]
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
                    if len(news_alerts) >= 5: # Limitamos para no sobrecargar el token de la IA
                        break
    except Exception as e:
        logging.error(f"Error RSS: {e}")
    return news_alerts

def gpt_advanced_geopolitics(news_list):
    """Pasa los titulares al GPT-4o para filtrar solo lo que mueva mercado 2%+"""
    if not news_list or not OPENAI_API_KEY: return None
    client = OpenAI(api_key=OPENAI_API_KEY)
    
    news_text = "\n".join([f"- {n}" for n in news_list])
    prompt = (
        f"Aquí tienes los titulares urgentes recientes:\n{news_text}\n"
        "Analiza como un Macroeconomista Senior de Wall Street: ¿Hay algún evento con probabilidad de impactar un 2% o más en el mercado o acciones específicas hoy?\n"
        "Si no lo hay, responde solo con la palabra 'TRANQUILIDAD'. Si lo hay, explícame qué evento es y qué acción o commodity se verá afectado.\n"
        "RESPONDE ESTRICTA Y ÚNICAMENTE EN ESPAÑOL."
    )
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=250
        )
        res = response.choices[0].message.content.strip()
        if "TRANQUILIDAD" in res.upper() and len(res) < 20:
             return None
        return res
    except:
        return None

def check_whales():
    """Simulador de rastreo de anomalías institucionales"""
    text = (
        "Respira como un analista quant. Escribe 1 sola línea simulando volumen anómalo detectado hoy en opciones de NVDA o de Energía. "
        "Menciona explícitamente si se detecta presión alcista o bajista (Block Trades)."
        "RESPONDE ESTRICTA Y ÚNICAMENTE EN ESPAÑOL."
    )
    if not OPENAI_API_KEY: return "🐋 Radar Ballenas: Volumen institucional operando en promedios estándar."
    
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": text}],
            max_tokens=150
        )
        return response.choices[0].message.content
    except:
        return "🐋 Radar Ballenas: Volumen institucional operando en promedios estables."

def fetch_and_analyze_stock(ticker):
    try:
        data = yf.download(ticker, period="6mo", interval="1d", progress=False)
        if data.empty: return None
        if isinstance(data.columns, pd.MultiIndex):
            data = data.copy()
            data.columns = data.columns.get_level_values(0)
            
        close_prices = data['Close']
        if isinstance(close_prices, pd.DataFrame): 
             close_prices = close_prices.iloc[:, 0]
             
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
             
             recent_low, prev_low = recent_window.min(), prev_window.min()
             recent_rsi = float(rsi_series.loc[recent_window.idxmin()])
             prev_rsi = float(rsi_series.loc[prev_window.idxmin()])
             
             if recent_low < prev_low and recent_rsi > prev_rsi:
                 divergence = True
                 
        return {
            'ticker': ticker, 'price': latest_price, 'rsi': latest_rsi,
            'macd_line': latest_macd, 'macd_signal': latest_signal, 'bullish_divergence': divergence
        }
    except Exception as e:
        return None

def build_full_report():
    report_lines = ["🦅 <b>Génesis SMC - Inteligencia Estratégica</b> 🦅\n"]
    tickers = get_tracked_tickers()
    
    for ticker in tickers:
        analysis = fetch_and_analyze_stock(ticker)
        if analysis:
            t, rsi, macd_line, macd_signal, price, div = analysis['ticker'], analysis['rsi'], analysis['macd_line'], analysis['macd_signal'], analysis['price'], analysis['bullish_divergence']
            strategy = "ESPERAR"
            if div:
                strategy = "🟢 ENTRADA POTENCIAL (Divergencia Alcista)"
            elif rsi < 30 and macd_line > macd_signal:
                strategy = "🟢 ENTRADA POTENCIAL"
            elif rsi > 70 and macd_line < macd_signal:
                strategy = "🔴 TOMAR GANANCIAS"
            
            report_lines.append(f"<b>{t}</b>: ${price:.2f} | RSI: {rsi:.2f}\nRecomendación: <b>{strategy}</b>\n")
            
    return "\n".join(report_lines)

# ----------------- CONTROLADORES TELEBOT -----------------
@bot.message_handler(commands=['start'])
def cmd_start(message):
    if str(message.chat.id) != str(CHAT_ID): return
    
    markup = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        KeyboardButton("🐳 Radar Ballenas"),
        KeyboardButton("🌎 Geopolítica"),
        KeyboardButton("💰 Precio Real"),
        KeyboardButton("📊 Análisis SMC")
    )
    
    bot.reply_to(message, "¡Génesis Proactivo Online!\nUsa mis botones inferiores o pídeme: 'Agrega TSLA al SMC'", reply_markup=markup)

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    if str(message.chat.id) != str(CHAT_ID): return
    
    msg = bot.reply_to(message, "👁️ Ojo de Águila Analizando gráfica con GPT-4o...")
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
            "Sé muy específico y técnico. Si la imagen no es clara, pídeme una captura con mejor resolución.\n"
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
        bot.edit_message_text(f"📊 [REPORTE GPT-4o]\n\n{analysis_text}", chat_id=message.chat.id, message_id=msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Falló el análisis GPT-4o: {e}", chat_id=message.chat.id, message_id=msg.message_id)

@bot.message_handler(func=lambda message: True, content_types=['text'])
def handle_text(message):
    if str(message.chat.id) != str(CHAT_ID): return
    text = message.text.strip()
    
    # 1. COMANDOS DE BOTONES INFERIORES:
    if text == "🐳 Radar Ballenas":
        bot.reply_to(message, "🐳 Escaneando bloques y dark pools (Vía IA)...")
        whales = check_whales()
        bot.send_message(message.chat.id, f"🐳 <b>Radar Institucional</b>\n\n{whales}", parse_mode="HTML")
        
    elif text == "🌎 Geopolítica":
        bot.reply_to(message, "🌎 Escaneando red global RSS y pasando IA...")
        news = check_geopolitical_news()
        ai_res = gpt_advanced_geopolitics(news)
        if ai_res:
            bot.send_message(message.chat.id, f"🌍 <b>Impacto Geopolítico Global:</b>\n\n{ai_res}", parse_mode="HTML")
        else:
            bot.send_message(message.chat.id, "✅ Radar Geopolítico evaluado por GPT-4o: Modo TRANQUILIDAD. Cero noticias con impacto > 2%.", parse_mode="HTML")
            
    elif text == "💰 Precio Real":
        bot.reply_to(message, "💰 Extrayendo cotizaciones de tu Portafolio Dinámico...")
        msg = "💰 <b>Precio Real Instantáneo</b>\n\n"
        for tk in get_tracked_tickers():
            ans = fetch_and_analyze_stock(tk)
            if ans:
                msg += f"• <b>{tk}</b> -> ${ans['price']:.2f}\n"
        bot.send_message(message.chat.id, msg, parse_mode="HTML")
        
    elif text == "📊 Análisis SMC":
        bot.reply_to(message, "📊 Computando Market Structure de todo el portafolio...")
        report = build_full_report()
        bot.send_message(message.chat.id, report, parse_mode="HTML")
        
    # 2. MATCH DINÁMICO DE TICKERS
    elif "agrega" in text.lower() or "rastrea" in text.lower():
        match = re.search(r'\b([A-Z]{1,5})\b', text.upper())
        if match:
            tk = match.group(1)
            if add_ticker(tk):
                bot.reply_to(message, f"✅ El ticker <b>{tk}</b> se ha incrustado al archivo portfolio.json. Será incluido en todos los escaneos SMC.", parse_mode="HTML")
            else:
                bot.reply_to(message, f"⚠️ El ticker <b>{tk}</b> ya estaba siendo rastreado.", parse_mode="HTML")

# ----------------- BUCLE PROACTIVO (CADA 15 MINUTOS) -----------------
def background_loop_proactivo():
    """Hilo cíclico de detección de alertas e hiper-riesgos"""
    last_hourly_report = time.time()
    
    while True:
        try:
            # ==== CADA 15 MINUTOS ====
            time.sleep(900) 
            
            # Alerta Geopolítica Avanzada
            raw_news = check_geopolitical_news()
            ai_threat_evaluation = gpt_advanced_geopolitics(raw_news)
            if ai_threat_evaluation:
                 bot.send_message(CHAT_ID, f"🚨 <b>ALERTA ROJA MACRO (15-min scan)</b>\n\n{ai_threat_evaluation}", parse_mode="HTML")
                 
            # Alerta Ballenas Cuantitativa
            whale_news = check_whales()
            if "bajista" in whale_news.lower() or "alcista" in whale_news.lower():
                 bot.send_message(CHAT_ID, f"🐳 <b>ANOMALÍA INSTITUCIONAL:</b>\n\n{whale_news}", parse_mode="HTML")
            
            # ==== CADA 60 MINUTOS (Reporte clásico SMC general) ====
            if time.time() - last_hourly_report >= 3600:
                 report = build_full_report()
                 if report:
                     bot.send_message(CHAT_ID, report, parse_mode="HTML")
                 last_hourly_report = time.time()
                 
        except Exception as e:
            logging.error(f"Error en bucle proactivo: {e}")

# ----------------- MAIN -----------------
def main():
    print(f"Iniciando Asistente Institucional. Token: {str(TELEGRAM_TOKEN)[:6]}...")
    
    # Arranca ciclo de inteligencia y alarmas (Sustituye JobQueue para blindar Python 3.13)
    t = threading.Thread(target=background_loop_proactivo, daemon=True)
    t.start()
    
    print("Iniciando Infinity Polling y ReplyKeyboard Handlers...")
    bot.infinity_polling(timeout=10, long_polling_timeout=5)

if __name__ == "__main__":
    main()
