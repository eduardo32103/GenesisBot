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

# --- LIBRERÍA TELEBOT (TOTALMENTE SYNCRONA, EVITA BUGS DE ASYNCIO EN PY 3.13) ---

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')

# Validar en arranque
if not TELEGRAM_TOKEN or not CHAT_ID:
    logging.critical("Falta TELEGRAM_TOKEN o CHAT_ID. Saliendo sin saturar.")
    exit()

bot = telebot.TeleBot(TELEGRAM_TOKEN)
ALERTED_NEWS = set()

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
                link = item.find('link').text
                
                if link in ALERTED_NEWS:
                    continue
                    
                is_high_impact = any(re.search(rf"\b{kw}\b", title, re.IGNORECASE) for kw in HIGH_IMPACT_KEYWORDS)
                
                if is_high_impact:
                    news_alerts.append(title)
                    ALERTED_NEWS.add(link)
                    if len(news_alerts) >= 1:
                        break
    except Exception as e:
        logging.error(f"Error RSS: {e}")
    return news_alerts

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

def generate_strategic_report(analysis):
    if not analysis: return ""
    ticker, rsi, macd_line, macd_signal, price, div = analysis['ticker'], analysis['rsi'], analysis['macd_line'], analysis['macd_signal'], analysis['price'], analysis['bullish_divergence']
    
    strategy = "ESPERAR"
    opportunity = ""
    if div:
        strategy = "🟢 ENTRADA POTENCIAL"
        opportunity = "⚠️ ALERTA: Divergencia Alcista Detectada!"
    elif rsi < 30 and macd_line > macd_signal:
        strategy = "🟢 ENTRADA POTENCIAL"
    elif rsi > 70 and macd_line < macd_signal:
        strategy = "🔴 TOMAR GANANCIAS"

    report = f"<b>{ticker}</b>: ${price:.2f} | RSI: {rsi:.2f}\nRecomendación: <b>{strategy}</b>"
    if opportunity: report += f"\n<i>{opportunity}</i>"
    return report

def build_full_report():
    report_lines = ["🦅 <b>Génesis 2.0 - Inteligencia Estratégica</b> 🦅\n"]
    has_data = False
    
    for ticker in ["NVDA", "BNO"]:
        analysis = fetch_and_analyze_stock(ticker)
        if analysis:
            report_lines.append(generate_strategic_report(analysis) + "\n")
            has_data = True
            
    news = check_geopolitical_news()
    if news:
        report_lines.append("🌍 <b>Riesgo Geopolítico Inminente detectado:</b>")
        for n in news: report_lines.append(f"▪️ {n}")
        has_data = True
        
    return "\n".join(report_lines) if has_data else ""

# ----------------- CONTROLADORES TELEBOT (SYNCRONOS) -----------------
@bot.message_handler(commands=['start'])
def cmd_start(message):
    if str(message.chat.id) != str(CHAT_ID): return
    bot.reply_to(message, "¡Génesis V2.0 Online! Mándame una gráfica para analizar.")

@bot.message_handler(commands=['analisis'])
def cmd_analisis(message):
    if str(message.chat.id) != str(CHAT_ID): return
    bot.reply_to(message, "🔍 Computando métricas globales...")
    report = build_full_report()
    if report:
        bot.send_message(message.chat.id, report, parse_mode="HTML")

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
            "Sé muy específico y técnico. Si la imagen no es clara, pídeme una captura con mejor resolución."
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

# ----------------- BUCLE EN SEGUNDO PLANO -----------------
def background_loop():
    """Bucle horario asegurado."""
    while True:
        try:
            report = build_full_report()
            if report:
                bot.send_message(CHAT_ID, report, parse_mode="HTML")
        except Exception as e:
            logging.error(f"Error en loop: {e}")
            
        time.sleep(3600)  # Duerme una hora

# ----------------- MAIN -----------------
def main():
    print(f"Iniciando Bot Telebot Syncrono. Token: {str(TELEGRAM_TOKEN)[:6]}...")
    
    # Hilo de monitoreo que inicia SOLO UNA VEZ.
    t = threading.Thread(target=background_loop, daemon=True)
    t.start()
    
    # Arranca el servidor de telegram seguro
    print("Iniciando Infinity Polling...")
    bot.infinity_polling(timeout=10, long_polling_timeout=5)

if __name__ == "__main__":
    main()
