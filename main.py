import logging
import base64
import requests
import re
import xml.etree.ElementTree as ET
import pandas as pd
import yfinance as yf
from openai import OpenAI
import os

# Imports estables de python-telegram-bot v20.8
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CONFIGURACIONES ESTRATÉGICAS (ENV VARIABLES) ---
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')

ALERTED_NEWS = set()

# ----------------- NÚCLEO DE MERCADO -----------------
def check_geopolitical_news():
    logging.info("Monitoreando Radar Geopolítico de Alto Impacto...")
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
        logging.error(f"Error obteniendo noticias RSS: {e}")
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
        logging.error(f"Error analizando {ticker}: {e}")
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

# ----------------- CONTROLADORES DE TELEGRAM -----------------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando manual de inicio"""
    if str(update.message.chat_id) != str(CHAT_ID): return
    await update.message.reply_text("¡Génesis V2.0 Online! Mándame una gráfica para analizar.")

async def cmd_analisis(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.message.chat_id) != str(CHAT_ID): return
    await update.message.reply_text("🔍 Computando métricas globales...")
    report = build_full_report()
    if report:
        await update.message.reply_text(report, parse_mode="HTML")

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Integración Segura de OpenAI GPT-4o con manejo de errores profundos"""
    if str(update.message.chat_id) != str(CHAT_ID): return
        
    await update.message.reply_text("👁️ Ojo de Águila Analizando gráfica con GPT-4o...")
    
    try:
        photo_file = update.message.photo[-1]
        file = await context.bot.get_file(photo_file.file_id)
        image_bytes = await file.download_as_bytearray()
        
        base64_image = base64.b64encode(image_bytes).decode('utf-8')
        
        if not OPENAI_API_KEY:
            raise ValueError("La variable de entorno OPENAI_API_KEY no se cargó correctamente en Railway.")
            
        client = OpenAI(api_key=OPENAI_API_KEY)
        
        prompt = (
            "Eres un Senior Trader cuantitativo. Analiza de inmediato esta gráfica y responde de forma estricta:\n"
            "1. Tendencia general.\n"
            "2. Zonas de Soportes y Resistencias críticas.\n"
            "3. Divergencias visibles.\n"
            "4. Veredicto de Riesgo/Beneficio.\n"
            "No asumas datos, describe únicamente lo que el gráfico muestra analíticamente."
        )
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{base64_image}"
                            }
                        }
                    ]
                }
            ],
            max_tokens=800
        )
        
        analysis_text = response.choices[0].message.content
        await update.message.reply_text(f"📊 [REPORTE GPT-4o VISION]\n\n{analysis_text}")
        
    except Exception as e:
        logging.error("Crash absoluto procesando imagen IA:", exc_info=True)
        await update.message.reply_text(f"❌ Falló el análisis de OpenAI. Motivo del servidor:\n`{str(e)}`", parse_mode="Markdown")

# ----------------- TAREAS SCHEDULED Y POST_INIT -----------------

async def routine_hourly_report(context: ContextTypes.DEFAULT_TYPE):
    report = build_full_report()
    if report:
         await context.bot.send_message(chat_id=CHAT_ID, text=report, parse_mode="HTML")

async def post_init(application: Application):
    """Callback de arranque: Notifica + Envía el reporte inicial"""
    try:
        logging.info("Enviando mensaje de arranque a Telegram...")
        
        await application.bot.send_message(
            chat_id=CHAT_ID, 
            text="🚀 <b>¡Génesis V2.0 ONLINE EN SERVIDOR!</b>",
            parse_mode="HTML"
        )
        
        report = build_full_report()
        if report:
             await application.bot.send_message(chat_id=CHAT_ID, text=report, parse_mode="HTML")
             
    except Exception as e:
         logging.error(f"Error despachando mensajes de post_init: {e}")

    application.job_queue.run_repeating(routine_hourly_report, interval=3600, first=3600)

# ----------------- INICIO -----------------
def main():
    print("Iniciando validación de dependencias...")
    
    if not TELEGRAM_TOKEN:
        logging.critical("FALTA TELEGRAM_TOKEN en tus Variables de Entorno. Apagando...")
        return
        
    print(f"Token cargado en Railway: {TELEGRAM_TOKEN[:5]}...***")
    print("Bot intentando conectar con Telegram...")
    
    app = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("analisis", cmd_analisis))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    
    logging.info("Polling infinito en Railway iniciado...")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    main()
