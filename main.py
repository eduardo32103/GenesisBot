import logging
import yfinance as yf
import pandas as pd
import re
import xml.etree.ElementTree as ET
import requests
import base64
from openai import OpenAI

# Librerías asíncronas de Telegram
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes, Application

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CONFIGURACIONES ESTRATÉGICAS ---
TELEGRAM_TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
TELEGRAM_CHAT_ID = "5426620320"
OPENAI_API_KEY = "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"

# Inicializar Cliente OpenAI
client = OpenAI(api_key=OPENAI_API_KEY)

# Memoria local para evitar spam de geopolítica
ALERTED_NEWS = set()

# ----------------- NÚCLEO DE MERCADO -----------------

def check_geopolitical_news():
    """Filtra noticias de interés geopolítico."""
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
                    if len(news_alerts) >= 2: break
    except Exception as e:
        logging.error(f"Error obteniendo noticias RSS: {e}")
    return news_alerts

def fetch_and_analyze_stock(ticker):
    """Análisis estricto manual del RSI y MACD, buscando divergencias."""
    try:
        data = yf.download(ticker, period="6mo", interval="1d", progress=False)
        if data.empty: return None
        if isinstance(data.columns, pd.MultiIndex):
            data = data.copy()
            data.columns = data.columns.get_level_values(0)
            
        close_prices = data['Close']
        if isinstance(close_prices, pd.DataFrame): 
             close_prices = close_prices.iloc[:, 0]
             
        # RSI 14
        delta = close_prices.diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)
        ema_up = up.ewm(com=13, adjust=False).mean()
        ema_down = down.ewm(com=13, adjust=False).mean()
        rs = ema_up / ema_down
        rsi_series = 100 - (100 / (1 + rs))
        rsi_series[ema_down == 0] = 100 
        
        # MACD
        macd_line = close_prices.ewm(span=12, adjust=False).mean() - close_prices.ewm(span=26, adjust=False).mean()
        macd_signal = macd_line.ewm(span=9, adjust=False).mean()
        
        latest_price = float(close_prices.iloc[-1])
        latest_rsi = float(rsi_series.iloc[-1])
        latest_macd = float(macd_line.iloc[-1])
        latest_signal = float(macd_signal.iloc[-1])
        
        # Driver de Divergencias
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
    """Retorna la estrategia basada en la estructura del mercado."""
    if not analysis: return ""
    ticker, rsi, macd_line, macd_signal, price, div = analysis['ticker'], analysis['rsi'], analysis['macd_line'], analysis['macd_signal'], analysis['price'], analysis['bullish_divergence']
    
    strategy = "ESPERAR"
    opportunity = ""
    if div:
        strategy = "🟢 ENTRADA POTENCIAL"
        opportunity = "⚠️ ALERTA DE OPORTUNIDAD: ¡Divergencia Alcista Detectada!"
    elif rsi < 30 and macd_line > macd_signal:
        strategy = "🟢 ENTRADA POTENCIAL"
    elif rsi > 70 and macd_line < macd_signal:
        strategy = "🔴 TOMAR GANANCIAS"

    report = f"<b>{ticker}</b>: ${price:.2f} | RSI: {rsi:.2f}\nRecomendación: <b>{strategy}</b>"
    if opportunity: report += f"\n<i>{opportunity}</i>"
    return report

def build_full_report():
    report_lines = ["🦅 <b>Génesis 1.0 - Estado Actual de Inteligencia</b> 🦅\n"]
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

# ----------------- CONTROLADORES (HANDLERS) DE TELEGRAM -----------------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.message.chat_id) != TELEGRAM_CHAT_ID: return
    await update.message.reply_text("🤖 ¡Bienvenido Eduardo! \nEl módulo 🦅 Águila Génesis 1.0 está **EN LÍNEA**.\n\n👁️ Motor de visión **GPT-4o (OpenAI)** activo y conectado. Mándame cualquier foto de una gráfica de mercado y recibirás un diagnóstico exhaustivo de niveles y riesgo.", parse_mode="Markdown")

async def cmd_analisis(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.message.chat_id) != TELEGRAM_CHAT_ID: return
    await update.message.reply_text("🔍 Chequeando métricas RSI/MACD y Escaneo Geopolítico de Alto Impacto...")
    report = build_full_report()
    if report:
        await update.message.reply_text(report, parse_mode="HTML")

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Usa OpenAI Vision para observar los soportes y tendencias de las imágenes subidas."""
    if str(update.message.chat_id) != TELEGRAM_CHAT_ID: return
        
    await update.message.reply_text("👁️ Ojo de Águila Analizando. Evaluando gráfica con OpenAI GPT-4o...")
    try:
        photo_file = update.message.photo[-1]
        file = await context.bot.get_file(photo_file.file_id)
        image_bytes = await file.download_as_bytearray()
        
        base64_image = base64.b64encode(image_bytes).decode('utf-8')
        
        prompt = (
            "Eres un Senior Trader y analista quant sumamente experto. Analiza detalladamente la gráfica y dime:\n"
            "1. Tendencia general dominante (Alcista, Bajista, Lateral).\n"
            "2. Zonas de Soportes y Resistencias críticas o relevantes evidentes.\n"
            "3. Divergencias visibles o anomalías (si las hay).\n"
            "4. Recomendación de Riesgo/Beneficio del 1 al 10.\n"
            "Sé muy conciso y directo."
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
                                "url": f"data:image/jpeg;base64,{base64_image}",
                                "detail": "high"
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
        logging.error(f"Error procesando imagen GPT-4o: {e}")
        await update.message.reply_text("❌ Fallo conectando a OpenAI con la foto. Comprueba tokens y subida.")

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.message.chat_id) != TELEGRAM_CHAT_ID: return
    await update.message.reply_text("Dime /analisis para métricas RSI, o manda foto para GPT-4o Vision.")

# ----------------- TAREAS NATIVAS DE COLA (JOBQUEUE) -----------------

async def routine_hourly_report(context: ContextTypes.DEFAULT_TYPE):
    report = build_full_report()
    if report:
        await context.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=report, parse_mode="HTML")

async def routine_interrupt_divergences(context: ContextTypes.DEFAULT_TYPE):
    for ticker in ["NVDA", "BNO"]:
        analysis = fetch_and_analyze_stock(ticker)
        if analysis and analysis['bullish_divergence']:
            msg = f"🦅 <b>¡ALERTA TÁCTICA! Oportunidad de Divergencia Oculta</b> 🦅\n\nEl precio de <b>{ticker}</b> busca un fondo nuevo (${analysis['price']}), pero la fortaleza del RSI reacciona en reversa ({analysis['rsi']:.2f})."
            await context.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg, parse_mode="HTML")

async def post_init(application: Application):
    """Callback hook obligatorio para arrancar colas seguras tras construirse el bot en memoria."""
    logging.info("Arrancando Rutinas Seguras en post_init...")
    # Cada 1 hora
    application.job_queue.run_repeating(routine_hourly_report, interval=3600, first=5)
    # Cada 30 minutos (sólo interrumpir si hay anomalías pesadas)
    application.job_queue.run_repeating(routine_interrupt_divergences, interval=1800, first=60)

# ----------------- INICIO Y DEPLOYMENT OFICIAL -----------------
def main():
    logging.info("🚀 Arquitectura JobQueue Segura (OpenAI) Desplegada.")
    
    # Inyectamos el post_init hook explícito
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("analisis", cmd_analisis))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text))
    
    logging.info("🦅 Polling iniciado. Bot vivo.")
    app.run_polling()

if __name__ == "__main__":
    main()
