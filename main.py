# Módulos base y HTTP
import json
import requests
from bs4 import BeautifulSoup
import schedule
import time
import logging
import yfinance as yf
import pandas as pd
import asyncio
from telegram import Bot

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CONFIGURACIONES DE TELEGRAM ---
TELEGRAM_TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
TELEGRAM_CHAT_ID = "5426620320"

def send_telegram_alert(message):
    """Envía una alerta de manera síncrona a la API de Telegram."""
    if TELEGRAM_TOKEN == "TU_TOKEN_AQUI" or TELEGRAM_CHAT_ID == "TU_CHAT_ID_AQUI":
        logging.warning("Telegram Token o Chat ID pendientes de configurar. Omitiendo envío a Telegram.")
        return
        
    async def _send():
        try:
            bot = Bot(token=TELEGRAM_TOKEN)
            await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
            logging.info("✅ Alerta enviada a Telegram con éxito.")
        except Exception as e:
            logging.error(f"❌ Error al enviar mensaje a Telegram: {e}")
            
    try:
        asyncio.run(_send())
    except Exception as e:
        logging.error(f"❌ Error de ejecución asíncrona para Telegram: {e}")

def setup_telegram():
    global TELEGRAM_CHAT_ID
    if TELEGRAM_CHAT_ID == "TU_CHAT_ID_AQUI":
        logging.info("🔍 Buscando tu Chat ID de Telegram...")
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
        try:
            response = requests.get(url)
            data = response.json()
            if data.get("ok") and data.get("result"):
                # Tomar el id del último mensaje
                last_update = data["result"][-1]
                chat_id = str(last_update["message"]["chat"]["id"])
                username = last_update["message"]["chat"].get("username", "Usuario")
                
                logging.info(f"✅ ¡Se encontró Chat ID! Usuario: @{username} | Chat ID: {chat_id}")
                logging.info(f"👉 IMPORTANTE: Copia '{chat_id}' y reemplázalo en 'TELEGRAM_CHAT_ID' dentro del código para dejarlo fijo.")
                
                # Asignarlo temporalmente en memoria para poder enviar el primer mensaje
                TELEGRAM_CHAT_ID = chat_id
                
                msg = "¡Génesis 1.0 en línea! Analizando NVIDIA e Irán para Eduardo."
                send_telegram_alert(msg)
            else:
                logging.warning("⚠️ No hay mensajes en el Bot. Envíale un simple mensaje (ej. 'hola') al bot desde tu Telegram y vuelve a ejecutar este script para descubrir tu Chat ID.")
        except Exception as e:
            logging.error(f"Error obteniendo Updates de Telegram: {e}")

def load_portfolio(filepath="portfolio.json"):
    try:
        with open(filepath, "r") as f:
            portfolio = json.load(f)
            logging.info(f"Portafolio cargado correctamente: {portfolio}")
            return portfolio
    except Exception as e:
        logging.error(f"Error al cargar portafolio: {e}")
        return None

def fetch_and_analyze_stock(ticker):
    """Descarga datos de yfinance y calcula RSI y MACD de forma manual con Pandas"""
    logging.info(f"Analizando indicadores técnicos para {ticker}...")
    try:
        # Descargamos los últimos 6 meses de datos diarios
        data = yf.download(ticker, period="6mo", interval="1d", progress=False)
        
        if data.empty:
            logging.warning(f"No se pudieron descargar datos para {ticker}.")
            return None
            
        # Acomodar MultiIndex si existe (dependiente de la versión de yfinance)
        if isinstance(data.columns, pd.MultiIndex):
            data = data.copy()
            data.columns = data.columns.get_level_values(0)
            
        # Extraer precios de Cierre
        close_prices = data['Close']
        if isinstance(close_prices, pd.DataFrame): 
             # En casos muy raros, si se devuelven múltiples columnas al aplanar
             close_prices = close_prices.iloc[:, 0]
             
        # -------------
        # Calcular RSI (14 periodos)
        # -------------
        delta = close_prices.diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)
        
        # Usando Media Móvil Exponencial (Wilder's Smoothing)
        ema_up = up.ewm(com=14-1, adjust=False).mean()
        ema_down = down.ewm(com=14-1, adjust=False).mean()
        
        rs = ema_up / ema_down
        rsi_series = 100 - (100 / (1 + rs))
        # Para evitar división por cero cuando no hay pérdida
        rsi_series[ema_down == 0] = 100 
        
        data['RSI'] = rsi_series
        
        # -------------
        # Calcular MACD (12, 26, 9)
        # -------------
        ema_12 = close_prices.ewm(span=12, adjust=False).mean()
        ema_26 = close_prices.ewm(span=26, adjust=False).mean()
        
        macd_line = ema_12 - ema_26
        macd_signal = macd_line.ewm(span=9, adjust=False).mean()
        
        data['MACD_Line'] = macd_line
        data['MACD_Signal'] = macd_signal
        
        # Extraer el registro más reciente
        latest = data.iloc[-1]
        
        rsi_val = float(latest['RSI']) if not pd.isna(latest['RSI']) else 0.0
        macd_line_val = float(latest['MACD_Line']) if not pd.isna(latest['MACD_Line']) else 0.0
        macd_signal_val = float(latest['MACD_Signal']) if not pd.isna(latest['MACD_Signal']) else 0.0
        price_val = float(latest['Close'])
        
        logging.info(f"[{ticker}] Precio: ${price_val:.2f} | RSI: {rsi_val:.2f} | MACD: {macd_line_val:.2f} | MACD Señal: {macd_signal_val:.2f}")
        
        return {
            'ticker': ticker,
            'price': price_val,
            'rsi': rsi_val,
            'macd_line': macd_line_val,
            'macd_signal': macd_signal_val
        }
    except Exception as e:
        logging.error(f"Error analizando {ticker}: {e}")
        return None

def generate_trading_signals(analysis):
    """Genera señales de COMPRA o VENTA basadas en RSI y MACD y notifica a Telegram."""
    if not analysis:
        return
        
    ticker = analysis['ticker']
    rsi = analysis['rsi']
    macd_line = analysis['macd_line']
    macd_signal = analysis['macd_signal']
    price = analysis['price']
    
    signal = "(MANTENER)"
    reason = ""
    
    # Lógica de señales
    if rsi < 30 and macd_line > macd_signal:
        signal = "¡COMPRAR!"
        reason = f"RSI en sobreventa ({rsi:.2f}) y MACD cruzó al alza."
    elif rsi > 70 and macd_line < macd_signal:
        signal = "¡VENDER!"
        reason = f"RSI en sobrecompra ({rsi:.2f}) y MACD cruzó a la baja."
        
    logging.info(f"--- SEÑAL {ticker}: {signal} ---")
    
    if signal in ["¡COMPRAR!", "¡VENDER!"]:
        logging.info(f"Razón: {reason}")
        msg = f"🟢🔴 SEÑAL DE TRADING: {signal}\n\nActivo: {ticker}\nPrecio Actual: ${price:.2f}\nRSI (14): {rsi:.2f}\nMotivo: {reason}"
        send_telegram_alert(msg)

def check_nvidia_news():
    logging.info("Monitoreando actividad de ballenas de NVIDIA...")
    search_url = "https://news.google.com/search?q=NVIDIA+whales+stock"
    try:
        response = requests.get(search_url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            headlines = soup.find_all('a', class_='JtKRv', limit=3)
            for hl in headlines:
                logging.info(f"[NVIDIA News] {hl.text}")
    except Exception as e:
        logging.error(f"Error de red: {e}")

def check_geopolitical_news():
    logging.info("Monitoreando noticias geopolíticas (Irán/Energía)...")
    search_url = "https://news.google.com/search?q=Iran+Energy+geopolitics+oil"
    try:
        response = requests.get(search_url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            headlines = soup.find_all('a', class_='JtKRv', limit=3)
            for hl in headlines:
                logging.info(f"[IRÁN/ENERGÍA News] {hl.text}")
    except Exception as e:
        logging.error(f"Error de red: {e}")

def main_job():
    logging.info("="*50)
    logging.info("Iniciando ciclo de análisis del bot para el portafolio...")
    logging.info("="*50)
    
    portfolio = load_portfolio()
    
    # Análisis Técnico de NVDA y BNO usando cálculo manual
    for ticker in ["NVDA", "BNO"]:
        analysis = fetch_and_analyze_stock(ticker)
        generate_trading_signals(analysis)
        
    # Análisis de Sentimiento y Noticias Globales
    check_nvidia_news()
    check_geopolitical_news()
    
    logging.info("="*50)
    logging.info("Ciclo de análisis finalizado. Esperando la próxima ejecución.")
    logging.info("="*50 + "\n")

if __name__ == "__main__":
    logging.info("🚀 Bot Analista Senior Iniciado y en Ejecución (Modo Pandas Manual)")
    
    setup_telegram()
    
    main_job()
    schedule.every(60).minutes.do(main_job)
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Bot Analista detenido manualmente por el usuario.")
