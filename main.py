import json
import requests
import schedule
import time
import logging
import yfinance as yf
import pandas as pd

# Configurar logging para Railway
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CONFIGURACIONES DE TELEGRAM ---
TELEGRAM_TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
TELEGRAM_CHAT_ID = "5426620320"

def send_telegram_alert(message):
    """Envía una alerta usando la API REST pura, ideal para evitar crasheos de asyncio en Railway."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            logging.info("✅ Alerta enviada a Telegram con éxito.")
        else:
            logging.error(f"❌ Error de Telegram: {response.text}")
    except Exception as e:
        logging.error(f"❌ Error de red al enviar a Telegram: {e}")

def fetch_and_analyze_stock(ticker):
    """Descarga datos de yfinance y calcula RSI y MACD manualmente con Pandas."""
    logging.info(f"Analizando técnico para {ticker}...")
    try:
        data = yf.download(ticker, period="6mo", interval="1d", progress=False)
        if data.empty:
            return None
            
        # Compatibilidad con MultiIndex de nuevas versiones de yfinance
        if isinstance(data.columns, pd.MultiIndex):
            data = data.copy()
            data.columns = data.columns.get_level_values(0)
            
        close_prices = data['Close']
        if isinstance(close_prices, pd.DataFrame): 
             close_prices = close_prices.iloc[:, 0]
             
        # Cálculo de RSI (14 periodos)
        delta = close_prices.diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)
        ema_up = up.ewm(com=14-1, adjust=False).mean()
        ema_down = down.ewm(com=14-1, adjust=False).mean()
        rs = ema_up / ema_down
        rsi_series = 100 - (100 / (1 + rs))
        rsi_series[ema_down == 0] = 100 
        
        # Cálculo de MACD (12, 26, 9)
        ema_12 = close_prices.ewm(span=12, adjust=False).mean()
        ema_26 = close_prices.ewm(span=26, adjust=False).mean()
        macd_line = ema_12 - ema_26
        macd_signal = macd_line.ewm(span=9, adjust=False).mean()
        
        latest_price = float(close_prices.iloc[-1])
        latest_rsi = float(rsi_series.iloc[-1])
        latest_macd = float(macd_line.iloc[-1])
        latest_signal = float(macd_signal.iloc[-1])
        
        return {
            'ticker': ticker,
            'price': latest_price,
            'rsi': latest_rsi,
            'macd_line': latest_macd,
            'macd_signal': latest_signal
        }
    except Exception as e:
        logging.error(f"Error analizando {ticker}: {e}")
        return None

def generate_trading_signals(analysis):
    """Genera el reporte de estado de compra o venta en texto HTML."""
    if not analysis:
        return ""
        
    ticker = analysis['ticker']
    rsi = analysis['rsi']
    macd_line = analysis['macd_line']
    macd_signal = analysis['macd_signal']
    price = analysis['price']
    
    signal = "(MANTENER)"
    
    if rsi < 30 and macd_line > macd_signal:
        signal = "🟢 ¡COMPRAR!"
    elif rsi > 70 and macd_line < macd_signal:
        signal = "🔴 ¡VENDER!"
        
    logging.info(f"[{ticker}] Precio: ${price:.2f} | RSI: {rsi:.2f} | Señal: {signal}")
    return f"<b>{ticker}</b>: ${price:.2f} | RSI: {rsi:.2f} | Estado: {signal}"

def main_job():
    """Flujo principal que escanea todo el mercado de la lista y te notifica de un golpe."""
    logging.info("="*40)
    logging.info("Iniciando escaneo del mercado (NVDA, BNO)...")
    
    report_lines = ["🚨 <b>Génesis 1.0 - Reporte de Mercado</b> 🚨\n"]
    
    for ticker in ["NVDA", "BNO"]:
        analysis = fetch_and_analyze_stock(ticker)
        if analysis:
            status = generate_trading_signals(analysis)
            report_lines.append(status)
            
    final_message = "\n".join(report_lines)
    send_telegram_alert(final_message)
    
    logging.info("Ciclo de análisis finalizado. Esperando la próxima hora...")
    logging.info("="*40)

if __name__ == "__main__":
    logging.info("🚀 Bot Analista Senior Iniciado.")
    
    # 1. IMPORTANTE: Llamada obligatoria para enviar el reporte en el segundo 1
    main_job()
    
    # 2. Programamos las siguientes repeticiones
    schedule.every(60).minutes.do(main_job)
    
    # 3. Bucle infinito para Railway
    while True:
        schedule.run_pending()
        time.sleep(1)
