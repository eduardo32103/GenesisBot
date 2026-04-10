import os, requests, base64, time, telebot, datetime

# --- TRUCO PARA RAILWAY ---
try:
    import yfinance as yf
except ImportError:
    # Si no existe, lo instalamos a la fuerza
    os.system('pip install yfinance')
    import yfinance as yf

from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TU_CHAT_ID = "5426620320"

bot = telebot.TeleBot(TOKEN, threaded=False)
portafolio = []

def obtener_precio_real(ticker):
    try:
        # Ajuste para criptos
        if ticker in ["BTC", "ETH", "SOL", "XRP"]:
            ticker = f"{ticker}-USD"
        stock = yf.Ticker(ticker)
        precio = stock.fast_info['last_price']
        return round(precio, 2)
    except:
        return None

# (Aquí va el resto de tu lógica de cerebro_genesis y los handlers de antes)

@bot.message_handler(func=lambda message: message.text == "📊 Rendimiento")
def ver_rendimiento(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ Vacío.")
        return
    status = bot.reply_to(message, "⏳ Buscando precios REALES...")
    reporte = "📊 **DATA EN VIVO**\n"
    for op in portafolio:
        px = obtener_precio_real(op['ticker'])
        if px:
            reporte += f"🔹 {op['ticker']}: ${px}\n"
    bot.edit_message_text(reporte, message.chat.id, status.message_id)

if __name__ == "__main__":
    bot.infinity_polling()
