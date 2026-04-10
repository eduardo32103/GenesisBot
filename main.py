import os
import subprocess
import sys

# --- FORZAR INSTALACIÓN AL ARRANCAR ---
def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

try:
    import telebot
    import yfinance as yf
except ImportError:
    install('pyTelegramBotAPI')
    install('yfinance')
    install('requests')
    import telebot
    import yfinance as yf

from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
bot = telebot.TeleBot(TOKEN, threaded=False)

portafolio = []

def obtener_precio_real(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH"]: t = f"{t}-USD"
        stock = yf.Ticker(t)
        # Usamos un método más directo para evitar fallos de caché
        precio = stock.fast_info['last_price']
        return round(float(precio), 2)
    except:
        return None

# --- BOT LOGIC ---
@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(
        message.chat.id, 
        "🦅 **GÉNESIS V16: SISTEMA FORZADO**\nConexión directa con Yahoo Finance establecida.", 
        reply_markup=menu_principal()
    )

def menu_principal():
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btns = ["📊 Rendimiento", "🚀 Operar"]
    markup.add(*[types.KeyboardButton(b) for b in btns])
    return markup

@bot.message_handler(func=lambda message: message.text == "🚀 Operar")
def instruccion_auto(message):
    bot.reply_to(message, "Escribe: `Comprar 10 TSLA` o `Comprar 5 NVDA`.")

@bot.message_handler(func=lambda message: message.text.lower().startswith("comprar "))
def auto_registro(message):
    try:
        partes = message.text.split()
        cantidad = float(partes[1])
        ticker = partes[2].upper()
        
        status = bot.reply_to(message, f"🔍 Consultando {ticker}...")
        precio = obtener_precio_real(ticker)
        
        if precio:
            portafolio.append({"ticker": ticker, "cant": cantidad, "px_in": precio})
            bot.edit_message_text(f"✅ **REGISTRADO**\n{ticker} | ${precio}\nCant: {cantidad}", message.chat.id, status.message_id)
        else:
            bot.edit_message_text(f"❌ No encontré el ticker {ticker}.", message.chat.id, status.message_id)
    except:
        bot.reply_to(message, "❌ Formato: `Comprar 10 TSLA`")

@bot.message_handler(func=lambda message: message.text == "📊 Rendimiento")
def ver_rendimiento(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ Sin trades.")
        return
    status = bot.reply_to(message, "⏳ Actualizando...")
    res = "📊 **PORTAFOLIO EN VIVO**\n"
    total_pnl = 0
    for op in portafolio:
        px = obtener_precio_real(op['ticker'])
        if px:
            pnl = (px - op['px_in']) * op['cant']
            res += f"🔹 {op['ticker']}: ${px} (P&L: ${round(pnl, 2)})\n"
            total_pnl += pnl
    res += f"\n**Total P&L: ${round(total_pnl, 2)}**"
    bot.edit_message_text(res, message.chat.id, status.message_id)

if __name__ == "__main__":
    bot.infinity_polling()
