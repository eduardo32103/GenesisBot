import os
import telebot
import yfinance as yf
from telebot import types

# --- CONFIGURACIÓN ---
# He quitado OpenAI de aquí para que no de errores si la clave no está en Railway
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"

bot = telebot.TeleBot(TOKEN, threaded=False)
portafolio = []

def obtener_precio_real(ticker):
    try:
        t = ticker.upper().strip()
        if t == "BTC": t = "BTC-USD"
        stock = yf.Ticker(t)
        precio = stock.fast_info['last_price']
        return round(float(precio), 2)
    except:
        return None

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V14.1: ONLINE**\n\nSi lees esto, el bot ya despertó.", reply_markup=menu_principal())

def menu_principal():
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btns = ["📊 Rendimiento", "🚀 Operar"]
    markup.add(*[types.KeyboardButton(b) for b in btns])
    return markup

@bot.message_handler(func=lambda message: message.text == "🚀 Operar")
def instruccion_auto(message):
    bot.reply_to(message, "Escribe: `Comprar 10 TSLA`")

@bot.message_handler(func=lambda message: message.text.lower().startswith("comprar "))
def auto_registro(message):
    try:
        partes = message.text.split()
        cantidad = float(partes[1])
        ticker = partes[2].upper()
        precio = obtener_precio_real(ticker)
        if precio:
            portafolio.append({"ticker": ticker, "cant": cantidad, "px_in": precio})
            bot.reply_to(message, f"✅ **{ticker}** registrado a ${precio}")
        else:
            bot.reply_to(message, "❌ No encontré ese ticker.")
    except:
        bot.reply_to(message, "❌ Usa: Comprar 10 TSLA")

@bot.message_handler(func=lambda message: message.text == "📊 Rendimiento")
def ver_rendimiento(message):
    if not portafolio:
        bot.reply_to(message, "Vacío.")
        return
    res = "📊 **PORTAFOLIO:**\n"
    for op in portafolio:
        px = obtener_precio_real(op['ticker'])
        res += f"🔹 {op['ticker']}: ${px}\n"
    bot.reply_to(message, res)

if __name__ == "__main__":
    bot.infinity_polling()
