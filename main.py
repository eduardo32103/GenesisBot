import os
import requests
import base64
import telebot
import datetime
import yfinance as yf
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TU_CHAT_ID = "5426620320"

bot = telebot.TeleBot(TOKEN, threaded=False)

# Memoria de operaciones en lista de diccionarios
portafolio = []

def obtener_precio_real(ticker):
    """Extrae precio real de Yahoo Finance sin errores"""
    try:
        t = ticker.upper().strip()
        # Mapeo rápido para activos comunes
        if t == "BTC": t = "BTC-USD"
        if t == "ETH": t = "ETH-USD"
        if t == "ORO": t = "GC=F"
        
        stock = yf.Ticker(t)
        # Intentamos obtener el precio más reciente disponible
        data = stock.history(period="1d")
        if not data.empty:
            precio = data['Close'].iloc[-1]
            return round(float(precio), 2)
        return None
    except:
        return None

# --- HANDLERS ---
@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(
        message.chat.id, 
        "🦅 **GÉNESIS V13: OPERATIVO**\n\nUsa los botones para gestionar tus activos con precios REALES.", 
        reply_markup=menu_principal()
    )

def menu_principal():
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btns = ["📊 Rendimiento", "🚀 Operar (Auto)", "📈 Escáner SMT", "📊 Análisis SMC"]
    markup.add(*[types.KeyboardButton(b) for b in btns])
    return markup

@bot.message_handler(func=lambda message: message.text == "🚀 Operar (Auto)")
def instruccion_auto(message):
    bot.reply_to(message, "📝 Escribe: `Comprar [Cantidad] [Ticker]`\nEjemplo: `Comprar 10 TSLA` o `Comprar 0.5 BTC`")

@bot.message_handler(func=lambda message: message.text.lower().startswith(("comprar ", "vender ")))
def auto_registro(message):
    try:
        partes = message.text.split()
        if len(partes) < 3:
            bot.reply_to(message, "❌ Usa el formato: `Comprar 10 NVDA`")
            return

        cantidad = float(partes[1])
        ticker = partes[2].upper()
        
        status = bot.reply_to(message, f"🔍 Consultando mercado por {ticker}...")
        precio = obtener_precio_real(ticker)
        
        if precio:
            portafolio.append({"ticker": ticker, "cant": cantidad, "px_in": precio})
            bot.edit_message_text(
                f"✅ **OPERACIÓN REGISTRADA**\n\nActivo: {ticker}\nCantidad: {cantidad}\nPrecio Entrada: ${precio}", 
                message.chat.id, status.message_id
            )
        else:
            bot.edit_message_text(f"❌ No se encontró el precio de {ticker}. Verifica el Ticker.", message.chat.id, status.message_id)
    except:
        bot.reply_to(message, "❌ Error en el formato. Asegúrate de poner un número en la cantidad.")

@bot.message_handler(func=lambda message: message.text == "📊 Rendimiento")
def ver_rendimiento(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ No tienes operaciones abiertas.")
        return
    
    status = bot.reply_to(message, "⏳ Actualizando precios en tiempo real...")
    reporte = "📊 **RESUMEN DE PORTAFOLIO**\n━━━━━━━━━━━━━━\n"
    total_pnl = 0
    
    for op in portafolio:
        px_act = obtener_precio_real(op['ticker'])
        if px_act:
            pnl = (px_act - op['px_in']) * op['cant']
            pct = ((px_act / op['px_in']) - 1) * 100
            reporte += f"🔹 **{op['ticker']}**\nEntrada: ${op['px_in']} | Actual: ${px_act}\nP&L: ${round(pnl, 2)} ({round(pct, 2)}%)\n\n"
            total_pnl += pnl
        else:
            reporte += f"🔹 **{op['ticker']}**: Error al actualizar precio.\n\n"
    
    reporte += f"━━━━━━━━━━━━━━\n**Balance Total P&L: ${round(total_pnl, 2)}**"
    bot.edit_message_text(reporte, message.chat.id, status.message_id)

if __name__ == "__main__":
    bot.infinity_polling()
