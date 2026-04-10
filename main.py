import os
import base64
import requests
import telebot
import yfinance as yf
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

bot = telebot.TeleBot(TOKEN, threaded=False)

# --- MOTOR DE PRECIOS RESILIENTE ---
def obtener_precio_v46(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        
        # Intento 1: Ticker directo (más rápido)
        asset = yf.Ticker(t)
        precio = asset.fast_info.get('last_price')
        
        if precio:
            return round(float(precio), 2)
        
        # Intento 2: Histórico pequeño
        hist = asset.history(period="1d")
        if not hist.empty:
            return round(float(hist['Close'].iloc[-1]), 2)
            
        return None
    except:
        return None

# --- CEREBRO GÉNESIS (TODO EN UNO) ---
def cerebro_genesis(query, img_b64=None):
    if not OPENAI_API_KEY: return "🚨 ERROR: Falta API KEY en variables de Railway."
    
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    system_msg = (
        "Eres GÉNESIS V46. Analista de Mercados y Geopolítica. "
        "Misión: 1. Analizar niveles SMC (BOS, FVG, OB). 2. Reportar Geopolítica. "
        "3. Rastrear Ballenas (flujos institucionales). Respuestas rápidas y técnicas."
    )
    
    payload = {
        "model": "gpt-4o",
        "messages": [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": query if query else "Analiza esta imagen."}
        ],
        "temperature": 0.2
    }

    if img_b64:
        payload["messages"][1]["content"] = [
            {"type": "text", "text": "ANALIZA SMC: Identifica estructura y Order Blocks."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except:
        return "🚨 La IA no respondió a tiempo. Intenta de nuevo."

# --- INTERFAZ ---
@bot.message_handler(commands=['start'])
def start(message):
    m = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    m.add("📊 Precio Real", "🌍 Geopolítica", "🐋 Radar Ballenas", "⚠️ Alertas")
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V46: SISTEMA REFORZADO**\nGeopolítica, Ballenas y SMC activos.", reply_markup=m)

@bot.message_handler(func=lambda m: m.text == "🐋 Radar Ballenas")
def radar_ballenas(message):
    status = bot.reply_to(message, "📡 Escaneando flujos de ballenas...")
    res = cerebro_genesis("Muestra los movimientos de ballenas más importantes de hoy en Crypto y Stocks.")
    bot.edit_message_text(f"🐋 **REPORTE DE BALLENAS:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "🌍 Geopolítica")
def geopolitica(message):
    status = bot.reply_to(message, "🌍 Analizando mapa geopolítico...")
    res = cerebro_genesis("Resumen geopolítico de hoy e impacto en Oro y DXY.")
    bot.edit_message_text(f"🌍 **GEOPOLÍTICA:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "📊 Precio Real")
def precio_real(message):
    bot.send_message(message.chat.id, "Escribe el ticker (ej: BTC, TSLA, NVDA):")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    f_info = bot.get_file(message.photo[-1].file_id)
    img_b64 = base64.b64encode(bot.download_file(f_info.file_path)).decode('utf-8')
    status = bot.reply_to(message, "🎯 Analizando niveles institucionales...")
    res = cerebro_genesis(None, img_b64)
    bot.edit_message_text(f"🎯 **REPORTE SMC:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: True)
def respuesta_general(message):
    if len(message.text) <= 6:
        p = obtener_precio_v46(message.text)
        if p:
            bot.reply_to(message, f"📈 Precio actual de {message.text.upper()}: **${p}**")
        else:
            bot.reply_to(message, "❌ No pude obtener el precio. Intenta con otro ticker.")

if __name__ == "__main__":
    bot.infinity_polling()
