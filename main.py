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

# --- MOTOR DE PRECIOS PROFESIONAL ---
def obtener_precio_real(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        # Usamos Ticker() que es más ligero que download()
        asset = yf.Ticker(t)
        # Intentamos obtener el precio de varias formas por si una falla
        precio = asset.fast_info.get('last_price')
        if precio:
            return round(float(precio), 2)
        
        # Plan B si fast_info falla
        hist = asset.history(period="1d")
        if not hist.empty:
            return round(float(hist['Close'].iloc[-1]), 2)
        return None
    except:
        return None

# --- NÚCLEO DE INTELIGENCIA (SMC + GEOPOLÍTICA + BALLENAS) ---
def cerebro_genesis(query, img_b64=None):
    if not OPENAI_API_KEY: return "🚨 ERROR: Variable OPENAI_API_KEY no configurada."
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    
    system_msg = (
        "Eres GÉNESIS V45. Analista de Smart Money Concepts y Geopolítica. "
        "Misión: Identificar niveles BOS/FVG y rastrear movimientos de ballenas (volúmenes institucionales). "
        "Tus respuestas son técnicas, breves y precisas."
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
            {"type": "text", "text": "ANALIZA SMC: Estructura, BOS y Order Blocks."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except:
        return "🚨 Error de conexión con el cerebro IA."

# --- INTERFAZ ---
@bot.message_handler(commands=['start'])
def start(message):
    m = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    m.add("📊 Precio Real", "🌍 Geopolítica", "🐋 Radar Ballenas", "⚠️ Alertas")
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V45: SISTEMA RESTAURADO**\nBallenas y Precios en tiempo real activos.", reply_markup=m)

@bot.message_handler(func=lambda m: m.text == "🐋 Radar Ballenas")
def btn_ballenas(message):
    status = bot.reply_to(message, "📡 Rastreando flujos de capital institucional...")
    res = cerebro_genesis("Reporte de movimientos de ballenas crypto y stock de las últimas 6 horas.")
    bot.edit_message_text(f"🐋 **RADAR DE BALLENAS:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "🌍 Geopolítica")
def btn_geo(message):
    status = bot.reply_to(message, "🌍 Analizando tensiones globales...")
    res = cerebro_genesis("Resumen geopolítico de hoy e impacto en Oro y DXY.")
    bot.edit_message_text(f"🌍 **GEOPOLÍTICA:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "📊 Precio Real")
def btn_precio(message):
    bot.send_message(message.chat.id, "Escribe el ticker (ej: BTC, NVDA, AAPL):")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    f = bot.get_file(message.photo[-1].file_id)
    b = base64.b64encode(bot.download_file(f.file_path)).decode('utf-8')
    status = bot.reply_to(message, "🎯 Analizando estructura institucional...")
    res = cerebro_genesis(None, b)
    bot.edit_message_text(f"🎯 **ANÁLISIS SMC:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: True)
def procesar_texto(message):
    if len(message.text) <= 6:
        p = obtener_precio_real(message.text)
        if p:
            bot.reply_to(message, f"📈 El precio real de {message.text.upper()} es: **${p}**")
        else:
            bot.reply_to(message, "❌ No se encontraron datos para ese ticker.")

if __name__ == "__main__":
    bot.infinity_polling()
