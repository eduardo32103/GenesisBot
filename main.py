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
def obtener_precio_v49(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        
        asset = yf.Ticker(t)
        # Intento rápido
        precio = asset.fast_info.get('last_price')
        
        if not precio:
            # Intento por historial (Plan B)
            hist = asset.history(period="1d")
            if not hist.empty:
                precio = hist['Close'].iloc[-1]
                
        return round(float(precio), 2) if precio else None
    except Exception as e:
        print(f"Error técnico en precio: {e}")
        return None

# --- NÚCLEO GÉNESIS (SMC + GEO + BALLENAS) ---
def cerebro_genesis(query, img_b64=None):
    if not OPENAI_API_KEY: return "🚨 ERROR: API KEY ausente en Railway."
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    
    system_msg = (
        "Eres GÉNESIS V49. Analista de Smart Money Concepts y Geopolítica. "
        "Misión: Identificar niveles de liquidez, BOS y reportar movimientos de ballenas. "
        "Respuestas técnicas, secas y profesionales."
    )
    
    payload = {
        "model": "gpt-4o",
        "messages": [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": query if query else "Analiza esta imagen."}
        ],
        "temperature": 0.1
    }
    
    if img_b64:
        payload["messages"][1]["content"] = [
            {"type": "text", "text": "ANALIZA SMC: BOS, FVG y niveles del eje Y."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except:
        return "🚨 IA saturada. Intenta de nuevo."

# --- BOTONES ---
@bot.message_handler(commands=['start'])
def start(message):
    m = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    m.add("📊 Precio Real", "🌍 Geopolítica", "🐋 Radar Ballenas", "⚠️ Alertas")
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V49: ARSENAL COMPLETO**", reply_markup=m)

@bot.message_handler(func=lambda m: m.text == "🌍 Geopolítica")
def btn_geo(message):
    status = bot.reply_to(message, "🌍 Analizando tensiones globales...")
    res = cerebro_genesis("Reporte geopolítico de hoy e impacto en Oro/DXY.")
    bot.edit_message_text(f"🌍 **GEOPOLÍTICA:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "🐋 Radar Ballenas")
def btn_ballenas(message):
    status = bot.reply_to(message, "🐋 Buscando huellas institucionales...")
    res = cerebro_genesis("Muestra los flujos de ballenas más relevantes de las últimas horas.")
    bot.edit_message_text(f"🐋 **RADAR:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "📊 Precio Real")
def btn_precio(message):
    bot.send_message(message.chat.id, "Escribe el ticker (ej: BTC, TSLA, NVDA):")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    f_info = bot.get_file(message.photo[-1].file_id)
    img_b64 = base64.b64encode(bot.download_file(f_info.file_path)).decode('utf-8')
    status = bot.reply_to(message, "🎯 Escaneando estructura de mercado...")
    res = cerebro_genesis(None, img_b64)
    bot.edit_message_text(f"🎯 **REPORTE SMC:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: True)
def text_handler(message):
    if len(message.text) <= 6:
        p = obtener_precio_v49(message.text)
        if p:
            bot.reply_to(message, f"📈 Precio actual de {message.text.upper()}: **${p}**")
        else:
            bot.reply_to(message, "❌ No pude jalar el precio. Intenta con otro ticker.")

if __name__ == "__main__":
    bot.infinity_polling()
