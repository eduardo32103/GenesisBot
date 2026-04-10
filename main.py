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

# --- MOTOR DUAL DE PRECIOS (ANTI-BLOQUEO) ---
def obtener_precio_v51(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        
        # MÉTODO 1: Ticker Fast Info
        asset = yf.Ticker(t)
        precio = asset.fast_info.get('last_price')
        
        if precio:
            return round(float(precio), 2)
        
        # MÉTODO 2: Descarga de emergencia (1 día)
        data = yf.download(t, period="1d", interval="1m", progress=False, timeout=10)
        if not data.empty:
            return round(float(data['Close'].iloc[-1]), 2)
            
        return None
    except:
        return None

# --- NÚCLEO GÉNESIS (SMC + GEO + BALLENAS) ---
def cerebro_genesis(query, img_b64=None):
    if not OPENAI_API_KEY: return "🚨 ERROR: Variable OPENAI_API_KEY no configurada."
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    system_msg = "Eres GÉNESIS V51. Analista experto en SMC, Geopolítica y Ballenas. Sé técnico y breve."
    
    payload = {
        "model": "gpt-4o",
        "messages": [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": query if query else "Analiza esta imagen."}
        ],
        "temperature": 0
    }
    
    if img_b64:
        payload["messages"][1]["content"] = [
            {"type": "text", "text": "ANALIZA SMC: BOS, OB, FVG y liquidez."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except:
        return "🚨 IA ocupada. Intenta de nuevo."

# --- INTERFAZ ---
@bot.message_handler(commands=['start'])
def start(message):
    m = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    m.add("📊 Precio Real", "🌍 Geopolítica", "🐋 Radar Ballenas", "⚠️ Alertas")
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V51: SISTEMA DUAL ACTIVO**", reply_markup=m)

@bot.message_handler(func=lambda m: m.text == "🌍 Geopolítica")
def btn_geo(message):
    status = bot.reply_to(message, "🌍 Analizando tensiones globales...")
    res = cerebro_genesis("Reporte geopolítico actual e impacto en Oro y DXY.")
    bot.edit_message_text(f"🌍 **GEOPOLÍTICA:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "🐋 Radar Ballenas")
def btn_ballenas(message):
    status = bot.reply_to(message, "🐋 Rastreando flujos de capital...")
    res = cerebro_genesis("Reporte de movimientos de ballenas de las últimas 12 horas.")
    bot.edit_message_text(f"🐋 **RADAR:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "📊 Precio Real")
def btn_pre(message):
    bot.send_message(message.chat.id, "Escribe el ticker (ej: BTC, TSLA, AAPL):")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    f_info = bot.get_file(message.photo[-1].file_id)
    img_b64 = base64.b64encode(bot.download_file(f_info.file_path)).decode('utf-8')
    status = bot.reply_to(message, "🎯 Analizando estructura...")
    res = cerebro_genesis(None, img_b64)
    bot.edit_message_text(f"🎯 **REPORTE SMC:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: True)
def text_input(message):
    if len(message.text) <= 6:
        p = obtener_precio_v51(message.text)
        if p:
            bot.reply_to(message, f"📈 Precio de {message.text.upper()}: **${p}**")
        else:
            bot.reply_to(message, "❌ Yahoo Finance bloqueó la conexión. Intenta más tarde o con otro ticker.")

if __name__ == "__main__":
    bot.infinity_polling()
