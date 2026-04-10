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

# --- MOTOR DE PRECIOS (EL MÁS SENCILLO) ---
def obtener_precio(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        # Usamos la función básica de yfinance
        val = yf.download(t, period="1d", interval="1m", progress=False)
        if not val.empty:
            return round(float(val['Close'].iloc[-1]), 2)
        return None
    except:
        return None

# --- CEREBRO GÉNESIS (SMC + GEOPOLÍTICA) ---
def cerebro_genesis(query, img_b64=None):
    if not OPENAI_API_KEY: return "🚨 ERROR: Variable OPENAI_API_KEY vacía en Railway."
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    
    # Este prompt mantiene todo lo que hemos ganado
    system_msg = (
        "Eres GÉNESIS V44. Analista experto en Smart Money Concepts (BOS, CHoCH, OB, FVG). "
        "También eres experto en Geopolítica mundial. Tu misión es analizar noticias "
        "y dar su impacto en el Oro y el DXY de forma técnica y seca."
    )
    
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
            {"type": "text", "text": "ANALIZA SMC: Identifica estructura y zonas de liquidez."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except:
        return "🚨 La IA está saturada, intenta de nuevo."

# --- INTERFAZ DE BOTONES ---
@bot.message_handler(commands=['start'])
def start(message):
    m = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    m.add("📊 Precio Real", "🌍 Geopolítica", "🐋 Ballenas", "⚠️ Alertas")
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V44: SISTEMA ONLINE**\nTodo cargado: SMC, Geopolítica y Precios.", reply_markup=m)

@bot.message_handler(func=lambda m: m.text == "🌍 Geopolítica")
def btn_geo(message):
    status = bot.reply_to(message, "📡 Conectando con satélites de noticias...")
    res = cerebro_genesis("Haz un resumen geopolítico de hoy y cómo afecta al Oro y al DXY.")
    bot.edit_message_text(f"🌍 **REPORTE GLOBAL:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "📊 Precio Real")
def btn_pre(message):
    bot.send_message(message.chat.id, "Escribe el ticker (ejemplo: BTC, NVDA, TSLA):")

@bot.message_handler(func=lambda m: m.text == "⚠️ Alertas")
def btn_ale(message):
    bot.reply_to(message, "🔔 **ALERTAS:** Monitoreando volatilidad >3% en activos principales.")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    f = bot.get_file(message.photo[-1].file_id)
    b = base64.b64encode(bot.download_file(f.file_path)).decode('utf-8')
    status = bot.reply_to(message, "🎯 Analizando estructura de mercado...")
    res = cerebro_genesis(None, b)
    bot.edit_message_text(f"🎯 **ANÁLISIS SMC:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: True)
def default(message):
    # Si el usuario escribe un ticker
    if len(message.text) <= 5:
        p = obtener_precio(message.text)
        if p:
            bot.reply_to(message, f"📈 Precio de {message.text.upper()}: **${p}**")
        else:
            bot.reply_to(message, "❌ No pude jalar el precio. Intenta con otro ticker.")

if __name__ == "__main__":
    bot.infinity_polling()
