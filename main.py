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

# --- MOTOR DE DATOS REALES (Optimizado) ---
def obtener_precio(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        data = yf.download(t, period="1d", interval="1m", progress=False)
        if not data.empty:
            # Extraemos el último precio de cierre
            precio = data['Close'].iloc[-1]
            return round(float(precio), 2)
        return None
    except Exception as e:
        print(f"Error en yfinance: {e}")
        return None

# --- CEREBRO GÉNESIS (SMC + GEOPOLÍTICA) ---
def cerebro_genesis(query, img_b64=None):
    if not OPENAI_API_KEY: return "🚨 ERROR: Falta OPENAI_API_KEY en Railway."
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    system_msg = (
        "Eres GÉNESIS V43. Analista de Smart Money Concepts (SMC) y Geopolítica. "
        "Das niveles de BOS, FVG y Order Blocks. Analizas noticias geopolíticas "
        "y su impacto en Oro/DXY. Sé breve y técnico."
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
            {"type": "text", "text": "ANALIZA SMC: Identifica BOS, OB y FVG."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Error: La IA no respondió."

# --- INTERFAZ ---
@bot.message_handler(commands=['start'])
def start(message):
    m = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    m.add("📊 Precio Real", "🌍 Geopolítica", "🐋 Ballenas", "⚠️ Alertas")
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V43: ACTIVADO**\nDatos en tiempo real y Geopolítica listos.", reply_markup=m)

@bot.message_handler(func=lambda m: m.text == "📊 Precio Real")
def btn_precio(message):
    bot.send_message(message.chat.id, "Escribe el ticker (ejemplo: BTC, TSLA, NVDA):")

@bot.message_handler(func=lambda m: m.text == "🌍 Geopolítica")
def btn_geo(message):
    status = bot.reply_to(message, "📡 Escaneando noticias...")
    res = cerebro_genesis("Reporte geopolítico de hoy e impacto en Oro/DXY.")
    bot.edit_message_text(f"🌍 **GEOPOLÍTICA:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    f = bot.get_file(message.photo[-1].file_id)
    b = base64.b64encode(bot.download_file(f.file_path)).decode('utf-8')
    status = bot.reply_to(message, "🎯 Analizando gráfica...")
    res = cerebro_genesis(None, b)
    bot.edit_message_text(f"🎯 **REPORTE SMC:**\n{res}", message.chat.id, status.message_id)

# Handler para cuando escribas un ticker suelto
@bot.message_handler(func=lambda m: len(m.text) <= 5)
def precio_ticker(message):
    p = obtener_precio(message.text)
    if p:
        bot.reply_to(message, f"📈 El precio actual de {message.text.upper()} es: **${p}**")
    else:
        bot.reply_to(message, "❌ No pude obtener datos. Verifica el ticker.")

if __name__ == "__main__":
    bot.infinity_polling()
