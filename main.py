import os
import base64
import requests
import datetime
import telebot  # Esto funcionará SI el requirements está bien
import yfinance as yf
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

bot = telebot.TeleBot(TOKEN, threaded=False)
portafolio = []

# --- MOTOR DE DATOS ---
def get_live_price(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        stock = yf.Ticker(t)
        return round(float(stock.fast_info['last_price']), 2)
    except:
        return None

# --- CEREBRO DE INTELIGENCIA ---
def cerebro_genesis(query, img_b64=None):
    if not OPENAI_API_KEY:
        return "🚨 ERROR: Configura OPENAI_API_KEY en Railway."
    
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    system_msg = (
        "Eres GÉNESIS V38. Terminal de Inteligencia. "
        "MISIONES: 1. Analizar SMC en gráficas (BOS, OB, FVG). "
        "2. Reportar Geopolítica y Noticias. 3. Alertas técnicas."
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
            {"type": "text", "text": "ANALIZA LA GRÁFICA: Identifica estructura y niveles clave."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except:
        return "🚨 Error de comunicación con el cerebro."

# --- MENÚ Y BOTONES ---
def main_menu():
    m = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    m.add("📊 Rendimiento", "🚀 Operar", "🌍 Geopolítica", "🐋 Radar Ballenas", "⚠️ Alertas")
    return m

@bot.message_handler(commands=['start'])
def start(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V38: CONEXIÓN ESTABLE**", reply_markup=main_menu())

@bot.message_handler(func=lambda m: m.text == "🌍 Geopolítica")
def geopolitica(message):
    status = bot.reply_to(message, "📡 Escaneando noticias...")
    res = cerebro_genesis("Analiza la geopolítica de hoy y su impacto en el mercado.")
    bot.edit_message_text(f"🌍 **GEOPOLÍTICA:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "🐋 Radar Ballenas")
def radar(message):
    status = bot.reply_to(message, "📡 Rastreando ballenas...")
    res = cerebro_genesis("Reporta transacciones masivas de las últimas 3 horas.")
    bot.edit_message_text(f"🐋 **RADAR:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    try:
        f_info = bot.get_file(message.photo[-1].file_id)
        downloaded = bot.download_file(f_info.file_path)
        img_b64 = base64.b64encode(downloaded).decode('utf-8')
        status = bot.reply_to(message, "🎯 Analizando niveles...")
        res = cerebro_genesis(None, img_b64)
        bot.edit_message_text(f"🎯 **REPORTE SMC:**\n{res}", message.chat.id, status.message_id)
    except Exception as e:
        bot.reply_to(message, f"🚨 Error: {str(e)}")

if __name__ == "__main__":
    bot.infinity_polling()
