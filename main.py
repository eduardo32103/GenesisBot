import os
import sys
import subprocess
import base64
import requests
import datetime

# --- INSTALADOR DE EMERGENCIA INTERNO ---
def instalar_yfinance():
    try:
        import yfinance
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--no-cache-dir", "yfinance"])

instalar_yfinance()

import telebot
import yfinance as yf
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

bot = telebot.TeleBot(TOKEN, threaded=False)
portafolio = []

def get_price(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        stock = yf.Ticker(t)
        return round(float(stock.fast_info['last_price']), 2)
    except: return None

def cerebro_genesis(query, img_b64=None):
    if not OPENAI_API_KEY: return "🚨 Configura la KEY en Railway."
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    system_msg = (
        "Eres GÉNESIS V40. Terminal de Inteligencia. "
        "1. Analiza SMC en gráficas (BOS, OB, FVG). 2. Reporta Geopolítica. "
        "3. Emite alertas técnicas. Sin sermones."
    )
    payload = {
        "model": "gpt-4o",
        "messages": [{"role": "system", "content": system_msg}, {"role": "user", "content": query if query else "Analiza."}],
        "temperature": 0
    }
    if img_b64:
        payload["messages"][1]["content"] = [
            {"type": "text", "text": "ANALIZA LA GRÁFICA: Indica estructura y niveles técnicos."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Error de comunicación."

# --- INTERFAZ ---
@bot.message_handler(commands=['start'])
def start(message):
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    markup.add("📊 Rendimiento", "🚀 Operar", "🌍 Geopolítica", "🐋 Radar Ballenas", "⚠️ Alertas")
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V40: SISTEMA REESTABLECIDO**", reply_markup=markup)

@bot.message_handler(func=lambda m: m.text == "🌍 Geopolítica")
def geo(message):
    status = bot.reply_to(message, "📡 Escaneando noticias...")
    res = cerebro_genesis("Impacto geopolítico de hoy en Oro/DXY.")
    bot.edit_message_text(f"🌍 **GEOPOLÍTICA:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "🐋 Radar Ballenas")
def radar(message):
    status = bot.reply_to(message, "📡 Rastreando ballenas...")
    res = cerebro_genesis("Movimientos de ballenas (>1M USD) recientes.")
    bot.edit_message_text(f"🐋 **RADAR:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    f_info = bot.get_file(message.photo[-1].file_id)
    img_b64 = base64.b64encode(bot.download_file(f_info.file_path)).decode('utf-8')
    status = bot.reply_to(message, "🎯 Analizando niveles SMC...")
    res = cerebro_genesis(None, img_b64)
    bot.edit_message_text(f"🎯 **REPORTE SMC:**\n{res}", message.chat.id, status.message_id)

if __name__ == "__main__":
    bot.infinity_polling()
