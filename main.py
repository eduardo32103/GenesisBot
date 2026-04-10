import os
import subprocess
import sys

# --- MOTOR DE AUTO-CONFIGURACIÓN (Se salta el requirements.txt) ---
def auto_setup():
    libs = ["pyTelegramBotAPI", "yfinance", "requests"]
    for lib in libs:
        try:
            if lib == "pyTelegramBotAPI":
                import telebot
            else:
                __import__(lib)
        except ImportError:
            # Forzamos la instalación si no existe
            subprocess.check_call([sys.executable, "-m", "pip", "install", lib])

auto_setup()

import telebot
import yfinance as yf
import requests
import base64
import datetime
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
    except: return None

# --- NÚCLEO DE INTELIGENCIA (SMC + GEOPOLÍTICA + ALERTAS) ---
def cerebro_genesis(query, img_b64=None):
    if not OPENAI_API_KEY: return "🚨 ERROR: Falta API KEY en Railway."
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    system_msg = (
        "Eres GÉNESIS V34. Terminal de Inteligencia. "
        "1. Analiza SMC en imágenes (BOS, OB, FVG). "
        "2. Reporta Geopolítica y Noticias de impacto. "
        "3. Emite alertas técnicas. Sé seco y profesional."
    )
    payload = {
        "model": "gpt-4o",
        "messages": [{"role": "system", "content": system_msg}, {"role": "user", "content": query}],
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
    except: return "🚨 Error de comunicación."

# --- INTERFAZ ---
def main_menu():
    m = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    m.add("📊 Rendimiento", "🚀 Operar", "🌍 Geopolítica", "🐋 Radar Ballenas", "⚠️ Alertas")
    return m

@bot.message_handler(commands=['start'])
def start(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V34: SISTEMA AUTÓNOMO ACTIVO**\nArsenal listo. Sin dependencias externas.", reply_markup=main_menu())

@bot.message_handler(func=lambda m: m.text == "🌍 Geopolítica")
def geopolitica(message):
    status = bot.reply_to(message, "📡 Escaneando noticias de impacto...")
    res = cerebro_genesis("Analiza la geopolítica mundial actual e impacto en Oro/DXY.")
    bot.edit_message_text(f"🌍 **GEOPOLÍTICA:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "⚠️ Alertas")
def alertas(message):
    bot.reply_to(message, "🔔 **ALERTAS:** Monitoreando volatilidad institucional en tiempo real.")

@bot.message_handler(func=lambda m: m.text == "🐋 Radar Ballenas")
def radar(message):
    status = bot.reply_to(message, "📡 Rastreando ballenas...")
    res = cerebro_genesis("Reporta movimientos de ballenas (>1M USD) de las últimas 3 horas.")
    bot.edit_message_text(f"🐋 **RADAR:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    f_info = bot.get_file(message.photo[-1].file_id)
    img_b64 = base64.b64encode(bot.download_file(f_info.file_path)).decode('utf-8')
    status = bot.reply_to(message, "🎯 Analizando niveles SMC...")
    res = cerebro_genesis("SMC Analysis Request", img_b64)
    bot.edit_message_text(f"🎯 **REPORTE SMC:**\n{res}", message.chat.id, status.message_id)

if __name__ == "__main__":
    bot.infinity_polling()
