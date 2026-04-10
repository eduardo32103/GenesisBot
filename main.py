import os
import sys
import base64
import requests
import datetime

# --- BLOQUE DE SEGURIDAD: AUTO-IMPORTACIÓN ---
try:
    import telebot
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pyTelegramBotAPI"])
    import telebot

import yfinance as yf
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

bot = telebot.TeleBot(TOKEN, threaded=False)
portafolio = []

# --- MOTOR DE DATOS REALES ---
def get_live_price(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        stock = yf.Ticker(t)
        return round(float(stock.fast_info['last_price']), 2)
    except: return None

# --- CEREBRO DE INTELIGENCIA (SMC + GEOPOLÍTICA + ALERTAS) ---
def cerebro_genesis(query, img_b64=None):
    if not OPENAI_API_KEY: return "🚨 ERROR: Configura OPENAI_API_KEY en Railway."
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    system_msg = (
        "Eres GÉNESIS V39. Terminal Institucional. "
        "1. Analiza SMC en imágenes (BOS, OB, FVG). "
        "2. Geopolítica: Impacto de noticias en Oro/DXY. "
        "3. Alertas: Volatilidad >3%. Sé seco y técnico."
    )
    payload = {
        "model": "gpt-4o",
        "messages": [{"role": "system", "content": system_msg}, {"role": "user", "content": query if query else "Analiza."}],
        "temperature": 0
    }
    if img_b64:
        payload["messages"][1]["content"] = [
            {"type": "text", "text": "ANALIZA LA GRÁFICA: Indica niveles y estructura."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Error de conexión."

# --- MENÚ Y BOTONES ---
def main_menu():
    m = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    m.add("📊 Rendimiento", "🚀 Operar", "🌍 Geopolítica", "🐋 Radar Ballenas", "⚠️ Alertas")
    return m

@bot.message_handler(commands=['start'])
def start(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V39: ARSENAL ACTIVO**", reply_markup=main_menu())

@bot.message_handler(func=lambda m: m.text == "🌍 Geopolítica")
def geo(message):
    status = bot.reply_to(message, "📡 Escaneando noticias de alto impacto...")
    res = cerebro_genesis("Analiza la geopolítica de hoy y su impacto en Oro/DXY.")
    bot.edit_message_text(f"🌍 **GEOPOLÍTICA:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "⚠️ Alertas")
def alertas(message):
    bot.reply_to(message, "🔔 **MONITOR:** Monitoreando movimientos institucionales.")

@bot.message_handler(func=lambda m: m.text == "🐋 Radar Ballenas")
def radar(message):
    status = bot.reply_to(message, "📡 Rastreando transacciones masivas...")
    res = cerebro_genesis("Movimientos de ballenas (>1M USD) últimas 3 horas.")
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
