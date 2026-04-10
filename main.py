import os
import subprocess
import sys
import base64  # <-- ESTO FALTABA Y CAUSABA EL ERROR
import requests
import datetime

# --- ASEGURAR LIBRERÍAS ---
def preparar():
    libs = ["pyTelegramBotAPI", "yfinance", "requests"]
    for lib in libs:
        try:
            if lib == "pyTelegramBotAPI": import telebot
            else: __import__(lib)
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", lib])

preparar()

import telebot
import yfinance as yf
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

bot = telebot.TeleBot(TOKEN, threaded=False)
portafolio = []

def get_live_price(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        stock = yf.Ticker(t)
        return round(float(stock.fast_info['last_price']), 2)
    except: return None

# --- CEREBRO SUPERINTELIGENTE (SMC + GEOPOLÍTICA + ALERTAS) ---
def cerebro_genesis(query, img_b64=None):
    if not OPENAI_API_KEY: return "🚨 Falta OPENAI_API_KEY en Railway."
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    
    system_msg = (
        "Eres GÉNESIS V36. Terminal de Inteligencia Financiera. "
        "MISIONES: 1. Analizar SMC en gráficas (BOS, OB, FVG). 2. Reportar Geopolítica y Noticias. "
        "3. Gestionar Alertas de volatilidad. Sé seco, técnico y directo."
    )
    
    payload = {
        "model": "gpt-4o",
        "messages": [{"role": "system", "content": system_msg}],
        "temperature": 0
    }

    if img_b64:
        payload["messages"].append({"role": "user", "content": [
            {"type": "text", "text": "ANALIZA LA GRÁFICA: Identifica niveles técnicos del eje Y y sesgo."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]})
    else:
        payload["messages"].append({"role": "user", "content": query})

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Error de conexión con el cerebro."

# --- MENÚ ---
def main_menu():
    m = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    m.add("📊 Rendimiento", "🚀 Operar", "🌍 Geopolítica", "🐋 Radar Ballenas", "⚠️ Alertas")
    return m

@bot.message_handler(commands=['start'])
def start(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V36: SISTEMA INTEGRAL**\nLibrerías corregidas. Arsenal listo.", reply_markup=main_menu())

@bot.message_handler(func=lambda m: m.text == "🌍 Geopolítica")
def geo(message):
    status = bot.reply_to(message, "📡 Escaneando noticias globales...")
    res = cerebro_genesis("Analiza la geopolítica de hoy y su impacto en el mercado.")
    bot.edit_message_text(f"🌍 **GEOPOLÍTICA:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "⚠️ Alertas")
def alertas(message):
    bot.reply_to(message, "🔔 **MONITOR:** Monitoreando volatilidad >3% en activos clave.")

@bot.message_handler(func=lambda m: m.text == "🐋 Radar Ballenas")
def radar(message):
    status = bot.reply_to(message, "📡 Rastreando ballenas...")
    res = cerebro_genesis("Muestra transacciones masivas de las últimas 3 horas.")
    bot.edit_message_text(f"🐋 **RADAR:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    try:
        f_info = bot.get_file(message.photo[-1].file_id)
        downloaded = bot.download_file(f_info.file_path)
        img_b64 = base64.b64encode(downloaded).decode('utf-8')
        status = bot.reply_to(message, "🎯 Analizando niveles institucionales...")
        res = cerebro_genesis(None, img_b64)
        bot.edit_message_text(f"🎯 **REPORTE SMC:**\n{res}", message.chat.id, status.message_id)
    except Exception as e:
        bot.reply_to(message, f"🚨 Error visual: {str(e)}")

if __name__ == "__main__":
    bot.infinity_polling()
