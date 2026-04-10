import os
import subprocess
import sys
import base64
import requests
import telebot
from telebot import types

# --- ESCUDO DE INSTALACIÓN (Fuerza Bruta) ---
def forzar_librerias():
    libs = ["pyTelegramBotAPI", "yfinance", "requests"]
    for lib in libs:
        try:
            __import__(lib.replace("pyTelegramBotAPI", "telebot"))
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", lib])

forzar_librerias()
import yfinance as yf

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

bot = telebot.TeleBot(TOKEN, threaded=False)
portafolio = []

# --- MOTOR DE DATOS REALES ---
def get_stock_price(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        data = yf.Ticker(t).fast_info
        return round(float(data['last_price']), 2)
    except: return None

# --- NÚCLEO DE INTELIGENCIA (Radar y Análisis) ---
def cerebro_genesis(query, img_b64=None):
    if not OPENAI_API_KEY: return "🚨 ERROR: Falta OPENAI_API_KEY."
    
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    system_msg = "Eres GÉNESIS. Terminal de datos. Cero sermones. Solo niveles de SMC y flujos de ballenas."

    payload = {
        "model": "gpt-4o",
        "messages": [{"role": "system", "content": system_msg}],
        "temperature": 0
    }

    if img_b64:
        payload["messages"].append({"role": "user", "content": [
            {"type": "text", "text": "Analiza niveles de BOS, CHoCH y FVG en esta gráfica."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]})
    else:
        payload["messages"].append({"role": "user", "content": query})

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Error de conexión con el cerebro."

# --- HANDLERS ---
@bot.message_handler(commands=['start'])
def start(message):
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    markup.add("📊 Rendimiento", "🚀 Operar", "📈 Escáner SMT", "🐋 Radar Ballenas")
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V23: SISTEMA BLINDADO**", reply_markup=markup)

@bot.message_handler(func=lambda m: m.text == "🐋 Radar Ballenas")
def radar(message):
    status = bot.reply_to(message, "📡 Rastreando dinero institucional...")
    res = cerebro_genesis("Busca transacciones de ballenas (>1M USD) recientes en BTC/ETH.")
    bot.edit_message_text(f"🐋 **RADAR:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "📊 Rendimiento")
def rend(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ Vacío.")
        return
    status = bot.reply_to(message, "⏳ Verificando precios reales...")
    res = "📊 **REPORTE:**\n"
    for o in portafolio:
        p = get_stock_price(o['t'])
        res += f"🔹 {o['t']}: ${p if p else 'Error'}\n"
    bot.edit_message_text(res, message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text.lower().startswith("comprar "))
def buy(message):
    try:
        p = message.text.split()
        cant, ticker = float(p[1]), p[2].upper()
        price = get_stock_price(ticker)
        if price:
            portafolio.append({"t": ticker, "c": cant, "p": price})
            bot.reply_to(message, f"✅ Registrado: {ticker} a ${price}")
    except: bot.reply_to(message, "Error. Usa: Comprar 10 TSLA")

@bot.message_handler(content_types=['photo'])
def photo(message):
    file_info = bot.get_file(message.photo[-1].file_id)
    downloaded = bot.download_file(file_info.file_path)
    img_b64 = base64.b64encode(downloaded).decode('utf-8')
    status = bot.reply_to(message, "🎯 Analizando gráfica...")
    res = cerebro_genesis(None, img_b64)
    bot.edit_message_text(f"🎯 **ANÁLISIS:**\n{res}", message.chat.id, status.message_id)

if __name__ == "__main__":
    bot.infinity_polling()
