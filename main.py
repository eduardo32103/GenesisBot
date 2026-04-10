import os
import subprocess
import sys

# --- MOTOR DE AUTO-INSTALACIÓN (CERO ERRORES) ---
def preparar_sistema():
    librerias = ["pyTelegramBotAPI", "yfinance", "requests"]
    for lib in librerias:
        try:
            if lib == "pyTelegramBotAPI":
                import telebot
            else:
                __import__(lib)
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", lib])

preparar_sistema()

import telebot
import yfinance as yf
import requests
import base64
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

bot = telebot.TeleBot(TOKEN, threaded=False)
portafolio = []

# --- MOTOR DE PRECIOS ---
def obtener_precio(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        stock = yf.Ticker(t)
        # Usamos fast_info para evitar que Railway se cuelgue
        precio = stock.fast_info['last_price']
        return round(float(precio), 2)
    except:
        return None

# --- CEREBRO INTELIGENTE ---
def cerebro_ia(query, img_b64=None):
    if not OPENAI_API_KEY: return "🚨 Configura la KEY en Railway."
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    
    msg_content = [{"type": "text", "text": query}]
    if img_b64:
        msg_content.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}})

    payload = {
        "model": "gpt-4o",
        "messages": [
            {"role": "system", "content": "Eres GÉNESIS. Terminal de trading. Solo das niveles técnicos (BOS, FVG, OB) y precios. Sin rodeos."},
            {"role": "user", "content": msg_content}
        ],
        "temperature": 0
    }
    
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except:
        return "🚨 Error de conexión con la IA."

# --- INTERFAZ ---
@bot.message_handler(commands=['start'])
def start(message):
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    markup.add("📊 Rendimiento", "🚀 Operar", "📈 Escáner SMT", "🐋 Radar Ballenas")
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V26: SISTEMA REPARADO**", reply_markup=markup)

@bot.message_handler(func=lambda m: m.text == "🚀 Operar")
def operar(message):
    bot.reply_to(message, "Escribe: `Comprar 10 TSLA` o `Vender 1 BTC`")

@bot.message_handler(func=lambda m: m.text.lower().startswith("comprar "))
def comprar(message):
    try:
        p = message.text.split()
        cant, ticker = float(p[1]), p[2].upper()
        px = obtener_precio(ticker)
        if px:
            portafolio.append({"t": ticker, "c": cant, "p": px})
            bot.reply_to(message, f"✅ Registrado: {ticker} a ${px}")
        else:
            bot.reply_to(message, "❌ Ticker no encontrado.")
    except:
        bot.reply_to(message, "❌ Usa: Comprar 10 NVDA")

@bot.message_handler(func=lambda m: m.text == "📊 Rendimiento")
def rendimiento(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ No hay trades.")
        return
    status = bot.reply_to(message, "⏳ Consultando mercado...")
    res = "📊 **REPORTE:**\n"
    for o in portafolio:
        act = obtener_precio(o['t'])
        res += f"🔹 {o['t']}: ${act if act else 'Error'}\n"
    bot.edit_message_text(res, message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "🐋 Radar Ballenas")
def radar(message):
    bot.reply_to(message, cerebro_ia("Busca transacciones de ballenas recientes de alto volumen."))

@bot.message_handler(content_types=['photo'])
def imagen(message):
    f_info = bot.get_file(message.photo[-1].file_id)
    downloaded = bot.download_file(f_info.file_path)
    img_b64 = base64.b64encode(downloaded).decode('utf-8')
    status = bot.reply_to(message, "🎯 Analizando gráfica...")
    res = cerebro_ia("Identifica BOS, CHoCH y FVG en esta imagen con sus precios.", img_b64)
    bot.edit_message_text(f"🎯 **ANÁLISIS TÉCNICO:**\n{res}", message.chat.id, status.message_id)

if __name__ == "__main__":
    bot.infinity_polling()
