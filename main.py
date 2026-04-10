import os
import sys
import base64
import requests
import telebot
import datetime
from telebot import types

# --- CARGA DE LIBRERÍAS CON PREVENCIÓN DE ERRORES ---
try:
    import yfinance as yf
except ImportError:
    os.system('pip install yfinance')
    import yfinance as yf

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

bot = telebot.TeleBot(TOKEN, threaded=False)
portafolio = []

# --- MOTOR DE PRECIOS (VERIFICADO) ---
def obtener_precio_real(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        stock = yf.Ticker(t)
        # Intentamos obtener el último precio de cierre para mayor estabilidad
        data = stock.history(period="1d")
        if not data.empty:
            return round(float(data['Close'].iloc[-1]), 2)
        return None
    except Exception as e:
        print(f"Error en precio: {e}")
        return None

# --- CEREBRO DE INTELIGENCIA ---
def cerebro_genesis(query, img_b64=None):
    if not OPENAI_API_KEY: return "🚨 Falta API KEY en variables de Railway."
    
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    system_prompt = (
        "Eres GÉNESIS V24. Analista institucional. "
        "Tu misión es identificar niveles de SMC (BOS, CHoCH, OB, FVG) en gráficas. "
        "No des definiciones. No des consejos. Solo niveles y sesgo de mercado."
    )

    payload = {
        "model": "gpt-4o",
        "messages": [{"role": "system", "content": system_prompt}],
        "temperature": 0
    }

    if img_b64:
        payload["messages"].append({"role": "user", "content": [
            {"type": "text", "text": "Analiza esta gráfica. Dame niveles de liquidez y estructura técnica."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]})
    else:
        payload["messages"].append({"role": "user", "content": query})

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Error de respuesta de IA."

# --- HANDLERS DE COMANDOS ---
@bot.message_handler(commands=['start'])
def inicio(message):
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    markup.add("📊 Rendimiento", "🚀 Operar", "📈 Escáner SMT", "🐋 Radar Ballenas")
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V24: SISTEMA ESTABILIZADO**\nLibrerías verificadas. Operativo.", reply_markup=markup)

@bot.message_handler(func=lambda m: m.text == "🐋 Radar Ballenas")
def ballenas(message):
    status = bot.reply_to(message, "📡 Escaneando flujos de liquidez...")
    res = cerebro_genesis("Reporta movimientos de ballenas en las últimas 6 horas.")
    bot.edit_message_text(f"🐋 **RADAR:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "📊 Rendimiento")
def rend(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ No hay activos.")
        return
    status = bot.reply_to(message, "⏳ Consultando precios...")
    res = "📊 **REPORTE DE MERCADO:**\n"
    for o in portafolio:
        p = obtener_precio_real(o['t'])
        res += f"🔹 {o['t']}: ${p if p else 'N/A'}\n"
    bot.edit_message_text(res, message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text.lower().startswith("comprar "))
def comprar(message):
    try:
        partes = message.text.split()
        cant, ticker = float(partes[1]), partes[2].upper()
        precio = obtener_precio_real(ticker)
        if precio:
            portafolio.append({"t": ticker, "c": cant, "p": precio})
            bot.reply_to(message, f"✅ Registrado: {ticker} a ${precio}")
        else: bot.reply_to(message, "❌ Ticker no encontrado.")
    except: bot.reply_to(message, "Usa: Comprar 10 TSLA")

@bot.message_handler(content_types=['photo'])
def imagen(message):
    file_info = bot.get_file(message.photo[-1].file_id)
    downloaded = bot.download_file(file_info.file_path)
    img_b64 = base64.b64encode(downloaded).decode('utf-8')
    status = bot.reply_to(message, "🎯 Analizando gráfica institucional...")
    res = cerebro_genesis(None, img_b64)
    bot.edit_message_text(f"🎯 **ANÁLISIS SMC:**\n{res}", message.chat.id, status.message_id)

if __name__ == "__main__":
    bot.infinity_polling()
