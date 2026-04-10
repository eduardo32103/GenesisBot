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

# --- MOTOR DE PRECIOS (SIN SERMONES) ---
def obtener_precio_v53(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        session = requests.Session()
        session.headers.update({'User-Agent': 'Mozilla/5.0'})
        asset = yf.Ticker(t, session=session)
        data = asset.history(period="1d", interval="1m")
        return round(float(data['Close'].iloc[-1]), 2) if not data.empty else None
    except: return None

# --- CEREBRO GÉNESIS (SMC + GEO + BALLENAS) ---
def cerebro_genesis(query, mode="general", img_b64=None):
    if not OPENAI_API_KEY: return "🚨 ERROR: API KEY NO DETECTADA"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    
    # INSTRUCCIONES ESTRICTAS PARA EVITAR SERMONES
    if mode == "ballenas":
        prompt = "Actúa como Whale Alert. Solo lista movimientos grandes de las últimas horas. Ejemplo: '5,000 BTC movidos de Wallet desconocida a Binance'. Prohibido dar consejos o introducciones."
    elif mode == "geo":
        prompt = "Resumen geopolítico flash. Solo hechos e impacto en Oro/DXY. Máximo 3 puntos clave. Sin sermones."
    else:
        prompt = "Analista SMC puro. Identifica BOS, OB y FVG. Solo datos técnicos."

    payload = {
        "model": "gpt-4o",
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": query if query else "Analiza."}
        ],
        "temperature": 0
    }

    if img_b64:
        payload["messages"][1]["content"] = [
            {"type": "text", "text": "SMC LEVELS ONLY: BOS, FVG, OB."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 IA ERROR: CONNECTION FAILED"

# --- INTERFAZ ---
@bot.message_handler(commands=['start'])
def start(message):
    m = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    m.add("📊 Precio Real", "🌍 Geopolítica", "🐋 Radar Ballenas", "⚠️ Alertas")
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V53: DATA TERMINAL**\nSelecciona radar.", reply_markup=m)

@bot.message_handler(func=lambda m: m.text == "🐋 Radar Ballenas")
def btn_ballenas(message):
    status = bot.reply_to(message, "📡 Escaneando Ledger Institucional...")
    res = cerebro_genesis("Lista los movimientos >$10M USD de las últimas 12h.", mode="ballenas")
    bot.edit_message_text(f"🐋 **WHALE ALERT:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "🌍 Geopolítica")
def btn_geo(message):
    status = bot.reply_to(message, "🌍 Flash Geopolítico...")
    res = cerebro_genesis("Hechos clave hoy e impacto DXY/XAU.", mode="geo")
    bot.edit_message_text(f"🌍 **IMPACTO:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "📊 Precio Real")
def btn_precio(message):
    bot.send_message(message.chat.id, "TICKER:")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    f_info = bot.get_file(message.photo[-1].file_id)
    img_b64 = base64.b64encode(bot.download_file(f_info.file_path)).decode('utf-8')
    status = bot.reply_to(message, "🎯 Mapping SMC...")
    res = cerebro_genesis(None, img_b64=img_b64)
    bot.edit_message_text(f"🎯 **LEVELS:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: True)
def ticker_handler(message):
    if len(message.text) <= 6:
        p = obtener_precio_v53(message.text)
        if p: bot.reply_to(message, f"📈 {message.text.upper()}: **${p}**")
        else: bot.reply_to(message, "❌ DATA ERROR")

if __name__ == "__main__":
    bot.infinity_polling()
