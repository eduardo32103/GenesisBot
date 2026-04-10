import os, base64, requests, telebot, datetime
import yfinance as yf
import pandas as pd
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
bot = telebot.TeleBot(TOKEN, threaded=False)

# Memoria volátil para registro de compras (Se borra si reinicias Railway)
portafolio = []

# --- MOTOR DE PRECIOS ---
def obtener_precio(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        asset = yf.Ticker(t)
        precio = asset.fast_info.get('last_price') or asset.history(period="1d")['Close'].iloc[-1]
        return round(float(precio), 2)
    except: return None

# --- CEREBRO GÉNESIS V54 ---
def cerebro_genesis(query, mode="general", img_b64=None):
    if not OPENAI_API_KEY: return "🚨 ERROR: API KEY"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    
    prompts = {
        "ballenas": "Whale Alert Mode: Lista movimientos >$10M (Origen -> Destino). Solo datos.",
        "geo": "Analista Geopolítico: Reporta hechos. Evalúa si el impacto moverá el mercado >2%. Solo hechos.",
        "smc": "SMC Expert: Identifica BOS, CHoCH, Order Blocks y FVG. Da soportes y resistencias exactos.",
        "general": "Terminal GÉNESIS: Responde de forma técnica y breve. Sin sermones."
    }

    payload = {
        "model": "gpt-4o",
        "messages": [
            {"role": "system", "content": prompts.get(mode, prompts["general"])},
            {"role": "user", "content": query if query else "Analiza."}
        ],
        "temperature": 0
    }

    if img_b64:
        payload["messages"][1]["content"] = [
            {"type": "text", "text": "ANALIZA GRÁFICA: Identifica estructura SMC, soportes y resistencias."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 IA ERROR"

# --- INTERFAZ ---
@bot.message_handler(commands=['start'])
def start(message):
    m = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    m.add("📊 Precio Real", "🚀 Operar", "🌍 Geopolítica", "🐋 Radar Ballenas", "📋 Portafolio")
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V54: TERMINAL ACTIVA**", reply_markup=m)

@bot.message_handler(func=lambda m: m.text == "🌍 Geopolítica")
def btn_geo(message):
    status = bot.reply_to(message, "📡 Escaneando noticias de alto impacto (>2%)...")
    res = cerebro_genesis("Analiza noticias actuales. ¿Hay riesgo de movimiento >2% en XAU/DXY?", mode="geo")
    bot.edit_message_text(f"🌍 **IMPACTO GEOPOLÍTICO:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "🐋 Radar Ballenas")
def btn_ballenas(message):
    status = bot.reply_to(message, "🐋 Rastreando transacciones institucionales...")
    res = cerebro_genesis("Lista movimientos de ballenas de las últimas 6 horas.", mode="ballenas")
    bot.edit_message_text(f"🐋 **DATA CRUDA:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "🚀 Operar")
def btn_operar(message):
    bot.send_message(message.chat.id, "Formato: COMPRA [Ticker] [Cantidad]\nEjemplo: COMPRA BTC 0.5")

@bot.message_handler(func=lambda m: m.text.startswith("COMPRA"))
def registrar_compra(message):
    try:
        partes = message.text.split()
        ticker = partes[1].upper()
        cantidad = float(partes[2])
        precio = obtener_precio(ticker)
        if precio:
            op = {"ticker": ticker, "cantidad": cantidad, "precio": precio, "fecha": datetime.datetime.now().strftime("%d/%m %H:%M")}
            portafolio.append(op)
            bot.reply_to(message, f"✅ **REGISTRADO:**\n{cantidad} {ticker} a ${precio}")
        else: bot.reply_to(message, "❌ Error de precio.")
    except: bot.reply_to(message, "❌ Usa: COMPRA TICKER CANTIDAD")

@bot.message_handler(func=lambda m: m.text == "📋 Portafolio")
def ver_portafolio(message):
    if not portafolio:
        bot.send_message(message.chat.id, "Portafolio vacío.")
        return
    res = "📋 **TUS POSICIONES:**\n"
    for o in portafolio:
        res += f"• {o['cantidad']} {o['ticker']} @ ${o['precio']} ({o['fecha']})\n"
    bot.send_message(message.chat.id, res)

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    f_info = bot.get_file(message.photo[-1].file_id)
    img_b64 = base64.b64encode(bot.download_file(f_info.file_path)).decode('utf-8')
    status = bot.reply_to(message, "🎯 Ejecutando escaneo SMC...")
    res = cerebro_genesis(None, mode="smc", img_b64=img_b64)
    bot.edit_message_text(f"🎯 **ANÁLISIS TÉCNICO:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: True)
def default_handler(message):
    if len(message.text) <= 6:
        p = obtener_precio(message.text)
        if p: bot.reply_to(message, f"📈 {message.text.upper()}: **${p}**")
        else: bot.reply_to(message, "❌ Ticker no reconocido.")

if __name__ == "__main__":
    bot.infinity_polling()
