import os, base64, requests, telebot, datetime, time, threading
import yfinance as yf
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MI_CHAT_ID = "6348873730" # Reemplaza con tu ID real de Telegram para las alertas automáticas
bot = telebot.TeleBot(TOKEN, threaded=True)

portafolio = []

# --- MOTOR DE PRECIOS ---
def obtener_precio(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        asset = yf.Ticker(t)
        data = asset.history(period="1d", interval="1m")
        return round(float(data['Close'].iloc[-1]), 2) if not data.empty else None
    except: return None

# --- CEREBRO GÉNESIS (SMC + ALARMAS) ---
def cerebro_genesis(query, mode="general", img_b64=None):
    if not OPENAI_API_KEY: return "🚨 ERROR: API KEY"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    
    prompts = {
        "alarma": "Eres un monitor de crisis. Analiza noticias de última hora. Si hay un evento que pueda mover el mercado >2%, genera una ALERTA ROJA breve. Si no, responde: 'CALMA'.",
        "smc": "Analista SMC Pro. Identifica BOS, CHoCH, OB y FVG. Da puntos de entrada y stop loss. Sin sermones.",
        "ballenas": "Whale Alert: Lista movimientos >$10M (Origen -> Destino). Solo datos.",
        "general": "Terminal GÉNESIS: Responde técnico y breve."
    }

    payload = {
        "model": "gpt-4o",
        "messages": [
            {"role": "system", "content": prompts.get(mode, prompts["general"])},
            {"role": "user", "content": query}
        ],
        "temperature": 0
    }

    if img_b64:
        payload["messages"][1]["content"] = [
            {"type": "text", "text": "SMC ANALYSIS: Identify Structure and Levels."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=45)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 ERROR DE CONEXIÓN"

# --- VIGILANTE GEOPOLÍTICO 24/7 (HILO SEPARADO) ---
def vigilante_24_7():
    while True:
        try:
            # La IA escanea eventos globales
            alerta = cerebro_genesis("Busca noticias de última hora que afecten Oro o BTC.", mode="alarma")
            if "ALERTA ROJA" in alerta.upper():
                bot.send_message(MI_CHAT_ID, f"⚠️ **ALERTA GEOPOLÍTICA 24/7:**\n{alerta}")
        except Exception as e:
            print(f"Error en vigilante: {e}")
        time.sleep(600) # Revisa cada 10 minutos para no saturar la API

# Iniciar el vigilante en segundo plano
threading.Thread(target=vigilante_24_7, daemon=True).start()

# --- INTERFAZ ---
@bot.message_handler(commands=['start'])
def start(message):
    m = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    m.add("📊 Precio Real", "🚀 Operar", "🌍 Geopolítica", "🐋 Radar Ballenas", "📋 Portafolio")
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V55: VIGILANCIA 24/7 ACTIVADA**", reply_markup=m)

@bot.message_handler(func=lambda m: m.text == "🌍 Geopolítica")
def btn_geo(message):
    status = bot.reply_to(message, "🌍 Analizando impacto inmediato...")
    res = cerebro_genesis("Resumen de impacto en mercado >2%.", mode="general")
    bot.edit_message_text(f"🌍 **INFORME:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "🐋 Radar Ballenas")
def btn_ballenas(message):
    status = bot.reply_to(message, "🐋 Escaneando Ledger...")
    res = cerebro_genesis("Movimientos ballenas últimas 6h.", mode="ballenas")
    bot.edit_message_text(f"🐋 **RADAR:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "🚀 Operar")
def btn_operar(message):
    bot.send_message(message.chat.id, "Usa: COMPRA [Ticker] [Cantidad]")

@bot.message_handler(func=lambda m: m.text.startswith("COMPRA"))
def registrar_compra(message):
    try:
        partes = message.text.split()
        t, c = partes[1].upper(), float(partes[2])
        p = obtener_precio(t)
        if p:
            portafolio.append({"t": t, "c": c, "p": p, "f": datetime.datetime.now().strftime("%H:%M")})
            bot.reply_to(message, f"✅ REGISTRADO: {c} {t} a ${p}")
        else: bot.reply_to(message, "❌ Precio no disponible.")
    except: bot.reply_to(message, "❌ Formato: COMPRA BTC 0.5")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    f_info = bot.get_file(message.photo[-1].file_id)
    img_b64 = base64.b64encode(bot.download_file(f_info.file_path)).decode('utf-8')
    status = bot.reply_to(message, "🎯 Analizando SMC...")
    res = cerebro_genesis(None, mode="smc", img_b64=img_b64)
    bot.edit_message_text(f"🎯 **REPORTE TÉCNICO:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: True)
def default(message):
    if len(message.text) <= 6:
        p = obtener_precio(message.text)
        if p: bot.reply_to(message, f"📈 {message.text.upper()}: **${p}**")

if __name__ == "__main__":
    bot.infinity_polling()
