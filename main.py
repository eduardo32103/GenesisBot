import os, base64, requests, telebot, yfinance as yf
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
bot = telebot.TeleBot(TOKEN, threaded=False)

# --- MOTOR DE PRECIOS (SIN FALLAS) ---
def obtener_precio(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        asset = yf.Ticker(t)
        # Obtenemos el precio actual de la forma más directa
        data = asset.history(period="1d", interval="1m")
        if not data.empty:
            return round(float(data['Close'].iloc[-1]), 2)
        return None
    except: return None

# --- CEREBRO GÉNESIS (ANTI-SERMONES) ---
def cerebro_genesis(query, mode="general", img_b64=None):
    if not OPENAI_API_KEY: return "🚨 ERROR: API KEY NO CONFIGURADA"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    
    # Instrucciones agresivas para que no te dé sermones
    prompts = {
        "ballenas": "Actúa como Whale Alert. Lista movimientos >$10M USD de las últimas horas (Wallet -> Exchange). Solo datos crudos.",
        "geo": "Analista Geopolítico. Reporta hechos críticos que muevan el mercado >2%. Impacto en Oro/DXY. Solo hechos.",
        "smc": "Analista SMC Pro. Identifica BOS, CHoCH, OB y FVG. Da precios exactos de entrada y liquidez. Sin sermones.",
        "general": "Terminal GÉNESIS. Solo responde con datos técnicos, cifras y hechos. Prohibido el relleno."
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
            {"type": "text", "text": "SMC LEVELS: Identify structure and Order Blocks."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 ERROR DE CONEXIÓN CON IA"

# --- INTERFAZ ---
@bot.message_handler(commands=['start'])
def start(message):
    m = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    m.add("📊 Precio Real", "🌍 Geopolítica", "🐋 Radar Ballenas", "🎯 Análisis SMC")
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V58: ONLINE**\nTerminal de datos lista.", reply_markup=m)

@bot.message_handler(func=lambda m: m.text == "🌍 Geopolítica")
def btn_geo(message):
    status = bot.reply_to(message, "🌍 Escaneando impacto geopolítico...")
    res = cerebro_genesis("Eventos actuales con impacto >2%.", mode="geo")
    bot.edit_message_text(f"🌍 **INFORME:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "🐋 Radar Ballenas")
def btn_ballenas(message):
    status = bot.reply_to(message, "🐋 Rastreando movimientos institucionales...")
    res = cerebro_genesis("Lista movimientos grandes recientes.", mode="ballenas")
    bot.edit_message_text(f"🐋 **WHALE ALERT:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "🎯 Análisis SMC")
def btn_smc_inst(message):
    bot.send_message(message.chat.id, "Mándame la captura de la gráfica para analizar niveles.")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    f_info = bot.get_file(message.photo[-1].file_id)
    img_b64 = base64.b64encode(bot.download_file(f_info.file_path)).decode('utf-8')
    status = bot.reply_to(message, "🎯 Mapeando niveles institucionales...")
    res = cerebro_genesis(None, mode="smc", img_b64=img_b64)
    bot.edit_message_text(f"🎯 **REPORTE TÉCNICO:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: True)
def ticker_handler(message):
    if len(message.text) <= 6:
        p = obtener_precio(message.text)
        if p: bot.reply_to(message, f"📈 {message.text.upper()}: **${p}**")
        else: bot.reply_to(message, "❌ No pude jalar datos reales.")

if __name__ == "__main__":
    bot.infinity_polling()
