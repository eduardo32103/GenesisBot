import os, base64, requests, telebot, yfinance as yf
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
bot = telebot.TeleBot(TOKEN, threaded=False)

# --- MOTOR DE PRECIOS ---
def obtener_precio(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        asset = yf.Ticker(t)
        data = asset.history(period="1d", interval="1m")
        return round(float(data['Close'].iloc[-1]), 2) if not data.empty else None
    except: return None

# --- CEREBRO GÉNESIS ---
def cerebro_genesis(query, mode="general", img_b64=None):
    if not OPENAI_API_KEY: return "🚨 ERROR: API KEY NO CONFIGURADA"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    
    prompts = {
        "ballenas": "Whale Alert: Lista movimientos >$10M (Origen -> Destino). Solo datos, sin texto de relleno.",
        "geo": "Analista Geopolítico: Reporta hechos que muevan mercado >2%. Impacto Oro/DXY. Solo hechos.",
        "smc": "Analista SMC: Identifica BOS, CHoCH, OB y FVG. Da precios exactos. Cero sermones.",
        "general": "Terminal GÉNESIS: Responde técnico y breve. Prohibido el relleno."
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
            {"type": "text", "text": "SMC MAP: Identifica BOS y niveles de liquidez."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 IA FUERA DE LÍNEA"

# --- INTERFAZ ---
@bot.message_handler(commands=['start'])
def start(message):
    m = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    m.add("📊 Precio Real", "🌍 Geopolítica", "🐋 Radar Ballenas", "🎯 Análisis SMC")
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V59: LISTO PARA EL DESPLIEGUE**", reply_markup=m)

@bot.message_handler(func=lambda m: m.text == "🌍 Geopolítica")
def btn_geo(message):
    status = bot.reply_to(message, "🌍 Escaneando impacto >2%...")
    res = cerebro_genesis("Eventos actuales de alto impacto.", mode="geo")
    bot.edit_message_text(f"🌍 **IMPACTO:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "🐋 Radar Ballenas")
def btn_ballenas(message):
    status = bot.reply_to(message, "🐋 Rastreando movimientos institucionales...")
    res = cerebro_genesis("Movimientos grandes recientes.", mode="ballenas")
    bot.edit_message_text(f"🐋 **WHALE ALERT:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    f_info = bot.get_file(message.photo[-1].file_id)
    img_b64 = base64.b64encode(bot.download_file(f_info.file_path)).decode('utf-8')
    status = bot.reply_to(message, "🎯 Mapeando niveles...")
    res = cerebro_genesis(None, mode="smc", img_b64=img_b64)
    bot.edit_message_text(f"🎯 **REPORTE SMC:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: True)
def ticker_handler(message):
    if len(message.text) <= 6:
        p = obtener_precio(message.text)
        if p: bot.reply_to(message, f"📈 {message.text.upper()}: **${p}**")

if __name__ == "__main__":
    bot.infinity_polling()
