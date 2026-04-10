import os, base64, requests, telebot, datetime, time, threading
import yfinance as yf
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MI_CHAT_ID = "6348873730" 
bot = telebot.TeleBot(TOKEN, threaded=True)

# Lista de activos para el motor de vigilancia
WATCHLIST = ["BTC-USD", "GC=F", "DX-Y.NYB", "NVDA"]

# --- MOTOR DE PRECIOS LIMPIO ---
def obtener_precio(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        asset = yf.Ticker(t)
        # Pedimos solo el último cierre para no saturar
        data = asset.history(period="1d", interval="1m")
        return round(float(data['Close'].iloc[-1]), 2) if not data.empty else None
    except: return None

# --- CEREBRO GÉNESIS (SISTEMA DE DATOS CRUDOS) ---
def cerebro_genesis(query, mode="general", img_b64=None):
    if not OPENAI_API_KEY: return "🚨 ERROR: API KEY"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    
    # Instrucciones cortas para evitar sermones
    prompts = {
        "alarma": "Radar de crisis. Analiza noticias. Si hay impacto >2%, suelta ALERTA ROJA y el hecho. Si no, di CALMA. Cero sermones.",
        "smc": "Analista SMC. Solo niveles: BOS, CHoCH, OB, FVG y Coordenadas. Prohibido texto de relleno.",
        "general": "Terminal Financiera. Solo hechos, cifras y datos técnicos directos."
    }

    payload = {
        "model": "gpt-4o",
        "messages": [
            {"role": "system", "content": prompts.get(mode, prompts["general"])},
            {"role": "user", "content": query}
        ],
        "temperature": 0 # Para que no invente historias
    }

    if img_b64:
        payload["messages"][1]["content"] = [
            {"type": "text", "text": "MAP SMC LEVELS: BOS, OB, FVG. PRICE TARGETS ONLY."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=40)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 ERROR CONEXIÓN IA"

# --- VIGILANTE 24/7 ---
def vigilante_activo():
    while True:
        try:
            # Busca noticias de impacto real cada 10 min
            res = cerebro_genesis("Busca eventos geopolíticos de impacto >2% en Oro/BTC.", mode="alarma")
            if "ALERTA" in res.upper():
                bot.send_message(MI_CHAT_ID, f"⚠️ **VIGILANCIA 24/7:**\n{res}")
        except: pass
        time.sleep(600)

threading.Thread(target=vigilante_activo, daemon=True).start()

# --- INTERFAZ ---
@bot.message_handler(commands=['start'])
def start(message):
    m = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    m.add("📊 Precio Real", "🌍 Geopolítica", "🐋 Radar Ballenas", "📋 Watchlist")
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V57: SISTEMA REESTABLECIDO**\nSin sermones. Solo datos.", reply_markup=m)

@bot.message_handler(func=lambda m: m.text == "🌍 Geopolítica")
def btn_geo(message):
    status = bot.reply_to(message, "🌍 Escaneando impacto real...")
    res = cerebro_genesis("Hechos geopolíticos hoy e impacto >2%.", mode="general")
    bot.edit_message_text(f"🌍 **IMPACTO:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "🐋 Radar Ballenas")
def btn_ballenas(message):
    status = bot.reply_to(message, "🐋 Rastreando Ledger...")
    res = cerebro_genesis("Lista movimientos ballenas >$10M últimas 12h.", mode="general")
    bot.edit_message_text(f"🐋 **RADAR:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "📋 Watchlist")
def btn_watch(message):
    res = "📋 **VIGILANCIA EN VIVO:**\n"
    for t in WATCHLIST:
        p = obtener_precio(t)
        res += f"• {t}: **${p if p else 'N/A'}**\n"
    bot.send_message(message.chat.id, res)

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    f_info = bot.get_file(message.photo[-1].file_id)
    img_b64 = base64.b64encode(bot.download_file(f_info.file_path)).decode('utf-8')
    status = bot.reply_to(message, "🎯 Escaneando SMC...")
    res = cerebro_genesis(None, mode="smc", img_b64=img_b64)
    bot.edit_message_text(f"🎯 **NIVELES:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: True)
def default(message):
    if len(message.text) <= 6:
        p = obtener_precio(message.text)
        if p: bot.reply_to(message, f"📈 {message.text.upper()}: **${p}**")

if __name__ == "__main__":
    bot.infinity_polling()
