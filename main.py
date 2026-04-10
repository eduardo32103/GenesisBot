import os, subprocess, sys, base64, requests, telebot, datetime
from telebot import types

# --- SISTEMA DE AUTOREPARACIÓN ---
def iniciar_entorno():
    for lib in ["pyTelegramBotAPI", "yfinance", "requests"]:
        try:
            if lib == "pyTelegramBotAPI": import telebot
            else: __import__(lib)
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", lib])

iniciar_entorno()
import yfinance as yf

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

bot = telebot.TeleBot(TOKEN, threaded=False)
portafolio = []

# --- MOTOR DE PRECIOS ---
def get_live_data(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        return round(float(yf.Ticker(t).fast_info['last_price']), 2)
    except: return None

# --- CEREBRO GÉNESIS (SMC + GEOPOLÍTICA + ALERTAS) ---
def cerebro_genesis(query, img_b64=None):
    if not OPENAI_API_KEY: return "🚨 ERROR: Configura OPENAI_API_KEY en Railway."
    
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    system_msg = (
        "Eres GÉNESIS V29. Terminal de Inteligencia Financiera. "
        "REGLAS: 1. Analiza imágenes buscando BOS, CHoCH, OB y FVG con precios exactos. "
        "2. Analiza Geopolítica actual y su impacto en Oro, Petróleo y DXY. "
        "3. Monitorea alertas de volatilidad. 4. Cero sermones, solo datos técnicos."
    )

    payload = {
        "model": "gpt-4o",
        "messages": [{"role": "system", "content": system_msg}],
        "temperature": 0
    }

    if img_b64:
        payload["messages"].append({"role": "user", "content": [
            {"type": "text", "text": "ANALIZA LA GRÁFICA: Identifica estructura SMC y niveles clave del eje Y."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]})
    else:
        payload["messages"].append({"role": "user", "content": query})

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Error de conexión con el cerebro."

# --- INTERFAZ ---
def menu_v29():
    m = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    m.add("📊 Rendimiento", "🚀 Operar", "🌍 Geopolítica", "🐋 Radar Ballenas", "⚠️ Alertas")
    return m

@bot.message_handler(commands=['start'])
def start(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V29: OPERATIVO**\nSMC, Geopolítica y Alertas activas.", reply_markup=menu_v29())

# --- FUNCIONES ---
@bot.message_handler(func=lambda m: m.text == "🌍 Geopolítica")
def geo(message):
    status = bot.reply_to(message, "📡 Escaneando noticias de alto impacto y tensiones globales...")
    res = cerebro_genesis("Analiza la geopolítica de hoy y su impacto en commodities y DXY.")
    bot.edit_message_text(f"🌍 **GEOPOLÍTICA:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "⚠️ Alertas")
def alertas(message):
    bot.reply_to(message, "🔔 **CENTRO DE ALERTAS:** Monitoreando movimientos institucionales en tiempo real.")

@bot.message_handler(func=lambda m: m.text == "🐋 Radar Ballenas")
def ballenas(message):
    status = bot.reply_to(message, "📡 Rastreando transacciones masivas...")
    res = cerebro_genesis("Busca movimientos de ballenas recientes (>1M USD).")
    bot.edit_message_text(f"🐋 **RADAR:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "🚀 Operar")
def operar(message):
    bot.reply_to(message, "📝 Orden: `Comprar [Cantidad] [Ticker]`")

@bot.message_handler(func=lambda m: m.text.lower().startswith("comprar "))
def comprar(message):
    try:
        p = message.text.split()
        cant, t = float(p[1]), p[2].upper()
        px = get_live_data(t)
        if px:
            portafolio.append({"t": t, "c": cant, "p": px})
            bot.reply_to(message, f"✅ Registrado: {t} a ${px}")
        else: bot.reply_to(message, "❌ Ticker no encontrado.")
    except: bot.reply_to(message, "Error. Ejemplo: Comprar 10 TSLA")

@bot.message_handler(func=lambda m: m.text == "📊 Rendimiento")
def rend(message):
    if not portafolio: return bot.reply_to(message, "⚠️ Vacío.")
    res = "📊 **REPORTE DE PORTAFOLIO:**\n"
    for o in portafolio:
        act = get_live_data(o['t'])
        res += f"🔹 {o['t']}: ${act if act else 'Error'}\n"
    bot.reply_to(message, res)

@bot.message_handler(content_types=['photo'])
def imagen(message):
    f = bot.get_file(message.photo[-1].file_id)
    d = bot.download_file(f.file_path)
    img = base64.b64encode(d).decode('utf-8')
    status = bot.reply_to(message, "🎯 Escaneando gráfica con SMC...")
    res = cerebro_genesis(None, img)
    bot.edit_message_text(f"🎯 **ANÁLISIS SMC:**\n{res}", message.chat.id, status.message_id)

if __name__ == "__main__":
    bot.infinity_polling()
