import os, subprocess, sys, base64, requests, telebot, datetime
from telebot import types

# --- BOOTSTRAP (Instalación Forzada) ---
def instalar():
    for lib in ["pyTelegramBotAPI", "yfinance", "requests"]:
        try:
            if lib == "pyTelegramBotAPI": import telebot
            else: __import__(lib)
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", lib])

instalar()
import yfinance as yf

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

bot = telebot.TeleBot(TOKEN, threaded=False)
portafolio = []

# --- MOTOR DE DATOS REALES ---
def get_price(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        return round(float(yf.Ticker(t).fast_info['last_price']), 2)
    except: return None

# --- NÚCLEO DE INTELIGENCIA (SMC + GEOPOLÍTICA) ---
def cerebro_genesis(query, img_b64=None):
    if not OPENAI_API_KEY: return "🚨 ERROR: Configura OPENAI_API_KEY en Railway."
    
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    system_msg = (
        "Eres GÉNESIS V27, IA de Inteligencia Financiera y Geopolítica. "
        "REGLAS: 1. No sermones. 2. Verifica niveles de SMC (BOS, OB, FVG) en imágenes. "
        "3. Analiza el impacto de noticias geopolíticas en el mercado (Oro, Petróleo, Índices). "
        "4. Sé seco, técnico y 100% preciso."
    )

    payload = {
        "model": "gpt-4o",
        "messages": [{"role": "system", "content": system_msg}],
        "temperature": 0
    }

    if img_b64:
        payload["messages"].append({"role": "user", "content": [
            {"type": "text", "text": "ANALIZA ESTA GRÁFICA: Indica sesgo, BOS, CHoCH y FVG con precios del eje Y. Sé exacto."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]})
    else:
        payload["messages"].append({"role": "user", "content": query})

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Error de respuesta del cerebro."

# --- INTERFAZ ---
def menu():
    m = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    m.add("📊 Rendimiento", "🚀 Operar", "🌍 Geopolítica VIVO", "🐋 Radar Ballenas", "📈 Escáner SMT")
    return m

@bot.message_handler(commands=['start'])
def start(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V27: ARSENAL COMPLETO**\nSMC, Geopolítica y Radar Ballenas activos.", reply_markup=menu())

# --- FUNCIONES DE BOTONES ---
@bot.message_handler(func=lambda m: m.text == "🌍 Geopolítica VIVO")
def geopolitica(message):
    status = bot.reply_to(message, "📡 Escaneando tensiones globales y mercado de commodities...")
    res = cerebro_genesis("Dame un reporte de las 3 noticias geopolíticas más importantes de hoy y cómo afectan al Oro y al DXY.")
    bot.edit_message_text(f"🌍 **INFORME GEOPOLÍTICO:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "🐋 Radar Ballenas")
def ballenas(message):
    status = bot.reply_to(message, "📡 Rastreando flujos de ballenas...")
    res = cerebro_genesis("Reporta movimientos de ballenas de alto volumen (>1M USD) en la última hora.")
    bot.edit_message_text(f"🐋 **RADAR:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "🚀 Operar")
def operar(message):
    bot.reply_to(message, "📝 Orden: `Comprar 10 TSLA` o `Vender 2 BTC`.")

@bot.message_handler(func=lambda m: m.text.lower().startswith("comprar "))
def registro(message):
    try:
        p = message.text.split()
        cant, t = float(p[1]), p[2].upper()
        px = get_price(t)
        if px:
            portafolio.append({"t": t, "c": cant, "p": px})
            bot.reply_to(message, f"✅ Registrado: {t} a ${px}")
    except: bot.reply_to(message, "Error. Usa: Comprar 10 NVDA")

@bot.message_handler(func=lambda m: m.text == "📊 Rendimiento")
def rend(message):
    if not portafolio: return bot.reply_to(message, "⚠️ Vacío.")
    res = "📊 **REPORTE VERIFICADO:**\n"
    for o in portafolio:
        act = get_price(o['t'])
        res += f"🔹 {o['t']}: ${act if act else 'Error'}\n"
    bot.reply_to(message, res)

@bot.message_handler(content_types=['photo'])
def imagen(message):
    f = bot.get_file(message.photo[-1].file_id)
    d = bot.download_file(f.file_path)
    img = base64.b64encode(d).decode('utf-8')
    status = bot.reply_to(message, "🎯 Analizando niveles institucionales...")
    res = cerebro_genesis(None, img)
    bot.edit_message_text(f"🎯 **ANÁLISIS SMC:**\n{res}", message.chat.id, status.message_id)

if __name__ == "__main__":
    bot.infinity_polling()
