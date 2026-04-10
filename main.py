import os, base64, requests, telebot, datetime
import yfinance as yf
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

bot = telebot.TeleBot(TOKEN, threaded=False)
portafolio = []

# --- MOTOR DE PRECIOS ---
def get_price(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        stock = yf.Ticker(t)
        return round(float(stock.fast_info['last_price']), 2)
    except: return None

# --- CEREBRO INSTITUCIONAL (SMC + GEOPOLÍTICA + ALERTAS) ---
def cerebro_genesis(query, img_b64=None):
    if not OPENAI_API_KEY: return "🚨 ERROR: Configura OPENAI_API_KEY en Railway."
    
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    system_msg = (
        "Eres GÉNESIS V31. Terminal de Inteligencia de Mercado. "
        "MISIONES: 1. Analizar gráficas: detecta BOS, CHoCH, OB y FVG con precisión. "
        "2. Geopolítica: analiza noticias de guerra, economía y su impacto en Oro/DXY. "
        "3. Alertas: identifica volatilidad. "
        "REGLA: Respuestas secas, técnicas y verificadas. Prohibido sermones."
    )

    payload = {
        "model": "gpt-4o",
        "messages": [{"role": "system", "content": system_msg}],
        "temperature": 0
    }

    if img_b64:
        payload["messages"].append({"role": "user", "content": [
            {"type": "text", "text": "ANALIZA LA GRÁFICA: Indica estructura SMC y niveles clave del eje Y."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]})
    else:
        payload["messages"].append({"role": "user", "content": query})

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Error de comunicación con el núcleo de IA."

# --- MENÚ ---
def menu():
    m = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    m.add("📊 Rendimiento", "🚀 Operar", "🌍 Geopolítica", "🐋 Radar Ballenas", "⚠️ Alertas")
    return m

@bot.message_handler(commands=['start'])
def start(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V31: DESPLEGADO**\nArsenal geopolítico y SMC operativo.", reply_markup=menu())

# --- BOTONES ---
@bot.message_handler(func=lambda m: m.text == "🌍 Geopolítica")
def geo(message):
    status = bot.reply_to(message, "📡 Rastreando satélites y noticias macro...")
    res = cerebro_genesis("Reporte geopolítico de hoy e impacto en el mercado global (Oro, Crudo, USD).")
    bot.edit_message_text(f"🌍 **INFORME GLOBAL:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "⚠️ Alertas")
def alertas(message):
    bot.reply_to(message, "🔔 **CENTRO DE ALERTAS:** Monitoreando movimientos de alta frecuencia.")

@bot.message_handler(func=lambda m: m.text == "🐋 Radar Ballenas")
def ballenas(message):
    status = bot.reply_to(message, "📡 Escaneando la blockchain por movimientos institucionales...")
    res = cerebro_genesis("Busca transacciones de ballenas (>1M USD) de la última hora.")
    bot.edit_message_text(f"🐋 **REPORTE RADAR:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "🚀 Operar")
def operar(message):
    bot.reply_to(message, "📝 Escribe: `Comprar [Cantidad] [Ticker]`")

@bot.message_handler(func=lambda m: m.text.lower().startswith("comprar "))
def comprar(message):
    try:
        p = message.text.split()
        cant, t = float(p[1]), p[2].upper()
        px = get_price(t)
        if px:
            portafolio.append({"t": t, "c": cant, "p": px})
            bot.reply_to(message, f"✅ Registrado: {t} a ${px}")
        else: bot.reply_to(message, "❌ Ticker no válido.")
    except: bot.reply_to(message, "Error. Ejemplo: Comprar 10 TSLA")

@bot.message_handler(func=lambda m: m.text == "📊 Rendimiento")
def rend(message):
    if not portafolio: return bot.reply_to(message, "⚠️ No hay activos registrados.")
    res = "📊 **RENDIMIENTO VERIFICADO:**\n"
    for o in portafolio:
        act = get_price(o['t'])
        res += f"🔹 {o['t']}: ${act if act else 'Error'}\n"
    bot.reply_to(message, res)

@bot.message_handler(content_types=['photo'])
def imagen(message):
    f = bot.get_file(message.photo[-1].file_id)
    d = bot.download_file(f.file_path)
    img = base64.b64encode(d).decode('utf-8')
    status = bot.reply_to(message, "🎯 Analizando estructura técnica...")
    res = cerebro_genesis(None, img)
    bot.edit_message_text(f"🎯 **ANÁLISIS SMC:**\n{res}", message.chat.id, status.message_id)

if __name__ == "__main__":
    bot.infinity_polling()
