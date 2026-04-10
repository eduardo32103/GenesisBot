import os, subprocess, sys, base64, requests, telebot, datetime
from telebot import types

# --- SISTEMA DE ARRANQUE ---
def boot():
    try:
        import yfinance
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyTelegramBotAPI", "yfinance", "requests"])

boot()
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
        stock = yf.Ticker(t)
        return round(float(stock.fast_info['last_price']), 2)
    except: return None

# --- NÚCLEO DE INTELIGENCIA (MODO RADAR) ---
def cerebro_genesis(query, is_whale_radar=False, img_b64=None):
    if not OPENAI_API_KEY: return "🚨 ERROR: API KEY NO CONFIGURADA."
    
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    
    # Si es el radar de ballenas, cambiamos el chip a 'Modo Rastreo'
    if is_whale_radar:
        instruction = "Busca y reporta las transacciones más grandes de ballenas en las últimas 4 horas (BTC, ETH, Stablecoins). Dame: Activo, Monto en USD y Destino (Exchange/Wallet). Solo datos crudos."
    else:
        instruction = query

    system_msg = "Eres GÉNESIS, una terminal de inteligencia de mercado. No sermoneas, no saludas. Solo entregas datos técnicos y flujos de dinero real."

    payload = {
        "model": "gpt-4o",
        "messages": [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": instruction}
        ],
        "temperature": 0.2
    }

    if img_b64:
        payload["messages"][1]["content"] = [
            {"type": "text", "text": "Analiza esta gráfica: estructura SMC, niveles de OB y FVG con precios exactos del eje Y."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]
    
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Error en conexión de datos."

# --- HANDLERS ---
@bot.message_handler(commands=['start'])
def welcome(message):
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    markup.add("📊 Rendimiento", "🚀 Operar", "📈 Escáner SMT", "🐋 Radar Ballenas")
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V22: RADAR ACTIVO**", reply_markup=markup)

@bot.message_handler(func=lambda m: m.text == "🐋 Radar Ballenas")
def radar_ballenas(message):
    status = bot.reply_to(message, "📡 **Escaneando la Blockchain... Rastreando movimientos institucionales...**")
    reporte = cerebro_genesis(None, is_whale_radar=True)
    bot.edit_message_text(f"🐋 **REPORTE DE BALLENAS:**\n{reporte}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "📊 Rendimiento")
def rend(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ No hay trades.")
        return
    res = "📊 **RENDIMIENTO ACTUAL:**\n"
    for o in portafolio:
        now = get_price(o['t'])
        res += f"🔹 {o['t']}: ${now}\n"
    bot.reply_to(message, res)

@bot.message_handler(func=lambda m: m.text.lower().startswith("comprar "))
def compra(message):
    try:
        p = message.text.split()
        cant, ticker = float(p[1]), p[2].upper()
        price = get_price(ticker)
        if price:
            portafolio.append({"t": ticker, "c": cant, "p": price})
            bot.reply_to(message, f"✅ {ticker} registrado a ${price}")
    except: bot.reply_to(message, "Error en formato.")

@bot.message_handler(content_types=['photo'])
def handle_chart(message):
    file_info = bot.get_file(message.photo[-1].file_id)
    downloaded = bot.download_file(file_info.file_path)
    img_b64 = base64.b64encode(downloaded).decode('utf-8')
    status = bot.reply_to(message, "🎯 **Analizando niveles visuales...**")
    analisis = cerebro_genesis(None, False, img_b64)
    bot.edit_message_text(f"🎯 **ANÁLISIS SMC:**\n{analisis}", message.chat.id, status.message_id)

if __name__ == "__main__":
    bot.infinity_polling()
