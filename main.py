import os, subprocess, sys, base64, requests, telebot, datetime
from telebot import types

# --- SEGURIDAD DE ARRANQUE ---
def boot_check():
    try:
        import yfinance
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyTelegramBotAPI", "yfinance", "requests"])

boot_check()
import yfinance as yf

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

bot = telebot.TeleBot(TOKEN, threaded=False)
portafolio = []

def get_real_price(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        stock = yf.Ticker(t)
        return round(float(stock.fast_info['last_price']), 2)
    except: return None

# --- CEREBRO CON VISIÓN FORZADA ---
def cerebro_genesis(img_b64, prompt_texto=None):
    if not OPENAI_API_KEY: return "🚨 ERROR: API KEY NO DETECTADA."
    
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    
    # Prompt agresivo de análisis visual
    content = [
        {
            "type": "text", 
            "text": "Analiza esta gráfica de TradingView. Prohibido dar definiciones. Dame: 1. Sesgo (Bullish/Bearish). 2. Niveles de BOS y CHoCH detectados. 3. Zonas de Liquidez (FVG/OB) con precios aproximados según el eje Y. Sé directo y técnico."
        },
        {
            "type": "image_url", 
            "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}
        }
    ]

    payload = {
        "model": "gpt-4o",
        "messages": [
            {"role": "system", "content": "Eres una terminal de análisis técnico. No saludas. No sermoneas. Solo entregas niveles de la imagen analizada."},
            {"role": "user", "content": content}
        ],
        "temperature": 0
    }
    
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        res = r.json()
        return res['choices'][0]['message']['content']
    except Exception as e:
        return f"🚨 Error de visión: {str(e)}"

# --- HANDLERS ---
@bot.message_handler(commands=['start'])
def start(message):
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    markup.add("📊 Rendimiento", "🚀 Operar", "📈 Escáner SMT", "🐋 Radar Ballenas")
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V20: VISION READY**\nEnvíame una gráfica para análisis inmediato.", reply_markup=markup)

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    file_info = bot.get_file(message.photo[-1].file_id)
    downloaded = bot.download_file(file_info.file_path)
    img_b64 = base64.b64encode(downloaded).decode('utf-8')
    
    status = bot.reply_to(message, "🎯 **ESCANEANDO PIXELES... VERIFICANDO ESTRUCTURA...**")
    
    # Análisis directo
    analisis = cerebro_genesis(img_b64)
    bot.edit_message_text(f"🎯 **REPORTE TÉCNICO:**\n{analisis}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "📊 Rendimiento")
def rend(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ Vacío.")
        return
    status = bot.reply_to(message, "⏳ Cruzando datos reales...")
    res = "📊 **DATA:**\n"
    for o in portafolio:
        now = get_real_price(o['t'])
        res += f"🔹 {o['t']}: ${now}\n"
    bot.edit_message_text(res, message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "🚀 Operar")
def op(message):
    bot.reply_to(message, "Usa: `Comprar 10 TSLA`")

@bot.message_handler(func=lambda m: m.text.lower().startswith("comprar "))
def reg(message):
    try:
        p = message.text.split()
        cant, ticker = float(p[1]), p[2].upper()
        price = get_real_price(ticker)
        if price:
            portafolio.append({"t": ticker, "c": cant, "p": price})
            bot.reply_to(message, f"✅ {ticker} registrado a ${price}")
    except: bot.reply_to(message, "Error en formato.")

if __name__ == "__main__":
    bot.infinity_polling()
