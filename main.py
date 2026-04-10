import os, subprocess, sys, base64, requests, telebot, datetime
from telebot import types

# --- SISTEMA DE ARRANQUE Y LIBRERÍAS ---
def check_environment():
    try:
        import yfinance
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyTelegramBotAPI", "yfinance", "requests"])

check_environment()
import yfinance as yf

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

bot = telebot.TeleBot(TOKEN, threaded=False)
portafolio = []

# --- MOTOR DE DATOS REALES (VERIFICACIÓN EXTERNA) ---
def fetch_ticker_data(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        stock = yf.Ticker(t)
        return round(float(stock.fast_info['last_price']), 2)
    except: return None

# --- NÚCLEO DE INTELIGENCIA VISUAL (SMC EXPERT) ---
def smart_analysis(img_b64):
    if not OPENAI_API_KEY: return "🚨 ERROR: API KEY NO CONFIGURADA."
    
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    
    # Prompt de razonamiento profundo
    instruction = (
        "Actúa como un Analista Senior de Fondos de Cobertura. Analiza esta gráfica detalladamente. "
        "Sigue este protocolo de verificación: "
        "1. Identifica el Ticker y la Temporalidad si son visibles. "
        "2. Identifica la estructura: ¿Hay un Break of Structure (BOS) o Change of Character (CHoCH) real? "
        "3. Localiza Inbalances (FVG) y Order Blocks (OB) con precisión de precios según el eje Y. "
        "4. Determina la liquidez (BSL/SSL). "
        "REGLA: Prohibido usar lenguaje introductorio o definiciones. Solo entrega el 'Technical Report'. "
        "Si la imagen es borrosa o no tiene niveles claros, indícalo. No inventes precios."
    )

    payload = {
        "model": "gpt-4o",
        "messages": [
            {
                "role": "system", 
                "content": "Eres una terminal de inteligencia artificial de grado militar. Tu lenguaje es técnico, seco y 100% preciso. No sermoneas."
            },
            {
                "role": "user", 
                "content": [
                    {"type": "text", "text": instruction},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
                ]
            }
        ],
        "temperature": 0 # Rigidez total
    }
    
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except Exception as e:
        return f"🚨 Error de procesamiento visual: {str(e)}"

# --- INTERFAZ Y COMANDOS ---
@bot.message_handler(commands=['start'])
def boot_system(message):
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    markup.add("📊 Rendimiento", "🚀 Operar", "📈 Escáner SMT", "🐋 Radar Ballenas")
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V21: MODO INTELIGENCIA ACTIVO**\nProtocolos de verificación listos.", reply_markup=markup)

@bot.message_handler(content_types=['photo'])
def handle_visual_input(message):
    file_id = message.photo[-1].file_id
    file_info = bot.get_file(file_id)
    downloaded = bot.download_file(file_info.file_path)
    img_b64 = base64.b64encode(downloaded).decode('utf-8')
    
    status = bot.reply_to(message, "🎯 **CORROBORANDO ESTRUCTURA... ANALIZANDO PIXELES...**")
    
    report = smart_analysis(img_b64)
    bot.edit_message_text(f"🎯 **REPORTE INSTITUCIONAL:**\n{report}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "📊 Rendimiento")
def account_status(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ No hay trades registrados.")
        return
    status = bot.reply_to(message, "⏳ Verificando precios en tiempo real...")
    res = "📊 **ESTADO DE CUENTA (VERIFICADO):**\n"
    for o in portafolio:
        now = fetch_ticker_data(o['t'])
        pnl = (now - o['p']) * o['c'] if now else 0
        res += f"🔹 {o['t']}: ${now if now else 'ERROR'} | P&L: ${round(pnl, 2)}\n"
    bot.edit_message_text(res, message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text.lower().startswith("comprar "))
def trade_exec(message):
    try:
        p = message.text.split()
        cant, ticker = float(p[1]), p[2].upper()
        price = fetch_ticker_data(ticker)
        if price:
            portafolio.append({"t": ticker, "c": cant, "p": price})
            bot.reply_to(message, f"✅ **EJECUTADO:** {ticker} a ${price}")
        else: bot.reply_to(message, "❌ Ticker inválido.")
    except: bot.reply_to(message, "❌ Formato: Comprar 10 TSLA")

if __name__ == "__main__":
    bot.infinity_polling()
