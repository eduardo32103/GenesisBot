import os
import telebot
import yfinance as yf
import requests
import base64
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

bot = telebot.TeleBot(TOKEN, threaded=False)
portafolio = []

# --- MOTOR DE PRECIOS ---
def obtener_precio(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        stock = yf.Ticker(t)
        precio = stock.fast_info['last_price']
        return round(float(precio), 2)
    except:
        return None

# --- CEREBRO INTELIGENTE ---
def analizar_con_ia(query, img_b64=None):
    if not OPENAI_API_KEY: return "🚨 ERROR: Configura OPENAI_API_KEY en Railway."
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    
    msg_content = [{"type": "text", "text": query}]
    if img_b64:
        msg_content.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}})

    payload = {
        "model": "gpt-4o",
        "messages": [
            {"role": "system", "content": "Eres GÉNESIS. Analista SMC. Solo das niveles técnicos y precios. Sin sermones."},
            {"role": "user", "content": msg_content}
        ],
        "temperature": 0
    }
    
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except:
        return "🚨 Error en el cerebro de IA."

# --- HANDLERS ---
@bot.message_handler(commands=['start'])
def start(message):
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    markup.add("📊 Rendimiento", "🚀 Operar", "📈 Escáner SMT", "🐋 Radar Ballenas")
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V25: SISTEMA ONLINE**", reply_markup=markup)

@bot.message_handler(func=lambda m: m.text == "🚀 Operar")
def operar(message):
    bot.reply_to(message, "Escribe: `Comprar 10 TSLA` o `Vender 2 BTC`")

@bot.message_handler(func=lambda m: m.text.lower().startswith("comprar "))
def registrar(message):
    try:
        p = message.text.split()
        cant, ticker = float(p[1]), p[2].upper()
        px = obtener_precio(ticker)
        if px:
            portafolio.append({"t": ticker, "c": cant, "p": px})
            bot.reply_to(message, f"✅ Registrado: {ticker} a ${px}")
        else:
            bot.reply_to(message, "❌ Ticker no encontrado.")
    except:
        bot.reply_to(message, "❌ Error. Ejemplo: Comprar 10 NVDA")

@bot.message_handler(func=lambda m: m.text == "📊 Rendimiento")
def rendimiento(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ Vacío.")
        return
    res = "📊 **PORTAFOLIO:**\n"
    for o in portafolio:
        act = obtener_precio(o['t'])
        res += f"🔹 {o['t']}: ${act if act else 'N/A'}\n"
    bot.reply_to(message, res)

@bot.message_handler(func=lambda m: m.text == "🐋 Radar Ballenas")
def radar(message):
    bot.reply_to(message, analizar_con_ia("Busca movimientos de ballenas recientes."))

@bot.message_handler(content_types=['photo'])
def imagen(message):
    f_info = bot.get_file(message.photo[-1].file_id)
    downloaded = bot.download_file(f_info.file_path)
    img_b64 = base64.b64encode(downloaded).decode('utf-8')
    bot.reply_to(message, "🎯 Analizando niveles SMC...")
    res = analizar_con_ia("Analiza niveles de BOS y FVG en esta imagen.", img_b64)
    bot.reply_to(message, res)

if __name__ == "__main__":
    bot.infinity_polling()
