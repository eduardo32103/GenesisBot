import os
import subprocess
import sys
import base64
import requests
import telebot
import datetime
from telebot import types

# --- SEGURIDAD DE LIBRERÍAS ---
def instalar_librerias():
    try:
        import yfinance
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyTelegramBotAPI", "yfinance", "requests"])

instalar_librerias()
import yfinance as yf

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") # Configúrala en Railway

bot = telebot.TeleBot(TOKEN, threaded=False)
portafolio = []

# --- MOTOR DE PRECIOS REALES ---
def obtener_precio_real(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL", "BNB"]: t = f"{t}-USD"
        stock = yf.Ticker(t)
        precio = stock.fast_info['last_price']
        return round(float(precio), 2)
    except:
        return None

# --- CEREBRO INTELIGENTE (OpenAI) ---
def cerebro_genesis(texto_usuario, img_b64=None):
    if not OPENAI_API_KEY:
        return "🚨 Configura la OPENAI_API_KEY en Railway para usar esta función."
    
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    system_prompt = (
        "Eres GÉNESIS, terminal de trading institucional. Analiza con Smart Money Concepts (SMC). "
        "Sé seco, directo y entrega datos. Cero sermones."
    )
    
    mensajes = [{"role": "system", "content": system_prompt}]
    if img_b64:
        mensajes.append({"role": "user", "content": [
            {"type": "text", "text": texto_usuario},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]})
    else:
        mensajes.append({"role": "user", "content": texto_usuario})

    payload = {"model": "gpt-4o", "messages": mensajes, "temperature": 0}
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=50)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Error en el cerebro de IA."

# --- INTERFAZ ---
def menu_principal():
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btns = ["📊 Rendimiento", "🚀 Operar", "📈 Escáner SMT", "🐋 Radar Ballenas", "⚖️ Gestión Riesgo"]
    markup.add(*[types.KeyboardButton(b) for b in btns])
    return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V17: FULL ARSENAL**\nSistemas 100% operativos.", reply_markup=menu_principal())

# --- LÓGICA DE PORTAFOLIO ---
@bot.message_handler(func=lambda message: message.text == "🚀 Operar")
def instruccion_auto(message):
    bot.reply_to(message, "📝 Escribe: `Comprar 10 TSLA` o `Vender 2 BTC`.")

@bot.message_handler(func=lambda message: message.text.lower().startswith(("comprar ", "vender ")))
def auto_registro(message):
    try:
        partes = message.text.split()
        cantidad = float(partes[1])
        ticker = partes[2].upper()
        status = bot.reply_to(message, f"🔍 Consultando {ticker}...")
        precio = obtener_precio_real(ticker)
        if precio:
            portafolio.append({"ticker": ticker, "cant": cantidad, "px_in": precio})
            bot.edit_message_text(f"✅ **REGISTRADO**\n{ticker} | ${precio}\nCant: {cantidad}", message.chat.id, status.message_id)
        else:
            bot.edit_message_text(f"❌ Ticker {ticker} no encontrado.", message.chat.id, status.message_id)
    except: bot.reply_to(message, "❌ Formato: Comprar 10 NVDA")

@bot.message_handler(func=lambda message: message.text == "📊 Rendimiento")
def ver_rendimiento(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ Portafolio vacío.")
        return
    status = bot.reply_to(message, "⏳ Actualizando precios...")
    res = "📊 **ESTADO DE CUENTA:**\n"
    total_pnl = 0
    for op in portafolio:
        px = obtener_precio_real(op['ticker'])
        if px:
            pnl = (px - op['px_in']) * op['cant']
            res += f"🔹 {op['ticker']}: ${px} (P&L: ${round(pnl, 2)})\n"
            total_pnl += pnl
    res += f"\n**Total P&L: ${round(total_pnl, 2)}**"
    bot.edit_message_text(res, message.chat.id, status.message_id)

# --- OTRAS FUNCIONES ---
@bot.message_handler(func=lambda message: message.text == "📈 Escáner SMT")
def smt(message):
    bot.reply_to(message, cerebro_genesis("Busca divergencias SMT entre NASDAQ y SP500 hoy."))

@bot.message_handler(func=lambda message: message.text == "🐋 Radar Ballenas")
def ballenas(message):
    bot.reply_to(message, cerebro_genesis("Dame informe de Whale Alert de las últimas 2 horas."))

@bot.message_handler(func=lambda message: message.text == "⚖️ Gestión Riesgo")
def riesgo(message):
    bot.reply_to(message, "Envía: `Riesgo: [Capital], [Riesgo%], [Pips]`")

@bot.message_handler(func=lambda message: message.text.lower().startswith("riesgo:"))
def calc_riesgo(message):
    bot.reply_to(message, cerebro_genesis(message.text))

# --- ANÁLISIS DE IMÁGENES (SMC) ---
@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    file_info = bot.get_file(message.photo[-1].file_id)
    img_data = bot.download_file(file_info.file_path)
    status = bot.reply_to(message, "🎯 **Analizando Liquidez y POIs...**")
    res = cerebro_genesis("Analiza esta gráfica con SMC: BOS, CHoCH, FVG y Liquidez.", base64.b64encode(img_data).decode('utf-8'))
    bot.edit_message_text(f"🎯 **ANÁLISIS SMC:**\n{res}", message.chat.id, status.message_id)

if __name__ == "__main__":
    bot.infinity_polling()
