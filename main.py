import os
import subprocess
import sys
import base64
import requests
import telebot
import datetime
from telebot import types

# --- SISTEMA DE ARRANQUE SEGURO ---
def check_libs():
    try:
        import yfinance
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyTelegramBotAPI", "yfinance", "requests"])

check_libs()
import yfinance as yf

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

bot = telebot.TeleBot(TOKEN, threaded=False)
portafolio = []

# --- MOTOR DE DATOS REALES ---
def get_live_price(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        stock = yf.Ticker(t)
        # Fast_info para evitar latencia
        return round(float(stock.fast_info['last_price']), 2)
    except:
        return None

# --- CEREBRO DE VERIFICACIÓN (GPT-4o) ---
def cerebro_genesis(query, img_b64=None):
    if not OPENAI_API_KEY:
        return "🚨 ERROR: Falta API KEY en Railway."
    
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    
    # SYSTEM PROMPT: PROTOCOLO DE CERO ERROR
    system_prompt = (
        f"Fecha: {datetime.datetime.now().strftime('%Y-%m-%d')}. Eres GÉNESIS, una Terminal de Inteligencia Financiera. "
        "REGLAS CRÍTICAS: 1. Corrobora cada dato antes de hablar. 2. En gráficas, sé específico: indica precios exactos de BOS/FVG. "
        "3. Prohibido alucinar o inventar movimientos que no existen. 4. Prohibidos los sermones educativos. "
        "Si detectas una anomalía o falta de claridad en la imagen, admítelo. "
        "Tu análisis debe ser útil para un trader de alta frecuencia."
    )
    
    mensajes = [{"role": "system", "content": system_prompt}]
    
    if img_b64:
        mensajes.append({
            "role": "user",
            "content": [
                {"type": "text", "text": "ANÁLISIS DE GRÁFICA: Identifica estructura de mercado (Bullish/Bearish), niveles de Order Blocks y FVG. Corrobora con los datos visuales de los ejes."},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
            ]
        })
    else:
        mensajes.append({"role": "user", "content": query})

    payload = {
        "model": "gpt-4o",
        "messages": mensajes,
        "temperature": 0 # Precisión máxima, cero creatividad.
    }
    
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except Exception as e:
        return f"🚨 Error de procesamiento: {str(e)}"

# --- INTERFAZ ---
def main_menu():
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    markup.add("📊 Rendimiento", "🚀 Operar", "📈 Escáner SMT", "🐋 Radar Ballenas", "⚖️ Gestión Riesgo")
    return markup

@bot.message_handler(commands=['start'])
def welcome(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V19: PROTOCOLO DE VERIFICACIÓN**\nSistema de alta precisión activo.", reply_markup=main_menu())

# --- FUNCIONES DE TRADING ---
@bot.message_handler(func=lambda m: m.text == "🚀 Operar")
def op_manual(message):
    bot.reply_to(message, "📝 Orden: `Comprar [Cant] [Ticker]`\nEjemplo: `Comprar 10 NVDA`")

@bot.message_handler(func=lambda m: m.text.lower().startswith(("comprar ", "vender ")))
def register_trade(message):
    try:
        p = message.text.split()
        cant, ticker = float(p[1]), p[2].upper()
        status = bot.reply_to(message, f"🔍 Corroborando precio de {ticker}...")
        price = get_live_price(ticker)
        if price:
            portafolio.append({"t": ticker, "c": cant, "p": price})
            bot.edit_message_text(f"✅ **REGISTRADO**\n{ticker} | Price: ${price} | Cant: {cant}", message.chat.id, status.message_id)
        else:
            bot.edit_message_text("❌ Ticker no válido.", message.chat.id, status.message_id)
    except:
        bot.reply_to(message, "❌ Error. Usa: `Comprar 10 TSLA`")

@bot.message_handler(func=lambda m: m.text == "📊 Rendimiento")
def show_pnl(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ Sin datos.")
        return
    status = bot.reply_to(message, "⏳ Verificando mercado actual...")
    res = "📊 **REPORTE DE PRECISIÓN**\n"
    total = 0
    for o in portafolio:
        now = get_live_price(o['t'])
        if now:
            pnl = (now - o['p']) * o['c']
            res += f"🔹 {o['t']}: ${now} | P&L: ${round(pnl, 2)}\n"
            total += pnl
    res += f"\n**Balance Total: ${round(total, 2)}**"
    bot.edit_message_text(res, message.chat.id, status.message_id)

# --- ANÁLISIS VISUAL SMC ---
@bot.message_handler(content_types=['photo'])
def handle_chart(message):
    file_id = message.photo[-1].file_id
    file_info = bot.get_file(file_id)
    downloaded = bot.download_file(file_info.file_path)
    
    status = bot.reply_to(message, "🎯 **Escaneando gráfica... Verificando niveles...**")
    
    img_b64 = base64.b64encode(downloaded).decode('utf-8')
    # Pedimos al cerebro que analice la imagen
    analisis = cerebro_genesis(None, img_b64)
    
    bot.edit_message_text(f"🎯 **ANÁLISIS TÉCNICO:**\n{analisis}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text in ["📈 Escáner SMT", "🐋 Radar Ballenas", "⚖️ Gestión Riesgo"])
def tool_handler(message):
    bot.reply_to(message, cerebro_genesis(f"Ejecuta {message.text}. Solo resultados finales."))

if __name__ == "__main__":
    bot.infinity_polling()
