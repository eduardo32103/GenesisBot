import os
import subprocess
import sys
import base64
import requests
import telebot
import datetime
from telebot import types

# --- SEGURIDAD DE LIBRERÍAS (Railway Fix) ---
def instalar_librerias():
    try:
        import yfinance
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyTelegramBotAPI", "yfinance", "requests"])

instalar_librerias()
import yfinance as yf

# --- CONFIGURACIÓN ---
# Token y Key. Asegúrate de configurar la OPENAI_API_KEY en Railway.
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

bot = telebot.TeleBot(TOKEN, threaded=False)
portafolio = []

# --- MOTOR DE PRECIOS REALES (Yahoo Finance) ---
def obtener_precio_real(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL", "BNB"]: t = f"{t}-USD"
        stock = yf.Ticker(t)
        precio = stock.fast_info['last_price']
        return round(float(precio), 2)
    except:
        return None

# --- CEREBRO INTELIGENTE VISUAL (OpenAI GPT-4o) ---
# Esta es la función que he modificado para el análisis directo
def cerebro_genesis(texto_usuario, img_b64=None):
    if not OPENAI_API_KEY:
        return "🚨 Configura la OPENAI_API_KEY en Railway para usar esta función."
    
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    
    # PROMPT DE INSTRUCCIÓN RADICAL: Prohibido sermones y definiciones.
    system_prompt = (
        f"Fecha y hora actual: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}. "
        "Eres GÉNESIS, un analista técnico institucional experto en Smart Money Concepts (SMC). "
        "TU MISIÓN: Analizar gráficas de trading. Si el usuario envía una imagen, "
        "DEBES realizar un análisis visual y técnico **EXCLUSIVAMENTE** basado en lo que ves en esa gráfica. "
        "REGLAS DE ORO: 1. No definas términos (ej. No digas 'BOS es...'); "
        "2. No digas obviedades como 'la gráfica muestra velas...'; "
        "3. No inventes datos que no están en la imagen; "
        "4. No des consejos financieros ni de autoayuda. "
        "Formato: Solo datos crudos y niveles técnicos identificados visualmente. BOS, CHoCH, FVG, Liquidez. "
        "Si no ves nada claro, dilo directamente."
    )
    
    mensajes = [{"role": "system", "content": system_prompt}]
    
    if img_b64:
        # Estructura correcta para enviar imagen a GPT-4o
        mensajes.append({
            "role": "user", 
            "content": [
                {"type": "text", "text": "Analiza esta gráfica. Identifica BOS, CHoCH, FVG y zonas de liquidez visualmente claras. Dame solo los niveles técnicos."},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
            ]
        })
    else:
        mensajes.append({"role": "user", "content": texto_usuario})

    payload = {
        "model": "gpt-4o", # Modelo más capaz para análisis visual
        "messages": mensajes,
        "temperature": 0 # Frío total para evitar inventos
    }
    
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        data = r.json()
        
        # Validación extra para no crashear
        if 'choices' in data and len(data['choices']) > 0:
            return data['choices'][0]['message']['content']
        else:
            return "🚨 Error: El cerebro de IA no respondió datos válidos."
    except Exception as e:
        return f"🚨 Error en el cerebro de IA: {str(e)}"

# --- INTERFAZ ---
def menu_principal():
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btns = ["📊 Rendimiento", "🚀 Operar", "📈 Escáner SMT", "🐋 Radar Ballenas", "⚖️ Gestión Riesgo"]
    markup.add(*[types.KeyboardButton(b) for b in btns])
    return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(
        message.chat.id, 
        "Eagle eye initiated.🦅 **GÉNESIS V18: ANÁLISIS VISUAL DIRECTO**\nMándame tu gráfica y la analizaré con Smart Money Concepts (SMC). No sermons.", 
        reply_markup=menu_principal()
    )

# --- LÓGICA DE PORTAFOLIO ---
@bot.message_handler(func=lambda message: message.text == "🚀 Operar")
def instruccion_auto(message):
    bot.reply_to(message, "📝 Dime qué compraste. Ejemplo: `Comprar 10 TSLA` o `Vender 2 BTC`")

@bot.message_handler(func=lambda message: message.text.lower().startswith(("comprar ", "vender ")))
def auto_registro(message):
    partes = message.text.split()
    if len(partes) < 3:
        bot.reply_to(message, "❌ Formato incorrecto. Usa: Comprar 10 NVDA")
        return

    cantidad = partes[1]
    ticker = partes[2].upper()
    
    status = bot.reply_to(message, f"🔍 Buscando precio real de {ticker}...")
    precio = obtener_precio_real(ticker)
    
    if precio:
        # Guardamos en el portafolio
        portafolio.append({"ticker": ticker, "cant": cantidad, "px_in": precio})
        bot.edit_message_text(f"✅ **REGISTRADO**\nActivo: {ticker}\nCantidad: {cantidad}\nPrecio Mercado: ${precio}", message.chat.id, status.message_id)
    else:
        bot.edit_message_text(f"❌ No encontré el precio de {ticker}. Verifica el ticker.", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: message.text == "📊 Rendimiento")
def ver_rendimiento(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ Portafolio vacío.")
        return
    
    status = bot.reply_to(message, "⏳ Actualizando precios en vivo...")
    reporte = "📊 **ESTADO DE CUENTA**\n━━━━━━━━━━━━━━\n"
    total_pnl = 0
    
    for op in portafolio:
        px_actual = obtener_precio_real(op['ticker'])
        if px_actual:
            pnl = (px_actual - op['px_in']) * float(op['cant'])
            porcentaje = ((px_actual / op['px_in']) - 1) * 100
            reporte += f"🔹 **{op['ticker']}**\nEntrada: ${op['px_in']} | Actual: ${px_actual}\nP&L: ${round(pnl, 2)} ({round(porcentaje, 2)}%)\n\n"
            total_pnl += pnl
    
    reporte += f"━━━━━━━━━━━━━━\n**Balance Total P&L: ${round(total_pnl, 2)}**"
    bot.edit_message_text(reporte, message.chat.id, status.message_id)

# --- OTRAS FUNCIONES ---
@bot.message_handler(func=lambda message: message.text == "📈 Escáner SMT")
def smt(message):
    bot.reply_to(message, cerebro_genesis("Busca divergencias SMT institucionales hoy."))

@bot.message_handler(func=lambda message: message.text == "🐋 Radar Ballenas")
def ballenas(message):
    bot.reply_to(message, cerebro_genesis("Informe Whale Alert de las últimas 2 horas."))

@bot.message_handler(func=lambda message: message.text == "⚖️ Gestión Riesgo")
def riesgo(message):
    bot.reply_to(message, "Envía: `Riesgo: [Capital], [Riesgo%], [Pips]`")

@bot.message_handler(func=lambda message: message.text.lower().startswith("riesgo:"))
def calc_riesgo(message):
    bot.reply_to(message, cerebro_genesis(message.text))

# --- ANÁLISIS DE IMÁGENES (SMC Directo) ---
@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    file_info = bot.get_file(message.photo[-1].file_id)
    img_data = bot.download_file(file_info.file_path)
    
    status = bot.reply_to(message, "🎯 **Analizando Liquidez y POIs visuales...**")
    
    # Codificamos la imagen y la enviamos al cerebro modificado
    img_b64 = base64.b64encode(img_data).decode('utf-8')
    res = cerebro_genesis("Analysis request.", img_b64)
    
    # Actualizamos el mensaje con la respuesta directa
    bot.edit_message_text(f"🎯 **ANÁLISIS SMC:**\n{res}", message.chat.id, status.message_id)

if __name__ == "__main__":
    bot.infinity_polling()
