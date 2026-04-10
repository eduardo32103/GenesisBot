import os, requests, base64, time, telebot, datetime
import yfinance as yf # <--- El motor de búsqueda real
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TU_CHAT_ID = "5426620320"

bot = telebot.TeleBot(TOKEN, threaded=False)

# Memoria de operaciones: [{ticker, cantidad, precio_entrada}]
portafolio = []

def obtener_precio_real(ticker):
    """Obtiene el precio exacto de Yahoo Finance"""
    try:
        data = yf.Ticker(ticker)
        # Intentamos obtener el precio actual
        precio = data.fast_info['last_price']
        return round(precio, 2)
    except:
        return None

def cerebro_genesis(texto_usuario, img_b64=None):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    system_prompt = (
        "Eres GÉNESIS, una terminal financiera. Tu misión es analizar datos. "
        "No inventes precios. Si el usuario te pasa datos, analízalos con rigor técnico (SMC/SMT). "
        "Usa lenguaje seco, profesional y sin sermones."
    )
    
    mensajes = [{"role": "system", "content": system_prompt}, {"role": "user", "content": texto_usuario}]
    if img_b64:
        mensajes[-1]["content"] = [{"type": "text", "text": texto_usuario}, {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}]

    payload = {"model": "gpt-4o", "messages": mensajes, "temperature": 0}
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=50)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Error de comunicación con el cerebro."

# --- INTERFAZ ---
def menu_principal():
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btns = ["📊 Rendimiento", "🚀 Operar (Auto)", "📈 Escáner SMT", "🐋 Radar Ballenas", "📊 Análisis SMC"]
    markup.add(*[types.KeyboardButton(b) for b in btns])
    return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V11: CONEXIÓN YAHOO FINANCE**\nPrecios reales, cero adivinanzas.", reply_markup=menu_principal())

@bot.message_handler(func=lambda message: message.text == "🚀 Operar (Auto)")
def instruccion_auto(message):
    bot.reply_to(message, "📝 Escribe: `Comprar [Cantidad] [Ticker]`\nEjemplo: `Comprar 10 TSLA` o `Comprar 5 NVDA`")

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
        # Guardamos en el portafolio como objeto para poder calcular
        portafolio.append({"ticker": ticker, "cant": cantidad, "px_in": precio})
        bot.edit_message_text(f"✅ **REGISTRADO**\nActivo: {ticker}\nCantidad: {cantidad}\nPrecio Mercado: ${precio}", message.chat.id, status.message_id)
    else:
        bot.edit_message_text(f"❌ No pude encontrar el ticker '{ticker}'. Revisa que esté bien escrito.", message.chat.id, status.message_id)

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

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    file_info = bot.get_file(message.photo[-1].file_id)
    img_data = bot.download_file(file_info.file_path)
    res = cerebro_genesis("Analiza esta gráfica. Zonas de liquidez.", base64.b64encode(img_data).decode('utf-8'))
    bot.reply_to(message, f"🎯 **SMC:**\n{res}")

@bot.message_handler(func=lambda message: True)
def handle_all(message):
    bot.reply_to(message, cerebro_genesis(message.text))

if __name__ == "__main__":
    bot.infinity_polling()
