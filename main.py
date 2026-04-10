import os, requests, base64, time, threading, telebot, datetime
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TU_CHAT_ID = "5426620320"

bot = telebot.TeleBot(TOKEN, threaded=False)

# --- BASE DE DATOS DE PRECIOS (2026) ---
# Aquí grabamos los precios para que el bot NO tenga que inventar
PRECIOS_MERCADO = {
    "NVDA": 183.91,
    "NVIDIA": 183.91,
    "BTC": 98500.00,
    "NASDAQ": 18250.00,
    "ORO": 2350.00
}

portafolio = []

def cerebro_genesis(texto_usuario, img_b64=None):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    ahora = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # Prompt agresivo de solo datos
    system_prompt = (
        f"Fecha: {ahora}. Eres GÉNESIS, una terminal de datos. "
        f"PRECIOS ACTUALES: {PRECIOS_MERCADO}. "
        "INSTRUCCIÓN: Eduardo es un trader pro. No des sermones ni definiciones. "
        "Si pide rendimiento, usa los PRECIOS ACTUALES que te di arriba. "
        "Formato: Activo | Entrada | Actual | P&L. "
        "No uses LaTeX. Solo texto plano y números."
    )
    
    payload = {
        "model": "gpt-4o", 
        "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": texto_usuario}],
        "temperature": 0
    }
    
    if img_b64:
        payload["messages"][-1]["content"] = [
            {"type": "text", "text": texto_usuario},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=50)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Error de respuesta."

# --- INTERFAZ ---
def menu_principal():
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btns = ["🐋 Radar Ballenas", "📊 Análisis SMC", "📈 Escáner SMT", "⚖️ Gestión Riesgo", "🚀 Operar", "📊 Mi Rendimiento"]
    markup.add(*[types.KeyboardButton(b) for b in btns])
    return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V6.0: MODO PRECISIÓN**", reply_markup=menu_principal())

@bot.message_handler(func=lambda message: message.text == "📊 Mi Rendimiento")
def ver_rendimiento(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ No hay trades registrados.")
        return
    status = bot.reply_to(message, "⚡ **Calculando con base de datos 2026...**")
    query = f"Calcula el rendimiento de mi portafolio: {portafolio}. Usa los precios que tienes en tu sistema."
    res = cerebro_genesis(query)
    bot.edit_message_text(f"📊 **RESULTADOS:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: message.text == "🚀 Operar")
def ejecutar_op(message):
    bot.reply_to(message, "Escribe: `Comprar [Activo] a [Precio]`")

@bot.message_handler(func=lambda message: message.text.lower().startswith(("comprar ", "vender ")))
def abrir_posicion(message):
    portafolio.append(message.text)
    bot.reply_to(message, f"✅ Registrado: {message.text}")

@bot.message_handler(func=lambda message: True)
def handle_all(message):
    bot.reply_to(message, cerebro_genesis(message.text))

if __name__ == "__main__":
    bot.infinity_polling()
