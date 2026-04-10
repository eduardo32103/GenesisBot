import os, requests, base64, time, threading, telebot, datetime
from flask import Flask
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TU_CHAT_ID = "5426620320"

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

# --- MEMORIA DINÁMICA ---
watchlist = ["BTC", "Oro", "NASDAQ", "NVDA"]
portafolio = []

def cerebro_genesis(texto_usuario, img_b64=None):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    ahora = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # SYSTEM PROMPT: Prohibición estricta de LaTeX
    system_prompt = (
        f"Fecha: {ahora}. Eres GÉNESIS, una terminal de trading. "
        "REGLA DE ORO: NO uses fórmulas matemáticas tipo LaTeX (sin barras laterales ni llaves). "
        "Escribe como un humano: usa '$', '%' y texto normal. "
        "Si calculas rendimiento, hazlo paso a paso en texto simple. "
        "NVIDIA (NVDA) precio real 2026: ~$183.91. Sé directo."
    )
    
    contenido = [{"type": "text", "text": texto_usuario}]
    if img_b64:
        contenido.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}})

    payload = {
        "model": "gpt-4o", 
        "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": contenido}],
        "temperature": 0
    }
    
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=50)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Error de conexión."

# --- INTERFAZ ---
def menu_principal():
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btns = ["🐋 Radar Ballenas", "📊 Análisis SMC", "📈 Escáner SMT", "⚖️ Gestión Riesgo", "🚀 Operar", "📊 Mi Rendimiento"]
    markup.add(*[types.KeyboardButton(b) for b in btns])
    return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V5.2: FORMATO LIMPIO**", reply_markup=menu_principal())

@bot.message_handler(func=lambda message: message.text == "📊 Mi Rendimiento")
def ver_rendimiento(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ No hay trades.")
        return
    status = bot.reply_to(message, "⚖️ **Calculando...**")
    query = f"PORTAFOLIO: {portafolio}. Calcula el rendimiento con precios reales. USA TEXTO PLANO, SIN CÓDIGO MATEMÁTICO."
    res = cerebro_genesis(query)
    bot.edit_message_text(f"📊 **ESTADO DE CUENTA**\n━━━━━━━━━━━━━━\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: message.text == "🚀 Operar")
def ejecutar_op(message):
    bot.reply_to(message, "📝 Escribe: `Comprar [Activo] a [Precio]`")

@bot.message_handler(func=lambda message: message.text.lower().startswith(("comprar ", "vender ")))
def abrir_posicion(message):
    portafolio.append(message.text)
    bot.reply_to(message, f"✅ Guardado: {message.text}")

@bot.message_handler(func=lambda message: message.text == "📈 Escáner SMT")
def smt(message):
    bot.reply_to(message, cerebro_genesis("Divergencias SMT ahora."))

@bot.message_handler(func=lambda message: message.text == "🐋 Radar Ballenas")
def ballenas(message):
    bot.reply_to(message, cerebro_genesis("Whale Alert ahora."))

@bot.message_handler(func=lambda message: message.text == "⚖️ Gestión Riesgo")
def gest_riesgo(message):
    bot.reply_to(message, "Envía: `Riesgo: [Capital], [Riesgo%], [Pips]`")

@bot.message_handler(func=lambda message: message.text.lower().startswith("riesgo:"))
def calc_riesgo(message):
    bot.reply_to(message, cerebro_genesis(message.text))

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    file_info = bot.get_file(message.photo[-1].file_id)
    img_data = bot.download_file(file_info.file_path)
    res = cerebro_genesis("Analiza SMC.", base64.b64encode(img_data).decode('utf-8'))
    bot.reply_to(message, f"🎯 **SMC:**\n{res}")

@bot.message_handler(func=lambda message: True)
def handle_all(message):
    bot.reply_to(message, cerebro_genesis(message.text))

if __name__ == "__main__":
    bot.infinity_polling()
