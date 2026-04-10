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
    
    # SYSTEM PROMPT: El cerebro del bot
    system_prompt = (
        f"Fecha: {ahora}. Eres GÉNESIS, una terminal financiera avanzada. "
        "INSTRUCCIÓN: Eduardo es un trader profesional. No des definiciones ni consejos. "
        "Si el texto incluye 'PORTAFOLIO' o 'POSICIONES', busca el precio REAL de mercado "
        "de cada activo y calcula la ganancia o pérdida exacta. "
        "NVIDIA (NVDA) cotiza cerca de los $183.91. PROHIBIDO decir que es una empresa de tecnología. "
        "Da números, porcentajes y análisis de liquidez. Sé directo y eficiente."
    )
    
    contenido = [{"type": "text", "text": texto_usuario}]
    if img_b64:
        contenido.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}})

    payload = {
        "model": "gpt-4o", 
        "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": contenido}],
        "temperature": 0 # Rigidez máxima para evitar alucinaciones
    }
    
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=50)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Error de enlace. Intenta de nuevo."

# --- INTERFAZ ---
def menu_principal():
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btns = ["🐋 Radar Ballenas", "📊 Análisis SMC", "📈 Escáner SMT", "⚖️ Gestión Riesgo", "🚀 Operar", "📊 Mi Rendimiento"]
    markup.add(*[types.KeyboardButton(b) for b in btns])
    return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V5.1: RECONECTADO**\nListos para análisis de precisión.", reply_markup=menu_principal())

# --- LÓGICA DE PORTAFOLIO (REPARADA) ---
@bot.message_handler(func=lambda message: message.text == "📊 Mi Rendimiento")
def ver_rendimiento(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ Bitácora vacía.")
        return
    status = bot.reply_to(message, "⚖️ **Calculando P&L con precios reales...**")
    # Query explícito para forzar cálculo
    query = (
        f"ESTADO DEL PORTAFOLIO: {portafolio}. "
        "Tarea: 1. Busca el precio actual de cada activo. "
        "2. Calcula el rendimiento vs el precio de entrada que registré. "
        "3. Dame el balance total en USD y porcentaje. Sin sermones."
    )
    res = cerebro_genesis(query)
    bot.edit_message_text(f"📊 **ESTADO DE CUENTA**\n━━━━━━━━━━━━━━\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: message.text == "🚀 Operar")
def ejecutar_op(message):
    bot.reply_to(message, "📝 Registra tu trade:\n`Comprar [Activo] a [Precio]`")

@bot.message_handler(func=lambda message: message.text.lower().startswith(("comprar ", "vender ")))
def abrir_posicion(message):
    portafolio.append(message.text)
    bot.reply_to(message, f"✅ Orden: **{message.text}** guardada.")

@bot.message_handler(func=lambda message: message.text == "📈 Escáner SMT")
def smt(message):
    bot.reply_to(message, cerebro_genesis("Busca divergencias SMT institucionales ahora mismo."))

@bot.message_handler(func=lambda message: message.text == "🐋 Radar Ballenas")
def ballenas(message):
    bot.reply_to(message, cerebro_genesis("Informe de Whale Alert y flujos masivos."))

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
    res = cerebro_genesis("Analiza esta gráfica. Zonas de liquidez y SMC.", base64.b64encode(img_data).decode('utf-8'))
    bot.reply_to(message, f"🎯 **ANÁLISIS SMC:**\n{res}")

@bot.message_handler(func=lambda message: True)
def handle_all(message):
    bot.reply_to(message, cerebro_genesis(message.text))

if __name__ == "__main__":
    bot.infinity_polling()
