import os, requests, base64, time, threading, telebot, datetime
from flask import Flask
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TU_CHAT_ID = "5426620320"

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

# --- MEMORIA ---
portafolio = []

def cerebro_genesis(texto_usuario, img_b64=None):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    ahora = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # PROMPT DE INYECCIÓN DE DATOS - PROHIBIDO SERMONES
    system_prompt = (
        f"Fecha: {ahora}. Eres GÉNESIS, una terminal de datos financieros. "
        "INSTRUCCIÓN RADICAL: Prohibido usar frases como 'no tengo acceso', 'como IA', 'recomiendo consultar'. "
        "Si no tienes el dato exacto, USA EL ÚLTIMO PRECIO CONOCIDO DE 2026: NVDA $183.91, BTC $95k, etc. "
        "Tu formato de respuesta debe ser: [ACTIVO] | [PRECIO] | [CAMBIO]. "
        "No des explicaciones. Solo entrega los números solicitados."
    )
    
    contenido = [{"type": "text", "text": texto_usuario}]
    if img_b64:
        contenido.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}})

    payload = {
        "model": "gpt-4o", 
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": "Rendimiento de NVDA"},
            {"role": "assistant", "content": "NVDA | Precio: $183.91 | Rendimiento: +2.4% | Estado: Bullish"},
            {"role": "user", "content": texto_usuario}
        ],
        "temperature": 0
    }
    
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=50)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Terminal Offline. Reintenta."

# --- INTERFAZ ---
def menu_principal():
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btns = ["🐋 Radar Ballenas", "📊 Análisis SMC", "📈 Escáner SMT", "⚖️ Gestión Riesgo", "🚀 Operar", "📊 Mi Rendimiento"]
    markup.add(*[types.KeyboardButton(b) for b in btns])
    return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V5.4: ABSOLUTE ZERO**\nSin sermones. Solo data.", reply_markup=menu_principal())

@bot.message_handler(func=lambda message: message.text == "📊 Mi Rendimiento")
def ver_rendimiento(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ No data.")
        return
    query = f"INFORME TÉCNICO: {portafolio}. Solo datos y balance final."
    res = cerebro_genesis(query)
    bot.send_message(message.chat.id, f"📊 **BALANCE FINAL:**\n{res}")

@bot.message_handler(func=lambda message: message.text == "🚀 Operar")
def ejecutar_op(message):
    bot.reply_to(message, "Escribe: `Comprar [Activo] a [Precio]`")

@bot.message_handler(func=lambda message: message.text.lower().startswith(("comprar ", "vender ")))
def abrir_posicion(message):
    portafolio.append(message.text)
    bot.reply_to(message, "✅ Data Ingested.")

@bot.message_handler(func=lambda message: message.text == "📈 Escáner SMT")
def smt(message):
    bot.reply_to(message, cerebro_genesis("SMC/SMT Divergence: NASDAQ vs SP500. Solo niveles."))

@bot.message_handler(func=lambda message: message.text == "🐋 Radar Ballenas")
def ballenas(message):
    bot.reply_to(message, cerebro_genesis("Whale Alert: Activo/Monto/Exchange."))

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    file_info = bot.get_file(message.photo[-1].file_id)
    img_data = bot.download_file(file_info.file_path)
    res = cerebro_genesis("Analyze Liquidity Levels.", base64.b64encode(img_data).decode('utf-8'))
    bot.reply_to(message, f"🎯 **DATA:**\n{res}")

@bot.message_handler(func=lambda message: True)
def handle_all(message):
    bot.reply_to(message, cerebro_genesis(message.text))

if __name__ == "__main__":
    bot.infinity_polling()
