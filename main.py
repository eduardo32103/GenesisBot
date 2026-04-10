import os, requests, base64, time, threading, telebot, datetime
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TU_CHAT_ID = "5426620320"

bot = telebot.TeleBot(TOKEN, threaded=False)

# Memoria de operaciones
portafolio = []

def cerebro_genesis(texto_usuario):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    ahora = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    
    system_prompt = (
        f"Fecha: {ahora}. Eres GÉNESIS, una terminal de datos. "
        "Misión: Dar el precio actual de activos. NVDA está en $183.91 aprox. "
        "Responde corto: ACTIVO | PRECIO. Sin sermones."
    )
    
    payload = {
        "model": "gpt-4o", 
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": texto_usuario}
        ],
        "temperature": 0
    }

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=40)
        r.raise_for_status() # Verifica si hubo error de red
        data = r.json()
        
        # Validación de seguridad para evitar el crash
        if 'choices' in data and len(data['choices']) > 0:
            return data['choices'][0]['message']['content']
        else:
            return "🚨 OpenAI no devolvió datos válidos."
    except Exception as e:
        return f"🚨 Error de conexión: {str(e)}"

# --- INTERFAZ ---
def menu_principal():
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btns = ["🐋 Radar Ballenas", "📊 Análisis SMC", "📈 Escáner SMT", "⚖️ Gestión Riesgo", "🚀 Operar (Auto)", "📊 Rendimiento"]
    markup.add(*[types.KeyboardButton(b) for b in btns])
    return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V8.1: ANTI-CRASH**\nSistemas estabilizados.", reply_markup=menu_principal())

@bot.message_handler(func=lambda message: message.text == "🚀 Operar (Auto)")
def instruccion_auto(message):
    bot.reply_to(message, "📝 Dime qué compraste (Ej: 100 NVDA)")

@bot.message_handler(func=lambda message: message.text.lower().startswith(("comprar ", "vender ")))
def auto_registro(message):
    status = bot.reply_to(message, "🔍 **Buscando precio...**")
    res = cerebro_genesis(f"Precio actual de: {message.text}")
    
    if "🚨" not in res:
        portafolio.append(f"{message.text} | {res}")
        bot.edit_message_text(f"✅ **REGISTRADO**\n{res}", message.chat.id, status.message_id)
    else:
        bot.edit_message_text(res, message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: message.text == "📊 Rendimiento")
def ver_rendimiento(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ Vacío.")
        return
    status = bot.reply_to(message, "⏳ **Procesando...**")
    res = cerebro_genesis(f"Calcula P&L de: {portafolio}. Solo datos.")
    bot.edit_message_text(f"📊 **ESTADO**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: True)
def handle_all(message):
    bot.reply_to(message, cerebro_genesis(message.text))

if __name__ == "__main__":
    bot.infinity_polling()
