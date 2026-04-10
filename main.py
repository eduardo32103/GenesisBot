import os, requests, base64, time, threading, telebot, datetime
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TU_CHAT_ID = "5426620320"

bot = telebot.TeleBot(TOKEN, threaded=False)

# Memoria de operaciones
portafolio = []

def cerebro_genesis(texto_usuario, system_role="Terminal de Datos"):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    ahora = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    
    system_prompt = (
        f"Hoy es {ahora}. Eres GÉNESIS, una IA financiera con acceso a internet. "
        "INSTRUCCIÓN: Eduardo te dará comandos como 'Comprar 5000 NVDA'. "
        "Tu tarea es: 1. Buscar el precio actual de mercado de ese activo. "
        "2. Responder SOLO con el formato: 'REGISTRO: [Activo] | Cantidad: [Cant] | Precio: [Precio Encontrado]'. "
        "NUNCA digas que no puedes buscar. Si el mercado está cerrado, usa el precio de cierre más reciente. "
        "No des sermones ni consejos. Solo datos."
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
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Error de conexión al buscar precio."

# --- INTERFAZ ---
def menu_principal():
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btns = ["🐋 Radar Ballenas", "📊 Análisis SMC", "📈 Escáner SMT", "⚖️ Gestión Riesgo", "🚀 Operar (Auto)", "📊 Mi Rendimiento"]
    markup.add(*[types.KeyboardButton(b) for b in btns])
    return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V7.0: AUTO-PRICE ACTIVADO**\nSolo dime qué compraste y yo busco el precio.", reply_markup=menu_principal())

@bot.message_handler(func=lambda message: message.text == "🚀 Operar (Auto)")
def instruccion_auto(message):
    bot.reply_to(message, "📝 **Mándame la orden.**\nEjemplo: `Comprar 50 NVDA` o `Vender 1 BTC`")

@bot.message_handler(func=lambda message: message.text.lower().startswith(("comprar ", "vender ")))
def auto_registro(message):
    status = bot.reply_to(message, "🔍 **Buscando precio de mercado actual...**")
    
    # El bot busca el precio por ti
    resultado = cerebro_genesis(message.text)
    
    if "REGISTRO:" in resultado:
        portafolio.append(resultado)
        bot.edit_message_text(f"✅ **OPERACIÓN REGISTRADA**\n━━━━━━━━━━━━━━\n{resultado}", message.chat.id, status.message_id)
    else:
        bot.edit_message_text(f"❌ No pude encontrar el precio. Intenta poner: `Comprar NVDA a [Precio]`", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: message.text == "📊 Mi Rendimiento")
def ver_rendimiento(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ Portafolio vacío.")
        return
    status = bot.reply_to(message, "⏳ **Actualizando precios y calculando P&L...**")
    query = f"PORTAFOLIO: {portafolio}. Busca el precio actual de estos activos y calcula rendimiento total. Solo tabla de datos."
    res = cerebro_genesis(query)
    bot.edit_message_text(f"📊 **BALANCE TOTAL**\n━━━━━━━━━━━━━━\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: True)
def handle_all(message):
    bot.reply_to(message, cerebro_genesis(message.text))

if __name__ == "__main__":
    bot.infinity_polling()
