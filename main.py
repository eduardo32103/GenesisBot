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
    
    # SYSTEM PROMPT: PROTOCOLO DE VERIFICACIÓN
    system_prompt = (
        f"Fecha actual: {ahora}. Eres GÉNESIS, una terminal de DATOS REALES. "
        "INSTRUCCIÓN MÁXIMA: Si Eduardo te pide un precio, DEBES verificarlo. "
        "Si no tienes el dato exacto de este segundo, NO INVENTES. "
        "Referencia de control: NVDA está cerca de $183.91. Cualquier dato arriba de $200 es ERROR. "
        "Responde solo con: ACTIVO | PRECIO | FUENTE. Sin sermones."
    )
    
    payload = {
        "model": "gpt-4o", 
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Busca el precio actual de mercado y responde solo con datos: {texto_usuario}"}
        ],
        "temperature": 0 # Frío total para evitar inventos
    }

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Error de conexión."

# --- INTERFAZ ---
def menu_principal():
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btns = ["🐋 Radar Ballenas", "📊 Análisis SMC", "📈 Escáner SMT", "⚖️ Gestión Riesgo", "🚀 Operar (Auto)", "📊 Rendimiento"]
    markup.add(*[types.KeyboardButton(b) for b in btns])
    return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V8.0: MODO FRANCOTIRADOR**\nProtocolo de verificación de precios activado.", reply_markup=menu_principal())

@bot.message_handler(func=lambda message: message.text == "🚀 Operar (Auto)")
def instruccion_auto(message):
    bot.reply_to(message, "📝 Dime qué compraste (Ej: 100 NVDA)")

@bot.message_handler(func=lambda message: message.text.lower().startswith(("comprar ", "vender ")))
def auto_registro(message):
    # Limpiamos el texto para que el bot no se confunda
    orden = message.text.lower().replace("comprar ", "").replace("vender ", "").strip()
    status = bot.reply_to(message, f"🔍 **Verificando precio real de {orden}...**")
    
    # El bot busca el precio
    datos_reales = cerebro_genesis(f"Precio actual de {orden}")
    
    if "|" in datos_reales:
        portafolio.append(f"{message.text} | {datos_reales}")
        bot.edit_message_text(f"✅ **REGISTRADO AL CENTAVO**\n{datos_reales}", message.chat.id, status.message_id)
    else:
        bot.edit_message_text(f"❌ Error en datos. Mejor ponlo manual: `Comprar {orden} a [Precio]`", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: message.text == "📊 Rendimiento")
def ver_rendimiento(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ Vacío.") return
    status = bot.reply_to(message, "⏳ **Cruzando datos con el mercado...**")
    query = f"Calcula P&L de: {portafolio}. Solo tabla, sin sermones."
    res = cerebro_genesis(query)
    bot.edit_message_text(f"📊 **ESTADO REAL**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: True)
def handle_all(message):
    bot.reply_to(message, cerebro_genesis(message.text))

if __name__ == "__main__":
    bot.infinity_polling()
