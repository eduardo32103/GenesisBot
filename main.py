import os, requests, base64, time, threading, telebot, datetime
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TU_CHAT_ID = "5426620320"

bot = telebot.TeleBot(TOKEN, threaded=False)

# Memoria volátil de operaciones
portafolio = []

def cerebro_genesis(texto_usuario, img_b64=None):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    ahora = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # Prompt de Hierro: Sin errores, sin sermones, sin código técnico.
    system_prompt = (
        f"Hoy es {ahora}. Eres GÉNESIS, una terminal financiera de alta precisión. "
        "INSTRUCCIÓN: Eduardo es un trader experto. Responde solo con datos crudos y niveles técnicos. "
        "Si pide rendimiento, busca el precio real de 2026 (NVDA ~$183.91). "
        "PROHIBIDO: Enviar código JSON, enviar diccionarios de python o dar consejos de autoayuda. "
        "Formato: Texto limpio, negritas y emojis."
    )
    
    # Construcción correcta del mensaje para evitar el error anterior
    mensajes = [{"role": "system", "content": system_prompt}]
    
    if img_b64:
        mensajes.append({
            "role": "user", 
            "content": [
                {"type": "text", "text": texto_usuario},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
            ]
        })
    else:
        mensajes.append({"role": "user", "content": texto_usuario})

    payload = {
        "model": "gpt-4o",
        "messages": mensajes,
        "temperature": 0
    }

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=50)
        r.raise_for_status()
        data = r.json()
        return data['choices'][0]['message']['content']
    except Exception as e:
        return f"🚨 Error de respuesta: Intenta de nuevo. (Detalle: {str(e)})"

# --- INTERFAZ ---
def menu_principal():
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btns = ["🐋 Radar Ballenas", "📊 Análisis SMC", "📈 Escáner SMT", "⚖️ Gestión Riesgo", "🚀 Operar (Auto)", "📊 Rendimiento"]
    markup.add(*[types.KeyboardButton(b) for b in btns])
    return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V9.0: RESTAURACIÓN TOTAL**\nSistemas de datos purificados.", reply_markup=menu_principal())

@bot.message_handler(func=lambda message: message.text == "📊 Rendimiento")
def ver_rendimiento(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ No hay trades registrados.")
        return
    status = bot.reply_to(message, "⚡ **Calculando P&L con precisión decimal...**")
    query = f"Analiza estas posiciones y busca precios actuales de mercado: {portafolio}. Solo tabla de resultados."
    res = cerebro_genesis(query)
    bot.edit_message_text(f"📊 **ESTADO DE CUENTA**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: message.text == "🚀 Operar (Auto)")
def instruccion_auto(message):
    bot.reply_to(message, "📝 Dime qué compraste (Ej: Comprar 100 NVDA)")

@bot.message_handler(func=lambda message: message.text.lower().startswith(("comprar ", "vender ")))
def auto_registro(message):
    status = bot.reply_to(message, "🔍 **Buscando precio de mercado...**")
    res = cerebro_genesis(f"Busca el precio actual de {message.text} y confírmalo como REGISTRO.")
    portafolio.append(res)
    bot.edit_message_text(f"✅ **REGISTRADO**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: message.text == "📈 Escáner SMT")
def smt(message):
    bot.reply_to(message, cerebro_genesis("Busca divergencias SMT institucionales ahora."))

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
    res = cerebro_genesis("Analiza esta gráfica. Zonas de liquidez y POI.", base64.b64encode(img_data).decode('utf-8'))
    bot.reply_to(message, f"🎯 **ANÁLISIS SMC:**\n{res}")

@bot.message_handler(func=lambda message: True)
def handle_all(message):
    bot.reply_to(message, cerebro_genesis(message.text))

if __name__ == "__main__":
    bot.infinity_polling()
