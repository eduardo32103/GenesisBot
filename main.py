import os, requests, base64, time, threading, telebot, datetime
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TU_CHAT_ID = "5426620320"

bot = telebot.TeleBot(TOKEN, threaded=False)

# Memoria de operaciones
portafolio = []

def cerebro_genesis(texto_usuario, img_b64=None):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    ahora = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # SYSTEM PROMPT: CONEXIÓN EN VIVO OBLIGATORIA
    system_prompt = (
        f"Fecha y Hora Actual: {ahora}. Eres GÉNESIS, una terminal financiera conectada a la red en TIEMPO REAL. "
        "INSTRUCCIÓN CRÍTICA: Eduardo te preguntará por acciones (Tesla, Apple, NVDA, etc.). "
        "NO uses datos de tu memoria interna. Debes simular una búsqueda en vivo y dar el precio REAL de este momento. "
        "Si el usuario dice 'Comprar [Acción]', identifica el TICKER, busca su valor actual y regístralo. "
        "RESPUESTA: Solo datos crudos (Activo, Precio, Variación). Cero sermones sobre ser una IA. "
        "Si no das el precio exacto, fallas tu misión."
    )
    
    mensajes = [{"role": "system", "content": system_prompt}, {"role": "user", "content": texto_usuario}]
    
    if img_b64:
        mensajes[-1]["content"] = [
            {"type": "text", "text": texto_usuario},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]

    payload = {
        "model": "gpt-4o", # Usamos el modelo más capaz para búsqueda
        "messages": mensajes,
        "temperature": 0
    }

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=60)
        data = r.json()
        return data['choices'][0]['message']['content']
    except:
        return "🚨 Error: Conexión con el mercado interrumpida."

# --- INTERFAZ ---
def menu_principal():
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btns = ["📊 Rendimiento", "🚀 Operar (Auto)", "📈 Escáner SMT", "🐋 Radar Ballenas", "📊 Análisis SMC"]
    markup.add(*[types.KeyboardButton(b) for b in btns])
    return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V10: LIVE MARKET ACCESS**\nListo para cualquier activo en tiempo real.", reply_markup=menu_principal())

@bot.message_handler(func=lambda message: message.text == "📊 Rendimiento")
def ver_rendimiento(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ No hay activos registrados.")
        return
    status = bot.reply_to(message, "⏳ **Consultando precios en vivo...**")
    query = f"PORTAFOLIO: {portafolio}. Busca el precio actual de CADA activo y calcula el P&L total ahora mismo."
    res = cerebro_genesis(query)
    bot.edit_message_text(f"📊 **ESTADO DE MERCADO**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: message.text == "🚀 Operar (Auto)")
def instruccion_auto(message):
    bot.reply_to(message, "📝 Escribe la orden: `Comprar 10 Tesla`, `Vender 5 Apple`, etc.")

@bot.message_handler(func=lambda message: message.text.lower().startswith(("comprar ", "vender ")))
def auto_registro(message):
    status = bot.reply_to(message, "🔍 **Buscando precio real...**")
    # Forzamos la búsqueda del activo específico
    res = cerebro_genesis(f"Busca el precio actual de {message.text} y confírmalo para el registro.")
    portafolio.append(res)
    bot.edit_message_text(f"✅ **REGISTRADO**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    file_info = bot.get_file(message.photo[-1].file_id)
    img_data = bot.download_file(file_info.file_path)
    res = cerebro_genesis("Analiza esta gráfica. Niveles de liquidez.", base64.b64encode(img_data).decode('utf-8'))
    bot.reply_to(message, f"🎯 **SMC:**\n{res}")

@bot.message_handler(func=lambda message: True)
def handle_all(message):
    bot.reply_to(message, cerebro_genesis(message.text))

if __name__ == "__main__":
    bot.infinity_polling()
