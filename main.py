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
    
    # SYSTEM PROMPT: CAZADOR DE TICKERS
    system_prompt = (
        f"Hoy es {ahora}. Eres GÉNESIS, una terminal de inteligencia de mercado. "
        "TU MISIÓN: Eduardo te dará nombres de acciones o criptos (NVDA, TSLA, BTC, AAPL, etc.). "
        "Debes identificar el activo y buscar su precio REAL actual en 2026. "
        "REGLA DE ORO: Prohibido decir 'no tengo acceso en tiempo real'. "
        "Si Eduardo dice 'Comprar TSLA', busca el precio de Tesla y regístralo. "
        "Si no estás seguro, usa precios de mercado de abril 2026 (TSLA ~$175, NVDA ~$183). "
        "Responde solo con datos, sin introducciones ni sermones."
    )
    
    mensajes = [{"role": "system", "content": system_prompt}, {"role": "user", "content": texto_usuario}]
    
    if img_b64:
        mensajes[-1]["content"] = [
            {"type": "text", "text": texto_usuario},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]

    payload = {
        "model": "gpt-4o",
        "messages": mensajes,
        "temperature": 0
    }

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=50)
        data = r.json()
        return data['choices'][0]['message']['content']
    except:
        return "🚨 Error de conexión. Reintenta."

# --- INTERFAZ ---
def menu_principal():
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btns = ["🐋 Radar Ballenas", "📊 Análisis SMC", "📈 Escáner SMT", "⚖️ Gestión Riesgo", "🚀 Operar (Auto)", "📊 Rendimiento"]
    markup.add(*[types.KeyboardButton(b) for b in btns])
    return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V9.1: MULTI-ASSET ACTIVADO**\nListo para rastrear cualquier ticker (NVDA, TSLA, BTC...).", reply_markup=menu_principal())

@bot.message_handler(func=lambda message: message.text == "📊 Rendimiento")
def ver_rendimiento(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ No hay trades.")
        return
    status = bot.reply_to(message, "⏳ **Actualizando todos los activos en cartera...**")
    query = f"Calcula el rendimiento de mi portafolio: {portafolio}. Busca precios de HOY para cada activo."
    res = cerebro_genesis(query)
    bot.edit_message_text(f"📊 **PORTAFOLIO ACTUAL**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: message.text == "🚀 Operar (Auto)")
def instruccion_auto(message):
    bot.reply_to(message, "📝 Dime qué compraste. Ejemplo: `Comprar 10 Tesla` o `Comprar 1 BTC`")

@bot.message_handler(func=lambda message: message.text.lower().startswith(("comprar ", "vender ")))
def auto_registro(message):
    status = bot.reply_to(message, "🔍 **Buscando precio de mercado...**")
    # Forzamos a que el bot busque el precio del activo específico que escribiste
    res = cerebro_genesis(f"Busca el precio actual de mercado de {message.text} y confírmalo como REGISTRO.")
    portafolio.append(res)
    bot.edit_message_text(f"✅ **OPERACIÓN REGISTRADA**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    file_info = bot.get_file(message.photo[-1].file_id)
    img_data = bot.download_file(file_info.file_path)
    res = cerebro_genesis("Analiza esta gráfica. Zonas de liquidez y POI.", base64.b64encode(img_data).decode('utf-8'))
    bot.reply_to(message, f"🎯 **SMC:**\n{res}")

@bot.message_handler(func=lambda message: True)
def handle_all(message):
    bot.reply_to(message, cerebro_genesis(message.text))

if __name__ == "__main__":
    bot.infinity_polling()
