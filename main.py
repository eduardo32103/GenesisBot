import os, requests, base64
from flask import Flask, request
import telebot
from telebot.types import ReplyKeyboardMarkup, KeyboardButton

TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

# --- SISTEMA DE INTELIGENCIA ---
def cerebro_genesis(texto, img_b64=None):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    
    # Prompt maestro enfocado en el radar y el 10% mensual
    system_prompt = (
        "Eres GÉNESIS, un radar de señales de alta precisión. Tu objetivo es ayudar al usuario a ganar un 10% mensual. "
        "Analizas movimientos institucionales (SMC), geopolítica y técnico. "
        "Si detectas una oportunidad, di: '⚠️ SEÑAL DETECTADA' y calcula el % de subida probable."
    )
    
    contenido = [{"type": "text", "text": texto}]
    if img_b64:
        contenido.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}", "detail": "low"}})

    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": contenido}
        ],
        "max_tokens": 700
    }
    
    r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=15)
    data = r.json()
    return data['choices'][0]['message']['content'] if 'choices' in data else "📡 Radar fuera de línea. Revisa conexión."

# --- BOT INTERFAZ ---
@bot.message_handler(commands=['start'])
def send_welcome(message):
    markup = ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    markup.add(KeyboardButton('📡 Activar Radar de Señales'), KeyboardButton('🌍 Geopolítica / Macro'))
    markup.add(KeyboardButton('📊 Análisis de Gráfica'), KeyboardButton('🐋 Huella Institucional'))
    
    bot.send_message(
        message.chat.id, 
        "🦅 **GÉNESIS RADAR ACTIVADO**\n\nBuscando el **10% mensual**. \n¿Qué quieres monitorear hoy?", 
        reply_markup=markup, 
        parse_mode="Markdown"
    )

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    status_msg = bot.reply_to(message, "🦅 Escaneando gráfica en el Radar...")
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        img_data = bot.download_file(file_info.file_path)
        img_b64 = base64.b64encode(img_data).decode('utf-8')
        
        # Le pedimos que busque la señal directamente en la imagen
        respuesta = cerebro_genesis("Busca señales institucionales o geopolíticas. ¿Ves el 10% aquí?", img_b64)
        bot.edit_message_text(f"📡 **REPORTE DEL RADAR:**\n\n{respuesta}", message.chat.id, status_msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)}", message.chat.id, status_msg.message_id)

@bot.message_handler(func=lambda message: True)
def handle_text(message):
    if message.text == '📡 Activar Radar de Señales':
        bot.reply_to(message, "Radar en espera... Pásame el nombre de una acción o una gráfica para buscar entradas de alto impacto.")
    else:
        # Aquí permitimos que el usuario pregunte cosas de texto (noticias, activos)
        status_msg = bot.reply_to(message, "🔍 GÉNESIS procesando datos del mercado...")
        respuesta = cerebro_genesis(f"Contexto: {message.text}. ¿Cómo afecta esto a nuestra meta del 10%? ¿Hay señales claras?")
        bot.edit_message_text(f"🦅 **GÉNESIS INFORMA:**\n\n{respuesta}", message.chat.id, status_msg.message_id)

@app.route('/webhook', methods=['POST'])
def webhook():
    update = telebot.types.Update.de_json(request.get_data().decode('utf-8'))
    bot.process_new_updates([update])
    return '', 200

@app.route('/')
def index(): return "RADAR GÉNESIS ONLINE", 200
