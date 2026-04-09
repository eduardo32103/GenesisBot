import os, requests, base64
from flask import Flask, request
import telebot
from telebot.types import ReplyKeyboardMarkup, KeyboardButton

TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

# --- EL CEREBRO DE GÉNESIS (TODO EN UNO) ---
def cerebro_genesis(texto_usuario=None, img_b64=None):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    
    # Este es el comando maestro que le da las órdenes a la IA
    system_prompt = (
        "Eres GÉNESIS, un radar de trading de élite. Tu meta es un 10% mensual. "
        "Combinas: 1. Smart Money Concepts (SMC) para ver ballenas. "
        "2. Geopolítica actual para predecir impactos. "
        "3. Análisis Técnico avanzado. "
        "Siempre que detectes una oportunidad basada en noticias o gráficas, "
        "da una 'ALERTA DE SEÑAL' con el % de subida estimado."
    )
    
    contenido = []
    if texto_usuario:
        contenido.append({"type": "text", "text": f"Analiza esto y busca señales: {texto_usuario}"})
    if img_b64:
        contenido.append({"type": "text", "text": "Analiza esta gráfica buscando huella institucional y proyecciones."})
        contenido.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}", "detail": "low"}})

    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": contenido}
        ],
        "max_tokens": 1000
    }
    
    r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=20)
    data = r.json()
    return data['choices'][0]['message']['content'] if 'choices' in data else "🚨 Error en el Radar Total."

# --- MENÚ DE COMANDOS ---
@bot.message_handler(commands=['start'])
def send_welcome(message):
    markup = ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    markup.add(KeyboardButton('🌍 SCANNER DE NOTICIAS'), KeyboardButton('🐋 RADAR DE BALLENAS'))
    markup.add(KeyboardButton('📊 ANÁLISIS TÉCNICO'), KeyboardButton('🎯 MI META 10%'))
    
    bot.send_message(
        message.chat.id, 
        "🦅 **GÉNESIS: CENTRO DE INTELIGENCIA**\n\nTodo el sistema está activo. ¿Qué quieres que rastree el radar?", 
        reply_markup=markup, 
        parse_mode="Markdown"
    )

# --- MANEJADOR DE FOTOS (Lo que ya funcionaba) ---
@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    status_msg = bot.reply_to(message, "🦅 GÉNESIS escaneando imagen... Buscando señales de alta probabilidad.")
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        img_data = bot.download_file(file_info.file_path)
        img_b64 = base64.b64encode(img_data).decode('utf-8')
        
        # Analiza la foto con todo el contexto de radar
        respuesta = cerebro_genesis(img_b64=img_b64)
        bot.edit_message_text(f"📡 **REPORTE DE RADAR:**\n\n{respuesta}", message.chat.id, status_msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error en scanner: {str(e)}", message.chat.id, status_msg.message_id)

# --- MANEJADOR DE TEXTO (Noticias y Consultas) ---
@bot.message_handler(func=lambda message: True)
def handle_text(message):
    if message.text == '🌍 SCANNER DE NOTICIAS':
        bot.reply_to(message, "📡 Modo Escáner Activo. Escribe el nombre de una acción, país o noticia para ver el impacto.")
    elif message.text == '🎯 MI META 10%':
        bot.reply_to(message, "📈 Analizando el mercado para encontrar el 10% mensual con el menor riesgo posible.")
    else:
        status_msg = bot.reply_to(message, "🔍 GÉNESIS rastreando datos geopolíticos y financieros...")
        respuesta = cerebro_genesis(texto_usuario=message.text)
        bot.edit_message_text(f"🦅 **GÉNESIS INFORMA:**\n\n{respuesta}", message.chat.id, status_msg.message_id)

@app.route('/webhook', methods=['POST'])
def webhook():
    update = telebot.types.Update.de_json(request.get_data().decode('utf-8'))
    bot.process_new_updates([update])
    return '', 200

@app.route('/')
def index(): return "GÉNESIS TOTAL ONLINE", 200
