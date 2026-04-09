import os, requests, base64
from flask import Flask, request
import telebot
from telebot.types import ReplyKeyboardMarkup, KeyboardButton

TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

# --- FUNCIONES DE APOYO ---
def analizar_grafica(img_b64, prompt):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {
                "role": "user", 
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}", "detail": "low"}}
                ]
            }
        ],
        "max_tokens": 500
    }
    r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=10)
    data = r.json()
    return data['choices'][0]['message']['content'] if 'choices' in data else "🚨 Error en análisis."

# --- MANEJADORES DE COMANDOS ---
@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    markup = ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btn1 = KeyboardButton('📊 Analizar Tendencia')
    btn2 = KeyboardButton('🎯 Puntos de Entrada')
    btn3 = KeyboardButton('🛡️ Gestión de Riesgo')
    markup.add(btn1, btn2, btn3)
    
    texto_bienvenida = (
        "🦅 **¡GÉNESIS PRO ACTIVO!**\n\n"
        "Estoy listo para analizar tus gráficas. ¿Qué quieres hacer?\n"
        "1. Selecciona un modo abajo.\n"
        "2. Envía la foto de tu gráfica."
    )
    bot.send_message(message.chat.id, texto_bienvenida, reply_markup=markup, parse_mode="Markdown")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    status_msg = bot.reply_to(message, "🦅 Leyendo gráfica... Dame un momento.")
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        img_data = bot.download_file(file_info.file_path)
        img_b64 = base64.b64encode(img_data).decode('utf-8')
        
        # Análisis estándar
        prompt = "Analiza esta gráfica de trading. Indica tendencia, soportes/resistencias y una recomendación breve."
        resultado = analizar_grafica(img_b64, prompt)
        
        bot.edit_message_text(f"🦅 **RESULTADO GÉNESIS:**\n\n{resultado}", message.chat.id, status_msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)}", message.chat.id, status_msg.message_id)

@bot.message_handler(func=lambda message: True)
def handle_text(message):
    if message.text == '📊 Analizar Tendencia':
        bot.reply_to(message, "Mándame la foto y te diré si el precio va para arriba o para abajo. 📈📉")
    elif message.text == '🎯 Puntos de Entrada':
        bot.reply_to(message, "Mándame la foto y buscaré zonas de compra o venta. 💸")
    elif message.text == '🛡️ Gestión de Riesgo':
        bot.reply_to(message, "Mándame tu gráfica y te diré dónde poner el Stop Loss. 🛑")

@app.route('/webhook', methods=['POST'])
def webhook():
    update = telebot.types.Update.de_json(request.get_data().decode('utf-8'))
    bot.process_new_updates([update])
    return '', 200

@app.route('/')
def index(): return "BOT CON BOTONES ONLINE", 200
