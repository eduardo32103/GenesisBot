import os, requests, base64
from flask import Flask, request
import telebot
from telebot.types import ReplyKeyboardMarkup, KeyboardButton

TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

# Diccionario para recordar qué quiere el usuario
user_modes = {}

def llamar_openai(img_b64, prompt):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": "Eres GÉNESIS, un estratega de trading de élite enfocado en rentabilidad del 10% mensual."},
            {"role": "user", "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}", "detail": "low"}}
            ]}
        ],
        "max_tokens": 800
    }
    r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=15)
    data = r.json()
    return data['choices'][0]['message']['content'] if 'choices' in data else "🚨 Error en conexión con el cerebro IA."

@bot.message_handler(commands=['start'])
def send_welcome(message):
    markup = ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btn1 = KeyboardButton('🐋 Radar Institucional (SMC)')
    btn2 = KeyboardButton('🌍 Geopolítica y Proyecciones %')
    btn3 = KeyboardButton('📊 Análisis Técnico Rápido')
    markup.add(btn1, btn2, btn3)
    
    bot.send_message(
        message.chat.id, 
        "🦅 **GÉNESIS ESTRATEGA ACTIVO**\n\nMeta establecida: **10% Mensual**.\nSelecciona tu herramienta de análisis:", 
        reply_markup=markup, 
        parse_mode="Markdown"
    )

@bot.message_handler(func=lambda message: True)
def handle_menu_clicks(message):
    if message.text == '🐋 Radar Institucional (SMC)':
        user_modes[message.chat.id] = "SMC"
        bot.reply_to(message, "📡 MODO BALLENA: Envía la gráfica para detectar manipulación institucional.")
    elif message.text == '🌍 Geopolítica y Proyecciones %':
        user_modes[message.chat.id] = "GEO"
        bot.reply_to(message, "⚖️ MODO MACRO: Envía la gráfica y dime en el pie de foto qué noticia geopolítica quieres evaluar.")
    elif message.text == '📊 Análisis Técnico Rápido':
        user_modes[message.chat.id] = "TEC"
        bot.reply_to(message, "📉 MODO TÉCNICO: Envía la gráfica para ver tendencia y niveles clave.")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    modo = user_modes.get(message.chat.id, "TEC") # Por defecto técnico
    status_msg = bot.reply_to(message, f"🦅 GÉNESIS trabajando en modo: {modo}...")
    
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        img_data = bot.download_file(file_info.file_path)
        img_b64 = base64.b64encode(img_data).decode('utf-8')
        
        if modo == "SMC":
            prompt = "Detecta Order Blocks, FVG y barridos de liquidez. Confirma si hay entrada institucional clara."
        elif modo == "GEO":
            prompt = "Analiza esta gráfica considerando el contexto geopolítico actual. Calcula la probabilidad de subir un % específico y dime si es viable para nuestra meta del 10% mensual."
        else:
            prompt = "Analiza tendencia, soportes y resistencias de forma rápida."

        resultado = llamar_openai(img_b64, prompt)
        bot.edit_message_text(f"🦅 **INFORME GÉNESIS ({modo}):**\n\n{resultado}", message.chat.id, status_msg.message_id)
        
    except Exception as e:
        bot.edit_message_text(f"❌ Error en sistema: {str(e)}", message.chat.id, status_msg.message_id)

@app.route('/webhook', methods=['POST'])
def webhook():
    update = telebot.types.Update.de_json(request.get_data().decode('utf-8'))
    bot.process_new_updates([update])
    return '', 200

@app.route('/')
def index(): return "GÉNESIS ESTRATEGA ONLINE", 200
