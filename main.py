import os, requests, base64
from flask import Flask, request
import telebot
from telebot.types import ReplyKeyboardMarkup, KeyboardButton

TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

# --- EL CEREBRO DE DETECCIÓN ---
def analizar_movimiento_institucional(img_b64):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {
                "role": "system",
                "content": "Eres un experto en Smart Money Concepts (SMC) e institucional. Tu meta es detectar huellas de grandes bancos y corporaciones en las gráficas."
            },
            {
                "role": "user", 
                "content": [
                    {
                        "type": "text", 
                        "text": (
                            "Analiza esta gráfica y busca: \n"
                            "1. Zonas de oferta y demanda institucional.\n"
                            "2. Order Blocks o Fair Value Gaps (FVG).\n"
                            "3. Manipulación de liquidez (barridos de stops).\n"
                            "Al final, dame una 'ALERTA GÉNESIS' si detectas que las corporaciones están entrando al mercado."
                        )
                    },
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}", "detail": "low"}}
                ]
            }
        ],
        "max_tokens": 600
    }
    r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=12)
    data = r.json()
    return data['choices'][0]['message']['content'] if 'choices' in data else "🚨 Error en el sensor institucional."

# --- INTERFAZ ---
@bot.message_handler(commands=['start'])
def send_welcome(message):
    markup = ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)
    btn1 = KeyboardButton('🔍 Detectar Institucionales (SMC)')
    markup.add(btn1)
    
    bot.send_message(
        message.chat.id, 
        "🦅 **GÉNESIS SISTEMA DE MONITOREO**\n\nPresiona el botón o envía tu gráfica para detectar movimientos de grandes corporaciones.", 
        reply_markup=markup, 
        parse_mode="Markdown"
    )

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    status_msg = bot.reply_to(message, "📡 Escaneando huellas institucionales... Buscando 'Smart Money'...")
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        img_data = bot.download_file(file_info.file_path)
        img_b64 = base64.b64encode(img_data).decode('utf-8')
        
        resultado = analizar_movimiento_institucional(img_b64)
        
        # Le damos un formato visual de alerta
        mensaje_final = f"🚨 **REPORTE DE MOVIMIENTO CORPORATIVO** 🚨\n\n{resultado}"
        bot.edit_message_text(mensaje_final, message.chat.id, status_msg.message_id, parse_mode="Markdown")
        
    except Exception as e:
        bot.edit_message_text(f"❌ Fallo en el radar: {str(e)}", message.chat.id, status_msg.message_id)

@app.route('/webhook', methods=['POST'])
def webhook():
    update = telebot.types.Update.de_json(request.get_data().decode('utf-8'))
    bot.process_new_updates([update])
    return '', 200

@app.route('/')
def index(): return "RADAR INSTITUCIONAL ACTIVO", 200
