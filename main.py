import os, io, requests, base64
from flask import Flask, request
import telebot

# --- CONFIGURACIÓN FINAL ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
# He puesto tu nueva Key aquí abajo
RAW_KEY = "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"
OPENAI_API_KEY = RAW_KEY.strip()

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

@app.route('/')
def index():
    return "GÉNESIS V21 ONLINE - CUMPLE EDUARDO", 200

@app.route('/webhook', methods=['POST'])
def webhook():
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        update = telebot.types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return '', 200
    return 'Error', 403

def analizar_con_gpt(img_b64):
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_API_KEY}"
    }
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Eres GÉNESIS, un trader experto en Smart Money Concepts. Analiza esta gráfica: estructura, Order Blocks y posibles entradas."},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
                ]
            }
        ],
        "max_tokens": 1000
    }
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=45)
        res_json = response.json()
        if 'choices' in res_json:
            return res_json['choices'][0]['message']['content']
        return f"⚠️ OpenAI dice: {res_json.get('error', {}).get('message', 'Error')}"
    except Exception as e:
        return f"⚠️ Error técnico: {str(e)}"

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.reply_to(message, "🦅 GÉNESIS PRO ACTIVO. Envíame tu gráfica.")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    bot.reply_to(message, "🎯 Analizando... dame un momento.")
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        img_b64 = base64.b64encode(downloaded_file).decode('utf-8')
        resultado = analizar_con_gpt(img_b64)
        bot.reply_to(message, f"🦅 **GÉNESIS PRO:**\n\n{resultado}")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {str(e)}")

@bot.message_handler(func=lambda m: True)
def echo_all(message):
    bot.reply_to(message, "👋 ¡Listo! Mándame una gráfica.")
