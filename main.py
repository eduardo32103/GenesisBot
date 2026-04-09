import os, io, requests, base64
from flask import Flask, request
import telebot

TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
RAW_KEY = "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"
OPENAI_API_KEY = RAW_KEY.strip()

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

@app.route('/')
def index():
    return "GÉNESIS V20 ESTÁ VIVO", 200

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
                    {"type": "text", "text": "Eres GÉNESIS, un trader experto en Smart Money Concepts y Price Action. Analiza esta gráfica de trading: identifica la estructura del mercado, zonas de oferta/demanda (Order Blocks), liquidez y posibles puntos de entrada. Sé técnico y directo."},
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
        error_msg = res_json.get('error', {}).get('message', 'Error desconocido')
        return f"⚠️ OpenAI dice: {error_msg}"
    except Exception as e:
        return f"⚠️ Error técnico: {str(e)}"

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    bot.reply_to(message, "✅ ¡GÉNESIS PRO CONECTADO! Envíame una foto de tu gráfica para analizarla.")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    bot.reply_to(message, "🎯 Recibido. Analizando gráfica con IA institucional, dame unos segundos...")
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        img_b64 = base64.b64encode(downloaded_file).decode('utf-8')
        
        resultado = analizar_con_gpt(img_b64)
        bot.reply_to(message, f"🦅 **GÉNESIS PRO:**\n\n{resultado}")
    except Exception as e:
        bot.reply_to(message, f"❌ Error al procesar la imagen: {str(e)}")

@bot.message_handler(func=lambda m: True)
def echo_all(message):
    bot.reply_to(message, "👋 ¡Hola! Estoy listo. Mándame una captura de tu gráfica.")
