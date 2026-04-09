import os, requests, base64
from flask import Flask, request
import telebot

TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
# Usamos la clave que ya verificamos que está completa
OPENAI_API_KEY = "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"

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def webhook():
    update = telebot.types.Update.de_json(request.get_data().decode('utf-8'))
    bot.process_new_updates([update])
    return '', 200

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    bot.reply_to(message, "🔍 GÉNESIS iniciando diagnóstico...")
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        img_data = bot.download_file(file_info.file_path)
        img_b64 = base64.b64encode(img_data).decode('utf-8')
        
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
        payload = {
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": [{"type": "text", "text": "Analiza esta grafica."}, {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}]}],
            "max_tokens": 300
        }
        
        response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
        res_data = response.json()
        
        if response.status_code == 200:
            bot.reply_to(message, f"✅ ¡LOGRADO!\n\n{res_data['choices'][0]['message']['content']}")
        else:
            # AQUÍ ES DONDE NOS VA A DECIR EL PROBLEMA REAL
            error_msg = res_data.get('error', {}).get('message', 'Error desconocido')
            error_type = res_data.get('error', {}).get('type', 'N/A')
            bot.reply_to(message, f"🚨 ERROR DETECTADO:\n\nStatus: {response.status_code}\nTipo: {error_type}\nDetalle: {error_msg}")

    except Exception as e:
        bot.reply_to(message, f"❌ FALLO DEL SERVIDOR: {str(e)}")

@app.route('/')
def index(): return "DIAGNOSTICO ONLINE", 200
