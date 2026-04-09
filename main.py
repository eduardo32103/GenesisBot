import os, requests, base64
from flask import Flask, request
import telebot

# Configuración
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# ESTA LÍNEA ES LA QUE FALTA EN TU PROYECTO:
app = Flask(__name__)
bot = telebot.TeleBot(TOKEN, threaded=False)

@app.route('/webhook', methods=['POST'])
def webhook():
    update = telebot.types.Update.de_json(request.get_data().decode('utf-8'))
    bot.process_new_updates([update])
    return '', 200

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    bot.reply_to(message, "🦅 Analizando gráfica... un momento.")
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        img_data = bot.download_file(file_info.file_path)
        img_b64 = base64.b64encode(img_data).decode('utf-8')
        
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
        payload = {
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": [{"type": "text", "text": "Analiza esta grafica de trading brevemente."}, {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}]}],
            "max_tokens": 500
        }
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
        res = r.json()
        
        if 'choices' in res:
            bot.reply_to(message, f"🦅 **GÉNESIS PRO:**\n\n{res['choices'][0]['message']['content']}")
        else:
            bot.reply_to(message, f"❌ OpenAI dice: {res.get('error', {}).get('message', 'Error')}")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {str(e)}")

@app.route('/')
def index(): return "ONLINE", 200
