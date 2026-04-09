import os, requests, base64
from flask import Flask, request
import telebot

TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
# Clave directa para evitar errores de pegado en Vercel
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
    bot.reply_to(message, "🦅 GÉNESIS analizando... (Revisando conexión)")
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        img_data = bot.download_file(file_info.file_path)
        img_b64 = base64.b64encode(img_data).decode('utf-8')
        
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
        payload = {
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": [{"type": "text", "text": "Analiza brevemente esta grafica."}, {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}]}],
            "max_tokens": 300
        }
        
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
        res = r.json()
        
        # SI FUNCIONA:
        if 'choices' in res:
            bot.reply_to(message, f"✅ ANÁLISIS:\n\n{res['choices'][0]['message']['content']}")
        # SI FALLA, QUE NOS DIGA QUÉ DIJO OPENAI:
        else:
            mensaje_error = res.get('error', {}).get('message', 'Error desconocido')
            bot.reply_to(message, f"⚠️ OpenAI rechazó la imagen:\n\n{mensaje_error}")

    except Exception as e:
        bot.reply_to(message, f"❌ Error del servidor: {str(e)}")

@app.route('/')
def index(): return "ONLINE", 200
