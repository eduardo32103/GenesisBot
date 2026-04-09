import os, requests, base64
from flask import Flask, request
import telebot

TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
# La clave va aquí adentro, sin espacios, en una sola línea
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
    # Mensaje inicial rápido para que Telegram no dé timeout
    status_msg = bot.reply_to(message, "🦅 GÉNESIS analizando... (Rápido)")
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        img_data = bot.download_file(file_info.file_path)
        img_b64 = base64.b64encode(img_data).decode('utf-8')
        
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
        payload = {
            "model": "gpt-4o-mini",
            "messages": [
                {
                    "role": "user", 
                    "content": [
                        {"type": "text", "text": "Analiza esta grafica de trading. Se muy breve, maximo 3 parrafos."},
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}", "detail": "low"}} # <--- DETALLE BAJO PARA QUE SEA RÁPIDO
                    ]
                }
            ],
            "max_tokens": 200
        }
        
        # Petición con tiempo límite
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=9)
        res = r.json()
        
        if 'choices' in res:
            bot.edit_message_text(res['choices'][0]['message']['content'], message.chat.id, status_msg.message_id)
        else:
            bot.edit_message_text(f"❌ Error de OpenAI: {res.get('error', {}).get('message', 'Desconocido')}", message.chat.id, status_msg.message_id)
            
    except Exception as e:
        bot.edit_message_text(f"⚠️ Tiempo agotado o error: Reintenta con una captura más pequeña.", message.chat.id, status_msg.message_id)

@app.route('/')
def index(): return "READY", 200
