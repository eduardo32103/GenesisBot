import os, io, requests, base64
from flask import Flask, request
import telebot

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
# Ahora el bot buscará la clave en la configuración de Vercel
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def webhook():
    update = telebot.types.Update.de_json(request.get_data().decode('utf-8'))
    bot.process_new_updates([update])
    return '', 200

def analizar_con_gpt(img_b64):
    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Analiza esta gráfica de trading."},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
                ]
            }
        ]
    }
    res = requests.post(url, headers=headers, json=payload)
    data = res.json()
    if 'choices' in data:
        return data['choices'][0]['message']['content']
    return f"⚠️ Error: {data.get('error', {}).get('message', 'Clave no configurada')}"

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    bot.reply_to(message, "🎯 Analizando... un momento.")
    file_info = bot.get_file(message.photo[-1].file_id)
    downloaded_file = bot.download_file(file_info.file_path)
    img_b64 = base64.b64encode(downloaded_file).decode('utf-8')
    bot.reply_to(message, analizar_con_gpt(img_b64))

@app.route('/')
def index(): return "ONLINE", 200
