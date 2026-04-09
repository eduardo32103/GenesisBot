import os, requests, base64
from flask import Flask, request
import telebot

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

def analizar_grafica(img_b64):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    payload = {
        "model": "gpt-4o-mini", # Es el más rápido de todos
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Analiza esta grafica de trading (SMC/Price Action). Se breve."},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
                ]
            }
        ],
        "max_tokens": 500 # Limitamos la respuesta para que sea más veloz
    }
    response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=25)
    return response.json()['choices'][0]['message']['content']

@app.route('/webhook', methods=['POST'])
def webhook():
    update = telebot.types.Update.de_json(request.get_data().decode('utf-8'))
    bot.process_new_updates([update])
    return '', 200

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    bot.reply_to(message, "🦅 GÉNESIS analizando... (esto toma 5-8 seg)")
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        img_data = bot.download_file(file_info.file_path)
        img_b64 = base64.b64encode(img_data).decode('utf-8')
        
        texto_analisis = analizar_grafica(img_b64)
        bot.reply_to(message, f"🦅 **RESULTADO:**\n\n{texto_analisis}")
    except Exception as e:
        bot.reply_to(message, "⚠️ OpenAI tardó demasiado. Intenta con una imagen más pequeña.")

@app.route('/')
def index(): return "ONLINE", 200
