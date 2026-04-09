import os, requests, base64
from flask import Flask, request
import telebot

# --- CONFIGURACIÓN DIRECTA ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
# Ponemos la clave aquí directamente para evitar errores de Vercel:
OPENAI_API_KEY = "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"

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def webhook():
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        update = telebot.types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return '', 200
    return 'Error', 403

def analizar_grafica(img_b64):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    payload = {
        "model": "gpt-4o-mini",
        "messages": [{"role": "user", "content": [{"type": "text", "text": "Analiza esta grafica de trading brevemente."}, {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}]}],
        "max_tokens": 500
    }
    r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
    data = r.json()
    if 'choices' in data:
        return data['choices'][0]['message']['content']
    return f"⚠️ OpenAI dice: {data.get('error', {}).get('message', 'Error desconocido')}"

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    bot.reply_to(message, "🦅 GÉNESIS analizando con tu saldo de $10... dame 5 segundos.")
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        img_data = bot.download_file(file_info.file_path)
        img_b64 = base64.b64encode(img_data).decode('utf-8')
        resultado = analizar_grafica(img_b64)
        bot.reply_to(message, f"🦅 **GÉNESIS PRO:**\n\n{resultado}")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {str(e)}")

@app.route('/')
def index(): return "GÉNESIS ONLINE", 200
