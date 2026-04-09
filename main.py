import os, io, requests, base64
from flask import Flask, request
import telebot

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
# Esta línea es la magia: busca la clave que pusiste en Vercel
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

@app.route('/')
def index():
    return "GÉNESIS V22 ONLINE", 200

@app.route('/webhook', methods=['POST'])
def webhook():
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        update = telebot.types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return '', 200
    return 'Error', 403

def analizar_con_gpt(img_b64):
    if not OPENAI_API_KEY:
        return "❌ ERROR: La clave OPENAI_API_KEY no está configurada en Vercel."
    
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
                    {"type": "text", "text": "Eres GÉNESIS, experto en trading. Analiza esta gráfica."},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
                ]
            }
        ]
    }
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=45)
        data = response.json()
        if 'choices' in data:
            return data['choices'][0]['message']['content']
        return f"⚠️ OpenAI dice: {data.get('error', {}).get('message', 'Error de llave')}"
    except Exception as e:
        return f"⚠️ Error: {str(e)}"

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    bot.reply_to(message, "🎯 Analizando gráfica... un momento.")
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
