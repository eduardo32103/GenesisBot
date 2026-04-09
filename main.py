import os, requests, base64
from flask import Flask, request
import telebot

# --- CONFIGURACIÓN ---
# El bot buscará la clave en Vercel automáticamente
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

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
        "messages": [
            {
                "role": "user", 
                "content": [
                    {"type": "text", "text": "Analiza esta grafica de trading de forma muy breve y profesional."},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}", "detail": "low"}}
                ]
            }
        ],
        "max_tokens": 300
    }
    # Timeout de 9 segundos para que Vercel no corte la conexión
    r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=9)
    data = r.json()
    
    if 'choices' in data:
        return data['choices'][0]['message']['content']
    
    # Si hay error, nos dirá exactamente qué dijo OpenAI
    error_detail = data.get('error', {}).get('message', 'Error desconocido')
    return f"🚨 OpenAI dice: {error_detail}"

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    status_msg = bot.reply_to(message, "🦅 GÉNESIS analizando gráfica...")
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        img_data = bot.download_file(file_info.file_path)
        img_b64 = base64.b64encode(img_data).decode('utf-8')
        
        resultado = analizar_grafica(img_b64)
        bot.edit_message_text(f"🦅 **GÉNESIS PRO:**\n\n{resultado}", message.chat.id, status_msg.message_id)
        
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)}", message.chat.id, status_msg.message_id)

@app.route('/')
def index():
    return "GÉNESIS ONLINE", 200
