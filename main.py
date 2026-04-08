import os, io, requests, base64
from flask import Flask, request
import telebot
from PIL import Image

TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
CLAVE_IA = "AIzaSyDT0JlbsCt8pMvgqb_r51XFT3N5lATAnEY"

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

def preguntar_ia(texto, img_b64=None):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={CLAVE_IA}"
    
    # Configuración para saltar bloqueos de "seguridad" en trading
    safety_settings = [
        {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
        {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
        {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
        {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}
    ]
    
    parts = [{"text": f"Eres GÉNESIS, un experto en análisis técnico. Analiza de forma educativa: {texto}"}]
    if img_b64:
        parts.append({"inline_data": {"mime_type": "image/jpeg", "data": img_b64}})
    
    payload = {
        "contents": [{"parts": parts}],
        "safetySettings": safety_settings
    }
    
    try:
        res = requests.post(url, json=payload, timeout=30).json()
        
        # Si la IA responde correctamente
        if 'candidates' in res and 'content' in res['candidates'][0]:
            return res['candidates'][0]['content']['parts'][0]['text']
        else:
            # Si la IA bloquea por seguridad
            return "⚠️ GÉNESIS: Google bloqueó esta imagen por sus reglas de seguridad. Intenta con una captura más clara o sin tantos logos."
    except Exception as e:
        return f"⚠️ Error de conexión: {str(e)}"

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        img = Image.open(io.BytesIO(downloaded_file)).convert("RGB")
        img.thumbnail((800, 800))
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=80)
        img_b64 = base64.b64encode(buf.getvalue()).decode('utf-8')
        
        bot.send_chat_action(message.chat.id, 'typing')
        res = preguntar_ia("Analiza esta gráfica de trading.", img_b64)
        bot.reply_to(message, f"🎯 **GÉNESIS:**\n\n{res}")
    except:
        bot.reply_to(message, "Error procesando la imagen.")

@bot.message_handler(func=lambda m: True)
def handle_text(message):
    bot.send_chat_action(message.chat.id, 'typing')
    res = preguntar_ia(message.text)
    bot.reply_to(message, res)

@app.route('/webhook', methods=['POST'])
def webhook():
    json_string = request.get_data().decode('utf-8')
    update = telebot.types.Update.de_json(json_string)
    bot.process_new_updates([update])
    return '', 200

@app.route('/')
def index():
    return "GÉNESIS V4 ONLINE", 200
