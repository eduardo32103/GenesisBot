import os, io, requests, base64
from flask import Flask, request
import telebot
from PIL import Image

# Configuración con tus credenciales
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
CLAVE_IA = "AIzaSyDT0JlbsCt8pMvgqb_r51XFT3N5lATAnEY"

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

def preguntar_ia(texto, img_b64=None):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={CLAVE_IA}"
    parts = [{"text": f"Eres GÉNESIS, un trader experto. Analiza: {texto}"}]
    if img_b64:
        parts.append({"inline_data": {"mime_type": "image/jpeg", "data": img_b64}})
    payload = {"contents": [{"parts": parts}]}
    try:
        res = requests.post(url, json=payload, timeout=30).json()
        return res['candidates'][0]['content']['parts'][0]['text']
    except:
        return "⚠️ Error al conectar con la IA de Google."

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
        res = preguntar_ia("Analiza esta gráfica de trading detalladamente.", img_b64)
        bot.reply_to(message, f"🎯 **GÉNESIS:**\n\n{res}")
    except Exception as e:
        bot.reply_to(message, "Error al procesar la imagen.")

@bot.message_handler(func=lambda m: True)
def handle_text(message):
    res = preguntar_ia(message.text)
    bot.reply_to(message, res)

@app.route('/webhook', methods=['POST'])
def webhook():
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        update = telebot.types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return '', 200
    return '', 403

@app.route('/')
def index():
    return "GÉNESIS V3 ONLINE", 200
