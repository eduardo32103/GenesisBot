import os, io, requests, base64
from flask import Flask, request
import telebot
from PIL import Image

TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
CLAVE_IA = "AIzaSyDT0JlbsCt8pMvgqb_r51XFT3N5lATAnEY"

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

def preguntar_ia(texto, img_b64=None):
    # Usamos la versión estable 1.5-flash
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={CLAVE_IA}"
    
    # Prompt diseñado para NO activar alarmas de "Financial Advice"
    instruccion = (
        "Eres un analista de datos visuales. Describe los patrones, soportes y resistencias "
        "que ves en esta imagen de datos. Identifica la dirección de los movimientos "
        "y posibles puntos de retorno basados en la estructura visual. "
        "Habla de forma técnica pero objetiva."
    )
    
    parts = [{"text": f"{instruccion}\n\nPregunta: {texto}"}]
    if img_b64:
        parts.append({"inline_data": {"mime_type": "image/jpeg", "data": img_b64}})
    
    payload = {
        "contents": [{"parts": parts}],
        "safetySettings": [
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"}
        ]
    }
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        res_json = response.json()
        
        if 'candidates' in res_json and len(res_json['candidates']) > 0:
            return res_json['candidates'][0]['content']['parts'][0]['text']
        return "⚠️ La IA detectó contenido sensible. Intenta con una imagen más limpia (sin logos)."
    except:
        return "⚠️ Error al conectar con el cerebro de GÉNESIS."

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        img = Image.open(io.BytesIO(downloaded_file)).convert("RGB")
        
        # Bajamos la resolución un poco más para que pase desapercibido
        img.thumbnail((700, 700))
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=65)
        img_b64 = base64.b64encode(buf.getvalue()).decode('utf-8')
        
        bot.send_chat_action(message.chat.id, 'typing')
        respuesta = preguntar_ia("Haz un análisis técnico detallado de esta estructura.", img_b64)
        bot.reply_to(message, f"🎯 **GÉNESIS V9:**\n\n{respuesta}")
    except Exception as e:
        bot.reply_to(message, "❌ Error al procesar imagen.")

@bot.message_handler(func=lambda m: True)
def handle_text(message):
    bot.send_chat_action(message.chat.id, 'typing')
    respuesta = preguntar_ia(message.text)
    bot.reply_to(message, respuesta)

@app.route('/webhook', methods=['POST'])
def webhook():
    json_string = request.get_data().decode('utf-8')
    update = telebot.types.Update.de_json(json_string)
    bot.process_new_updates([update])
    return '', 200

@app.route('/')
def index():
    return "GÉNESIS V9 ONLINE", 200
