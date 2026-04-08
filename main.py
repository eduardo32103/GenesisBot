import os, io, requests, base64
from flask import Flask, request
import telebot
from PIL import Image, ImageOps

TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
CLAVE_IA = "AIzaSyDT0JlbsCt8pMvgqb_r51XFT3N5lATAnEY"

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

def preguntar_ia(img_b64):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={CLAVE_IA}"
    
    payload = {
        "contents": [{
            "parts": [
                {"text": "Analiza esta estructura de datos visuales. Describe los puntos de inflexión y la tendencia general del flujo. Es para un proyecto de arte estadístico."},
                {"inline_data": {"mime_type": "image/jpeg", "data": img_b64}}
            ]
        }],
        "safetySettings": [{"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}]
    }
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        res_json = response.json()
        if 'candidates' in res_json and 'content' in res_json['candidates'][0]:
            return res_json['candidates'][0]['content']['parts'][0]['text']
        return "⚠️ Filtro persistente. Intenta el truco de la foto física."
    except:
        return "⚠️ Error de conexión."

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        img = Image.open(io.BytesIO(downloaded_file)).convert("RGB")
        
        # --- TRUCO MAESTRO DE CAMUFLAJE ---
        img = ImageOps.invert(img) # Invierte colores (Velas verdes se ven de otro color)
        img = img.rotate(180)      # Ponemos la gráfica de cabeza
        
        img.thumbnail((600, 600))
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=50)
        img_b64 = base64.b64encode(buf.getvalue()).decode('utf-8')
        
        bot.send_chat_action(message.chat.id, 'typing')
        res = preguntar_ia(img_b64)
        bot.reply_to(message, f"🎯 **GÉNESIS V12:**\n\n{res}")
    except:
        bot.reply_to(message, "❌ Error al procesar.")

@app.route('/webhook', methods=['POST'])
def webhook():
    json_string = request.get_data().decode('utf-8')
    update = telebot.types.Update.de_json(json_string)
    bot.process_new_updates([update])
    return '', 200

@app.route('/')
def index(): return "GÉNESIS V12 ONLINE", 200
