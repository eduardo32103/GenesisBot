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
    
    parts = [{"text": f"Eres GÉNESIS, analista experto. Analiza esto: {texto}"}]
    if img_b64:
        parts.append({"inline_data": {"mime_type": "image/jpeg", "data": img_b64}})
    
    payload = {"contents": [{"parts": parts}]}
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        res_json = response.json() # Aquí es donde fallaba antes
        
        if 'candidates' in res_json:
            cand = res_json['candidates'][0]
            if 'content' in cand:
                return cand['content']['parts'][0]['text']
            if cand.get('finishReason') == 'SAFETY':
                return "⚠️ GÉNESIS: Google bloqueó el análisis por sus reglas de seguridad (posible consejo financiero). Prueba con una gráfica más limpia."
        
        return "⚠️ La IA no pudo generar una respuesta clara."
    except Exception as e:
        return "⚠️ Error al procesar la respuesta de la IA."

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        img = Image.open(io.BytesIO(downloaded_file)).convert("RGB")
        
        # Reducir calidad para evitar bloqueos por tamaño
        img.thumbnail((800, 800))
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=70)
        img_b64 = base64.b64encode(buf.getvalue()).decode('utf-8')
        
        bot.send_chat_action(message.chat.id, 'typing')
        res = preguntar_ia("Analiza esta gráfica de trading.", img_b64)
        bot.reply_to(message, f"🎯 **GÉNESIS:**\n\n{res}")
    except:
        bot.reply_to(message, "❌ No pude procesar esa imagen.")

@bot.message_handler(func=lambda m: True)
def handle_text(message):
    bot.send_chat_action(message.chat.id, 'typing')
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
    return "GÉNESIS V6 ONLINE", 200
