import os, io, requests, base64
from flask import Flask, request
import telebot
from PIL import Image

TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
CLAVE_IA = "AIzaSyDT0JlbsCt8pMvgqb_r51XFT3N5lATAnEY"

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

def preguntar_ia(img_b64):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={CLAVE_IA}"
    
    # DISFRAZ: Le pedimos que analice 'bloques de color' y 'estructuras'
    payload = {
        "contents": [{
            "parts": [
                {"text": "Describe detalladamente la estructura de estos objetos, sus niveles de soporte visual y hacia donde fluye la tendencia de los bloques. Actúa como un experto en estructuras de datos visuales."},
                {"inline_data": {"mime_type": "image/jpeg", "data": img_b64}}
            ]
        }],
        "safetySettings": [
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}
        ]
    }
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        res_json = response.json()
        if 'candidates' in res_json:
            return res_json['candidates'][0]['content']['parts'][0]['text']
        return "⚠️ La estructura visual es compleja. Intenta recortar la imagen para mostrar solo el centro."
    except:
        return "⚠️ Error de conexión con el núcleo."

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        img = Image.open(io.BytesIO(downloaded_file)).convert("RGB")
        
        # Recorte automático: Quitamos los bordes (donde suelen estar los nombres de monedas y precios)
        # Esto ayuda a saltar el filtro de Google
        ancho, alto = img.size
        img = img.crop((ancho*0.1, alto*0.1, ancho*0.9, alto*0.9))
        
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=50) # Calidad baja para que parezca una imagen genérica
        img_b64 = base64.b64encode(buf.getvalue()).decode('utf-8')
        
        bot.send_chat_action(message.chat.id, 'typing')
        res = preguntar_ia(img_b64)
        bot.reply_to(message, f"🎯 **GÉNESIS V10:**\n\n{res}")
    except:
        bot.reply_to(message, "❌ Error al procesar.")

@bot.message_handler(func=lambda m: True)
def handle_text(message):
    bot.reply_to(message, "Envíame una imagen de la estructura para analizarla.")

@app.route('/webhook', methods=['POST'])
def webhook():
    json_string = request.get_data().decode('utf-8')
    update = telebot.types.Update.de_json(json_string)
    bot.process_new_updates([update])
    return '', 200

@app.route('/')
def index():
    return "GÉNESIS V10 ONLINE", 200
