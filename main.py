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
    
    # Formato simplificado para evitar el Error 500
    parts = [{"text": f"Eres GÉNESIS, analista técnico. Analiza esto de forma educativa: {texto}"}]
    if img_b64:
        parts.append({"inline_data": {"mime_type": "image/jpeg", "data": img_b64}})
    
    payload = {"contents": [{"parts": parts}]}
    
    try:
        res = requests.post(url, json=payload, timeout=30).json()
        # Si la respuesta es exitosa
        if 'candidates' in res and 'content' in res['candidates'][0]:
            return res['candidates'][0]['content']['parts'][0]['text']
        # Si Google bloquea por seguridad, nos dará un mensaje claro en vez de tronar
        elif 'promptFeedback' in res or ('candidates' in res and res['candidates'][0].get('finishReason') == 'SAFETY'):
            return "⚠️ GÉNESIS: Google bloqueó esta imagen por reglas de seguridad (posible consejo financiero). Intenta con una gráfica sin tantos indicadores o logos."
        else:
            return "⚠️ La IA no pudo procesar la respuesta."
    except Exception as e:
        return f"⚠️ Error de conexión: {str(e)}"

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        img = Image.open(io.BytesIO(downloaded_file)).convert("RGB")
        
        # Comprimir imagen para que Google no la rechace por pesada
        img.thumbnail((800, 800))
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=80)
        img_b64 = base64.b64encode(buf.getvalue()).decode('utf-8')
        
        bot.send_chat_action(message.chat.id, 'typing')
        res = preguntar_ia("Analiza esta gráfica de trading.", img_b64)
        bot.reply_to(message, f"🎯 **GÉNESIS:**\n\n{res}")
    except:
        bot.reply_to(message, "❌ Error al procesar la foto.")

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
    return "GÉNESIS V5 ONLINE", 200
