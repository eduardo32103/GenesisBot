import os, io, requests, base64
from flask import Flask, request
import telebot

# --- CONFIGURACIÓN ---
TOKEN_TELEGRAM = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
# La clave que me pasaste, asegurándonos de que no tenga espacios
RAW_KEY = "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"
OPENAI_API_KEY = RAW_KEY.strip()

bot = telebot.TeleBot(TOKEN_TELEGRAM, threaded=False)
app = Flask(__name__)

def analizar_con_gpt(img_b64):
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
                    {"type": "text", "text": "Eres un experto en trading. Analiza esta gráfica."},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
                ]
            }
        ]
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        res_json = response.json()
        
        if 'choices' in res_json:
            return res_json['choices'][0]['message']['content']
        
        # Esto nos dirá el error EXACTO que da OpenAI
        error_info = res_json.get('error', {})
        msg = error_info.get('message', 'Error desconocido')
        tipo = error_info.get('type', 'Unknown')
        return f"❌ ERROR DE OPENAI:\nTipo: {tipo}\nMensaje: {msg}"
        
    except Exception as e:
        return f"❌ ERROR DE CONEXIÓN: {str(e)}"

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    try:
        bot.send_chat_action(message.chat.id, 'typing')
        file_info = bot.get_file(message.photo[-1].file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        img_b64 = base64.b64encode(downloaded_file).decode('utf-8')
        
        resultado = analizar_con_gpt(img_b64)
        bot.reply_to(message, f"🦅 **GÉNESIS:**\n\n{resultado}")
    except Exception as e:
        bot.reply_to(message, f"❌ Error en el bot: {str(e)}")

@app.route('/webhook', methods=['POST'])
def webhook():
    update = telebot.types.Update.de_json(request.get_data().decode('utf-8'))
    bot.process_new_updates([update])
    return '', 200

@app.route('/')
def index(): return "V17 ONLINE", 200
