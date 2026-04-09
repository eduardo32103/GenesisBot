import os, io, requests, base64
from flask import Flask, request
import telebot

# --- CONFIGURACIÓN TOTAL ---
TOKEN_TELEGRAM = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
# He puesto la clave que me pasaste arriba
OPENAI_API_KEY = "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"

bot = telebot.TeleBot(TOKEN_TELEGRAM, threaded=False)
app = Flask(__name__)

def analizar_con_gpt4(img_b64):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_API_KEY}"
    }
    
    payload = {
        "model": "gpt-4o",
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text", 
                        "text": "Eres GÉNESIS, un trader experto en Smart Money y Price Action. Analiza esta gráfica: identifica tendencia, zonas de oferta/demanda y posibles entradas."
                    },
                    {
                        "type": "image_url", 
                        "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}
                    }
                ]
            }
        ],
        "max_tokens": 1000
    }
    
    try:
        response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=45)
        res_json = response.json()
        
        if 'choices' in res_json:
            return res_json['choices'][0]['message']['content']
        else:
            # Aquí capturamos el mensaje de error de OpenAI
            error_detail = res_json.get('error', {}).get('message', 'Error desconocido')
            return f"⚠️ Nota de OpenAI: {error_detail}\n\n(Si dice Quota, espera 15 min a que el saldo se active)."
    except Exception as e:
        return f"⚠️ Error crítico: {str(e)}"

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        img_b64 = base64.b64encode(downloaded_file).decode('utf-8')
        
        bot.send_chat_action(message.chat.id, 'typing')
        res = analizar_con_gpt4(img_b64)
        bot.reply_to(message, f"🦅 **GÉNESIS PRO:**\n\n{res}")
    except:
        bot.reply_to(message, "❌ Error al procesar la imagen.")

@bot.message_handler(func=lambda m: True)
def handle_text(message):
    bot.reply_to(message, "¡GÉNESIS PRO está listo! Envíame una captura de pantalla de tu gráfica para analizarla.")

@app.route('/webhook', methods=['POST'])
def webhook():
    json_string = request.get_data().decode('utf-8')
    update = telebot.types.Update.de_json(json_string)
    bot.process_new_updates([update])
    return '', 200

@app.route('/')
def index(): return "GÉNESIS PRO V14 ONLINE", 200
