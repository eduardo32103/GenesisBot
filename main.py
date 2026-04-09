import os, io, requests, base64
from flask import Flask, request
import telebot

TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = "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"

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

# Ruta principal para verificar que el bot está vivo
@app.route('/')
def index():
    return "GÉNESIS V20 ESTÁ VIVO", 200

# Ruta del Webhook (donde llega Telegram)
@app.route('/webhook', methods=['POST'])
def webhook():
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        update = telebot.types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return '', 200
    return 'Error', 403

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    bot.reply_to(message, "✅ ¡GÉNESIS PRO CONECTADO! Envíame una foto de tu gráfica.")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    bot.reply_to(message, "🎯 Recibido. Analizando con GPT-4o-mini...")
    # (Aquí iría la lógica de OpenAI que ya tienes, pero primero hagamos que responda esto)

@bot.message_handler(func=lambda m: True)
def echo_all(message):
    bot.reply_to(message, "👋 ¡Hola Eduardo! Estoy funcionando. Mándame una gráfica.")

if __name__ == "__main__":
    app.run(debug=True)
