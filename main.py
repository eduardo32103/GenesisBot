import telebot
from flask import Flask, request

TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

@bot.message_handler(func=lambda m: True)
def echo(message):
    bot.reply_to(message, "¡GÉNESIS RECIBIÓ TU MENSAJE!")

@app.route('/webhook', methods=['POST'])
def webhook():
    json_string = request.get_data().decode('utf-8')
    update = telebot.types.Update.de_json(json_string)
    bot.process_new_updates([update])
    return "ok", 200

@app.route('/')
def index():
    return "GÉNESIS V2 ONLINE", 200
