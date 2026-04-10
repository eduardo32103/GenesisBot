import os, requests, base64, time, threading
from flask import Flask, request
import telebot
from telebot.types import ReplyKeyboardMarkup, KeyboardButton

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
# El ID de tu chat para que el bot te mande alertas solo (Búscalo en el bot @userinfobot)
TU_CHAT_ID = "TU_CHAT_ID_AQUI" 

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

# --- CEREBRO GÉNESIS (ANÁLISIS DE SIEMPRE) ---
def cerebro_genesis(texto_usuario=None, img_b64=None, system_role="Radar de Trading"):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    system_prompt = (
        f"Eres GÉNESIS, un {system_role}. Meta: 10% mensual. "
        "Analiza SMC (ballenas), Geopolítica y Técnico. Si ves señal, da % de subida."
    )
    contenido = []
    if texto_usuario: contenido.append({"type": "text", "text": texto_usuario})
    if img_b64:
        contenido.append({"type": "text", "text": "Analiza esta gráfica."})
        contenido.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}", "detail": "low"}})

    payload = {
        "model": "gpt-4o-mini",
        "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": contenido}],
        "max_tokens": 1000
    }
    r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=25)
    data = r.json()
    return data['choices'][0]['message']['content'] if 'choices' in data else "🚨 Error en cerebro."

# --- RADAR AUTÓNOMO (LA NUEVA VIDA DEL BOT) ---
def monitor_activo():
    """Esta función corre 24/7 buscando noticias y ballenas."""
    while True:
        try:
            # 1. SIMULACIÓN DE ESCANEO (Aquí conectaremos APIs de noticias después)
            # Por ahora, GÉNESIS hace un barrido global cada 15 minutos
            analisis_flash = cerebro_genesis("Busca noticias de última hora sobre Trump, Elon Musk o conflictos en el mundo y dime si hay algo urgente para una acción.")
            
            if "⚠️ ALERTA" in analisis_flash or "URGENTE" in analisis_flash.upper():
                bot.send_message(TU_CHAT_ID, f"📡 **ALERTA DEL RADAR ACTIVO:**\n\n{analisis_flash}")
            
            time.sleep(900) # Duerme 15 minutos y vuelve a escanear solo
        except Exception as e:
            print(f"Error en monitor: {e}")
            time.sleep(60)

# Iniciar el radar en un hilo separado
threading.Thread(target=monitor_activo, daemon=True).start()

# --- MANEJADORES DE SIEMPRE (No quitamos nada) ---
@bot.message_handler(commands=['start'])
def send_welcome(message):
    markup = ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    markup.add(KeyboardButton('🌍 SCANNER DE NOTICIAS'), KeyboardButton('🐋 RADAR DE BALLENAS'))
    markup.add(KeyboardButton('📊 ANÁLISIS TÉCNICO'), KeyboardButton('🎯 MI META 10%'))
    bot.send_message(message.chat.id, "🦅 **GÉNESIS TOTAL ACTIVO**\nRadar autónomo encendido.", reply_markup=markup)

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    status_msg = bot.reply_to(message, "🦅 Escaneando imagen... (SMC + Técnico)")
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        img_data = bot.download_file(file_info.file_path)
        img_b64 = base64.b64encode(img_data).decode('utf-8')
        res = cerebro_genesis(img_b64=img_b64)
        bot.edit_message_text(f"📡 **REPORTE GÉNESIS:**\n\n{res}", message.chat.id, status_msg.message_id)
    except Exception as e: bot.edit_message_text(f"❌ Error: {e}", message.chat.id, status_msg.message_id)

@bot.message_handler(func=lambda message: True)
def handle_text(message):
    res = cerebro_genesis(texto_usuario=message.text)
    bot.reply_to(message, f"🦅 **GÉNESIS INFORMA:**\n\n{res}")

@app.route('/')
def index(): return "GÉNESIS 24/7 ONLINE", 200

# Para Railway no usamos webhooks de Vercel, usamos polling para que esté siempre vivo
if __name__ == "__main__":
    bot.infinity_polling()
