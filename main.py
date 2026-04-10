import os
import requests
import base64
import time
import threading
import telebot
from flask import Flask

# --- CONFIGURACIÓN CRÍTICA ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
# ¡CAMBIA ESTE NÚMERO por el que te dio @userinfobot!
TU_CHAT_ID = "PON_AQUI_TU_ID_DE_TELEGRAM" 

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

# --- CEREBRO GÉNESIS (ANÁLISIS GENERAL) ---
def cerebro_genesis(texto_usuario=None, img_b64=None, system_role="Analista Pro"):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
   system_prompt = (
        "Eres GÉNESIS, un analista financiero de ÉLITE con acceso a datos en tiempo real. "
        "Tu meta es el 10% mensual. NO des consejos genéricos ni educativos. "
        "Cuando el usuario te pregunte por un escaneo o ballenas, DEBES: "
        "1. Buscar noticias de última hora en portales financieros (Bloomberg, Reuters, Whale Alert). "
        "2. Identificar movimientos institucionales reales (Smart Money). "
        "3. Dar nombres de activos específicos (ej. BTC, NVDA, Tesla) y por qué hay que vigilarlos. "
        "Si no ves nada claro, di: 'Mercado lateral, sin huella de ballenas', pero nunca des teoría de soportes y resistencias."
    )
    
    
    contenido = []
    if texto_usuario:
        contenido.append({"type": "text", "text": texto_usuario})
    if img_b64:
        contenido.append({"type": "text", "text": "Analiza esta gráfica detalladamente."})
        contenido.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/jpeg;base64,{img_b64}", "detail": "low"}
        })

    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": contenido}
        ],
        "max_tokens": 1000
    }
    
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=30)
        data = r.json()
        return data['choices'][0]['message']['content']
    except Exception as e:
        return f"🚨 Error en el cerebro: {e}"

# --- RADAR EN VIVO (PATRULLAJE 24/7) ---
def monitor_activo():
    """Busca noticias cada 60 segundos y filtra impacto > 2%"""
    print("🦅 Radar de GÉNESIS iniciando patrullaje...")
    while True:
        try:
            query = (
                "Busca noticias financieras y geopolíticas de ÚLTIMA HORA (últimos minutos). "
                "Reporta ÚNICAMENTE si la noticia puede mover el mercado, una acción o cripto más de un 2%. "
                "Si encuentras algo, inicia con '⚡ ALERTA DE ALTO IMPACTO'."
            )
            
            alerta = cerebro_genesis(query, system_role="Radar Geopolítico de Alta Velocidad")
            
            # Si la IA detecta algo importante, te lo manda sin que preguntes
            if "⚡ ALERTA" in alerta or "ALTO IMPACTO" in alerta.upper():
                bot.send_message(TU_CHAT_ID, alerta)
                print("📡 Alerta enviada al usuario.")
            
            # Pausa de 60 segundos para estar 'En Vivo'
            time.sleep(60)
            
        except Exception as e:
            print(f"Error en radar: {e}")
            time.sleep(20)

# Iniciar el hilo del radar para que no bloquee el chat
threading.Thread(target=monitor_activo, daemon=True).start()

# --- MANEJADORES DE TELEGRAM (LO QUE YA TENÍAMOS) ---

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    welcome_text = (
        "🦅 **GÉNESIS PRO ACTIVO**\n\n"
        "Estoy patrullando el mercado 24/7 en vivo.\n"
        "1. **Mándame una gráfica** para análisis SMC.\n"
        "2. **Pregúntame** por cualquier noticia.\n"
        "3. **Espera mis alertas** de alto impacto (>2%)."
    )
    bot.reply_to(message, welcome_text, parse_mode="Markdown")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    status_msg = bot.reply_to(message, "📸 Capturando imagen... Analizando Smart Money Concepts...")
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        img_data = bot.download_file(file_info.file_path)
        img_b64 = base64.b64encode(img_data).decode('utf-8')
        
        analisis = cerebro_genesis(img_b64=img_b64, system_role="Experto en SMC y Acción del Precio")
        bot.edit_message_text(f"📊 **ANÁLISIS DE GRÁFICA:**\n\n{analisis}", message.chat.id, status_msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error al procesar imagen: {e}", message.chat.id, status_msg.message_id)

@bot.message_handler(func=lambda message: True)
def handle_all_text(message):
    # Esto responde a tus preguntas normales
    respuesta = cerebro_genesis(texto_usuario=message.text)
    bot.reply_to(message, respuesta)

# --- INICIO DEL SERVIDOR ---
if __name__ == "__main__":
    # Esto mantiene al bot escuchando en Railway
    print("🦅 GÉNESIS está escuchando...")
    bot.infinity_polling()
