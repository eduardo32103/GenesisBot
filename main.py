import os, requests, base64, time, threading, telebot
from flask import Flask
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TU_CHAT_ID = "5426620320" # Pon tu ID real

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

# --- MEMORIA DINÁMICA DE ACTIVOS ---
noticias_enviadas = []
# Lista inicial de vigilancia
watchlist = ["BTC", "Petróleo Brent", "Oro", "NASDAQ", "NVIDIA"]

def cerebro_genesis(texto_usuario=None, img_b64=None, system_role="Asesor Financiero Elite"):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    
    # Prompt que incluye la lista de vigilancia actual
    lista_actual = ", ".join(watchlist)
    system_prompt = (
        f"Eres GÉNESIS, mi Asesor de Inversiones. Tu meta es el 10% mensual. "
        f"Vigila el mundo, pero prioriza estos activos: {lista_actual}. "
        "Si detectas una oportunidad o movimiento >2%, repórtalo como '⚡ OPORTUNIDAD'."
    )
    
    contenido = []
    if texto_usuario: contenido.append({"type": "text", "text": texto_usuario})
    if img_b64:
        contenido.append({"type": "text", "text": "Analiza esta gráfica. Dame niveles de entrada."})
        contenido.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}})

    payload = {
        "model": "gpt-4o-mini",
        "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": contenido}],
        "max_tokens": 800, "temperature": 0.2
    }
    
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=25)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Reconectando sensores..."

# --- RADAR DE VIGILANCIA DINÁMICA ---
def monitor_activo():
    global noticias_enviadas
    while True:
        try:
            # GÉNESIS rastrea la watchlist y el mundo
            activos = ", ".join(watchlist)
            query = f"Escaneo global enfocado en: {activos}. ¿Hay alguna noticia o movimiento de ballenas de alto impacto (>2%)?"
            res = cerebro_genesis(query, system_role="Radar Dinámico")
            
            huella = res[:40]
            if huella not in noticias_enviadas:
                if "⚡" in res or "OPORTUNIDAD" in res.upper():
                    bot.send_message(TU_CHAT_ID, f"🎯 **RADAR DE ACTIVOS**\n━━━━━━━━━━━━━━\n{res}", parse_mode="Markdown")
                    noticias_enviadas.append(huella)
                    if len(noticias_enviadas) > 10: noticias_enviadas.pop(0)
            time.sleep(60)
        except: time.sleep(10)

threading.Thread(target=monitor_activo, daemon=True).start()

# --- MANEJADOR DE ÓRDENES DE VIGILANCIA ---
@bot.message_handler(func=lambda message: message.text.lower().startswith("vigila "))
def agregar_activo(message):
    global watchlist
    nuevo_activo = message.text.replace("vigila ", "").replace("Vigila ", "").strip()
    if nuevo_activo not in watchlist:
        watchlist.append(nuevo_activo)
        bot.reply_to(message, f"✅ Entendido, Eduardo. **{nuevo_activo}** ha sido agregado a mi radar de alta prioridad. Patrullando... 🦅")
    else:
        bot.reply_to(message, f"Aviso: {nuevo_activo} ya estaba en mi lista de vigilancia.")

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(message.chat.id, "🦅 **SISTEMA GÉNESIS ACTUALIZADO**\n\nUsa: 'Vigila [activo]' para añadir a mi radar.")

# (Se mantienen los otros botones y manejo de fotos igual que antes...)
@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    status = bot.reply_to(message, "🔍 Analizando...")
    file_info = bot.get_file(message.photo[-1].file_id)
    img_data = bot.download_file(file_info.file_path)
    res = cerebro_genesis(img_b64=base64.b64encode(img_data).decode('utf-8'))
    bot.edit_message_text(f"📊 **ANÁLISIS**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: True)
def handle_all_text(message):
    res = cerebro_genesis(texto_usuario=message.text)
    bot.reply_to(message, res)

if __name__ == "__main__":
    bot.infinity_polling()
