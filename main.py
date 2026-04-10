import os, requests, base64, time, threading, telebot
from flask import Flask

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TU_CHAT_ID = "5426620320" # Pon tu ID real

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

# MEMORIA PARA NO REPETIR NOTICIAS
noticias_enviadas = []

def cerebro_genesis(texto_usuario=None, img_b64=None, system_role="Analista Pro"):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    system_prompt = (
        "Eres GÉNESIS, una terminal de inteligencia financiera de ÉLITE. "
        "Tu meta: 10% mensual. REGLAS: 1. Sé estético y usa emojis. 2. NO te contradigas; si los datos son inciertos, di que el mercado está en espera. "
        "3. Analiza flujos de dinero real (Smart Money). 4. Solo reporta movimientos de ALTO IMPACTO (>2%)."
    )
    
    contenido = []
    if texto_usuario: contenido.append({"type": "text", "text": texto_usuario})
    if img_b64:
        contenido.append({"type": "text", "text": "Analiza esta gráfica. Busca Order Blocks y Liquidez."})
        contenido.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}", "detail": "low"}})

    payload = {
        "model": "gpt-4o-mini",
        "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": contenido}],
        "max_tokens": 800,
        "temperature": 0.3
    }
    
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=25)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Error de conexión."

# --- RADAR CON MEMORIA Y ESTÉTICA ---
def monitor_activo():
    global noticias_enviadas
    while True:
        try:
            # Pedimos un resumen de lo más importante del minuto
            query = "Busca la noticia financiera MÁS importante de este minuto. Reporta SOLO si es impacto >2% y NO repitas temas generales."
            res = cerebro_genesis(query, system_role="Radar Anti-Spam")
            
            # FILTRO DE REPETICIÓN: Extraemos las primeras 30 letras para comparar
            huella = res[:30] 
            if huella not in noticias_enviadas:
                # Si es una alerta real, le damos formato estético
                if "⚡" in res or "ALERTA" in res.upper():
                    formato_pro = (
                        f"💎 **GÉNESIS INTELLIGENCE** 💎\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"{res}\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"🕒 {time.strftime('%H:%M')} | Radar Activo 🛰️"
                    )
                    bot.send_message(TU_CHAT_ID, formato_pro, parse_mode="Markdown")
                    
                    # Guardar en memoria y limitar a las últimas 10 noticias
                    noticias_enviadas.append(huella)
                    if len(noticias_enviadas) > 10: noticias_enviadas.pop(0)
            
            time.sleep(60)
        except Exception as e:
            time.sleep(10)

threading.Thread(target=monitor_activo, daemon=True).start()

# --- COMANDOS ESTÉTICOS ---
@bot.message_handler(commands=['start'])
def send_welcome(message):
    msg = (
        "🦅 **SISTEMA GÉNESIS ONLINE**\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ **Estado:** Patrullando 24/7\n"
        "📈 **Objetivo:** 10% Mensual\n"
        "🛡️ **Filtro:** Solo Alto Impacto (>2%)\n\n"
        "Mándame una gráfica o espera mis señales de ballenas."
    )
    bot.send_message(message.chat.id, msg, parse_mode="Markdown")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    status = bot.reply_to(message, "🔍 **Analizando Huella Institucional...**", parse_mode="Markdown")
    file_info = bot.get_file(message.photo[-1].file_id)
    img_data = bot.download_file(file_info.file_path)
    res = cerebro_genesis(img_b64=base64.b64encode(img_data).decode('utf-8'))
    bot.edit_message_text(f"📊 **ANÁLISIS TÉCNICO**\n━━━━━━━━━━━━━━\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: True)
def handle_text(message):
    res = cerebro_genesis(texto_usuario=message.text)
    bot.reply_to(message, res)

if __name__ == "__main__":
    bot.infinity_polling()
