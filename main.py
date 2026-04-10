import os, requests, base64, time, threading, telebot
from flask import Flask
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TU_CHAT_ID = "5426620320" # Pon tu ID real aquí

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

noticias_enviadas = []

# --- CEREBRO GÉNESIS (VERSIÓN ASESOR EJECUTIVO) ---
def cerebro_genesis(texto_usuario=None, img_b64=None, system_role="Asesor Financiero Elite"):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    
    # Este prompt obliga a la IA a buscar y dar números, no excusas
    system_prompt = (
        "Eres GÉNESIS, mi Asesor de Inversiones de Élite con acceso a terminal de datos global. "
        "REGLA DE ORO: NUNCA digas que no tienes acceso a datos en tiempo real. "
        "Si necesitas información actual, utiliza tus capacidades de búsqueda interna para rastrear: "
        "1. Whale Alert, CryptoQuant y flujos de grandes exchanges. "
        "2. Terminales de noticias tipo Bloomberg/Reuters para petróleo, oro y forex. "
        "3. SIEMPRE da números específicos: 'Se detectó movimiento de X cantidad', 'El precio de X está en Y'. "
        "Si ves una oportunidad clara, dime: 'AUTORIZACIÓN REQUERIDA: Sugiero entrar en X porque Y'. "
        "Tu meta es mi 10% mensual. Sé breve, estético y muy directo."
    )
    
    contenido = []
    if texto_usuario: contenido.append({"type": "text", "text": texto_usuario})
    if img_b64:
        contenido.append({"type": "text", "text": "Analiza esta gráfica. Dime niveles exactos de entrada y salida basados en liquidez institucional."})
        contenido.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}", "detail": "high"}})

    payload = {
        "model": "gpt-4o-mini", # Este modelo tiene capacidad de búsqueda si el prompt lo exige
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": contenido}
        ],
        "max_tokens": 1000,
        "temperature": 0.1 # Bajamos la temperatura para que no invente y sea preciso
    }
    
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=30)
        res_json = r.json()
        if 'choices' in res_json:
            return res_json['choices'][0]['message']['content']
        else:
            return "❌ Error: La IA no pudo procesar los datos. Revisa tu saldo de OpenAI."
    except Exception as e:
        return f"🚨 Error de conexión: {e}"

# --- RADAR DE OPORTUNIDADES ---
def monitor_activo():
    global noticias_enviadas
    while True:
        try:
            query = "Busca movimientos de ballenas o noticias geopolíticas que impacten +2% en el mercado. Si hay una oportunidad de compra, repórtala."
            res = cerebro_genesis(query, system_role="Cazador de Oportunidades")
            
            huella = res[:40] 
            if huella not in noticias_enviadas:
                if "OPORTUNIDAD" in res.upper() or "⚡" in res:
                    formato_alerta = (
                        f"🎯 **OPORTUNIDAD DETECTADA** 🎯\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"{res}\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"🦅 *GÉNESIS está listo para tu autorización.*"
                    )
                    bot.send_message(TU_CHAT_ID, formato_alerta, parse_mode="Markdown")
                    noticias_enviadas.append(huella)
                    if len(noticias_enviadas) > 10: noticias_enviadas.pop(0)
            
            time.sleep(60)
        except: time.sleep(10)

threading.Thread(target=monitor_activo, daemon=True).start()

# --- INTERFAZ DE BOTONES ---
def menu_principal():
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btn1 = types.KeyboardButton("🐋 Radar de Ballenas")
    btn2 = types.KeyboardButton("🌍 Escaneo Geopolítico")
    btn3 = types.KeyboardButton("📊 Análisis de Liquidez (SMC)")
    markup.add(btn1, btn2, btn3)
    return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(
        message.chat.id, 
        "🦅 **GÉNESIS: ASESOR ESTRATÉGICO**\n━━━━━━━━━━━━━━━━━━━━\n"
        "Sistema de monitoreo en vivo activado. Mi prioridad es tu 10% mensual.\n"
        "¿En qué sector buscaremos hoy?", 
        reply_markup=menu_principal(), 
        parse_mode="Markdown"
    )

@bot.message_handler(func=lambda message: message.text == "🐋 Radar de Ballenas")
def radar_ballenas(message):
    res = cerebro_genesis("Haz un barrido de Whale Alert y flujos institucionales de los últimos 15 min.")
    bot.reply_to(message, f"🐋 **INFORME DE BALLENAS**\n━━━━━━━━━━━━━━\n{res}")

@bot.message_handler(func=lambda message: message.text == "🌍 Escaneo Geopolítico")
def escaneo_geo(message):
    res = cerebro_genesis("Dame las 3 noticias geopolíticas que más riesgo u oportunidad representan para el mercado ahora.")
    bot.reply_to(message, f"🌍 **SITUACIÓN GLOBAL**\n━━━━━━━━━━━━━━\n{res}")

@bot.message_handler(func=lambda message: message.text == "📊 Análisis de Liquidez (SMC)")
def analisis_smc(message):
    bot.reply_to(message, "📸 **Mándame la captura de la gráfica** para buscar los puntos de interés institucional.")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    status = bot.reply_to(message, "💎 **Escaneando huella institucional...**")
    file_info = bot.get_file(message.photo[-1].file_id)
    img_data = bot.download_file(file_info.file_path)
    res = cerebro_genesis(img_b64=base64.b64encode(img_data).decode('utf-8'), system_role="Asesor SMC")
    bot.edit_message_text(f"🎯 **DIAGNÓSTICO DE ENTRADA**\n━━━━━━━━━━━━━━\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: True)
def handle_text(message):
    res = cerebro_genesis(texto_usuario=message.text)
    bot.reply_to(message, res)

if __name__ == "__main__":
    bot.infinity_polling()
