import os, requests, base64, time, threading, telebot
from flask import Flask
from telebot import types

# --- CONFIGURACIÓN CRÍTICA ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TU_CHAT_ID = "5426620320" # <--- TU ID SE MANTIENE

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

# --- MEMORIA DINÁMICA ---
noticias_enviadas = []
watchlist = ["BTC", "Petróleo Brent", "Oro", "NASDAQ", "NVIDIA"]

def cerebro_genesis(texto_usuario=None, img_b64=None, system_role="Asesor Financiero Elite"):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    activos_str = ", ".join(watchlist)
    system_prompt = (
        f"Eres GÉNESIS, un Asesor de Inversiones de Élite. Meta: 10% mensual. "
        f"Vigila el mundo, pero prioriza estos activos: {activos_str}. "
        "REGLA DE ORO: NUNCA digas que no tienes acceso a datos. Usa tu capacidad de búsqueda "
        "para rastrear Whale Alert, flujos de capital, precios en vivo y noticias de Bloomberg/Reuters. "
        "Si detectas oportunidad o movimiento >2%, repórtalo con '⚡ OPORTUNIDAD'."
    )
    
    contenido = []
    if texto_usuario: contenido.append({"type": "text", "text": texto_usuario})
    if img_b64:
        contenido.append({"type": "text", "text": "Escanea esta gráfica. Busca huella institucional, Order Blocks y niveles de entrada."})
        contenido.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}})

    payload = {
        "model": "gpt-4o-mini",
        "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": contenido}],
        "max_tokens": 1000, "temperature": 0.1
    }
    
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=30)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Reconectando sensores..."

# --- RADAR DE VIGILANCIA ---
def monitor_activo():
    global noticias_enviadas
    while True:
        try:
            activos = ", ".join(watchlist)
            query = (
                f"ESCANEADO GLOBAL: Enfócate en {activos}. "
                "Busca movimientos de ballenas recientes (>10M USD) y noticias de impacto inmediato. "
                "No des teoría. Si hay algo real, repórtalo."
            )
            res = cerebro_genesis(query, system_role="Radar Dinámico")
            
            huella = res[:40]
            if huella not in noticias_enviadas:
                if "⚡" in res or "OPORTUNIDAD" in res.upper() or "BALLENA" in res.upper():
                    bot.send_message(TU_CHAT_ID, f"🎯 **ALERTA DE RADAR**\n━━━━━━━━━━━━━━\n{res}", parse_mode="Markdown")
                    noticias_enviadas.append(huella)
                    if len(noticias_enviadas) > 10: noticias_enviadas.pop(0)
            time.sleep(60)
        except: time.sleep(10)

threading.Thread(target=monitor_activo, daemon=True).start()

# --- INTERFAZ Y BOTONES ---
def menu_principal():
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btn1 = types.KeyboardButton("🐋 Radar de Ballenas")
    btn2 = types.KeyboardButton("🌍 Escaneo Geopolítico")
    btn3 = types.KeyboardButton("📊 Análisis de Liquidez (SMC)")
    btn4 = types.KeyboardButton("📋 Mi Watchlist")
    btn5 = types.KeyboardButton("📈 Escáner SMT (Correlaciones)") # <--- NUEVO BOTÓN
    markup.add(btn1, btn2, btn3, btn4, btn5)
    return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(
        message.chat.id, 
        "🦅 **GÉNESIS: CENTRO DE MANDO**\n━━━━━━━━━━━━━━━━━━━━\n"
        "ID Configurado: 5426620320\n"
        "Nuevos sensores SMT activados. Mi meta es tu 10% mensual.", 
        reply_markup=menu_principal(), 
        parse_mode="Markdown"
    )

@bot.message_handler(func=lambda message: message.text == "📋 Mi Watchlist")
def mostrar_watchlist(message):
    lista = "\n".join([f"🔹 {a}" for a in watchlist])
    reporte = (f"📋 **LISTA DE VIGILANCIA**\n━━━━━━━━━━━━━━━━━━━━\n{lista}")
    bot.send_message(message.chat.id, reporte, parse_mode="Markdown")

@bot.message_handler(func=lambda message: message.text.lower().startswith("vigila "))
def agregar_activo(message):
    nuevo = message.text.replace("vigila ", "").replace("Vigila ", "").strip()
    if nuevo not in watchlist:
        watchlist.append(nuevo)
        bot.reply_to(message, f"✅ **{nuevo}** añadido al radar. 🦅")
    else:
        bot.reply_to(message, "Ese activo ya está en el radar.")

@bot.message_handler(func=lambda message: message.text == "🐋 Radar de Ballenas")
def radar_ballenas(message):
    query = (
        "ESCANEADO URGENTE: Accede a Whale Alert y flujos de capital institucional. "
        "Reporta los movimientos MÁS recientes de más de $10M USD. "
        "No des teoría, dame activos, montos y hacia dónde se movieron (Exchanges o Wallets). "
        "Si no ves nada en el último minuto, busca en los últimos 30 minutos."
    )
    res = cerebro_genesis(query, system_role="Terminal de Datos de Ballenas")
    bot.reply_to(message, f"🐋 **INFORME DE BALLENAS**\n━━━━━━━━━━━━━━\n{res}")

@bot.message_handler(func=lambda message: message.text == "🌍 Escaneo Geopolítico")
def escaneo_geo(message):
    res = cerebro_genesis("Top 3 noticias geopolíticas de impacto financiero inmediato.")
    bot.reply_to(message, f"🌍 **SITUACIÓN GLOBAL**\n━━━━━━━━━━━━━━\n{res}")

@bot.message_handler(func=lambda message: message.text == "📈 Escáner SMT (Correlaciones)")
def escaneo_smt(message):
    status = bot.reply_to(message, "🔍 **Iniciando escaneo de divergencias institucionales...**")
    query = (
        "ESCANEADO TÉCNICO AVANZADO: Analiza la acción del precio en este preciso momento buscando "
        "Divergencias SMT (Smart Money Tool) entre activos correlacionados. Compara: "
        "1) NASDAQ vs S&P 500, 2) BTC vs ETH, 3) Oro vs Plata. "
        "Identifica si un activo está haciendo un Alto Más Alto (HH) mientras el otro hace un Bajo Más Alto (LH), "
        "o si uno hace un Bajo Más Bajo (LL) y el otro no. No me des clases de trading, dame directamente "
        "los datos actuales y concluye si hay una oportunidad de manipulación institucional para operar en contra del engaño."
    )
    res = cerebro_genesis(query, system_role="Especialista SMT")
    bot.edit_message_text(f"📈 **REPORTE DE DIVERGENCIAS SMT**\n━━━━━━━━━━━━━━\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: message.text == "📊 Análisis de Liquidez (SMC)")
def pedir_foto(message):
    bot.reply_to(message, "📸 **Mándame la captura de la gráfica** para buscar la huella institucional.")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    status = bot.reply_to(message, "🔍 **Analizando...**")
    file_info = bot.get_file(message.photo[-1].file_id)
    img_data = bot.download_file(file_info.file_path)
    res = cerebro_genesis(img_b64=base64.b64encode(img_data).decode('utf-8'))
    bot.edit_message_text(f"🎯 **DIAGNÓSTICO SMC**\n━━━━━━━━━━━━━━\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: True)
def handle_all(message):
    res = cerebro_genesis(texto_usuario=message.text)
    bot.reply_to(message, res)

if __name__ == "__main__":
    bot.infinity_polling()
