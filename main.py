import os, requests, base64, time, threading, telebot
from flask import Flask
from telebot import types

# --- CONFIGURACIÓN CRÍTICA ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TU_CHAT_ID = "5426620320"

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
            query = f"ESCANEADO GLOBAL: Enfócate en {activos}. Busca movimientos de ballenas recientes (>10M USD) y noticias de impacto."
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
    btn4 = types.KeyboardButton("📈 Escáner SMT (Correlaciones)")
    btn5 = types.KeyboardButton("📋 Mi Watchlist")
    btn6 = types.KeyboardButton("⚖️ Gestión de Riesgo") # <--- NUEVO
    markup.add(btn1, btn2, btn3, btn4, btn5, btn6)
    return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS: CENTRO DE MANDO**\n━━━━━━━━━━━━━━━━━━━━\nSistemas de gestión de riesgo y SMT activos.", reply_markup=menu_principal(), parse_mode="Markdown")

@bot.message_handler(func=lambda message: message.text == "📋 Mi Watchlist")
def mostrar_watchlist(message):
    lista = "\n".join([f"🔹 {a}" for a in watchlist])
    bot.send_message(message.chat.id, f"📋 **LISTA DE VIGILANCIA**\n━━━━━━━━━━━━━━━━━━━━\n{lista}", parse_mode="Markdown")

@bot.message_handler(func=lambda message: message.text == "⚖️ Gestión de Riesgo")
def gestion_riesgo(message):
    instrucciones = (
        "⚖️ **CALCULADORA DE RIESGO INSTITUCIONAL**\n━━━━━━━━━━━━━━━━━━━━\n"
        "Para calcular tu lotaje, envíame un mensaje con este formato:\n\n"
        "**Riesgo: [Capital], [Riesgo%], [Pips de Stop Loss]**\n\n"
        "Ejemplo: `Riesgo: 1000, 1, 25` \n"
        "*(Capital $1000, arriesgando 1%, con 25 pips de SL)*"
    )
    bot.reply_to(message, instrucciones, parse_mode="Markdown")

@bot.message_handler(func=lambda message: message.text.lower().startswith("riesgo:"))
def calcular_lote(message):
    query = f"Actúa como calculadora de riesgo. El usuario dice: {message.text}. Calcula el lotaje para Forex (pips) y Cripto (%). Dime cuánto dinero perderá si toca SL y cuánto ganará en un ratio 1:3."
    res = cerebro_genesis(query, system_role="Calculadora de Riesgo")
    bot.reply_to(message, f"⚖️ **PLAN DE TRADING**\n━━━━━━━━━━━━━━\n{res}")

@bot.message_handler(func=lambda message: message.text.lower().startswith("vigila "))
def agregar_activo(message):
    nuevo = message.text.replace("vigila ", "").replace("Vigila ", "").strip()
    watchlist.append(nuevo)
    bot.reply_to(message, f"✅ **{nuevo}** añadido al radar. 🦅")

@bot.message_handler(func=lambda message: message.text == "🐋 Radar de Ballenas")
def radar_ballenas(message):
    query = "ESCANEADO URGENTE: Whale Alert y flujos masivos (>10M USD). No teoría, solo datos de flujos."
    res = cerebro_genesis(query, system_role="Terminal de Ballenas")
    bot.reply_to(message, f"🐋 **INFORME DE BALLENAS**\n━━━━━━━━━━━━━━\n{res}")

@bot.message_handler(func=lambda message: message.text == "🌍 Escaneo Geopolítico")
def escaneo_geo(message):
    res = cerebro_genesis("Top 3 noticias geopolíticas de impacto financiero.")
    bot.reply_to(message, f"🌍 **SITUACIÓN GLOBAL**\n━━━━━━━━━━━━━━\n{res}")

@bot.message_handler(func=lambda message: message.text == "📈 Escáner SMT (Correlaciones)")
def escaneo_smt(message):
    status = bot.reply_to(message, "🔍 **Buscando divergencias...**")
    query = "Analiza NASDAQ vs SP500, BTC vs ETH y Oro vs Plata buscando Divergencias SMT actuales."
    res = cerebro_genesis(query, system_role="Especialista SMT")
    bot.edit_message_text(f"📈 **DIVERGENCIAS SMT**\n━━━━━━━━━━━━━━\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: message.text == "📊 Análisis de Liquidez (SMC)")
def pedir_foto(message):
    bot.reply_to(message, "📸 **Mándame la gráfica** para buscar la huella institucional.")

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
