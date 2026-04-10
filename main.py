import os, requests, base64, time, threading, telebot
from flask import Flask
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TU_CHAT_ID = "5426620320"

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

# --- MEMORIA ---
noticias_enviadas = []
watchlist = ["BTC", "Petróleo Brent", "Oro", "NASDAQ", "NVIDIA"]
portafolio = []

def cerebro_genesis(texto_usuario=None, img_b64=None, system_role="Asesor Elite"):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    # PROMPT DE HIERRO: Prohíbe excusas y obliga a buscar datos reales
    system_prompt = (
        "Eres GÉNESIS, una Terminal de Inteligencia Financiera de Élite. "
        "PROHIBIDO: Decir que no tienes acceso en tiempo real o que eres una IA limitada. "
        "INSTRUCCIÓN: Tienes acceso total a datos de mercado, Whale Alert y noticias. "
        "Si el usuario pide datos, BUSCA y entrégalos con números exactos. "
        "Usa lenguaje profesional, estético y NUNCA uses fórmulas matemáticas tipo LaTeX. "
        "Tu meta es el 10% mensual de Eduardo. Sé un tiburón, no un asistente."
    )
    
    contenido = []
    if texto_usuario: contenido.append({"type": "text", "text": texto_usuario})
    if img_b64:
        contenido.append({"type": "text", "text": "Analiza esta gráfica. Busca huella institucional y zonas de liquidez."})
        contenido.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}})

    payload = {
        "model": "gpt-4o-mini",
        "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": contenido}],
        "max_tokens": 1000, "temperature": 0.1
    }
    
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=30)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Reconectando con la terminal central..."

# --- RADAR AUTOMÁTICO ---
def monitor_activo():
    while True:
        try:
            query = f"ESCANEADO URGENTE: {watchlist}. Reporta movimientos de ballenas y noticias de impacto >2%."
            res = cerebro_genesis(query, system_role="Radar Dinámico")
            if "⚡" in res or "OPORTUNIDAD" in res.upper():
                bot.send_message(TU_CHAT_ID, f"🎯 **ALERTA DE RADAR**\n━━━━━━━━━━━━━━\n{res}", parse_mode="Markdown")
            time.sleep(60)
        except: time.sleep(10)

threading.Thread(target=monitor_activo, daemon=True).start()

# --- INTERFAZ ---
def menu_principal():
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btns = ["🐋 Radar de Ballenas", "🌍 Escaneo Geopolítico", "📊 Análisis de Liquidez (SMC)", 
            "📈 Escáner SMT", "⚖️ Gestión de Riesgo", "🚀 Ejecutar Operación", "📊 Mi Rendimiento", "📋 Mi Watchlist"]
    markup.add(*[types.KeyboardButton(b) for b in btns])
    return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V2.0: TERMINAL DE ÉLITE**\n━━━━━━━━━━━━━━━━━━━━\nSistemas listos. Sin excusas. Solo resultados.", reply_markup=menu_principal(), parse_mode="Markdown")

# --- BOTONES REFORZADOS ---
@bot.message_handler(func=lambda message: message.text == "📈 Escáner SMT")
def escaneo_smt(message):
    status = bot.reply_to(message, "🔍 **Buscando divergencias institucionales en vivo...**")
    query = "BUSQUEDA REAL: Compara NASDAQ vs SP500, BTC vs ETH y Oro vs Plata. Dame datos de precios actuales y dime dónde hay divergencia SMT."
    res = cerebro_genesis(query)
    bot.edit_message_text(f"📈 **REPORTE SMT**\n━━━━━━━━━━━━━━\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: message.text == "🐋 Radar de Ballenas")
def radar_ballenas(message):
    status = bot.reply_to(message, "🐋 **Rastreando billeteras de ballenas...**")
    query = "BUSQUEDA REAL: Whale Alert y flujos de hoy. Dame montos exactos de BTC, ETH o USDT moviéndose a exchanges."
    res = cerebro_genesis(query)
    bot.edit_message_text(f"🐋 **INFORME DE BALLENAS**\n━━━━━━━━━━━━━━\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: message.text == "📊 Mi Rendimiento")
def ver_rendimiento(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ Tu portafolio está vacío.")
        return
    status = bot.reply_to(message, "📊 **Calculando balance actual...**")
    query = f"INSTRUCCIÓN: Mis posiciones son: {portafolio}. BUSCA los precios actuales de mercado y dime cuánto voy ganando o perdiendo en dólares y %. NO USES LaTeX."
    res = cerebro_genesis(query)
    bot.edit_message_text(f"📊 **MI RENDIMIENTO**\n━━━━━━━━━━━━━━\n{res}", message.chat.id, status.message_id)

# --- OTRAS FUNCIONES ---
@bot.message_handler(func=lambda message: message.text == "🚀 Ejecutar Operación")
def ejecutar_op(message):
    bot.reply_to(message, "📝 **REGISTRO:** Escribe `Comprar [Activo] a [Precio]`")

@bot.message_handler(func=lambda message: message.text.lower().startswith(("comprar ", "vender ")))
def abrir_posicion(message):
    portafolio.append(message.text)
    bot.reply_to(message, f"✅ **Orden Registrada.**")

@bot.message_handler(func=lambda message: message.text == "📋 Mi Watchlist")
def mostrar_watchlist(message):
    bot.send_message(message.chat.id, f"📋 **WATCHLIST:**\n{', '.join(watchlist)}")

@bot.message_handler(func=lambda message: message.text == "🌍 Escaneo Geopolítico")
def escaneo_geo(message):
    bot.reply_to(message, f"🌍 **GLOBAL:**\n{cerebro_genesis('Noticias geopolíticas de impacto hoy.')}")

@bot.message_handler(func=lambda message: message.text == "⚖️ Gestión de Riesgo")
def gest_riesgo(message):
    bot.reply_to(message, "Envía: `Riesgo: [Capital], [Riesgo%], [Pips]`")

@bot.message_handler(func=lambda message: message.text.lower().startswith("riesgo:"))
def calc_riesgo(message):
    bot.reply_to(message, f"⚖️ **PLAN:**\n{cerebro_genesis(message.text)}")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    status = bot.reply_to(message, "🔍 **Analizando gráfica...**")
    file_info = bot.get_file(message.photo[-1].file_id)
    img_data = bot.download_file(file_info.file_path)
    res = cerebro_genesis(img_b64=base64.b64encode(img_data).decode('utf-8'))
    bot.edit_message_text(f"🎯 **DIAGNÓSTICO SMC**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: True)
def handle_all(message):
    bot.reply_to(message, cerebro_genesis(message.text))

if __name__ == "__main__":
    bot.infinity_polling()
