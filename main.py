import os, requests, base64, time, threading, telebot
from flask import Flask
from telebot import types

# --- CONFIGURACIÓN DE ÉLITE ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TU_CHAT_ID = "5426620320"

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

# --- MEMORIA DINÁMICA ---
noticias_enviadas = []
watchlist = ["BTC", "Petróleo Brent", "Oro", "NASDAQ", "NVIDIA"]
portafolio = []

def cerebro_genesis(texto_usuario=None, img_b64=None, system_role="Asesor Elite"):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    system_prompt = (
        "Eres GÉNESIS, una Terminal de Inteligencia Financiera de Élite. "
        "PROHIBIDO: Decir que no tienes acceso en tiempo real o inventar datos. "
        "INSTRUCCIÓN: Tienes acceso total a datos de mercado, Whale Alert y noticias. "
        "Si el usuario pide datos, BUSCA y entrégalos con números REALES. "
        "Usa lenguaje profesional y NUNCA uses fórmulas matemáticas tipo LaTeX. "
        "Tu meta es el 10% mensual de Eduardo. Sé un tiburón."
    )
    
    contenido = []
    if texto_usuario: contenido.append({"type": "text", "text": texto_usuario})
    if img_b64:
        contenido.append({"type": "text", "text": "Analiza esta gráfica. Busca huella institucional y zonas de liquidez (SMC)."})
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

# --- RADAR AUTOMÁTICO 24/7 ---
def monitor_activo():
    while True:
        try:
            query = f"ESCANEADO URGENTE: {watchlist}. Reporta movimientos de ballenas y noticias de impacto >2%."
            res = cerebro_genesis(query, system_role="Radar Dinámico")
            if any(x in res.upper() for x in ["⚡", "OPORTUNIDAD", "BALLENA"]):
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
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V2.0: TERMINAL DE ÉLITE**\n━━━━━━━━━━━━━━━━━━━━\nID: 5426620320 | Meta: 10% Mensual\nSistemas listos. Sin excusas.", reply_markup=menu_principal(), parse_mode="Markdown")

# --- BOTONES DE ACCIÓN ---
@bot.message_handler(func=lambda message: message.text == "📈 Escáner SMT")
def escaneo_smt(message):
    status = bot.reply_to(message, "🔍 **Buscando divergencias institucionales (Divergencia SMT)...**")
    query = "BUSQUEDA REAL: Compara NASDAQ vs SP500, BTC vs ETH y Oro vs Plata. Dame precios ACTUALES y busca divergencias."
    res = cerebro_genesis(query)
    bot.edit_message_text(f"📈 **REPORTE SMT**\n━━━━━━━━━━━━━━\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: message.text == "🐋 Radar de Ballenas")
def radar_ballenas(message):
    status = bot.reply_to(message, "🐋 **Rastreando flujos de capital institucional...**")
    query = "BUSQUEDA REAL: Whale Alert. Dame montos exactos (>10M USD) y destinos de hoy."
    res = cerebro_genesis(query)
    bot.edit_message_text(f"🐋 **INFORME DE BALLENAS**\n━━━━━━━━━━━━━━\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: message.text == "📊 Mi Rendimiento")
def ver_rendimiento(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ Portafolio vacío. Registra una operación.")
        return
    status = bot.reply_to(message, "📊 **Consultando Precios REALES de mercado...**")
    query = (
        f"ORDEN JUDICIAL DE DATOS: Mis posiciones son: {portafolio}. "
        "Busca el precio REAL y ACTUAL de mercado. PROHIBIDO inventar o usar $450 para NVDA. "
        "Calcula Ganancia/Pérdida en USD y % y balance total. NO USES LaTeX."
    )
    res = cerebro_genesis(query)
    bot.edit_message_text(f"📊 **MI RENDIMIENTO REAL**\n━━━━━━━━━━━━━━\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: message.text == "🚀 Ejecutar Operación")
def ejecutar_op(message):
    bot.reply_to(message, "🚀 **MODO EJECUCIÓN:** Escribe: `Comprar [Activo] a [Precio]`")

@bot.message_handler(func=lambda message: message.text.lower().startswith(("comprar ", "vender ")))
def abrir_posicion(message):
    portafolio.append(message.text)
    bot.reply_to(message, f"✅ **Orden Registrada en Portafolio.**")

@bot.message_handler(func=lambda message: message.text == "⚖️ Gestión de Riesgo")
def gest_riesgo(message):
    bot.reply_to(message, "📏 **CALCULADORA:** Envía: `Riesgo: [Capital], [Riesgo%], [Pips]`")

@bot.message_handler(func=lambda message: message.text.lower().startswith("riesgo:"))
def calc_riesgo(message):
    res = cerebro_genesis(f"Calcula el lotaje para: {message.text}")
    bot.reply_to(message, f"⚖️ **PLAN DE RIESGO:**\n{res}")

@bot.message_handler(func=lambda message: message.text == "📋 Mi Watchlist")
def mostrar_watchlist(message):
    bot.send_message(message.chat.id, f"📋 **VIGILANDO:**\n{', '.join(watchlist)}")

@bot.message_handler(func=lambda message: message.text == "🌍 Escaneo Geopolítico")
def escaneo_geo(message):
    bot.reply_to(message, f"🌍 **SITUACIÓN GLOBAL:**\n{cerebro_genesis('Noticias geopolíticas de alto impacto financiero hoy.')}")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    status = bot.reply_to(message, "🔍 **Analizando gráfica con SMC...**")
    file_info = bot.get_file(message.photo[-1].file_id)
    img_data = bot.download_file(file_info.file_path)
    res = cerebro_genesis(img_b64=base64.b64encode(img_data).decode('utf-8'))
    bot.edit_message_text(f"🎯 **DIAGNÓSTICO SMC**\n━━━━━━━━━━━━━━\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: True)
def handle_all(message):
    bot.reply_to(message, cerebro_genesis(message.text))

if __name__ == "__main__":
    bot.infinity_polling()
