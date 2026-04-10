import os, requests, base64, time, threading, telebot, datetime
from flask import Flask
from telebot import types

# --- CONFIGURACIÓN ---
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
    ahora = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # PROMPT DE PENSAMIENTO LÓGICO Y RAZONAMIENTO
    system_prompt = (
        f"FECHA: {ahora}. Eres GÉNESIS, un Sistema de Inteligencia Financiera con PENSAMIENTO CRÍTICO. "
        "INSTRUCCIÓN DE RAZONAMIENTO: Antes de responder, realiza una verificación interna. "
        "1. Identifica el activo. 2. Busca el precio REAL en tiempo real. 3. Compara con la fecha actual. "
        "REGLA DE ORO: Si detectas que vas a dar un precio viejo (ej. NVDA a 450), DETENTE. "
        "Busca el dato de 2026. Si no puedes confirmar el centavo exacto, busca en múltiples fuentes. "
        "PROHIBIDO INVENTAR. Si te equivocas en un número, fallas tu meta del 10%. "
        "Piensa paso a paso, verifica tu matemática y entrega resultados exactos sin LaTeX."
    )
    
    contenido = []
    if texto_usuario: contenido.append({"type": "text", "text": texto_usuario})
    if img_b64:
        contenido.append({"type": "text", "text": "Escanea la gráfica. No supongas, observa niveles reales."})
        contenido.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}})

    payload = {
        "model": "gpt-4o", 
        "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": contenido}],
        "max_tokens": 1200, "temperature": 0 # Temperatura 0 para eliminar la "creatividad" y dejar solo la lógica
    }
    
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=45)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Sistema en reflexión profunda. Reintenta en breve..."

# --- RADAR ---
def monitor_activo():
    while True:
        try:
            query = f"Analiza: {watchlist}. ¿Hay algún movimiento real que Eduardo deba saber hoy?"
            res = cerebro_genesis(query, system_role="Vigilancia Lógica")
            if any(x in res.upper() for x in ["⚡", "OPORTUNIDAD", "BALLENA"]):
                bot.send_message(TU_CHAT_ID, f"🎯 **ALERTA INTELIGENTE**\n━━━━━━━━━━━━━━\n{res}")
            time.sleep(150)
        except: time.sleep(30)

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
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V4.0: NÚCLEO DE RAZONAMIENTO ACTIVO**\n━━━━━━━━━━━━━━━━━━━━\nAhora proceso cada dato antes de entregártelo.", reply_markup=menu_principal())

# --- BOTONES REFORZADOS ---
@bot.message_handler(func=lambda message: message.text == "📊 Mi Rendimiento")
def ver_rendimiento(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ No hay datos para procesar.")
        return
    status = bot.reply_to(message, "🧠 **Razonando y verificando precios actuales...**")
    query = f"PORTAFOLIO: {portafolio}. Realiza un análisis de rendimiento. Verifica el precio de mercado de cada activo al segundo. Entrega un informe sin errores decimales."
    res = cerebro_genesis(query)
    bot.edit_message_text(f"📊 **ANÁLISIS DE RENDIMIENTO**\n━━━━━━━━━━━━━━\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: message.text == "📈 Escáner SMT")
def escaneo_smt(message):
    status = bot.reply_to(message, "🔍 **Analizando correlaciones institucionales...**")
    res = cerebro_genesis("Compara NASDAQ, SP500, BTC y ETH. ¿Hay divergencia real o es ruido?")
    bot.edit_message_text(f"📈 **ESCANEO SMT CRÍTICO**\n━━━━━━━━━━━━━━\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: message.text == "🚀 Ejecutar Operación")
def ejecutar_op(message):
    bot.reply_to(message, "🚀 **ORDEN:** Escribe `Comprar [Activo] a [Precio]`")

@bot.message_handler(func=lambda message: message.text.lower().startswith(("comprar ", "vender ")))
def abrir_posicion(message):
    portafolio.append(message.text)
    bot.reply_to(message, "✅ Operación registrada en la bitácora inteligente.")

@bot.message_handler(func=lambda message: message.text == "⚖️ Gestión de Riesgo")
def gest_riesgo(message):
    bot.reply_to(message, "📏 Envía: `Riesgo: [Capital], [Riesgo%], [Pips]`")

@bot.message_handler(func=lambda message: message.text.lower().startswith("riesgo:"))
def calc_riesgo(message):
    bot.reply_to(message, cerebro_genesis(message.text))

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    status = bot.reply_to(message, "🔍 **Razonando estructura de mercado...**")
    file_info = bot.get_file(message.photo[-1].file_id)
    img_data = bot.download_file(file_info.file_path)
    res = cerebro_genesis(img_b64=base64.b64encode(img_data).decode('utf-8'))
    bot.edit_message_text(f"🎯 **DIAGNÓSTICO LÓGICO SMC**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: True)
def handle_all(message):
    bot.reply_to(message, cerebro_genesis(message.text))

if __name__ == "__main__":
    bot.infinity_polling()
