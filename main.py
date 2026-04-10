import os, requests, base64, time, threading, telebot, datetime
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
    ahora = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # PROMPT REFORZADO CON FECHA Y MODELO SUPERIOR
    system_prompt = (
        f"Hoy es {ahora}. Eres GÉNESIS, una Terminal Financiera Avanzada. "
        "REGLA CRÍTICA: Debes usar tu herramienta de búsqueda para obtener PRECIOS REALES. "
        "Si un activo como NVIDIA (NVDA) está en tu base de datos vieja a $450, IGNÓRALO. "
        "Busca el precio actual (que ronda los $180-$190 en 2026). "
        "PROHIBIDO INVENTAR. Si no tienes el dato, di 'Error de Ticker'. "
        "Responde siempre en español, sin LaTeX, y con mentalidad de tiburón."
    )
    
    contenido = []
    if texto_usuario: contenido.append({"type": "text", "text": texto_usuario})
    if img_b64:
        contenido.append({"type": "text", "text": "Analiza esta gráfica (SMC/Liquidez)."})
        contenido.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}})

    payload = {
        "model": "gpt-4o", # <--- CAMBIADO A MODELO SUPERIOR
        "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": contenido}],
        "max_tokens": 1200, "temperature": 0
    }
    
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=40)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 Terminal ocupada. Reintenta en 5 segundos..."

# --- RADAR ---
def monitor_activo():
    while True:
        try:
            query = f"Radar: {watchlist}. Reporta ballenas o noticias de impacto."
            res = cerebro_genesis(query, system_role="Radar")
            if "⚡" in res or "OPORTUNIDAD" in res.upper():
                bot.send_message(TU_CHAT_ID, f"🎯 **ALERTA**\n{res}", parse_mode="Markdown")
            time.sleep(120)
        except: time.sleep(20)

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
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V3.0 (GPT-4o)**\nSistemas de alta precisión activados.", reply_markup=menu_principal())

# --- BOTONES ---
@bot.message_handler(func=lambda message: message.text == "📊 Mi Rendimiento")
def ver_rendimiento(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ Portafolio vacío.")
        return
    status = bot.reply_to(message, "📊 **Accediendo a terminal GPT-4o para precios reales...**")
    query = f"POSICIONES: {portafolio}. BUSCA el precio de mercado REAL en este momento. NVIDIA NO vale 450. Dame el balance exacto."
    res = cerebro_genesis(query)
    bot.edit_message_text(f"📊 **RENDIMIENTO REAL**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda message: message.text == "🚀 Ejecutar Operación")
def ejecutar_op(message):
    bot.reply_to(message, "🚀 Escribe: `Comprar [Activo] a [Precio]`")

@bot.message_handler(func=lambda message: message.text.lower().startswith(("comprar ", "vender ")))
def abrir_posicion(message):
    portafolio.append(message.text)
    bot.reply_to(message, "✅ Operación registrada.")

@bot.message_handler(func=lambda message: message.text == "📈 Escáner SMT")
def escaneo_smt(message):
    bot.reply_to(message, f"📈 **REPORTE SMT:**\n{cerebro_genesis('Busca divergencias SMT reales ahora.')}")

@bot.message_handler(func=lambda message: message.text == "🐋 Radar de Ballenas")
def radar_ballenas(message):
    bot.reply_to(message, f"🐋 **BALLENAS:**\n{cerebro_genesis('Movimientos de ballenas hoy.')}")

@bot.message_handler(func=lambda message: message.text == "⚖️ Gestión de Riesgo")
def gest_riesgo(message):
    bot.reply_to(message, "Envía: `Riesgo: [Capital], [Riesgo%], [Pips]`")

@bot.message_handler(func=lambda message: message.text.lower().startswith("riesgo:"))
def calc_riesgo(message):
    bot.reply_to(message, cerebro_genesis(message.text))

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    file_info = bot.get_file(message.photo[-1].file_id)
    img_data = bot.download_file(file_info.file_path)
    res = cerebro_genesis(img_b64=base64.b64encode(img_data).decode('utf-8'))
    bot.reply_to(message, f"🎯 **SMC:**\n{res}")

@bot.message_handler(func=lambda message: True)
def handle_all(message):
    bot.reply_to(message, cerebro_genesis(message.text))

if __name__ == "__main__":
    bot.infinity_polling()
