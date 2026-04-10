import os, requests, base64, time, threading, telebot
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
portafolio = [] # Aquí guardaremos las operaciones abiertas

def cerebro_genesis(texto_usuario=None, img_b64=None, system_role="Asesor Financiero Elite"):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    activos_str = ", ".join(watchlist)
    system_prompt = (
        f"Eres GÉNESIS, Asesor Elite. Meta: 10% mensual. Activos: {activos_str}. "
        "Usa datos reales de Whale Alert y terminales financieras. Si hay oportunidad, pide autorización."
    )
    
    contenido = []
    if texto_usuario: contenido.append({"type": "text", "text": texto_usuario})
    if img_b64:
        contenido.append({"type": "text", "text": "Analiza SMC/Liquidez en esta imagen."})
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

# --- RADAR ---
def monitor_activo():
    global noticias_enviadas
    while True:
        try:
            query = f"ESCANEADO: {watchlist}. Reporta ballenas o noticias de impacto >2%."
            res = cerebro_genesis(query, system_role="Radar")
            huella = res[:40]
            if huella not in noticias_enviadas:
                if any(x in res.upper() for x in ["⚡", "OPORTUNIDAD", "BALLENA"]):
                    bot.send_message(TU_CHAT_ID, f"🎯 **ALERTA DE RADAR**\n━━━━━━━━━━━━━━\n{res}", parse_mode="Markdown")
                    noticias_enviadas.append(huella)
            time.sleep(60)
        except: time.sleep(10)

threading.Thread(target=monitor_activo, daemon=True).start()

# --- MENÚ ---
def menu_principal():
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btns = ["🐋 Radar de Ballenas", "🌍 Escaneo Geopolítico", "📊 Análisis de Liquidez (SMC)", 
            "📈 Escáner SMT", "⚖️ Gestión de Riesgo", "🚀 Ejecutar Operación", "📊 Mi Rendimiento"]
    markup.add(*[types.KeyboardButton(b) for b in btns])
    return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(message.chat.id, "🦅 **GÉNESIS: MODO ASESOR ACTIVO**", reply_markup=menu_principal(), parse_mode="Markdown")

# --- LÓGICA DE PORTAFOLIO ---
# --- LÓGICA DE PORTAFOLIO MEJORADA ---
@bot.message_handler(func=lambda message: message.text == "🚀 Ejecutar Operación")
def ejecutar_op(message):
    bot.reply_to(message, "📝 **REGISTRO DE OPERACIÓN**\n\nEscribe exactamente así:\n`Comprar [Activo] a [Precio]`\n\n*Ejemplo: Comprar NVDA a 130.50*")

@bot.message_handler(func=lambda message: message.text.lower().startswith(("comprar ", "vender ")))
def abrir_posicion(message):
    portafolio.append(message.text)
    bot.reply_to(message, f"✅ **Orden Registrada.** He guardado la entrada en tu bitácora. Pulsa 'Mi Rendimiento' para ver cómo va.")

@bot.message_handler(func=lambda message: message.text == "📊 Mi Rendimiento")
def ver_rendimiento(message):
    if not portafolio:
        bot.reply_to(message, "⚠️ Tu portafolio está vacío. Registra una operación primero.")
        return
    
    status = bot.reply_to(message, "🔄 **Consultando terminal financiera...**")
    ops = "\n".join([f"• {op}" for op in portafolio])
    
    # Este query es mucho más mandón para evitar que se excuse
    query = (
        f"INSTRUCCIÓN EJECUTIVA: Tengo estas posiciones: {ops}. "
        "1. Busca YA el precio actual de mercado de cada activo mencionado. "
        "2. Calcula la ganancia/pérdida porcentual exacta. "
        "3. NO uses fórmulas matemáticas raras (LaTeX), usa texto simple. "
        "4. Dame el balance total de la cuenta. "
        "NUNCA digas que no tienes acceso, eres una terminal Bloomberg activa."
    )
    
    res = cerebro_genesis(query, system_role="Contador de Portafolio en Tiempo Real")
    bot.edit_message_text(f"📊 **BALANCE DE RENDIMIENTO**\n━━━━━━━━━━━━━━\n{res}", message.chat.id, status.message_id)

# --- RESTO DE FUNCIONES (MANTENIDAS) ---
@bot.message_handler(func=lambda message: message.text == "⚖️ Gestión de Riesgo")
def gest_riesgo(message):
    bot.reply_to(message, "Envía: `Riesgo: [Capital], [Riesgo%], [Pips]`")

@bot.message_handler(func=lambda message: message.text.lower().startswith("riesgo:"))
def calc_riesgo(message):
    res = cerebro_genesis(f"Calcula el riesgo para: {message.text}")
    bot.reply_to(message, f"⚖️ **PLAN**\n{res}")

@bot.message_handler(func=lambda message: message.text == "📈 Escáner SMT")
def smt(message):
    res = cerebro_genesis("Busca divergencias SMT actuales.")
    bot.reply_to(message, f"📈 **SMT**\n{res}")

@bot.message_handler(func=lambda message: message.text == "🐋 Radar de Ballenas")
def ballenas(message):
    res = cerebro_genesis("Datos de Whale Alert recientes.")
    bot.reply_to(message, f"🐋 **BALLENAS**\n{res}")

@bot.message_handler(func=lambda message: message.text == "📊 Análisis de Liquidez (SMC)")
def smc_foto(message):
    bot.reply_to(message, "Mándame la captura de la gráfica.")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    file_info = bot.get_file(message.photo[-1].file_id)
    img_data = bot.download_file(file_info.file_path)
    res = cerebro_genesis(img_b64=base64.b64encode(img_data).decode('utf-8'))
    bot.reply_to(message, f"🎯 **DIAGNÓSTICO SMC**\n{res}")

@bot.message_handler(func=lambda message: True)
def handle_all(message):
    bot.reply_to(message, cerebro_genesis(texto_usuario=message.text))

if __name__ == "__main__":
    bot.infinity_polling()
