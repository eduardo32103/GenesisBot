import os, base64, requests, telebot, datetime, time, threading
import yfinance as yf
from telebot import types

# --- CONFIGURACIÓN ---
TOKEN = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MI_CHAT_ID = "6348873730" 
bot = telebot.TeleBot(TOKEN, threaded=True)

# Lista de activos a vigilar 24/7
WATCHLIST = ["BTC-USD", "GC=F", "DX-Y.NYB", "NVDA", "TSLA"] 

# --- MOTOR DE PRECIOS ---
def obtener_precio(ticker):
    try:
        t = ticker.upper().strip()
        if t in ["BTC", "ETH", "SOL"]: t = f"{t}-USD"
        asset = yf.Ticker(t)
        data = asset.history(period="1d", interval="1m")
        return round(float(data['Close'].iloc[-1]), 2) if not data.empty else None
    except: return None

# --- CEREBRO GÉNESIS (ANTI-SERMONES) ---
def cerebro_genesis(query, mode="general", img_b64=None):
    if not OPENAI_API_KEY: return "🚨 ERROR: API KEY"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    
    prompts = {
        "alarma": "Eres un radar de crisis financiera. ANALIZA NOTICIAS REALES. Si hay un evento geopolítico o económico que moverá el mercado >2%, responde: '⚠️ ALERTA: [Evento] - IMPACTO ESTIMADO >2%'. Si no hay nada crítico, responde: 'CALMA'. PROHIBIDO EL TEXTO DE RELLENO.",
        "smc": "Analista SMC Profesional. Identifica BOS, CHoCH, OB y FVG. Da coordenadas de precio exactas. Sin sermones.",
        "general": "Terminal de Datos. Responde solo con hechos, cifras y niveles técnicos. Sé directo."
    }

    payload = {
        "model": "gpt-4o",
        "messages": [
            {"role": "system", "content": prompts.get(mode, prompts["general"])},
            {"role": "user", "content": query}
        ],
        "temperature": 0
    }

    if img_b64:
        payload["messages"][1]["content"] = [
            {"type": "text", "text": "SMC ANALYSIS: Identify structure and key liquidity zones."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
        ]

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=40)
        return r.json()['choices'][0]['message']['content']
    except: return "🚨 ERROR IA"

# --- VIGILANTE 24/7 (ACCIONES Y GEOPOLÍTICA) ---
def vigilante_activo():
    while True:
        try:
            # 1. Escaneo de Geopolítica Crítica
            noticia = cerebro_genesis("Busca noticias de última hora sobre conflictos, FED o BlackRock que muevan el mercado >2%.", mode="alarma")
            if "ALERTA" in noticia.upper():
                bot.send_message(MI_CHAT_ID, noticia)
            
            # 2. Escaneo de Watchlist (Movimientos >3% en 10 min)
            for ticker in WATCHLIST:
                p = obtener_precio(ticker)
                # Aquí podrías añadir lógica de comparación de precios si quisieras alertas de precio
                pass
                
        except Exception as e:
            print(f"Error Vigilante: {e}")
        time.sleep(300) # Revisa cada 5 minutos

threading.Thread(target=vigilante_activo, daemon=True).start()

# --- INTERFAZ ---
@bot.message_handler(commands=['start'])
def start(message):
    m = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    m.add("📊 Precio Real", "🚀 Operar", "🌍 Geopolítica", "🐋 Radar Ballenas", "📋 Watchlist")
    bot.send_message(message.chat.id, "🦅 **GÉNESIS V56: TERMINAL DE VIGILANCIA**\nRadar 24/7 configurado para impacto >2%.", reply_markup=m)

@bot.message_handler(func=lambda m: m.text == "🌍 Geopolítica")
def btn_geo(message):
    status = bot.reply_to(message, "📡 Filtrando ruido... Buscando impacto real.")
    res = cerebro_genesis("Reporte flash: Hechos geopolíticos con impacto >2% en Oro/DXY.", mode="general")
    bot.edit_message_text(f"🌍 **IMPACTO REAL:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: m.text == "📋 Watchlist")
def btn_watchlist(message):
    res = "📋 **ACTIVOS BAJO VIGILANCIA 24/7:**\n"
    for t in WATCHLIST:
        p = obtener_precio(t)
        res += f"• {t}: **${p if p else 'Cargando...'}**\n"
    bot.send_message(message.chat.id, res)

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    f_info = bot.get_file(message.photo[-1].file_id)
    img_b64 = base64.b64encode(bot.download_file(f_info.file_path)).decode('utf-8')
    status = bot.reply_to(message, "🎯 Escaneando SMC...")
    res = cerebro_genesis(None, mode="smc", img_b64=img_b64)
    bot.edit_message_text(f"🎯 **REPORTE:**\n{res}", message.chat.id, status.message_id)

@bot.message_handler(func=lambda m: True)
def default(message):
    if len(message.text) <= 6:
        p = obtener_precio(message.text)
        if p: bot.reply_to(message, f"📈 {message.text.upper()}: **${p}**")

if __name__ == "__main__":
    bot.infinity_polling()
