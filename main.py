import logging
import base64
import requests
import re
import xml.etree.ElementTree as ET
import pandas as pd
import yfinance as yf
import threading
import time
from openai import OpenAI
import os
import telebot
import json
from collections import deque
from telebot.types import ReplyKeyboardMarkup, KeyboardButton
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')

if not TELEGRAM_TOKEN or not CHAT_ID:
    logging.critical("Falta TELEGRAM_TOKEN o CHAT_ID. Saliendo sin saturar.")
    exit()

bot = telebot.TeleBot(TELEGRAM_TOKEN)
CARTERA_FILE = 'cartera.json'

WHALE_MEMORY = deque(maxlen=5) 
SMC_LEVELS_MEMORY = {} 

# --- NÚCLEO DE PERSISTENCIA (JSON UNIFICADO) ---
def get_cartera_data():
    if os.path.exists(CARTERA_FILE):
        try:
            with open(CARTERA_FILE, 'r') as f:
                return json.load(f)
        except: return {}
    return {}

def save_cartera_data(data):
    with open(CARTERA_FILE, 'w') as f:
        json.dump(data, f, indent=4)

def get_tracked_tickers():
    data = get_cartera_data()
    return list(data.keys())

def add_ticker(ticker):
    ticker = ticker.upper()
    if ticker == "BTC": ticker = "BTC-USD"
    data = get_cartera_data()
    
    if ticker not in data:
        data[ticker] = {"is_investment": False}
        save_cartera_data(data)
        
        # Inicializar niveles críticos de inmediato al radar SMC
        val = fetch_and_analyze_stock(ticker)
        if val: update_smc_memory(ticker, val)
        return True
    return False

def remove_ticker(ticker):
    ticker = ticker.upper()
    if ticker == "BTC": ticker = "BTC-USD"
    data = get_cartera_data()
    
    if ticker in data:
        del data[ticker]
        save_cartera_data(data)
        if ticker in SMC_LEVELS_MEMORY: 
            del SMC_LEVELS_MEMORY[ticker]
        return True
    return False

def add_investment(ticker, amount_usd, entry_price):
    ticker = ticker.upper()
    if ticker == "BTC": ticker = "BTC-USD"
    data = get_cartera_data()
    
    data[ticker] = {
        "is_investment": True,
        "amount_usd": float(amount_usd),
        "entry_price": float(entry_price),
        "timestamp": datetime.now().isoformat()
    }
    save_cartera_data(data)
    
    # Aseguramos que la topología de seguridad empiece a rastrearlo
    val = fetch_and_analyze_stock(ticker)
    if val: update_smc_memory(ticker, val)

def get_investments():
    data = get_cartera_data()
    investments = {}
    for tk, info in data.items():
        if info.get("is_investment", False):
            investments[tk] = info
    return investments

# ----------------- NÚCLEO DE MERCADO MACRO -----------------
def check_geopolitical_news():
    logging.info("Monitoreando Radar Geopolítico...")
    search_url = "https://news.google.com/rss/search?q=geopolitics+OR+Trump+OR+rates+OR+war+OR+economy"
    HIGH_IMPACT_KEYWORDS = ["war", "attack", "strike", "escalation", "missile", "sanction", "embargo", "explosion", "guerra", "ataque", "tensión", "misil", "sanciones", "rates", "fed", "trump", "powell"]
    news_alerts = []
    try:
        response = requests.get(search_url)
        if response.status_code == 200:
            root = ET.fromstring(response.text)
            for item in root.findall('.//item'):
                title = item.find('title').text
                if any(re.search(rf"\b{kw}\b", title, re.IGNORECASE) for kw in HIGH_IMPACT_KEYWORDS):
                    news_alerts.append(title)
                    if len(news_alerts) >= 5: break
    except: pass
    return news_alerts

def gpt_advanced_geopolitics(news_list, manual=False):
    if not news_list or not OPENAI_API_KEY: return None
    client = OpenAI(api_key=OPENAI_API_KEY)
    news_text = "\n".join([f"- {n}" for n in news_list])
    if manual:
        prompt = f"Titulares globales:\n{news_text}\nHaz un resumen y dime qué movería el mercado hoy. RESPONDE ESTRICTAMENTE EN ESPAÑOL."
    else:
        prompt = (f"Titulares recientes:\n{news_text}\nAnaliza si hay algo de nivel 'Alto Impacto' (>2%). Si no lo hay, responde 'TRANQUILIDAD'.\nSi lo hay: '⚠️ ALERTA URGENTE: [Resumen] - Impacto en [Acción/Sector]'\nRESPONDE ESTRICTA Y ÚNICAMENTE EN ESPAÑOL.")
    try:
        res = client.chat.completions.create(model="gpt-4o", messages=[{"role": "user", "content": prompt}], max_tokens=300).choices[0].message.content.strip()
        if not manual and ("TRANQUILIDAD" in res.upper() and len(res) < 20): return None
        return res
    except: return None

def fetch_intraday_data(ticker):
    try:
        data = yf.download(ticker, period="5d", interval="5m", progress=False)
        if data.empty: return None
        if isinstance(data.columns, pd.MultiIndex):
            data = data.copy(); data.columns = data.columns.get_level_values(0)
            
        close_prices = data['Close']; open_prices = data['Open']; volumes = data['Volume']
        if isinstance(close_prices, pd.DataFrame): 
             close_prices = close_prices.iloc[:, 0]; open_prices = open_prices.iloc[:, 0]; volumes = volumes.iloc[:, 0]
             
        vol_type = "Compra 🟢" if float(close_prices.iloc[-1]) >= float(open_prices.iloc[-1]) else "Venta 🔴"
        return {'ticker': ticker, 'latest_vol': float(volumes.iloc[-1]), 'avg_vol': float(volumes.mean()), 'vol_type': vol_type, 'latest_price': float(close_prices.iloc[-1])}
    except: return None

def fetch_and_analyze_stock(ticker):
    try:
        data = yf.download(ticker, period="6mo", interval="1d", progress=False)
        if data.empty: return None
        if isinstance(data.columns, pd.MultiIndex):
            data = data.copy(); data.columns = data.columns.get_level_values(0)
            
        close_prices = data['Close']; volume = data['Volume']
        if isinstance(close_prices, pd.DataFrame): 
             close_prices = close_prices.iloc[:, 0]; volume = volume.iloc[:, 0]
             
        delta = close_prices.diff()
        up = delta.clip(lower=0); down = -1 * delta.clip(upper=0)
        ema_up = up.ewm(com=13, adjust=False).mean(); ema_down = down.ewm(com=13, adjust=False).mean()
        rs = ema_up / ema_down
        rsi_series = 100 - (100 / (1 + rs)); rsi_series[ema_down == 0] = 100 
        
        macd_line = close_prices.ewm(span=12, adjust=False).mean() - close_prices.ewm(span=26, adjust=False).mean()
        macd_signal = macd_line.ewm(span=9, adjust=False).mean()
        latest_price = float(close_prices.iloc[-1]); latest_rsi = float(rsi_series.iloc[-1])
        
        smc_trend = "Alcista 🟢" if latest_price > close_prices.ewm(span=20).mean().iloc[-1] else "Bajista 🔴"
        recent_month = close_prices.iloc[-22:]
        smc_sup = float(recent_month.min()); smc_res = float(recent_month.max())
        vol_month = volume.iloc[-22:]; order_block_price = float(close_prices.loc[vol_month.idxmax()])
        
        return {'ticker': ticker, 'price': latest_price, 'rsi': latest_rsi, 'macd_line': float(macd_line.iloc[-1]), 'macd_signal': float(macd_signal.iloc[-1]), 'smc_sup': smc_sup, 'smc_res': smc_res, 'smc_trend': smc_trend, 'order_block': order_block_price}
    except: return None

def update_smc_memory(ticker, analysis):
    now = datetime.now()
    if ticker not in SMC_LEVELS_MEMORY or (now - SMC_LEVELS_MEMORY[ticker]['update_date']).total_seconds() > 43200:
        SMC_LEVELS_MEMORY[ticker] = {'sup': analysis['smc_sup'], 'res': analysis['smc_res'], 'alert_sup': False, 'alert_res': False, 'update_date': now}

def analyze_breakout_gpt(ticker, level_type, price):
    if not OPENAI_API_KEY: return "¿Qué hacer? Mantener cautela."
    client = OpenAI(api_key=OPENAI_API_KEY)
    prompt = f"El activo {ticker} rompió su {level_type} en ${price:.2f}. Consejo corto de 1 párrafo: ¿Qué hacer ahora? (Elige y resalta COMPRAR, VENDER o MANTENER) y por qué. ESPAÑOL ESTRICTO."
    try: return client.chat.completions.create(model="gpt-4o", messages=[{"role": "user", "content": prompt}], max_tokens=200).choices[0].message.content
    except: return "¿Qué hacer? Esperar al cierre del día."

def perform_deep_analysis(ticker):
    ticker = ticker.upper()
    if ticker == "BTC": ticker = "BTC-USD"
    tech_info = f"Información técnica no disponible para {ticker}."
    tech = fetch_and_analyze_stock(ticker)
    if tech: tech_info = f"Precio: ${tech['price']:.2f}\nRSI: {tech['rsi']:.2f}\nSMC Trend: {tech['smc_trend']}"
        
    news_str = ""
    try:
        news_str = "\n".join([f"- {n.get('title', '')}" for n in yf.Ticker(ticker).news[:3]])
    except: pass
        
    prompt = (f"Analiza profundamente '{ticker}'.\nTécnicos:\n{tech_info}\n\nNoticias:\n{news_str}\n" "Combina enfoques. Dictamina un VEREDICTO FINAL resaltado: 'COMPRAR', 'VENDER' o 'MANTENER/ESPERAR'. ESPAÑOL ESTRICTO.")
    if not OPENAI_API_KEY: return "Error: API API KEY MISSING."
    try: return OpenAI(api_key=OPENAI_API_KEY).chat.completions.create(model="gpt-4o", messages=[{"role": "user", "content": prompt}], max_tokens=600).choices[0].message.content
    except Exception as e: return f"Fallo al analizar: {e}"

def generate_capital_report():
    investments = get_investments()
    if not investments:
        return "---\n📊 *ESTADO DE CUENTA - GÉNESIS*\n---\n⚠️ Portafolio Vacío. Suma capital pidiendo: 'Compré 5000 NVDA'"
    
    total_invested = 0.0
    total_current = 0.0
    report_lines = ["---", "📊 *ESTADO DE CUENTA - GÉNESIS*", "---"]
    
    for tk, dt in investments.items():
        init_amount = dt['amount_usd']
        entry_p = dt['entry_price']
        
        intra = fetch_intraday_data(tk)
        if intra:
            live_price = intra['latest_price']
            roi_percent = ((live_price - entry_p) / entry_p)
            current_value = init_amount * (1 + roi_percent)
            profit = current_value - init_amount
            
            total_invested += init_amount
            total_current += current_value
            
            icon = "🟢" if profit >= 0 else "🔴"
            sign = "+" if profit >= 0 else ""
            
            report_lines.append(f"📈 <b>Activo:</b> {tk}")
            report_lines.append(f"💰 <b>Inversión:</b> ${init_amount:,.2f} (Entrada: ${entry_p:,.2f})")
            report_lines.append(f"🚀 <b>Valor actual:</b> ${current_value:,.2f}")
            report_lines.append(f"{icon} <b>PnL:</b> {sign}{roi_percent*100:.2f}% ({sign}${profit:,.2f})")
            report_lines.append("---")
            
    if total_invested > 0:
        total_pnl_percent = ((total_current - total_invested) / total_invested)
        total_profit = total_current - total_invested
        
        report_lines.append(f"🌐 <b>PATRIMONIO GLOBAL:</b> ${total_current:,.2f}")
        report_lines.append(f"⚖️ <b>Balance Neto:</b> {'🟢 +' if total_profit >= 0 else '🔴 '}${total_profit:,.2f} ({total_pnl_percent*100:.2f}%)")
        
        target_percent = 0.10
        if total_pnl_percent >= target_percent:
            report_lines.append(f"🎯 <b>Meta del Mes (10%):</b> ¡🎯 SUPERADA CON ÉXITO!")
        else:
            missing_percent = (target_percent - total_pnl_percent) * 100
            report_lines.append(f"🎯 <b>Meta del Mes (10%):</b> Falta un {missing_percent:.2f}% para el objetivo.")
            
    return "\n".join(report_lines)

# ----------------- CONTROLADORES TELEBOT (NLP & ACCIONES DIRECTAS) -----------------
@bot.message_handler(commands=['start'])
def cmd_start(message):
    if str(message.chat.id) != str(CHAT_ID): return
    markup = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(KeyboardButton("🌎 Geopolítica"), KeyboardButton("🐳 Radar Ballenas"), KeyboardButton("📉 SMC / Mi Cartera"))
    bot.reply_to(message, "¡Génesis Patrimonial Online!\nPersistencia Integrada y Cero Latencia. Botonera lista:", reply_markup=markup)

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    if str(message.chat.id) != str(CHAT_ID): return
    msg = bot.reply_to(message, "👁️ Analizando estructura visual...")
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        base_img = base64.b64encode(bot.download_file(file_info.file_path)).decode('utf-8')
        res = OpenAI(api_key=OPENAI_API_KEY).chat.completions.create(
            model="gpt-4o", messages=[{"role": "user", "content": [{"type": "text", "text": "Analiza gráfica SMC con rigor. Veredicto: 'Bullish' o 'Bearish'. ESPAÑOL ESTRICTO."}, {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base_img}"}}]}], max_tokens=800)
        bot.edit_message_text(f"---\n📊 *REPORTE VISUAL*\n---\n{res.choices[0].message.content}", chat_id=message.chat.id, message_id=msg.message_id)
    except Exception as e: bot.edit_message_text(f"❌ Falló visión: {e}", chat_id=message.chat.id, message_id=msg.message_id)

@bot.message_handler(func=lambda message: True, content_types=['text'])
def handle_text(message):
    if str(message.chat.id) != str(CHAT_ID): return
    text = message.text.strip()
    
    if text == "🐳 Radar Ballenas":
        bot.reply_to(message, "🐳 Memoria HFT Institucional invocada...")
        if not WHALE_MEMORY:
            bot.send_message(message.chat.id, "---\n🐋 *RADAR BALLENAS*\n---\nEl océano está quieto. Sin anomalías detectadas hoy.", parse_mode="HTML")
            return
        lines = ["---", "🐋 *ÚLTIMAS 5 BALLENAS*", "---"]
        for w in list(WHALE_MEMORY)[::-1]: lines.append(f"• <b>{w['ticker']}</b> | Vol: {w['vol_approx']:,} | Tipo: {w['type']} | {int((datetime.now() - w['timestamp']).total_seconds() / 60)} mins ago")
        bot.send_message(message.chat.id, "\n".join(lines), parse_mode="HTML")
        
    elif text == "🌎 Geopolítica":
        bot.reply_to(message, "🌎 Procesando macro Geopolítica Manual...")
        ai_res = gpt_advanced_geopolitics(check_geopolitical_news(), manual=True)
        bot.send_message(message.chat.id, f"---\n🌍 *INSIGHT GLOBAL*\n---\n{ai_res}" if ai_res else "✅ Radar limpio.", parse_mode="HTML")
            
    elif text == "📉 SMC / Mi Cartera":
        bot.reply_to(message, "📉 Computando Mapas SMC Instritucionales...")
        report_lines = ["---", "🦅 *GÉNESIS: SMC / NIVELES CRÍTICOS*", "---"]
        for tk in get_tracked_tickers():
            analysis = fetch_and_analyze_stock(tk)
            if analysis:
                update_smc_memory(tk, analysis)
                report_lines.extend([f"🏦 <b>{analysis['ticker']}</b> - ${analysis['price']:.2f}", f"• Tendencia SMC: {analysis['smc_trend']}", f"• Buy-side Liquidity: ${analysis['smc_sup']:.2f}", f"• Sell-side Liquidity: ${analysis['smc_res']:.2f}", f"• Order Block Institucional: ${analysis['order_block']:.2f}", "---"])
        bot.send_message(message.chat.id, "\n".join(report_lines) if len(report_lines)>3 else "Tu cartera está vacía.", parse_mode="HTML")
        
    elif text.upper().startswith("ANALIZA "):
        match = re.search(r'ANALIZA\s+([A-Za-z0-9\-]+)', text.upper())
        if match:
            tk = match.group(1)
            bot.reply_to(message, f"🔍 Análisis Profundo Institucional en {tk}...")
            bot.send_message(message.chat.id, f"---\n🏦 *RESEARCH: {tk}*\n---\n{perform_deep_analysis(tk)}", parse_mode="HTML")
            
    elif "AGREGA" in text.upper():
        match = re.search(r'AGREGA\s+([A-Za-z0-9\-]+)', text.upper())
        if match: bot.reply_to(message, f"✅ Activo <b>{match.group(1)}</b> integrado al radar." if add_ticker(match.group(1)) else f"⚠️ Ya existía.", parse_mode="HTML")
            
    elif "ELIMINA" in text.upper() or "BORRA" in text.upper():
        match = re.search(r'(?:ELIMINA|BORRA)\s+([A-Za-z0-9\-]+)', text.upper())
        if match: bot.reply_to(message, f"🗑️ Activo <b>{match.group(1)}</b> destrozado de memoria." if remove_ticker(match.group(1)) else f"⚠️ No residía en tu radar.", parse_mode="HTML")

    elif "COMPRÉ" in text.upper() or "COMPRE" in text.upper():
        match = re.search(r'COMPR[EÉ]\s+(?:DE\s+)?\$?(\d+(?:\.\d+)?)\s+(?:EN\s+|DE\s+|ACCIONES\s+DE\s+)?([A-Za-z0-9\-]+)', text.upper())
        if match:
            amt = match.group(1)
            tk = match.group(2)
            bot.reply_to(message, f"💸 Consultando a yfinance el precio de fijación para {tk}...")
            intra = fetch_intraday_data(tk)
            if intra:
                add_investment(tk, amt, intra['latest_price'])
                bot.send_message(message.chat.id, f"---\n✅ *CAPITAL REGISTRADO Y PERSISTIDO*\n---\n• Activo: {tk}\n• Capital Invertido: ${float(amt):,.2f} USD\n• Precio de Fijación (Entrada): ${intra['latest_price']:.2f}", parse_mode="HTML")
            else:
                bot.reply_to(message, f"❌ No pude fijar el precio real de {tk} ahora. Intenta de nuevo más tarde.")

    elif "RESUMEN MENSUAL" in text.upper() or "CÓMO VOY" in text.upper() or "COMO VOY" in text.upper():
        bot.reply_to(message, "📊 Computando flujo patrimonial y calculando PnL del JSON Integrado...")
        bot.send_message(message.chat.id, generate_capital_report(), parse_mode="HTML")

# ----------------- BUCLE CENTINELA MAESTRO -----------------
def boot_smc_levels_once():
    """Ejecutado al iniciar el Bot para recargar memorias e inicializar JSON Base si existe o arranca de cero"""
    logging.info("Arrancando Centinela y cruzando persistencia 'cartera.json'...")
    for tk in get_tracked_tickers():
        val = fetch_and_analyze_stock(tk)
        if val: update_smc_memory(tk, val)

def background_loop_proactivo():
    tick_count = 0
    boot_smc_levels_once() 
    while True:
        try:
            time.sleep(300) 
            tick_count += 1
            now = datetime.now()
            
            for tk in get_tracked_tickers():
                intra = fetch_intraday_data(tk)
                if not intra: continue
                cur_price = intra['latest_price']
                
                # NIVELES DE RUPTURA SMC
                topol = SMC_LEVELS_MEMORY.get(tk)
                if topol:
                    if cur_price > topol['res'] and not topol['alert_res']:
                        topol['alert_res'] = True
                        adv = analyze_breakout_gpt(tk, "Resistencia", cur_price)
                        bot.send_message(CHAT_ID, f"---\n🚨 *ALERTA DE RUPTURA*\n---\n<b>{tk}</b> rompió su Resistencia SMC logrando los <b>${cur_price:.2f}</b>.\n\n🤖 *DECISIÓN IA:*\n{adv}", parse_mode="HTML")
                    elif cur_price < topol['sup'] and not topol['alert_sup']:
                        topol['alert_sup'] = True
                        adv = analyze_breakout_gpt(tk, "Soporte", cur_price)
                        bot.send_message(CHAT_ID, f"---\n🚨 *ALERTA DE RUPTURA (DUMP)*\n---\n<b>{tk}</b> ha quebrado el Soporte SMC hundiéndose a <b>${cur_price:.2f}</b>.\n\n🤖 *DECISIÓN IA:*\n{adv}", parse_mode="HTML")
                
                # RASTREOS DE BALLENA 
                if intra['avg_vol'] > 0:
                    spike = intra['latest_vol'] / intra['avg_vol']
                    if spike >= 2.5: 
                        clean_amount = int(intra['latest_vol'])
                        WHALE_MEMORY.append({"ticker": tk, "vol_approx": clean_amount, "type": intra['vol_type'], "timestamp": now})
                        bot.send_message(CHAT_ID, f"---\n⚠️ *ALERTA DE BALLENA*\n---\nBloque masivo en <b>{tk}</b>: {clean_amount:,} unidades.\nPresión: {intra['vol_type']}", parse_mode="HTML")
                        
            # MACRO GEOPOLITICA
            if tick_count % 3 == 0:
                ai_threat_evaluation = gpt_advanced_geopolitics(check_geopolitical_news(), manual=False)
                if ai_threat_evaluation:
                     bot.send_message(CHAT_ID, f"---\n🚨 *VIGILANCIA GLOBAL ALTO RIESGO*\n---\n{ai_threat_evaluation}", parse_mode="HTML")
        except Exception as e:
            logging.error(f"Error HFT: {e}")

# ----------------- MAIN -----------------
def main():
    print(f"Iniciando Módulo de Gestor Cuantitativo: Persistencia Garantizada en '{CARTERA_FILE}'")
    t = threading.Thread(target=background_loop_proactivo, daemon=True)
    t.start()
    bot.infinity_polling(timeout=10, long_polling_timeout=5)

if __name__ == "__main__":
    main()
