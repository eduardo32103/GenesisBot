import os, io, asyncio, requests, base64
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, constants
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, filters
from PIL import Image

# 🔑 ACCESOS ACTUALIZADOS
TOKEN_TELEGRAM = "7708446894:AAEuY_BQlrJicPubna0UHsDNU85FjBJ7_D4"
CLAVE_IA = "AIzaSyDT0JlbsCt8pMvgqb_r51XFT3N5lATAnEY" 
MI_CHAT_ID = 5426620320 

# 🎮 INTERFAZ TÁCTICA
MENU_PRINCIPAL = ReplyKeyboardMarkup([
    [KeyboardButton("🐳 RADAR BALLENAS"), KeyboardButton("🦅 ANALIZAR GRÁFICA")],
    [KeyboardButton("🌍 NOTICIAS"), KeyboardButton("⚡ STATUS GÉNESIS")]
], resize_keyboard=True)

def preguntar_ia(texto, imagen_base64=None):
    # Usamos Gemini 1.5 Flash - El motor más avanzado y libre
    url = f"https://generativelanguage.googleapis.com/v1/models/gemini-1.5-flash:generateContent?key={CLAVE_IA}"
    
    payload = {
        "contents": [{
            "parts": [
                {"text": f"Eres GÉNESIS, un trader rudo y experto. Analiza de forma técnica y directa: {texto}"},
                {"inline_data": {"mime_type": "image/jpeg", "data": imagen_base64}} if imagen_base64 else {"text": ""}
            ]
        }],
        "safetySettings": [
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}
        ]
    }
    
    try:
        response = requests.post(url, json=payload, timeout=40)
        data = response.json()
        if 'candidates' in data and len(data['candidates']) > 0:
            return data['candidates'][0]['content']['parts'][0]['text']
        return f"❌ Error de Google: {data.get('error', {}).get('message', 'Reintenta en un momento.')}"
    except Exception as e:
        return f"❌ Fallo de enlace: {str(e)}"

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != MI_CHAT_ID: return
    await update.message.reply_text("📡 **GÉNESIS FLASH:** Escaneando mercado con visión de sniper...")
    try:
        photo_file = await update.message.photo[-1].get_file()
        img_bytes = await photo_file.download_as_bytearray()
        
        # Procesamiento ligero para asegurar velocidad
        img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
        buffer = io.BytesIO()
        img.save(buffer, format="JPEG", quality=90)
        img_b64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
        
        res = preguntar_ia("Analiza tendencia, soportes, resistencias y busca patrones de velas institucionales.", img_b64)
        await update.message.reply_text(f"🎯 **REPORTE TÁCTICO:**\n\n{res}")
    except Exception as e:
        await update.message.reply_text(f"❌ Error visual: {str(e)}")

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != MI_CHAT_ID: return
    t = update.message.text
    
    if t == "⚡ STATUS GÉNESIS":
        await update.message.reply_text("🛡️ **PROTOCOLO GÉNESIS v7.0 ONLINE**\n🟢 Motor: 1.5-Flash (Nueva Key)\n📡 Radar: Sincronizado", reply_markup=MENU_PRINCIPAL)
    elif t == "🐳 RADAR BALLENAS":
        res = preguntar_ia("Reporte rudo de movimientos de ballenas BTC y ETH ahora.")
        await update.message.reply_text(f"🔍 **RADAR:**\n\n{res}")
    elif t == "🌍 NOTICIAS":
        res = preguntar_ia("Noticia financiera más importante de este momento.")
        await update.message.reply_text(f"🌐 **NOTICIAS:**\n\n{res}")
    elif t == "🦅 ANALIZAR GRÁFICA":
        await update.message.reply_text("📸 **Mándame la captura de la gráfica para el escaneo inmediato.**")
    else:
        res = preguntar_ia(t)
        await update.message.reply_text(res)

def main():
    app = ApplicationBuilder().token(TOKEN_TELEGRAM).build()
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text))
    print("🛡️ GÉNESIS v7.0 DESPLEGADO.")
    app.run_polling()

if __name__ == '__main__':
    main()
