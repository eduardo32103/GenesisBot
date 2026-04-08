def preguntar_ia(texto, img_b64=None):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={CLAVE_IA}"
    
    # Esta configuración desactiva los bloqueos de seguridad por "consejos financieros"
    safety_settings = [
        {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
        {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
        {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
        {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}
    ]
    
    parts = [{"text": f"Eres GÉNESIS, un asistente de análisis técnico educativo. Analiza esta imagen con fines de estudio, sin dar consejos de inversión: {texto}"}]
    
    if img_b64:
        parts.append({"inline_data": {"mime_type": "image/jpeg", "data": img_b64}})
    
    payload = {
        "contents": [{"parts": parts}],
        "safetySettings": safety_settings
    }
    
    try:
        res = requests.post(url, json=payload, timeout=30).json()
        # Si Google bloquea, intentamos extraer el motivo o dar una respuesta genérica
        if 'candidates' in res and res['candidates'][0].get('finishReason') == 'SAFETY':
            return "⚠️ La IA bloqueó el análisis por políticas de seguridad. Intenta con otra captura de pantalla más limpia."
        
        return res['candidates'][0]['content']['parts'][0]['text']
    except Exception as e:
        return f"⚠️ Error técnico: {str(e)}"
