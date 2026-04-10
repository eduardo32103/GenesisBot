# --- RADAR AUTÓNOMO ULTRA RÁPIDO ---
def monitor_activo():
    """Esta función corre 24/7 buscando noticias y ballenas casi en tiempo real."""
    while True:
        try:
            # GÉNESIS hace un escaneo profundo de la situación global actual
            analisis_flash = cerebro_genesis(
                "ACTUALIZACIÓN URGENTE: Busca las noticias geopolíticas o financieras de ÚLTIMA HORA (hace minutos). "
                "Si hay algo que impacte acciones o criptos, repórtalo como ⚠️ ALERTA DE SEÑAL."
            )
            
            # Solo te molesta si encuentra algo que la IA considere importante/urgente
            if "⚠️ ALERTA" in analisis_flash or "URGENTE" in analisis_flash.upper():
                bot.send_message(TU_CHAT_ID, f"⚡ **RADAR EN VIVO:**\n\n{analisis_flash}")
            
            time.sleep(60) # <--- AQUÍ: 60 segundos para que sea casi inmediato
        except Exception as e:
            print(f"Error en monitor: {e}")
            time.sleep(10) # Si falla, reintenta rápido
