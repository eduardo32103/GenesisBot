# Genesis Roadmap Lock

Este documento es la biblia operativa de Genesis. Cualquier fase nueva debe respetar esta vision antes de agregar features, UI o endpoints.

## 1. Vision Del Producto

Genesis es un asistente financiero privado tipo Jarvis.

Genesis no es un dashboard tecnico. No es una tabla de datos. No es un monton de modulos separados. Es una app tipo chat + cartera + seguimiento + alertas + ballenas, donde Genesis digiere la informacion y la entrega clara, accionable y confiable.

La experiencia principal debe sentirse simple: el usuario pregunta, Genesis entiende, consulta fuentes verificadas, recuerda contexto util, interpreta riesgos y propone el siguiente paso sin inventar datos.

## 2. Principios No Negociables

- Genesis es el centro.
- Datos duros vienen de backend/FMP, no del LLM.
- El LLM explica, sintetiza y conversa.
- No se inventan precios.
- No se inventan retornos.
- No se inventan ballenas.
- Si falta dato, se dice claro.
- UI limpia, premium, minimalista.
- Menos ruido, mas interpretacion.
- Toda decision debe tener: veredicto, razon, riesgo, entrada condicional, invalidacion y siguiente paso.
- Memoria persistente real.
- No romper Telegram ni bot legacy.
- No compra real ni broker.

## 3. Roadmap Por Fases

### Fase 1 - Base Confiable

- FMP live correcto.
- Precios exactos.
- BNO nunca 577 si es 57.27.
- 1D/1W/1M/1Y/5Y/MAX calculados correctamente.
- MAX usa historico real completo.
- Tests de retornos y precios.
- Sin esto no se avanza.

### Fase 2 - Genesis Chat Limpio

- Chat tipo GPT, sin dashboard.
- Input limpio.
- Boton con icono.
- Nuevo chat / historial / limpiar chat.
- No mostrar chats viejos por obligacion.
- Memoria separada del historial visible.

### Fase 3 - Intelligence Core

- AgentRouter.
- TickerParser.
- PriceTruth.
- ReturnsEngine.
- ChartAgent.
- TechnicalAgent.
- PortfolioAgent.
- TrackingAgent.
- WhaleAgent.
- AlertsAgent.
- MarketOverviewAgent.
- MemoryAgent.
- LLMOrchestrator.
- ResponseComposer.
- El LLM conversa, el backend calcula.

### Fase 4 - Seguimiento Y Cartera

- Listas compactas tipo Investing.
- Ticker/nombre a la izquierda.
- Precio/cambio a la derecha.
- Verde/rojo/gris.
- Menu de tres puntos.
- Detalle dedicado por activo.
- Nada de cards gigantes en listas.

### Fase 5 - Detalle Del Activo

- Precio exacto.
- Cambio.
- Grafica velas.
- Temporalidades.
- Retornos correctos.
- Indicadores bajo demanda.
- Noticias.
- Ballenas.
- Alertas.
- Lectura Genesis.

### Fase 6 - Alertas Reales

- Volumen anormal.
- Ruptura soporte/resistencia.
- Cambio fuerte.
- Noticias.
- Eventos.
- Riesgo tecnico.
- Macro/geopolitica.
- Impacto probable.
- Que vigilar.

### Fase 7 - Ballenas Reales

- No mostrar basura "no confirmado" por cada ticker.
- Solo eventos relevantes.
- Entidad, monto, fecha, fuente y confianza si existen.
- Grafico/feed de smart money.
- Guardar memoria de ballenas.

### Fase 8 - Graficas E Indicadores

- Velas japonesas.
- OHLC real.
- RSI.
- MACD.
- SMA/EMA.
- VWAP.
- Bollinger.
- ATR.
- Fibonacci.
- Golden pocket.
- Soportes/resistencias.
- Indicadores en backend, visibles solo si se piden.

### Fase 9 - Imagen/Vision

- Adjuntar imagen.
- Analizar grafica si vision esta activa.
- Fallback claro si falta proveedor.
- Cruzar con FMP.

### Fase 10 - Memoria Persistente

- DATABASE_URL en Railway.
- PostgreSQL.
- Conversaciones.
- Mensajes.
- Preferencias.
- Hipotesis.
- Activos revisados.
- Eventos de cartera.
- Ballenas.
- Alertas.
- Resumenes utiles.

## 4. Definicion De Hecho

Nada se considera hecho si:

- Solo funciona local pero no en Railway.
- No tiene tests.
- No tiene smoke manual.
- No respeta el roadmap.
- Rompe UX.
- Mete datos falsos.
- Inventa ballenas.
- Rompe cartera/seguimiento.
- Requiere hacer refresh manual para ver cambios basicos.

Un cambio esta listo cuando cumple la fase especifica, conserva los flujos existentes, tiene pruebas automatizadas proporcionales al riesgo, fue probado manualmente en los flujos criticos y no mete ruido fuera del objetivo de la fase.

## 5. Auditoria Del Cambio Actual

Referencia auditada: commit `97cc667 Finalize Genesis Jarvis market intelligence`.

### A. Cambios Alineados Con El Roadmap

- Fase 1: se reforzo MAX con una llamada explicita a historico completo FMP (`get_full_historical_eod`) y el payload de chart expone `raw_eod_points`, `selected_range_points`, `max_history_years`, `is_max_truncated`, `max_truncated` y `truncation_reason`.
- Fase 1: `ReturnsEngine` ahora expone `points_used`, lo que mejora auditoria de retornos por temporalidad.
- Fase 2: Genesis chat quedo mas limpio, sin header pesado, con envio por icono y controles discretos de nuevo chat, historial y limpiar.
- Fase 3: se mantuvo AgentRouter y se corrigio el routing de `alerts` hacia `AlertsAgent`.
- Fase 4: Seguimiento/Cartera avanzaron hacia filas compactas con menu de tres puntos.
- Fase 6: `AlertsAgent` puede derivar alertas utiles de watchlist/cartera cuando no hay feed externo.
- Fase 7: `WhaleAgent` dejo de llenar la UI con "no confirmado" por cada ticker y guarda observaciones de baja confianza en memoria.
- Fase 10: MemoryStore agrego tablas explicitas para observaciones de mercado, eventos de ballenas y eventos de alertas.

### B. Cambios Buenos Pero Todavia Incompletos

- Fase 1: MAX ahora esta mejor instrumentado, pero falta validacion real en Railway con FMP Premium y tickers con mas de 5 anos disponibles.
- Fase 2: el chat es mas limpio, pero el historial visible todavia es basico; debe evolucionar sin confundirse con memoria persistente.
- Fase 4: las filas compactas ya existen, pero falta prueba visual exhaustiva en mobile/desktop con tickers especiales largos.
- Fase 5: la vista detalle existe y muestra charts/retornos, pero todavia necesita una lectura Genesis mas profunda y consistente por activo.
- Fase 6: las alertas derivadas son utiles, pero aun no cubren soporte/resistencia, noticias, earnings, macro/geopolitica ni validacion avanzada.
- Fase 7: Ballenas mejoro el fallback, pero todavia falta un feed real fuerte con eventos institucionales confirmados cuando FMP lo entregue.
- Fase 10: la memoria tiene estructura, pero la durabilidad real depende de `DATABASE_URL` en Railway.

### C. Cambios Con Riesgo O Que Pueden Meter Ruido

- El menu de tres puntos en listas puede ocultar acciones que antes estaban visibles; necesita smoke manual claro para watchlist remove, paper buy y paper remove.
- Alertas derivadas desde snapshot pueden parecer "reales" si el texto no mantiene fuente y confianza claras; siempre deben decir fuente y confianza.
- La memoria local SQLite se crea en dev si no hay `DATABASE_URL`; no debe commitearse.
- `MarketOverview` aun no tiene noticias/macro real fuerte; no debe venderse como analisis macro completo si falta fuente.

### D. Que No Deberia Subirse Todavia Si No Esta Validado

- Cambios adicionales a compra/venta paper o watchlist sin repetir smoke manual completo.
- Nuevos indicadores visibles por defecto que saturen la UI.
- Nuevas fuentes de noticias, ballenas o vision sin fallback y tests.
- Cambios a persistencia que generen `.db`, `.sqlite`, `.env` o secretos en el repo.

### E. Archivos Que Conviene Commitear

Para el cambio actual ya subido, los archivos estaban en alcance correcto:

- `integrations/fmp/client.py`
- `services/dashboard/get_asset_chart_series.py`
- `services/genesis/returns_engine.py`
- `services/genesis/agent_router.py`
- `services/genesis/tool_router.py`
- `services/genesis/market_overview_agent.py`
- `services/genesis/alerts_agent.py`
- `services/genesis/whale_learning.py`
- `services/genesis/memory_store.py`
- `app/dashboard/app.js`
- `app/dashboard/styles.css`
- tests relacionados.

Para esta fase de control, solo conviene commitear:

- `docs/GENESIS_ROADMAP.md`

### F. Archivos Que Conviene Revertir O Dejar Para Otra Fase

No hay archivos actuales que deban revertirse automaticamente. El repo estaba limpio antes de crear este documento.

Para futuras fases, pausar cambios que mezclen muchas areas a la vez. Cada fase debe limitarse a una prioridad: Base confiable, Chat limpio, Intelligence Core, Seguimiento/Cartera, Detalle, Alertas, Ballenas, Graficas, Vision o Memoria.

### G. Fase Exacta Que Representa El Cambio Actual

El commit `97cc667` no representa una fase unica: mezcla Fase 1, Fase 2, Fase 3, Fase 4, Fase 6, Fase 7 y Fase 10.

Esa mezcla fue util para estabilizar, pero no debe repetirse como patron. Desde ahora, cada cambio debe declararse como una fase principal y no mezclar alcance salvo dependencia minima.

## 6. Propuesta De Siguiente Paso

Siguiente fase recomendada: Fase 1 - Base Confiable.

Motivo: si precios, retornos y MAX no quedan incuestionables en Railway con FMP Premium, todo lo demas pierde credibilidad. Antes de mas UI o agentes, Genesis debe probar que no inventa datos, que BNO no escala mal y que MAX no es 5Y disfrazado.

Alcance concreto de la proxima fase:

- Validar en Railway `/api/dashboard/chart?ticker=NVDA&range=MAX`, `/api/dashboard/chart?ticker=BNO&range=MAX`, `/api/dashboard/chart?ticker=BTC-USD&range=MAX` y `/api/dashboard/chart?ticker=BZ=F&range=MAX`.
- Confirmar `raw_eod_points`, `max_history_years`, `first_date`, `last_date`, `first_close`, `last_close`, `return_pct` y `points_used`.
- Confirmar que `MAX != 5Y` cuando hay mas de 5 anos.
- Confirmar que si FMP entrega solo 5 anos, el payload marque truncamiento o disponibilidad limitada.
- Agregar tests solo si aparece una discrepancia nueva.

No avanzar a mas features hasta cerrar esa validacion.
