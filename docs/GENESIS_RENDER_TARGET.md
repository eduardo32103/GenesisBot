# Genesis Render Target

Este documento congela el render aprobado para Genesis App Mode.

## Principio

Genesis debe sentirse como un Jarvis financiero premium: oscuro, rapido, visual y util. La UI no debe parecer un dashboard tecnico ni una tabla. Cada pantalla tiene que mostrar datos digeridos, fuente clara, confianza y siguiente lectura accionable.

## Navegacion

- Bottom nav fijo: Seguimiento, Noticias, Genesis, Cartera, Alertas.
- Genesis va al centro y usa una G minimalista.
- Ballenas vive dentro de Alertas como subtab, nunca en bottom nav.
- Las busquedas estan colapsadas por defecto y se abren con lupa.

## Estilo Visual

- Fondo negro / graphite profundo.
- Cards dark glass / charcoal con bordes finos.
- Verde para sesgo positivo, rojo para presion, amarillo para vigilancia, azul/gris para dato estimado.
- Filas compactas tipo Investing, no tarjetas gigantes.
- Mini charts, barras, gauges, chips y metric cards antes que texto largo.
- Nada de markdown crudo, JSON, runtime, endpoint, UUID, Telegram, API keys o debug tecnico.

## Seguimiento

- Header compacto y card Pulso del mercado.
- Strip live de SPY, QQQ, BTC-USD, NVDA y Brent.
- Seccion "Tus activos vigilados".
- Cada fila muestra icono, nombre limpio, precio, cambio, sparkline, volumen, volumen relativo, estado y menu de tres puntos.
- Brent debe verse como Brent Crude Oil / Brent Front Month; BZ=F queda solo como ticker interno.

## Noticias

- Header: Noticias, "Lo que mueve los mercados, interpretado por IA".
- Filtros: Importantes, Ultimas, Mis activos, Global.
- Secciones: Importantes / influyentes y Ultimas noticias.
- Cada nota muestra imagen real o placeholder fotografico por categoria, titulo en espanol, fuente, hora, impacto, activos y mini lectura Genesis.
- El detalle abre por ID estable e incluye imagen grande, fuente, fecha, resumen Genesis, por que importa, activos afectados, impacto, que vigilar y link original.

## Genesis Chat

- Chat limpio tipo GPT con input fijo abajo.
- Botones discretos: nuevo chat, historial, limpiar.
- Respuestas por response_type: asset_analysis, market_summary, news_brief, alerts_digest, whale_flow, weather, comparison, chart_analysis, general_assistant.
- Un analisis de activo debe renderizar precio confirmado, cambio, confianza, chart, retornos, volumen, RSI, MACD, EMA, Fibonacci, soporte, resistencia, tesis, riesgos, noticias, alertas y ballenas relacionadas cuando existan.

## Cartera

- Hero con valor total, P/L, retorno y mini performance.
- Donut solo de posiciones paper.
- Lista compacta de posiciones con valor, unidades, P/L, cambio y peso.
- Cards de exposicion, lectura Genesis, ganadores y riesgos.
- No borrar watchlist ni paper positions. Si DATABASE_URL existe, PortfolioStore debe usar backend durable.

## Alertas

- Pulso del mercado arriba.
- Subtabs: Alertas y Ballenas.
- Cards visuales con precio, cambio, volumen, dollar volume, soporte, resistencia, tendencia, momentum, impacto, confianza y sparkline.
- Cada alerta abre su detalle por ID estable y nunca reutiliza el primer item.
- Genesis tambien debe escanear oportunidades externas importantes aunque no esten en cartera/watchlist.
- Toda oportunidad usa estrategia de validacion paper: entrada condicional, invalidacion, catalizador, score y recordatorio de que no es compra real.

## Ballenas

- Separar ballena confirmada de smart money estimado.
- Confirmada requiere entidad/wallet/institucion, cantidad o monto, fecha y fuente.
- Estimada muestra volumen vigilado, dollar volume vigilado, volumen relativo, direccion estimada y confianza.
- No mezclar monitored_dollar_volume con confirmed_amount_usd.
- Sanity guard contra montos absurdos o trillones irreales.
- El detalle explica que significa, que NO significa, impacto posible, cartera/watchlist afectada y que vigilar.
- Las graficas de flujo deben mostrar volumen, volumen $, precio usado y relativo como lectura interpretable; no barras decorativas sin escala.

## Datos

- FMP es fuente de verdad para quote, precio, cambio, volumen, OHLC, historico, retornos y noticias cuando existan.
- GPT-5.5 sintetiza y redacta; no inventa cifras.
- MemoryStore guarda contexto, chats, alertas importantes y eventos de ballenas.
- Open-Meteo es fallback de clima si no hay WEATHER_API_KEY.
- Source health debe existir en `/api/dashboard/source-health` sin exponer secretos.

## Memoria Inteligente

- Genesis no entrena GPT-5.5; aprende con memoria propia persistente.
- MemoryStore debe usar DATABASE_URL/PostgreSQL cuando exista y SQLite solo como fallback local.
- La memoria util se separa en conversaciones, mensajes, preferencias, entidades, asset_memory, signal_events, whale_events, alert_events, news_events, decision_notes, hypothesis_log, outcome_tracking y learned_context.
- Cada evento guarda id, event_type, ticker, asset_name, timestamp, source, confidence, raw_data_sanitized, genesis_reading, expected_direction, expected_impact, outcomes 1h/24h/7d, status, created_at y updated_at.
- Nunca se guardan API keys, tokens, passwords ni secretos.
- Limpiar chat solo limpia la conversacion visible; no borra cartera, watchlist, memoria de activos, alertas, noticias o ballenas.
- Los agentes consultan memoria cuando la pregunta lo requiere: "que aprendiste de NVDA", "que alertas funcionaron", "que ballenas afectan mi cartera" o "que paso esta semana".
- ResponseComposer y frontend deben mostrar memoria como digest visual: resumen ejecutivo, patrones, senales que funcionaron, senales pendientes, riesgos y que vigilar.
- La memoria es contexto, no fuente de verdad para precio live; cualquier precio actual vuelve a FMP/backend.
