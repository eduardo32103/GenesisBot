# Genesis UI Standard

Este documento define el estandar visual y de interaccion aprobado para Genesis App. El roadmap sigue siendo la fuente de verdad de producto; este archivo baja esa vision a reglas de UI concretas.

## Navegacion

- Bottom nav fijo: Seguimiento, Noticias, Genesis, Cartera, Alertas.
- Genesis vive al centro y se destaca con un acento sutil, sin exagerar.
- Ballenas no es tab inferior. Vive dentro de Alertas como subtab junto a Alertas.
- La navegacion debe sentirse de app movil premium: iconos finos, labels cortos, estado activo claro y sobrio.

## Genesis Chat

- Genesis es un chat limpio tipo GPT oscuro premium.
- No se muestra "Genesis activo" como header fijo.
- No hay card superior pesada ni contenedor de dashboard encerrando el chat.
- El input queda fijo abajo, con scroll independiente en mensajes.
- El boton de envio usa icono, nunca texto "Enviar".
- El boton de adjuntar imagen usa icono.
- Debe existir nuevo chat, historial y limpiar chat visible.
- Mensaje inicial sin historial: "Hola. ¿Qué quieres revisar hoy?"

## Respuestas Visuales

- Genesis no debe mostrar JSON crudo, markdown crudo, `###`, `**` ni bloques enormes sin digestion.
- Las respuestas financieras deben renderizarse en tarjetas visuales compactas.
- Una respuesta de analisis de activo debe incluir, cuando exista dato:
  - tesis principal
  - conviccion: Alcista, Neutral o Bajista
  - confianza
  - precio confirmado
  - mini chart o candlestick
  - soportes/resistencias
  - RSI, MACD, volumen y money flow si existen
  - noticias, alertas y ballenas relevantes
  - escenario probable, riesgos y que vigilar
- El backend calcula datos duros; el frontend presenta la lectura de forma escaneable.

## Seguimiento y Cartera

- Listas compactas tipo Investing.
- Nada de cards gigantes en listas principales.
- Izquierda: ticker o nombre limpio y nombre corto.
- Derecha: precio actual, cambio monetario y cambio porcentual.
- Colores:
  - positivo: verde
  - negativo: rojo
  - neutro o sin dato: gris
- Las acciones viven en menu de tres puntos.
- Click en fila abre el detalle dedicado del activo.

## Detalle Del Activo

- Pantalla dedicada, no modal pesado.
- Header con volver, ticker/nombre limpio y menu de tres puntos.
- Precio grande, cambio, cierre anterior, rango del dia, volumen y timestamp.
- Chart OHLC de velas japonesas.
- Temporalidades: 1D, 1W, 1M, 1Y, 5Y, MAX.
- MAX muestra anos reales disponibles o limitacion clara.
- Retornos por temporalidad usan first close y last close reales.
- Indicadores visibles bajo demanda o cuando el payload los traiga: volumen, RSI, MACD, EMA 50, Fibonacci.
- Incluir Lectura Genesis, escenario, noticias, alertas y ballenas relacionadas cuando existan.

## Alertas y Ballenas

- Alertas es el centro de eventos importantes.
- Subtabs: Alertas y Ballenas.
- Las alertas deben explicar impacto, confianza, hora, fuente y que vigilar.
- Ballenas solo muestra eventos relevantes confirmados por fuente activa.
- Si no hay ballenas, se muestra una unica tarjeta limpia: "No hay movimientos institucionales confirmados con la fuente activa."
- No se inventan entidades, montos ni causalidades.

## Noticias

- Noticias es contexto vivo, no duplicado de Alertas.
- Debe mostrar resumen macro, titulares, activos afectados, impacto probable, fuente, hora y confianza si hay datos.
- Si no hay fuente activa, usar fallback util y elegante sin llenar la pantalla de ruido.

## Datos, IA y Memoria

- FMP calcula y confirma precios, cambios, OHLC, volumen, historico y retornos.
- GPT-5.5 redacta, sintetiza, razona y conversa cuando `GENESIS_LLM_ENABLED=true` y existe `OPENAI_API_KEY`.
- GPT-5.5 no calcula precios, retornos, OHLC, P/L ni ballenas.
- MemoryStore recuerda conversaciones, preferencias, activos revisados, hipotesis, alertas, ballenas y contexto util.
- `DATABASE_URL` activa PostgreSQL durable en Railway.
- SQLite es fallback local de desarrollo y no debe commitearse.
- Weather usa `WEATHER_API_KEY` si existe; si no existe, usa Open-Meteo sin key.

## No Negociables Visuales

- Full black / graphite premium.
- Tipografia clara y legible.
- Menos ruido, mas interpretacion.
- No markdown visible.
- No debug tecnico feo en UI.
- No secrets en UI, logs ni payload enviado al LLM.
- No romper Seguimiento, Cartera, watchlist, paper buy ni paper remove.
