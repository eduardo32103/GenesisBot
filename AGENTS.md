# AGENTS.md

Guia corta para trabajar en GenesisBot sin redescubrir reglas.

## Estructura del repo

- `api/`: backend HTTP principal y rutas del dashboard.
- `app/dashboard/`: app movil web, UI, estilos y logica frontend.
- `services/genesis/`: razonamiento, memoria, respuestas, alertas, noticias, retornos.
- `services/dashboard/`: snapshots para pantallas, charts, alertas, ballenas y salud.
- `services/portfolio/`: cartera paper y persistencia de posiciones.
- `integrations/`: clientes externos como FMP.
- `tests/unit/`: pruebas unitarias relevantes.
- `docs/`: roadmap, estandar UI y render target.

## Comandos de test

- `python -m unittest discover tests\unit`
- `node --check app/dashboard/app.js`
- `git diff --check`

## Zonas que NO se tocan

- No tocar Telegram.
- No tocar bot legacy.
- No tocar broker.
- No implementar compra real.
- No borrar cartera paper.
- No borrar watchlist.

## Secretos y archivos sensibles

- No exponer API keys, tokens, secrets ni valores de `.env`.
- No subir `.env`, `.db`, `.sqlite`, `__pycache__`, `.pyc`, logs ni temporales.
- No subir `portfolio.json` ni `.genesis_memory.sqlite3` salvo orden explicita.

## Git

- No usar `git add .`.
- Agregar solo archivos exactos revisados.
- Respetar cambios del usuario; no revertir cambios ajenos.
- No hacer commit/push si el usuario lo prohibe.

## Bottom nav oficial

1. Seguimiento
2. Noticias
3. Genesis
4. Cartera
5. Alertas

Genesis va centrado y destacado. Ballenas vive dentro de Alertas como subtab, no en bottom nav.

## Genesis UI standard

- Mobile-first premium dark graphite/black.
- Estilo ChatGPT + fintech/investing premium.
- Cards limpias, bordes finos, tipografia clara, datos visuales antes que texto largo.
- Usar mini charts, barras, medidores, chips, sparkline y badges.
- Cero markdown visible, cero JSON crudo, cero textos pobres o placeholders feos.
- Todo Genesis debe estar en espanol.
- Brent se muestra como `Brent Crude Oil`; `BZ=F` solo es ticker interno.

## Datos y razonamiento

- FMP es fuente de verdad para precio, quote, volumen, OHLC, historico, retornos y noticias cuando exista.
- OpenAI se usa para sintesis, explicacion, traduccion, vision y lectura Genesis.
- No inventar datos: si algo no esta confirmado, decirlo claro.
- Las alertas deben usar todos los indicadores disponibles: precio, volumen, RSI, MACD, EMAs, Fibonacci, soporte, resistencia, noticias, flujo y contexto.
- Las recomendaciones deben dar entrada, invalidacion, volumen requerido y confianza cuando aplique.

## Memoria

- MemoryStore persiste conversaciones, alertas, noticias, ballenas, tesis, decisiones y resultados.
- Genesis debe aprender de aciertos/fallos, alertas abiertas, outcomes 1h/24h/7d y preferencias.
- Limpiar chat no debe borrar memoria util, cartera ni watchlist.

## Respuesta final requerida

Al cerrar una tarea, responder breve:

- diagnostico
- archivos tocados
- cambios principales
- tests ejecutados y resultado
- si hubo push/commit o no
- `git status --short`
- pendientes o riesgos, si existen
