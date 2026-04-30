# Fase 2.6 - Capacidad FMP

## Objetivo
Dejar documentado qué rutas presionan más a FMP, qué ya se mitigó en el runtime y qué criterio operativo usar para decidir si el plan actual todavía alcanza o si ya conviene subir capacidad.

## Rutas que consumen FMP
- `quote`
  - Base: `main.py::_fetch_fmp_quote`
  - Usos críticos: `get_safe_ticker_price`, `fetch_intraday_data`, `Analiza ...`, `Compre ...`, protección y validaciones.
- `historical eod`
  - Base: `main.py::_fetch_fmp_historical_eod`
  - Usos críticos: `fetch_and_analyze_stock`, `_build_chart_pack`, `_build_chart_pack_failsafe`, reconstrucción de `avgVolume`.
- `historical intraday`
  - Base: `main.py::_fetch_fmp_intraday_history`
  - Usos críticos: `_build_chart_pack` para temporalidades `1H` y `4H`.
- `stock-news`
  - Base: `main.py::_fetch_fmp_news` y `main.py::_fetch_fmp_ticker_news`
  - Usos críticos: geopolítica y centinela de noticias por ticker.

## Coste relativo
- `quote`: bajo por request, alto en volumen cuando se usa en loops.
- `stock-news`: medio, escala lineal con el número de símbolos consultados.
- `historical eod`: medio-alto; el endpoint `full` puede traer más payload del que el caller termina usando.
- `historical intraday`: alto; es la ruta más cara por ancho de banda.

## Presión real por flujo
1. `background_loop_proactivo`
   - Recorre el radar completo.
   - Llama `fetch_intraday_data(tk)` y puede terminar en `quote + eod`.
2. `Niveles SMC` / `fetch_and_analyze_stock`
   - Mezcla `quote + eod` por activo.
3. `Analiza ...`
   - Usa `quote + eod` y en temporalidades intradía puede usar `historical intraday`.
4. `verificar_noticias_cartera_v2`
   - Hace `stock-news` por ticker del radar.
5. geopolítica / snapshot macro
   - Usa `stock-news` agregado sobre símbolos vigilados + defaults.

## Qué ya mitigó Fase 2.3
- Cache corta en memoria:
  - `quote`: 15s
  - `eod`: 120s
  - `intraday`: 45s
- Cooldown tras fallo:
  - `QUOTA_LIMIT`: 25s
  - `ACCESS_RESTRICTED`: 20s
  - `UPSTREAM_ERROR`: 12s
- Menos duplicación inmediata dentro de `Analiza ...`
- Logs claros de `CACHE HIT`, `THROTTLE` y `BLOCKED`

## Qué sigue siendo externo al plan
- `429 Bandwidth Limit Reach`
- `401/402/403` por restricciones del plan o endpoint
- payload caro de rutas históricas aunque el runtime ya tenga cache/cooldown

## Señales para decidir si subir plan
Seguir con el plan actual:
- si aceptas degradación temporal en `Analiza ...`, `SMC` y radar
- si el radar vigilado es pequeño
- si el uso manual es ocasional

Subir plan:
- si `429` aparece durante uso normal, no solo en picos
- si `Analiza ...` y `Niveles SMC` son funciones core y se bloquean con frecuencia
- si el radar suele estar arriba de `8-10` tickers con loop activo 24/7
- si los logs `FMP USAGE` muestran presión sostenida en `quote/eod/intraday/news` aun con `cache_hit` y `throttle`

## Métrica operativa agregada
El runtime ahora emite `FMP USAGE` por tipo de endpoint (`quote`, `eod`, `intraday`, `news`) con:
- `fetch`
- `ok`
- `cache_hit`
- `throttle`
- `quota`
- `access`
- `no_data`
- `upstream`
- `no_key`
- `bytes`

Importante:
- `FMP USAGE` es una métrica operativa de presión real sobre el runtime.
- No sustituye el contador oficial de facturación de FMP.

## Criterio de cierre de Fase 2.6
Fase 2.6 queda cerrada cuando:
- el mapa de consumo queda identificado
- la presión por endpoint queda visible en logs
- el equipo puede decidir con evidencia si sigue igual o sube capacidad
- el problema restante ya se trata explícitamente como limitación externa, no como bug de código
