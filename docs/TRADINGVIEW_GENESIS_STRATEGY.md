# Genesis TradingView Strategy

Este documento acompana `tradingview/genesis_advantage_strategy_v1.pine`.

La estrategia no promete rentabilidad. Primero debe validarse con backtesting, paper trading y forward testing. Solo despues de demostrar ventaja medible se podria evaluar automatizacion, fuera de esta fase.

## Que Hace

Genesis Advantage Strategy v1 es una estrategia Pine Script v6 para TradingView. Combina regimen de mercado, tendencia, momentum, volumen, estructura, volatilidad, Fibonacci/golden pocket, riesgo/recompensa y un input manual llamado `Genesis Context Score`.

Sirve para:

- Backtesting visual en TradingView.
- Paper trading.
- Alertas por webhook hacia Genesis.
- Journal y aprendizaje dentro de MemoryStore.

No ejecuta broker real, no compra, no vende y no usa API keys dentro de Pine.

## Por Que Pine No Usa FMP/OpenAI Directo

TradingView Pine debe quedarse dentro del chart y de `request.security`. Por seguridad y arquitectura:

- Pine no llama a FMP.
- Pine no llama a OpenAI.
- Pine no llama a MemoryStore.
- Pine no contiene `FMP_API_KEY`, `OPENAI_API_KEY` ni secretos.

Genesis backend es quien puede enriquecer con FMP, OpenAI, noticias, ballenas, memoria y aprendizaje.

## Como Pegar El Script

1. Abre TradingView.
2. Abre un chart del activo que quieras probar.
3. Abre Pine Editor.
4. Pega el contenido de `tradingview/genesis_advantage_strategy_v1.pine`.
5. Guarda el script.
6. Click en `Add to chart`.
7. Revisa Strategy Tester antes de crear alertas.

## Inputs Principales

General:

- `Trade mode`: Long Only, Short Only o Long & Short.
- `Strategy mode`: Breakout, Pullback, Momentum, Mean Reversion o Hybrid Genesis.
- `Risk profile`: Conservador, Moderado o Agresivo.
- `Use market regime filter`: bloquea entradas contra regimen fuerte.
- `Use volume filter`: exige volumen relativo.
- `Use Genesis Context Score`: activa el input externo/manual de Genesis.
- `Genesis Context Score`: valor manual de -100 a +100.
- `Minimum signal score`: umbral minimo de entrada.

Riesgo:

- `Risk per trade %`
- `Max daily loss %`
- `Max drawdown %`
- `ATR stop multiplier`
- `Take profit R`
- `Trailing stop ATR`
- `Use time stop`
- `Max bars in trade`
- `Minimum risk/reward`
- `Max ATR % allowed`
- `Max bar range % allowed`
- `Min relative volume`

Contexto:

- `SPY`
- `QQQ`
- `CBOE:VIX`
- `TVC:DXY`
- `BINANCE:BTCUSDT`
- Higher timeframe default `1D`
- Timeframe superior default `240` (4H)

## Como Usar Genesis Context Score

Consulta el backend:

```text
GET /api/genesis/trading-context?ticker=NVDA
```

El endpoint devuelve `genesis_context_score` entre -100 y +100, `bias`, `confidence`, noticias, alertas, whale flow, notas de memoria, flags de riesgo y que vigilar.

Por ahora ese score se copia manualmente al input `Genesis Context Score` de TradingView. Ejemplo:

- `+60`: Genesis tiene contexto alcista con evidencia razonable.
- `0`: neutral o sin evidencia suficiente.
- `-45`: contexto bajista o riesgo elevado.

No trates ese score como verdad absoluta. Es una capa de contexto, no una orden.

## Genesis Brain como contexto

Genesis Brain es la capa backend que junta memoria, senales de TradingView, alertas, noticias, ballenas/smart money, cartera paper y Hedge Engine. Su salida principal vive en:

```text
GET /api/genesis/trading-context?ticker=NVDA
```

El payload incluye `technical_context`, `macro_context`, `news_context`, `whale_context`, `alerts_context`, `memory_context`, `portfolio_context`, `hedge_context`, `risk_flags`, `what_to_watch`, `genesis_reading` y `suggested_tradingview_inputs`.

`suggested_tradingview_inputs` puede copiarse manualmente a TradingView:

- `genesisContextScore` -> input `Genesis Context Score`.
- `hedgeScoreInput` -> input `Genesis Hedge Score`.
- `preset`, `minSignalScore`, `trendRunnerMode`, `useHedgeMode`, filtros de volumen/regimen/HTF y `tradeMode`.

Genesis Brain no ejecuta orden. Solo convierte datos disponibles en contexto, filtros, journal y aprendizaje.

## Genesis Sync Mode

`Genesis Sync Mode` es el modo normal de trabajo despues de confirmar que el script ya prende. En este modo `Safe Mode` y `Validation Mode` deben quedar apagados; esos dos sirven solo para diagnostico.

Flujo recomendado:

1. Consultar `GET /api/genesis/trading-context?ticker=VOO`.
2. Copiar `suggested_tradingview_inputs` a TradingView.
3. Mantener `useGenesisSync=true`.
4. Copiar `genesisContextScore`, `genesisBiasInput`, `genesisNewsScore`, `genesisWhaleScore`, `genesisMacroRiskScore`, `hedgeScoreInput` y `noTradeScoreInput`.
5. Crear alerta de estrategia con `{{strategy.order.alert_message}}`.
6. Dejar que Genesis guarde cada senal como journal/paper, no como orden real.

Genesis Sync intenta que la decision no dependa solo del chart. El score final pondera tendencia, momentum, volumen, regimen, HTF, estructura, riesgo/recompensa, hedge/no-trade y contexto de Genesis. Si Genesis marca `No-Trade` o hedge alto, Pine bloquea o reduce agresividad.

Para VOO/SPY/ETF, `Auto Profile Mode` fuerza internamente `Defensive ETF Core`: long-only, EMA200, EMA50, menos trades y comparacion contra buy & hold. Esto evita que el bot intente adivinar techos con shorts contra un ETF tendencial.

## Profit Guard / Edge Guard

`Profit Guard` no promete rentabilidad. Su funcion es cortar los comportamientos que destruyen una curva:

- ETFs como VOO/SPY/QQQ no abren shorts aunque `Trade mode` quede en `Long & Short`.
- Cripto como BTC/ETH/SOL se clasifica automaticamente como `Crypto`.
- Shorts solo se permiten en tendencia bajista fuerte con HTF bajista.
- El bot reduce sobreoperacion con cooldown efectivo y maximo de trades por dia.
- Si el propio paper/backtest acumula suficientes operaciones con profit factor bajo o perdida mayor al umbral, `Strategy Health Guard` pausa nuevas entradas y lo reporta como `health guard: perfil sin edge`.

Si un activo sigue sin edge despues de esta proteccion, la decision correcta es `No-Trade / Watch Only`, cambiar timeframe o cambiar perfil. Forzar operaciones para "arreglar" el resultado seria sobreoptimizar.

## Autopilot Edge

`Autopilot Edge` es el modo por defecto para no depender de configuracion manual. Usa familias de reglas conocidas:

- ETF/acciones liquidas: filtro de tendencia 200D, filtro semanal EMA40 y filtro mensual 10M/absolute momentum.
- Cripto: breakout Donchian 55 con salida Donchian 20/EMA y filtro HTF.
- Shorts apagados por defecto. Solo se pueden habilitar manualmente, y aun asi quedan bloqueados en ETFs.
- Exposicion de backtest por defecto: 80% de equity para que el core capture tendencia en vez de micro-operar.

Estas reglas se inspiran en literatura de momentum/trend following como Faber Tactical Asset Allocation, time-series momentum de Moskowitz/Ooi/Pedersen, absolute momentum de Antonacci y sistemas Donchian/Turtle. No garantizan rentabilidad; por eso Genesis registra outcomes, benchmark capture, health guard y no-trade decisions.

## Trend Runner Mode

`Trend Runner Mode` busca capturar mejor tendencias fuertes sin perseguir una entrada extendida. Se activa con `preset=Trend Runner` y `trendRunnerMode=true`.

Long trend fuerte requiere:

- Precio sobre EMA200.
- EMA20 > EMA50 > EMA200.
- HTF bias bullish.
- ADX suficiente o pendiente EMA positiva.
- Regimen de mercado no risk-off fuerte.

En ese contexto el script:

- Evita cerrar demasiado rapido por pequenas oscilaciones.
- Usa trailing ATR mas amplio con `runnerTrailATR`.
- Prioriza pullbacks a EMA20/EMA50/golden pocket y rupturas confirmadas.
- Evita shorts contra mega tendencia alcista si `avoidShortsInBullTrend=true`.

## Paper Quality Mode

`Paper Quality Mode` sigue siendo el modo de limpieza de senales. Reduce ruido con quality gates, filtro de chop, HTF, volumen, RR, cooldown y max trades por dia. Si el mercado esta en tendencia fuerte, `Trend Runner` puede ser mas apropiado que endurecer filtros hasta perder la tendencia.

## Capital Protection / Hedge Mode

`Hedge Mode / Capital Protection` no cubre perfecto ni evita toda perdida. Sirve para reducir agresividad cuando Genesis detecta deterioro de contexto.

Inputs relevantes:

- `useHedgeMode=true`
- `hedgeScoreInput`
- `avoidTradeIfHedgeScoreAbove`
- `reduceSizeIfHedgeScoreAbove`
- `capitalProtectionMode=true`
- `protectOpenProfit=true`
- `hedgeRiskProfile`

Pine no abre hedge en otro simbolo. Pine solo bloquea entradas, sube exigencia, protege stops, genera alerta y manda JSON a Genesis.

## Como usar /api/genesis/hedge-plan

Consulta:

```text
GET /api/genesis/hedge-plan?ticker=NVDA
```

Devuelve `hedge_score`, `hedge_needed`, `hedge_type`, `suggested_hedge_ratio`, `suggested_stop`, `suggested_inverse_symbol`, riesgos y `what_to_watch`.

Interpretacion de `hedge_score`:

- `0-30`: sin cobertura.
- `31-55`: vigilancia.
- `56-75`: cobertura parcial.
- `76-100`: defensa fuerte o reducir exposicion.

Para cartera completa:

```text
GET /api/genesis/portfolio-hedge
```

## Como copiar hedgeScoreInput a TradingView

1. Abre `/api/genesis/hedge-plan?ticker=NVDA`.
2. Copia `hedge_score`.
3. Pegalo en TradingView como `Genesis Hedge Score`.
4. Mantén `useHedgeMode=true`.
5. Si `hedge_score` supera `reduceSizeIfHedgeScoreAbove`, el script exige mas calidad.
6. Si supera `avoidTradeIfHedgeScoreAbove`, bloquea entradas nuevas y prioriza proteccion/journal.

## Por que no existe proteccion perfecta

Toda cobertura tiene costo, retraso, tracking error o riesgo de quedar mal calibrada. Una cobertura realista busca reducir drawdown, proteger ganancias abiertas y evitar trades sin ventaja; no elimina el riesgo ni convierte una estrategia en ganancia segura.

## Como comparar contra buy & hold

La tabla Pine muestra una comparacion aproximada:

- `Strategy Return`
- `Buy Hold Return`
- `Strategy vs Hold`
- `Benchmark Warning`

Si buy & hold supera ampliamente y el activo esta en tendencia fuerte, la tabla puede advertir: `Estrategia protege riesgo, pero esta dejando escapar tendencia.` Esa alerta ayuda a decidir si conviene usar `Trend Runner` en paper.

## Regimen De Mercado

La estrategia calcula `marketRegimeScore` de 0 a 100 con:

- SPY sobre/bajo EMA 50 y EMA 200.
- QQQ sobre/bajo EMA 50 y EMA 200.
- VIX bajo 20 y bajo EMA 50.
- DXY bajo EMA 50.
- BTC sobre EMA 50.
- Activo sobre EMA 200.
- Pendiente de EMA 50.

Clasificacion:

- `Alcista fuerte`
- `Alcista moderado`
- `Lateral`
- `Riesgo alto`
- `Bajista`

El fondo del chart cambia por regimen y la tabla muestra el score.

## Signal Score

El score final combina:

- Trend
- Momentum
- Volume
- Regime
- Structure
- Risk/reward
- Genesis Context, si esta activo

Reglas duras:

- No entra si `finalSignalScore` queda bajo el minimo.
- No entra si RR queda bajo el minimo.
- No entra si el volumen no confirma y el filtro esta activo.
- No entra si ATR% o rango de vela son extremos.
- No entra si el regimen contradice fuerte la operacion.

## Setups

Breakout long:

- Rompe resistencia.
- Volumen relativo suficiente.
- EMA 20 > EMA 50.
- Precio > EMA 200.
- RSI no extremo.
- MACD confirma.

Pullback long:

- Tendencia alcista.
- Retroceso a EMA 20/50 o golden pocket.
- Recupera VWAP/EMA.
- RSI rebota.
- Volumen confirma.

Momentum long:

- EMA 20 > EMA 50 > EMA 200.
- MACD positivo.
- RSI 50-70.
- Volumen relativo alto.

Mean reversion long:

- Toca banda inferior de Bollinger.
- RSI bajo pero recuperando.
- Soporte cercano.
- Regimen no bajista fuerte.

Short setups:

- Breakdown de soporte.
- EMA 20 < EMA 50.
- Precio bajo EMA 200.
- MACD bajista.
- Volumen confirma.

## Gestion De Riesgo

La estrategia usa:

- Stop loss por ATR.
- Stop por invalidacion tecnica.
- Take profit por multiples de R.
- Trailing stop por ATR.
- Time stop opcional.
- Cierre si score cae.
- Cierre si pierde EMA/nivel clave.
- `strategy.risk.max_intraday_loss`.
- `strategy.risk.max_drawdown`.
- `pyramiding=0`.

Regla practica: no subas riesgo para que el backtest se vea mejor. Primero valida robustez por activo, timeframe y condiciones de mercado.

## Alertas Y Webhook

Alertconditions disponibles:

- Long Signal
- Short Signal
- Exit Signal
- Stop Hit
- Take Profit Hit
- Strategy Invalidated
- Watch Only

Webhook Genesis:

```text
POST /api/genesis/tradingview-webhook
```

Mensaje JSON sugerido:

```json
{
  "source": "tradingview",
  "strategy": "Genesis Advantage Strategy v1",
  "ticker": "{{ticker}}",
  "time": "{{time}}",
  "action": "long_signal",
  "price": "{{close}}",
  "score": "{{plot(\"Final Score\")}}",
  "setup": "{{plot(\"Setup Code\")}}",
  "risk": "paper_only",
  "stop": "{{plot(\"Stop\")}}",
  "target": "{{plot(\"Target\")}}",
  "regime": "{{plot(\"Regime Score\")}}",
  "notes": "TradingView alert for Genesis journal, no broker execution."
}
```

El backend guarda la alerta y responde sin ejecutar orden.

## Error: can't parse argument number

TradingView puede mostrar `can't parse argument number` cuando un JSON con llaves `{}` se arma dentro de `str.format()`. Pine interpreta las llaves del JSON como placeholders de formato, por ejemplo `{0}`, y se rompe si encuentra `{"source":...}`.

La estrategia evita ese error usando un JSON builder por concatenacion segura (`buildAlertJson`) y no mete JSON dentro de `str.format()`.

Reglas aplicadas:

- `strategy.entry`, `strategy.exit` y `strategy.close` usan `alert_message=buildAlertJson(...)`.
- `alertcondition` usa mensajes constantes simples.
- El webhook dinamico debe salir de `{{strategy.order.alert_message}}`.
- No hay API keys ni secretos dentro del Pine.

Para crear la alerta en TradingView:

1. Pega el Pine.
2. Click en `Add to chart`.
3. Activa `freePlanMode=true`.
4. Activa `validationMode=true`.
5. Activa `debugMode=true`.
6. Create Alert sobre la estrategia.
7. En `Message`, usa exactamente:

```text
{{strategy.order.alert_message}}
```

Si usas alertconditions manuales, deja su mensaje constante o reemplazalo por `{{strategy.order.alert_message}}` cuando quieras enviar fills reales de estrategia al webhook Genesis.

## Memoria Y Journal

El webhook guarda:

- `strategy_signals`
- `tradingview_alerts`
- `strategy_outcomes`
- `backtest_notes`
- `trade_journal`

Campos guardados:

- ticker
- setup
- action
- score
- price
- stop
- target
- risk_reward
- market_regime
- genesis_context
- timestamp
- outcome_1h
- outcome_24h
- outcome_7d
- status

Genesis podra responder preguntas como:

- Que senales de TradingView tengo.
- Que aprendiste de mis senales.
- Que setups fallan.
- Que aprendiste de NVDA.
- Que debo evitar.

## Backtesting

En TradingView:

1. Abre Strategy Tester.
2. Revisa Net Profit, Max Drawdown, Total Trades, Percent Profitable y Profit Factor.
3. Cambia solo un input a la vez.
4. Prueba varios activos y timeframes.
5. Compara mercado alcista, lateral y bajista.

La tabla del script muestra score, regimen, setup, RR, stop, target y estado. TradingView calcula metricas de estrategia; Pine puede mostrar parte del contexto, pero las metricas finales se validan en Strategy Tester.

## Uso sin pagar Deep Backtesting

Deep Backtesting y el cambio manual de rango de fechas son funciones Premium en TradingView. En plan gratis, Genesis Advantage debe trabajar con las barras que ya estan cargadas en el chart normal.

## Bot apagado / sin operaciones

Si TradingView parece mostrar el bot "apagado", empieza por el modo estable:

- `safeMode=true`
- `freePlanMode=true`
- `validationMode=true`
- `debugMode=true`
- `showConditionMarkers=true`
- `showScoreTable=true`
- `validationPulseBars=12`
- `tradeMode=Long & Short`
- `genesisContextScore=100`
- `useHedgeMode=false` para la primera prueba
- `minSignalScore=40`

`Safe Mode estable` usa una logica minima para confirmar que la estrategia esta viva:

- Long si EMA20 cruza sobre EMA50, o si `close > EMA20`, `EMA20 > EMA50` y `RSI > 50`.
- Short si EMA20 cruza bajo EMA50, o si `close < EMA20`, `EMA20 < EMA50` y `RSI < 50`.
- Si no aparece cruce en el tramo cargado, `validationPulseBars` genera entradas de diagnostico cada N barras en validation mode.
- Salidas con stop ATR, target ATR/R y trailing.

Este modo no es la version operativa final. Sirve para comprobar que Pine compila, que `strategy.entry` y `strategy.exit` generan operaciones, que aparecen marcadores visuales y que el Strategy Tester no queda en `Sin datos`.

La tabla muestra:

- `Bot status`: `ACTIVO` si hay senales o `BLOQUEADO` si los filtros impiden operar.
- `Strategy Alive`: `YES/NO`, segun haya barras suficientes y señales raw.
- `Safe / Validation`: confirma si el modo estable esta activo.
- `Long Raw / Short Raw`: senales tecnicas crudas.
- `Long Final / Short Final`: senales despues de filtros.
- `Block Reason`: razon concreta, por ejemplo `sin barras suficientes`, `score bajo`, `hedge score bloquea`, `TradeMode bloquea` o `Raw condition no aparece`.

Despues de comprobar que el bot esta vivo, pasa a paper serio:

- `safeMode=false`
- `validationMode=false`
- `preset=Core Tactical`
- `trendRunnerMode=true`
- `useHedgeMode=true`
- `minSignalScore=60`

No uses Safe Mode como promesa de rentabilidad. Es un modo de diagnostico y validacion.

Para probar sin pagar:

- `freePlanMode=true`
- `safeMode=true`
- `validationMode=true`
- `validationPulseBars=12`
- `debugMode=true`
- `showConditionMarkers=true`
- `showScoreTable=true`
- `minSignalScore=40`
- `useMarketRegimeFilter=false`
- `useVolumeFilter=false`
- `genesisContextScore=100`
- `tradeMode=Long & Short`
- Timeframe recomendado: `1H` o `4H`
- Simbolos recomendados: `NVDA`, `SPY`, `QQQ` o `BTCUSD`

`validationMode=true` no es modo operativo final. Solo sirve para validar que el Strategy Tester genera operaciones con las barras cargadas, que las entradas/salidas funcionan y que las alertas visuales aparecen.

Si Strategy Tester muestra `Sin datos`, revisa la tabla de diagnostico:

- `Raw condition no aparece`: el chart aun no genero una condicion tecnica basica.
- `Score bajo`: baja `minSignalScore` en validacion o sube temporalmente `genesisContextScore`.
- `Volumen bloquea`: desactiva `useVolumeFilter` en validacion.
- `Regimen bloquea`: desactiva `useMarketRegimeFilter` en validacion.
- `RR insuficiente`: baja `Minimum risk/reward` o usa validation mode.
- `TradeMode bloquea`: cambia a `Long & Short`.
- `Sin suficientes barras`: carga mas historial o usa un timeframe mayor.

Para paper serio:

- `validationMode=false`
- `debugMode=false`
- `minSignalScore=65-75`
- `useMarketRegimeFilter=true`
- `useVolumeFilter=true`
- `riskPerTradePct` bajo
- Revisar drawdown, profit factor, cantidad de trades y rachas perdedoras antes de confiar en el setup.

## Como mejorar porcentaje de aciertos sin sobreoptimizar

La estrategia ahora incluye `Paper Quality Mode`, pensado para quedar entre el modo de validacion y un modo muy estricto. La idea no es forzar win rate perfecto, sino reducir entradas de baja calidad sin destruir profit factor, drawdown ni cantidad de muestras.

Win rate no se debe leer solo. Antes de confiar en un preset revisa:

- Profit factor.
- Drawdown.
- Total de trades.
- Rachas perdedoras.
- Comportamiento en varios activos y timeframes.
- Si las entradas siguen siendo explicables visualmente.

`Paper Quality Mode` agrega:

- Quality gate long/short para exigir tendencia, momentum, volumen, RR y estructura razonables.
- Filtro de chop/lateralidad con ADX, Bollinger width y pendiente de EMA.
- Confirmacion de timeframe superior.
- Penalizaciones por sobreextension, RSI extremo, volumen bajo y resistencia/soporte demasiado cercanos.
- Cooldown entre operaciones, limite de trades por dia y bloqueo de reversa inmediata.
- Stop a breakeven opcional despues de 1R, TP parcial opcional y salidas por deterioro de momentum.

Configuracion recomendada para validar que el tester funciona:

- `preset=Validation`
- `validationMode=true`
- `debugMode=true`

Configuracion recomendada para paper serio:

- `preset=Paper Quality`
- `validationMode=false`
- `paperQualityMode=true`
- `minSignalScore=60`
- `useHTFConfirmation=true`
- `chopFilterEnabled=true`
- `useVolumeFilter=true`
- `useMarketRegimeFilter=true`
- `maxTradesPerDay=3`
- `cooldownBars=10`

Configuracion conservadora:

- `preset=Conservative`
- `minSignalScore=70`
- `maxTradesPerDay=1-2`
- HTF obligatorio.

No optimices solo NVDA ni ajustes parametros hasta que una ventana historica quede perfecta. Prueba SPY, QQQ, activos grandes, cripto liquido y al menos dos timeframes. Despues haz forward testing y paper trading antes de considerar cualquier automatizacion.

Configuracion recomendada para probar Trend Runner:

- `preset=Trend Runner`
- `validationMode=false`
- `trendRunnerMode=true`
- `avoidShortsInBullTrend=true`
- `useHTFConfirmation=true`
- `useVolumeFilter=true`
- `useMarketRegimeFilter=true`
- `minSignalScore=60`
- `runnerTrailATR=2.5`
- `breakEvenAfterR=1.25`

Configuracion recomendada para probar Hedge Mode:

- `useHedgeMode=true`
- `capitalProtectionMode=true`
- `protectOpenProfit=true`
- `hedgeScoreInput=hedge_score de /api/genesis/hedge-plan?ticker=NVDA`
- `avoidTradeIfHedgeScoreAbove=75`
- `reduceSizeIfHedgeScoreAbove=55`
- `hedgeRiskProfile=Moderado`

## Core + Tactical Mode

La version defensiva protegia capital, pero podia quedarse fuera de tendencias grandes. `Core + Tactical Mode` intenta separar dos trabajos:

- `Core Position`: mantener exposicion long base cuando el activo esta claramente alcista.
- `Tactical Trades`: tomar pullbacks, breakouts o salidas tacticas sin cerrar el core por ruido menor.
- `Hedge Mode`: proteger solo cuando el riesgo contextual sube de verdad.

En Pine sigue existiendo una sola posicion por `pyramiding=0`, asi que Genesis lo implementa como una aproximacion operativa: si hay tendencia fuerte, el preset favorece entrada core, trailing amplio por `EMA50`/estructura/ATR y evita shorts contra mega tendencia. Las senales tacticas ajustan entradas y salidas, pero no deben forzar cierre total por RSI alto o un pullback normal.

Configuracion recomendada para activos fuertes tipo NVDA:

- `preset=Core Tactical`
- `coreTacticalMode=true`
- `trendRunnerMode=true`
- `avoidShortsInBullTrend=true`
- `hedgeImpactMode=Balanced` o `Light`
- `useHTFConfirmation=true`
- `useVolumeFilter=true`
- `minSignalScore=58-62`
- `coreTrailMode=EMA50`
- `coreATRMultiplier=3.0`
- `tacticalATRMultiplier=1.8`

Para mercados riesgosos:

- `hedgeImpactMode=Defensive`
- `minSignalScore=70`
- Reducir exposicion en paper/journal.

La tabla muestra `Core Tactical Mode`, `Core Active`, `Tactical Signal`, `Hedge Impact Mode`, `Benchmark Capture %`, `Strategy vs Hold` y `Suggested Mode`.

`Benchmark Capture %` compara de forma aproximada el retorno de la estrategia contra buy & hold usando las barras cargadas. Si aparece "Captura de tendencia baja: activar Core.", Genesis esta avisando que la defensa puede estar dejando escapar tendencia. No es una promesa de mejora: es una senal para paper trading y forward testing.

El endpoint `/api/genesis/trading-context?ticker=NVDA` devuelve sugerencias para copiar a TradingView:

- `suggested_mode`
- `suggested_core_tactical_mode`
- `suggested_hedge_impact_mode`
- `avoid_shorts`
- `suggested_min_signal_score`
- `suggested_trailing_mode`
- `reason`

La cobertura reduce riesgo, pero no elimina perdidas. No hay broker real ni orden automatica.

## Strategy Research Lab Y Auto Profile Mode

Una sola estrategia no sirve para todos los activos. Genesis ahora usa un `Strategy Research Lab` backend para clasificar el activo, comparar familias de estrategia y recomendar un perfil por ticker, timeframe y regimen.

Familias disponibles:

- `Trend Following`: EMA 20/50/200, ADX y trailing ATR.
- `Trend Pullback`: pullback a EMA20/EMA50/golden pocket con RSI/MACD mejorando.
- `Breakout Volume`: ruptura con cierre confirmado y volumen relativo.
- `Mean Reversion`: Bollinger + RSI extremo solo en lateralidad.
- `Defensive ETF Core`: ETFs como VOO/SPY, long-only, EMA200, baja frecuencia.
- `Crypto Momentum V4`: ATR amplio, BTC Long Term Edge, regimen BTC, breakout/retest, volatility expansion, hedge overlay y no-trade real en chop o 1H sin edge.
- `Commodity Regime`: tendencia + macro/geopolitica para oil/commodities.
- `Gold Defensive`: perfil defensivo sensible a dolar/tasas/risk-off.
- `Hedge / Capital Protection`: cobertura sugerida sin broker.

## BTC Edge Engine

BTC no usa la misma logica que VOO, NVDA u oro. Genesis agrega `BTC Edge Engine` para cripto liquido y evalua:

- Regimen BTC: `bull_trend`, `bear_trend`, `range`, `chop`, `breakout`, `breakdown`, `squeeze`, `volatility_expansion`, `liquidity_sweep`, `recovery`, `risk_off`.
- Volatilidad: ATR %, Bollinger width, compresion y expansion.
- Momentum: RSI, MACD, EMA slope, higher lows/lower highs.
- Volumen: volumen relativo, dollar volume estimado y volumen en ruptura.
- Riesgo externo: SPY/QQQ, DXY/VIX si hay contexto, noticias cripto, regulacion, ETF flows si existen, ballenas y memoria.
- Memoria: setups BTC ganadores/fallidos, timeframe mas estable y fallos recientes.

### BTC Crypto Momentum V4

`Crypto Momentum V4` usa:

- HTF confirmation.
- ATR stop amplio.
- Trail ATR amplio.
- BTC Regime Switch.
- BTC Long Term Edge Mode.
- Filtro de chop.
- Breakout Retest.
- Volatility Expansion.
- Trend Continuation.
- Mean Reversion solo en rango.
- Active Hedge Overlay.
- No-Trade cuando no hay edge.

Inputs recomendados para BTC:

- `assetProfile=Crypto`
- `preset=Crypto Momentum V4`
- `safeMode=false`
- `validationMode=false`
- `autoProfileMode=true`
- `noTradeMode=true`
- `blockIfNoEdge=true`
- `tradeMode=Long & Short`
- `useHTFConfirmation=true`
- `useVolumeFilter=true`
- `useMarketRegimeFilter=true`
- `cryptoAvoidChop=true`
- `cryptoV4Mode=true`
- `cryptoV3Mode=true`
- `btcLongTermMode=true`
- `cryptoUseRegimeSwitch=true`
- `cryptoUseBreakoutRetest=true`
- `cryptoUseVolExpansion=true`
- `useActiveHedgeOverlay=true`
- `hedgeShortAllowed=true`
- `cryptoAtrMultiplier=3.0`
- `cryptoTrailATR=3.8`
- `btcCooldownBars=24`
- `btcMinBarsAfterExit=12`
- `minSignalScore=62`

Si BTC 1H tiene PF menor a 1 o expectancy negativa, Genesis debe recomendar no operar 1H y probar 4H/1D. No debe forzar trades malos para "arreglar" el resultado.

### BTC Long Term Edge Mode

`btcLongTermMode=true` separa el ruido de BTC 1H de una lectura core mas seria. Cuando esta activo, la tabla muestra `Version: Genesis Advantage v10.13 BTC Edge`, `BTC Long Term Mode`, `Timeframe`, `Edge Status`, `No Trade Score`, `Suggested Action` y `Block Reason`.

Si el chart esta en 1H y Genesis o el Pine detectan que ese marco sobreopera, el script bloquea entradas y muestra `BTC 1H bloqueado: usar 4H/1D`. Eso no es un fallo: es No-Trade real para evitar seguir midiendo un perfil que ya mostro PF debil o expectancy negativa.

Para copiar contexto desde `/api/genesis/trading-context?ticker=BTCUSD`, usa:

- `genesisContextScore`
- `hedgeScoreInput`
- `noTradeScoreInput`
- `assetProfile=Crypto`
- `preset=Crypto Momentum V4`
- `recommended_timeframe=4H/1D`

Si la tabla no muestra `v10.13`, TradingView sigue usando codigo viejo.

### Breakout Retest

Para BTC, una ruptura no cuenta por una sola mecha. Genesis busca cierre sobre resistencia, retest del nivel y recuperacion con volumen relativo. Si no hay retest o volumen, la senal queda como vigilancia o No-Trade.

### Volatility Expansion

Cuando Bollinger/ATR vienen comprimidos, Genesis vigila expansion direccional con volumen. Si la expansion no confirma, no se persigue el precio.

### Active Hedge Overlay

`Active Hedge Overlay` no ejecuta una cobertura real ni toca broker. En Pine solo bloquea entradas debiles, sube la calidad minima, protege ganancia abierta con breakeven/trailing y manda alertas `hedge_needed`, `reduce_exposure` o `risk_off` a Genesis. Si `tradeMode` no permite shorts, el script no abre short y se queda en alerta/journal.

Campos que llegan por webhook:

- `btc_regime`
- `btc_long_term_mode`
- `crypto_v4_mode`
- `crypto_v3_mode`
- `strategy_version`
- `bot_status`
- `entry_quality_score`
- `hedge_overlay`
- `hedge_short_allowed`
- `hedge_score`
- `no_trade_reason`
- `order_policy=journal_only_no_broker`

La cobertura reduce riesgo, pero no elimina perdidas ni garantiza rentabilidad.

### Safe/Validation No Es Performance

`safeMode=true` y `validationMode=true` solo prueban que el script esta vivo, que `strategy.entry`/`strategy.exit` generan operaciones y que la alerta funciona.

No deben usarse para medir rentabilidad, profit factor, win rate ni edge real. En Pine la tabla muestra:

`VALIDACION: no usar para evaluar rentabilidad`

Para paper serio usa:

- `safeMode=false`
- `validationMode=false`
- preset especifico por activo
- `noTradeMode=true`
- `blockIfNoEdge=true`

## Edge Finder

`Edge Finder` es la capa que evita seguir usando un setup perdedor por costumbre. Genesis prueba familias de estrategia con una busqueda acotada de parametros razonables: EMA 10/20/21, EMA 50/55, EMA 100/200, ATR stop 1.5-3.0, trailing ATR 2.0-3.5, volumen relativo 0.8-1.5, ADX minimo y canales Donchian para momentum. No prueba cientos de combinaciones ni busca el mejor caso perfecto.

Para aceptar edge, Genesis exige:

- Profit factor minimo mayor a 1.20, ideal mayor a 1.35.
- Expectancy positiva.
- Retorno neto positivo.
- Muestra suficiente de operaciones.
- Drawdown razonable frente al retorno.
- Walk-forward simple con train/test y ventanas positivas.
- Comparacion contra buy & hold.

Si no pasa esas reglas, Genesis devuelve `no_trade_recommendation=true`, sube `noTradeScoreInput` y recomienda cambiar timeframe/perfil o esperar. Eso es intencional: No-Trade es parte de la ventaja, porque evita seguir operando cuando el setup no tiene evidencia.

`Auto Profile Mode` en Pine:

- `autoProfileMode=true`
- `assetProfile=Auto` o manual: `Mega-cap Growth`, `Index ETF`, `Crypto`, `Commodity`, `Gold Defensive`, `High Beta`, `Custom`.

Ejemplos:

- NVDA/MSFT/AAPL: `Mega-cap Growth`, `Core Tactical`, `Trend Pullback`, evitar shorts contra tendencia.
- VOO/SPY/QQQ: `Index ETF`, `Defensive ETF Core`, long-only, mejor en `4H/1D`.
- BTC: `Crypto Momentum V4`, ATR amplio, BTC Long Term Edge, breakout/retest, volatility expansion, hedge overlay y no stops apretados.
- BNO/Brent/Oil: `Commodity Regime`, exigir contexto macro/geopolitico.
- IAU/GLD: `Gold Defensive`, revisar DXY/tasas/risk-off.

Si usas VOO/SPY en 1H, la tabla muestra:

`ETF core funciona mejor en 4H/1D; 1H puede sobreoperar.`

Lee siempre:

- Profit factor
- Win rate
- Max drawdown
- Benchmark capture
- Strategy vs Hold
- Total trades
- Out-of-sample / forward testing

Si VOO tiene profit factor menor a 1 con un setup tactico, Genesis debe marcarlo como perfil fragil y recomendar `Defensive ETF Core`, no forzar la misma estrategia de NVDA.

## No-Trade Mode

No operar tambien es una decision valida. `No-Trade Mode` existe para que Genesis no fuerce senales cuando un activo, timeframe o perfil no muestra edge suficiente.

Inputs en TradingView:

- `noTradeMode=true`
- `noTradeScoreInput=score que devuelve Genesis`
- `blockIfNoEdge=true`
- `preset=No Trade / Watch Only` si quieres bloquear manualmente.

Si `noTradeScoreInput >= 70` y `blockIfNoEdge=true`, Pine bloquea entradas, muestra `Sin edge: no operar`, deja la alerta como vigilancia y manda al webhook campos como `no_trade_score`, `no_trade_block`, `edge_status` y `order_policy=journal_only_no_broker`.

Genesis marca No-Trade cuando detecta:

- Profit factor menor a 1.
- Expectancy negativa.
- Drawdown alto frente al retorno.
- Benchmark superando por mucho a la estrategia.
- Muy pocos trades para validar.
- Activo/timeframe incompatible, por ejemplo ETF en 1H con sobreoperacion.
- Hedge score demasiado alto.
- Memoria indicando fallos repetidos del setup.

Si un activo/timeframe tiene PF menor a 1 o expectancy negativa, Genesis debe recomendar no operar o cambiar perfil/timeframe.

Configuraciones utiles:

- NVDA/growth: probar `Core Tactical` o `Trend Runner`, pero bloquear si Genesis devuelve `noTradeScoreInput>=70`.
- VOO/SPY: usar `Defensive ETF Core`, `Long Only`, preferir `4H/1D`; si 1H no tiene edge, usar `No Trade / Watch Only`.
- BTC: usar `Crypto Momentum V4` con ATR amplio; bloquear cuando hay chop, 1H sin edge, risk-off, noTradeScore alto o hedge score alto.
- IAU/GLD: usar `Gold Defensive` y validar contra DXY/tasas.
- BNO/oil: usar `Commodity Regime` solo con catalizador macro/geopolitico claro.

## Como Evitar Overfitting

- No optimices solo sobre un ticker.
- No ajustes inputs para encajar un periodo perfecto.
- Usa muestras fuera de periodo.
- Valida en paper trading.
- Exige que la logica siga teniendo sentido visual.
- Mantiene riesgo pequeno mientras no exista evidencia.
- Revisa drawdown, profit factor, cantidad de trades y rachas perdedoras.

## Checklist Antes De Real Money

- Backtest positivo en varios periodos.
- Forward testing positivo.
- Paper trading con ejecuciones reales simuladas.
- Drawdown aceptable.
- Riesgo por trade definido.
- Alertas llegando a Genesis.
- Journal guardando outcomes.
- No hay API keys en Pine.
- No hay broker conectado desde Genesis.
- Entiendes que no hay garantia de rentabilidad.

Advertencia: Esta estrategia no garantiza rentabilidad. Primero debe validarse con backtesting, paper trading y forward testing.

## TradingView vs MetaTrader 5

TradingView y MetaTrader 5 tienen papeles distintos dentro de Genesis:

- TradingView: visual, Pine Script, alertas, backtesting rapido y lectura de chart.
- MT5: Expert Advisor local, Strategy Tester, demo/paper trading y journal de ejecucion.
- Genesis: cerebro central que combina contexto, memoria, no-trade, hedge y seleccion de estrategia.

Fase 11 agrega el MT5 Bridge con endpoints:

- `GET /api/genesis/mt5/health`
- `GET /api/genesis/mt5/config`
- `GET /api/genesis/mt5/decision?symbol=BTCUSD`
- `POST /api/genesis/mt5/account-sync`
- `POST /api/genesis/mt5/signal`
- `POST /api/genesis/mt5/order-request`
- `POST /api/genesis/mt5/order-result`

El bridge nace cerrado por seguridad:

- `MT5_ENABLED=false`
- `MT5_DEMO_ONLY=true`
- `MT5_LIVE_TRADING_ENABLED=false`
- `MT5_ORDER_EXECUTION_ENABLED=false`
- `MT5_KILL_SWITCH=true`

MetaTrader debe correr localmente en tu PC/VPS. Si usas el EA `mt5/GenesisBridgeEA.mq5`, primero habilita WebRequest hacia tu backend Genesis. Los inputs seguros del EA son `AllowLiveTrading=false`, `JournalOnly=true`, `DemoOnly=true` y `KillSwitch=true`.

Genesis puede devolver `BUY`, `SELL`, `WAIT`, `NO_TRADE`, `HEDGE` o `REDUCE`, pero en esta fase el backend responde siempre con `order_executed=false`, `broker_touched=false` y `order_policy=journal_only_no_broker`. Primero demo/backtest/forward testing; no hay promesa de rentabilidad.
