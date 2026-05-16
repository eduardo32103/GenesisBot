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
- Trend timeframe default `240` (4H)

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

Para probar sin pagar:

- `freePlanMode=true`
- `validationMode=true`
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
