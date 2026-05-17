# Genesis + MetaTrader 5 Bridge

Genesis is the brain. MetaTrader 5 is the local demo/backtest/journal surface.

This phase does not enable real broker execution. The default posture is:

- `MT5_ENABLED=false`
- `MT5_DEMO_ONLY=true`
- `MT5_LIVE_TRADING_ENABLED=false`
- `MT5_ORDER_EXECUTION_ENABLED=false`
- `MT5_KILL_SWITCH=true`
- `JournalOnly=true` in the EA
- `AllowLiveTrading=false` in the EA

## What This Bridge Does

- Reads Genesis decisions through `/api/genesis/mt5/decision?symbol=BTCUSD`.
- Sends account snapshots to `/api/genesis/mt5/account-sync`.
- Sends MT5 journal signals to `/api/genesis/mt5/signal`.
- Sends safe market ticks to `/api/genesis/mt5/tick` for forward-test/shadow metrics.
- Sends order requests to `/api/genesis/mt5/order-request`.
- Sends results/logs to `/api/genesis/mt5/order-result`.
- Stores everything in Genesis MemoryStore for learning.

It does not store MT5 passwords. It does not store API keys in MQL5. It does not execute real orders in this phase.

## Install MT5 Demo

1. Install MetaTrader 5 from your broker or MetaQuotes.
2. Open a demo account.
3. Confirm the account is demo inside MT5.
4. Open `Tools > Options > Expert Advisors`.
5. Enable WebRequest for listed URLs.
6. Add your Genesis backend URL, for example:
   `https://genesisbot-production.up.railway.app`

## Compile The EA

1. Open MetaEditor.
2. Create or open `GenesisBridgeEA.mq5`.
3. Paste the contents of `mt5/GenesisBridgeEA.mq5`.
4. Compile.
5. Attach it to a chart in MT5.

Default inputs are safe:

- `AllowLiveTrading=false`
- `DemoOnly=true`
- `JournalOnly=true`
- `KillSwitch=true`

With those values, the EA only polls Genesis and writes journal events.

## Backtesting In MT5

1. Open Strategy Tester.
2. Select `GenesisBridgeEA`.
3. Use a demo environment and historical data.
4. Start with visual mode.
5. Review drawdown, profit factor, number of trades and journal logs.
6. Do not evaluate a setup as real edge until it passes backtest, paper trading and forward testing.

## Safety Checklist

- Confirm `JournalOnly=true`.
- Confirm `AllowLiveTrading=false`.
- Confirm `KillSwitch=true`.
- Confirm backend returns `order_executed=false`.
- Confirm backend returns `broker_touched=false`.
- Confirm `/api/genesis/mt5/health` says live trading is disabled.
- Never paste MT5 passwords into Genesis.

## Symbol Mapping

Brokers use different symbol names. Configure:

```text
MT5_SYMBOL_MAP_JSON={"BTC-USD":"BTCUSD","IAU":"XAUUSD","BNO":"USOIL"}
MT5_ALLOWED_SYMBOLS=BTCUSD,NVDA,SPY,QQQ,XAUUSD
```

If a symbol is not mapped or not allowed, Genesis returns `NO_TRADE`.

## Endpoint Quick Test

```text
GET  /api/genesis/mt5/health
GET  /api/genesis/mt5/config
GET  /api/genesis/mt5/decision?symbol=BTCUSD
GET  /api/genesis/mt5/performance?symbol=BTC
GET  /api/genesis/mt5/forward-test?symbol=BTC
GET  /api/genesis/mt5/outcomes/recent?symbol=BTC
POST /api/genesis/mt5/account-sync
POST /api/genesis/mt5/signal
POST /api/genesis/mt5/tick
POST /api/genesis/mt5/order-request
POST /api/genesis/mt5/order-result
```

## Forward Test / Shadow Trading

Genesis can measure MT5 decisions without sending orders. The EA posts ticks with symbol, bid, ask, last, spread, timeframe, account/server metadata and `broker_touched=false`.

When Genesis receives a BUY/SELL signal with entry, stop and target, it creates a shadow trade in MemoryStore. Later ticks close that shadow trade as win/loss when simulated TP/SL is touched. NO_TRADE and HEDGE signals are tracked separately so Genesis can measure missed opportunities, avoided losses and hedge false alarms.

This is not real trading and does not prove future profitability. Use it for forward testing, paper review and journal learning before considering any automation.

Expected default behavior:

- `order_policy=journal_only_no_broker`
- `order_executed=false`
- `broker_touched=false`

## What This Does Not Mean

- It does not guarantee profitability.
- It does not remove losses.
- It does not execute real trades.
- It does not replace risk management.
- It is a controlled bridge for demo, backtesting, forward testing and journal learning.
