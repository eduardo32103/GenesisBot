# VETO000 Recovery and Exact-Data Rebuild

## Objective

Recover the persisted VETO000 final evaluation first. Rebuild only if recovery remains impossible.

This branch is research-only:
- no broker
- no live trading
- no production promotion
- no parameter changes after protected results
- no validation/holdout tuning
- fixed RR = 1:3
- mirrored long/short logic
- realistic costs plus 2x-cost stress

## Original persisted pipeline recovered from Supabase logs

1. v1209 — `RC050_3H_CRYPTO_ALIGN_MATRIX_A`
2. v1210 — `RC050_3H_CANON_ALIGN_2018_2025_A`
3. v1211 — `RC050_3H_CANON_BASE_2026_STRESS_A`
4. v1212 — `RC050_3H_VETO000_CANON_A`
5. v1214 — `GENESIS_UNSEEN_FREEZE_FIXED_V2_A`
6. v1215 — `RC050_3H_VETO000_UNSEEN_V2_A`
7. v1216 — `GENESIS_VETO000_UNSEEN_EVAL_V2_A`

The v1216 evaluator returned HTTP 200 and persisted a result to
`market_flow_research_results` with HTTP 201.

## Recovery route

`scripts/recover_veto000_result.py` calls only existing read-only Edge Function
routes:

- `read=veto_public`
- `read=veto_pool`
- `read=veto_pgb`
- `read=veto_direct`

It does not rerun the strategy or mutate Supabase.

## Frozen baseline universe

### Stocks

AAPL, MSFT, XOM, JPM, JNJ, WMT, PG, KO, PEP, CAT, BA, IBM, CSCO, INTC,
HD, MCD, CVX, GS, MMM, NKE

### Index futures

ES=F, NQ=F, YM=F, RTY=F

### Other futures

GC=F, CL=F, SI=F, HG=F, ZB=F, NG=F, ZC=F, ZS=F

### Crypto

BTC, ETH / BTCUSDT, ETHUSDT according to source partition.

## Frozen baseline dataset IDs observed in the original VETO000_CANON trace

### Stocks / Databento

- `GENESIS_DATABENTO_XNAS_STOCKS_1H_2018_2020_V1`
- `GENESIS_DATABENTO_XNAS_STOCKS_1H_2021_2024_V1`
- `GENESIS_DATABENTO_XNAS_STOCKS_1H_2024_2025_V1`

### Futures / Databento

- `GENESIS_DATABENTO_FUTURES_1H_2018_2020_V1`
- `GENESIS_DATABENTO_FUTURES_1H_2021_2024_V1`
- `GENESIS_DATABENTO_FUTURES_1H_20261001_V1`

### Crypto / Binance

- `GENESIS_BINANCE_SPOT_OHLC_1H_2018_2020_V1`
- `GENESIS_BINANCE_SPOT_1H_2021_2024_V1`
- `GENESIS_DEV_BINANCE_SPOT_20260930_V2`
- `GENESIS_VALID_BINANCE_SPOT_1H_2025_MERGED_V1`
- `GENESIS_BT_3H_2021_2026_V1`
- `GENESIS_BINANCE_SPOT_1H_2026SEP_HOLDOUT_V1`

### 2026 stocks observed in original trace

- `GENESIS_YAHOO_STOCKS_1H_2026_HOLDOUT_V1`
- `GENESIS_VALID_OHLCV_20260930_V1` was also consulted historically, but it had
  a known coverage defect and must not be silently substituted for a valid
  2026 stock holdout.

## Exact unseen universe recovered from v1216 trace

### 20 unseen stocks
AMZN, GOOGL, META, NVDA, ORCL, COST, DIS, UNH, TMO, AVGO, QCOM, TXN, AMGN,
SBUX, LOW, UPS, RTX, GE, GM, F

Dataset:
`GENESIS_UNSEEN_YAHOO_1H_2024_2026_V2`

### 4 unseen indices
^MID, ^N225, ^GDAXI, ^FTSE

Dataset:
`GENESIS_UNSEEN_YAHOO_1H_2024_2026_V2`

### 8 unseen futures
ZW=F, KE=F, LE=F, HE=F, KC=F, SB=F, CT=F, OJ=F

Dataset:
`GENESIS_UNSEEN_YAHOO_1H_2024_2026_V2`

### 8 unseen crypto
SOLUSDT, BNBUSDT, XRPUSDT, ADAUSDT, DOGEUSDT, LINKUSDT, AVAXUSDT, LTCUSDT

Dataset:
`GENESIS_UNSEEN_BINANCE_1H_2024_2026_V2`

Total unseen assets: **40**.

## Known base mechanics that must remain invariant in any clean replay

- exact 3H aggregation from consecutive 1H bars
- reject broken 3H grouping on gaps / iid changes
- RC050 setup: setup range <= 0.50 ATR20
- LOWVOL: ATR20 <= median of the prior 60 completed 3H ATR20 values
- neutral OCO: long at setup high, short at setup low
- activation: next contiguous 3H bar
- both sides touched before unambiguous activation => skip
- stop: opposite setup extreme
- target: exact +3R from real fill
- same-bar stop/target ambiguity => stop first
- no breakeven
- no trailing
- no partial exits
- no time exits

The exact three VETO000 state bits are **not reconstructed yet** and must not be
invented. If the persisted payload/rule cannot be recovered, a replacement
state filter must be developed only on the allowed research/development sample
and frozen before any protected period is inspected.

## Verification gates

A result may be labeled VERIFIED only if the persisted/replayed evidence
demonstrates all required gates, including:

- long PF >= 1.20
- short PF >= 1.20
- each of 8 category x side cells PF >= 1.15
- minimum sample requirement per required cell
- all 3 temporal terciles positive
- long positive at 2x costs
- short positive at 2x costs
- protected-period compliance
- unseen-assets compliance

No gate may be lowered after observing results.
