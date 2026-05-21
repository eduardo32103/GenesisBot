param(
    [string]$Symbol = "BTCUSD",
    [string]$OutputDir = "data/backtests",
    [int]$MaxRuntimeSeconds = 600,
    [int]$MaxEvaluations = 240
)

$ErrorActionPreference = "Stop"

$m30 = Join-Path $OutputDir "${Symbol}_M30_20000.csv"
$h1 = Join-Path $OutputDir "${Symbol}_H1_10000.csv"
$m15 = Join-Path $OutputDir "${Symbol}_M15_20000.csv"

Write-Host "Genesis extended sample validation (paper-only)."
Write-Host "No promoted profile mutation. No forward-state mutation. No broker."

foreach ($path in @($m30, $h1, $m15)) {
    if (-not (Test-Path $path)) {
        Write-Host "Missing CSV: $path"
        Write-Host "Run first: powershell -ExecutionPolicy Bypass -File scripts/export_mt5_extended_history.ps1"
        exit 2
    }
}

python scripts/run_capital_preservation_optimizer_from_csv.py `
    --symbol $Symbol `
    --timeframes M30,H1 `
    --profiles capital_preservation_v4_side_filtered,trend_continuation_v5_defense_aware,low_drawdown_v5_session_filtered,liquidity_sweep_v3_session_confirmed `
    --csv-path-m30 $m30 `
    --csv-path-h1 $h1 `
    --max-bars 20000 `
    --max-evaluations $MaxEvaluations `
    --risk-reward-values 0.8,1.0,1.2,1.5 `
    --time-stop-bars 1,2,3,4,6 `
    --score-min-values 50,55,60,65 `
    --spread-max-values 20,25,30 `
    --per-evaluation-timeout-seconds 4 `
    --max-runtime-seconds $MaxRuntimeSeconds `
    --progress-every 10

Write-Host ""
Write-Host "Done. broker_touched=false order_executed=false order_policy=journal_only_no_broker"
