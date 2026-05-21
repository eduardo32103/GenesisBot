param(
    [string]$Symbol = "BTCUSD",
    [string]$OutputDir = "data/backtests"
)

$ErrorActionPreference = "Stop"

Write-Host "Genesis MT5 H1 deep history export (read-only)."
Write-Host "No orders, no broker execution, no credentials are used by this script."

$jobs = @(
    @{ Bars = 25000; Output = Join-Path $OutputDir "${Symbol}_H1_25000.csv" },
    @{ Bars = 30000; Output = Join-Path $OutputDir "${Symbol}_H1_30000.csv" }
)

foreach ($job in $jobs) {
    Write-Host ""
    Write-Host "Exporting $Symbol H1 bars=$($job.Bars) -> $($job.Output)"
    python scripts/export_mt5_history.py --symbol $Symbol --timeframe H1 --bars $job.Bars --output $job.Output
}

Write-Host ""
Write-Host "Done. broker_touched=false order_executed=false order_policy=journal_only_no_broker"
