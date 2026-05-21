param(
    [string]$Symbol = "BTCUSD",
    [string]$OutputDir = "data/backtests"
)

$ErrorActionPreference = "Stop"

Write-Host "Genesis MT5 extended history export (read-only)."
Write-Host "No orders, no broker execution, no credentials are used by this script."

$jobs = @(
    @{ Timeframe = "M30"; Bars = 20000; Output = Join-Path $OutputDir "${Symbol}_M30_20000.csv" },
    @{ Timeframe = "H1";  Bars = 10000; Output = Join-Path $OutputDir "${Symbol}_H1_10000.csv" },
    @{ Timeframe = "M15"; Bars = 20000; Output = Join-Path $OutputDir "${Symbol}_M15_20000.csv" }
)

foreach ($job in $jobs) {
    Write-Host ""
    Write-Host "Exporting $Symbol $($job.Timeframe) bars=$($job.Bars) -> $($job.Output)"
    python scripts/export_mt5_history.py --symbol $Symbol --timeframe $job.Timeframe --bars $job.Bars --output $job.Output
}

Write-Host ""
Write-Host "Done. broker_touched=false order_executed=false order_policy=journal_only_no_broker"
