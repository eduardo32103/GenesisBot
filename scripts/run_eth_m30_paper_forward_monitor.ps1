param(
    [string]$BaseUrl = "https://genesisbot-production.up.railway.app",
    [string]$Symbol = "ETHUSD",
    [string]$Timeframe = "M30",
    [int]$IntervalSec = 300,
    [int]$Iterations = 12,
    [string]$OutputDir = "data/backtests/multisymbol",
    [int]$TimeoutSec = 20
)

$ErrorActionPreference = "Stop"

Write-Host "ETH M30 paper-forward monitor"
Write-Host "BaseUrl=$BaseUrl"
Write-Host "Symbol=$Symbol"
Write-Host "Timeframe=$Timeframe"
Write-Host "IntervalSec=$IntervalSec"
Write-Host "Iterations=$Iterations"
Write-Host "OutputDir=$OutputDir"
Write-Host "TimeoutSec=$TimeoutSec"

python -m services.mt5.mt5_eth_m30_paper_forward_monitor `
    --base-url $BaseUrl `
    --symbol $Symbol `
    --timeframe $Timeframe `
    --interval-sec $IntervalSec `
    --iterations $Iterations `
    --output-dir $OutputDir `
    --timeout-sec $TimeoutSec
