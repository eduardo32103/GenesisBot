param(
    [string]$BaseUrl = "https://genesisbot-production.up.railway.app",
    [string]$Symbol = "ETHUSD",
    [string]$Timeframe = "M30",
    [string]$OutputDir = "data/backtests/multisymbol",
    [int]$TimeoutSec = 20
)

$ErrorActionPreference = "Stop"

Write-Host "Smoke ETH M30 paper-forward monitor"
Write-Host "BaseUrl=$BaseUrl"
Write-Host "Symbol=$Symbol"
Write-Host "Timeframe=$Timeframe"

powershell -ExecutionPolicy Bypass -File scripts/run_eth_m30_paper_forward_monitor.ps1 `
    -BaseUrl $BaseUrl `
    -Symbol $Symbol `
    -Timeframe $Timeframe `
    -IntervalSec 0 `
    -Iterations 1 `
    -OutputDir $OutputDir `
    -TimeoutSec $TimeoutSec
