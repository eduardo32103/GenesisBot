param(
    [string]$CsvPath = "data/backtests/BTCUSD_M30_5000.csv",
    [string]$Symbol = "BTCUSD",
    [string]$Timeframe = "M30",
    [string]$Profile = "quality_loose",
    [string]$BaseUrl = "https://genesisbot-production.up.railway.app",
    [double]$InitialBalance = 100000,
    [double]$SpreadPoints = 30,
    [double]$SlippagePoints = 5,
    [double]$Commission = 0,
    [int]$MaxBars = 5000,
    [int]$TimeoutSec = 60,
    [int[]]$Checkpoints = @(10, 25, 50, 100),
    [switch]$Persist
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $CsvPath)) {
    throw "CSV not found: $CsvPath. Export MT5 history first."
}

$csvText = Get-Content -LiteralPath $CsvPath -Raw
if ([string]::IsNullOrWhiteSpace($csvText)) {
    throw "CSV is empty: $CsvPath"
}
$csvInfo = Get-Item -LiteralPath $CsvPath
$csvLines = (Get-Content -LiteralPath $CsvPath | Measure-Object -Line).Lines

$body = [ordered]@{
    symbol          = $Symbol
    timeframe       = $Timeframe
    profile         = $Profile
    csv_text        = $csvText
    initial_balance = $InitialBalance
    spread_points   = $SpreadPoints
    slippage_points = $SlippagePoints
    commission      = $Commission
    max_bars        = $MaxBars
    checkpoints     = $Checkpoints
    persist         = [bool]$Persist
}

$url = $BaseUrl.TrimEnd("/") + "/api/genesis/mt5/forward-replay/run"
$json = $body | ConvertTo-Json -Depth 12

Write-Host "Posting isolated paper-forward replay"
Write-Host "Endpoint      : $url"
Write-Host "CSV path      : $($csvInfo.FullName)"
Write-Host "CSV size bytes: $($csvInfo.Length)"
Write-Host "CSV lines     : $csvLines"
Write-Host "Max bars      : $MaxBars"
Write-Host "Timeout sec   : $TimeoutSec"

try {
    $response = Invoke-RestMethod -Method Post -Uri $url -ContentType "application/json" -Body $json -TimeoutSec $TimeoutSec -ErrorAction Stop
}
catch {
    $message = $_.Exception.Message
    Write-Error "Forward replay request failed or timed out after $TimeoutSec seconds. Endpoint=$url CSV=$($csvInfo.FullName). Error=$message"
    exit 1
}

[pscustomobject]@{
    profile            = $response.profile
    timeframe          = $response.timeframe
    bars_loaded        = $response.bars_loaded
    closed             = $response.closed
    wins               = $response.wins
    losses             = $response.losses
    win_rate           = $response.win_rate
    profit_factor      = $response.profit_factor
    expectancy         = $response.expectancy
    max_drawdown       = $response.max_drawdown
    degraded           = $response.degraded
    degradation_reason = $response.degradation_reason
    broker_touched     = $response.broker_touched
    order_executed     = $response.order_executed
} | Format-List

if ($response.checkpoints) {
    Write-Host ""
    Write-Host "Checkpoints"
    $response.checkpoints |
        Select-Object checkpoint, reached, closed, wins, losses, win_rate, profit_factor, expectancy, max_drawdown, degraded, degradation_reason |
        Format-Table -AutoSize
}
