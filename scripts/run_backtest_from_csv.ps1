param(
    [string]$CsvPath = "data/backtests/BTCUSD_H1.csv",
    [string]$Symbol = "BTCUSD",
    [string]$Timeframe = "H1",
    [string]$BaseUrl = "https://genesisbot-production.up.railway.app",
    [double]$InitialBalance = 100000,
    [double]$SpreadPoints = 30,
    [double]$SlippagePoints = 5,
    [double]$Commission = 0,
    [string]$FilterProfile = "quality_v2"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $CsvPath)) {
    throw "CSV not found: $CsvPath. Run scripts/export_mt5_history.py first."
}

$rows = Import-Csv -LiteralPath $CsvPath
if (-not $rows -or $rows.Count -eq 0) {
    throw "CSV is empty: $CsvPath"
}

$bars = foreach ($row in $rows) {
    [pscustomobject]@{
        time   = [string]$row.time
        open   = [double]$row.open
        high   = [double]$row.high
        low    = [double]$row.low
        close  = [double]$row.close
        volume = if ($row.volume -ne $null -and $row.volume -ne "") { [double]$row.volume } else { 0 }
    }
}

$body = [ordered]@{
    symbol          = $Symbol
    timeframe       = $Timeframe
    source          = "mt5_csv"
    bars_data       = $bars
    initial_balance = $InitialBalance
    spread_points   = $SpreadPoints
    slippage_points = $SlippagePoints
    commission      = $Commission
    filter_profile  = $FilterProfile
    mode            = "paper"
    save_results    = $true
}

$url = $BaseUrl.TrimEnd("/") + "/api/genesis/mt5/backtest/run"
$json = $body | ConvertTo-Json -Depth 12

Write-Host "Posting paper-only backtest to $url"
$response = Invoke-RestMethod -Method Post -Uri $url -ContentType "application/json" -Body $json -TimeoutSec 120

[pscustomobject]@{
    status         = $response.status
    symbol         = $response.symbol
    timeframe      = $response.timeframe
    filter_profile = $response.filter_profile
    total_trades   = $response.total_trades
    closed         = $response.closed
    wins           = $response.wins
    losses         = $response.losses
    win_rate       = $response.win_rate
    profit_factor  = $response.profit_factor
    expectancy     = $response.expectancy
    max_drawdown   = $response.max_drawdown
    baseline_pf    = $response.filter_comparison.baseline_pf
    quality_v2_pf  = $response.filter_comparison.quality_v2_pf
    broker_touched = $response.broker_touched
    order_executed = $response.order_executed
} | Format-List
