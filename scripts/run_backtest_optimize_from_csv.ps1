param(
    [string]$CsvPath = "data/backtests/BTCUSD_H1.csv",
    [string]$Symbol = "BTCUSD",
    [string]$Timeframe = "H1",
    [string]$BaseUrl = "https://genesisbot-production.up.railway.app",
    [double]$InitialBalance = 100000,
    [double]$SpreadPoints = 30,
    [double]$SlippagePoints = 5,
    [double]$Commission = 0,
    [string[]]$Profiles = @("baseline", "quality_v2", "quality_loose", "quality_strict", "momentum_v1", "trend_v1", "anti_chop_v1", "rsi_reversal_safe"),
    [switch]$RollingWindows
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
    profiles        = $Profiles
    walk_forward    = $true
    rolling_windows = [bool]$RollingWindows
    initial_balance = $InitialBalance
    spread_points   = $SpreadPoints
    slippage_points = $SlippagePoints
    commission      = $Commission
    mode            = "paper"
    save_results    = $true
}

$url = $BaseUrl.TrimEnd("/") + "/api/genesis/mt5/backtest/optimize"
$json = $body | ConvertTo-Json -Depth 12

Write-Host "Posting paper-only optimizer to $url"
$response = Invoke-RestMethod -Method Post -Uri $url -ContentType "application/json" -Body $json -TimeoutSec 180

$response.table | Select-Object `
    profile,
    trades,
    win_rate,
    profit_factor,
    expectancy,
    max_drawdown,
    test_profit_factor,
    test_expectancy,
    test_max_drawdown,
    robustness_score,
    promoted | Format-Table -AutoSize

[pscustomobject]@{
    status         = $response.status
    symbol         = $response.symbol
    timeframe      = $response.timeframe
    best_profile   = $response.best_profile
    promoted       = ($response.promoted_profiles -join ",")
    broker_touched = $response.broker_touched
    order_executed = $response.order_executed
    duration_ms    = $response.duration_ms
} | Format-List
