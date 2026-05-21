param(
    [string]$Symbols = "BTCUSD,ETHUSD,XAUUSD,NAS100,US500,EURUSD,GBPUSD",
    [string]$Timeframes = "M15,M30,H1",
    [int]$Bars = 20000,
    [string]$OutputDir = "data/backtests/multisymbol",
    [string]$Python = "python"
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$exportScript = Join-Path $repoRoot "scripts/export_mt5_history.py"
$outputRoot = Join-Path $repoRoot $OutputDir

Write-Host "Genesis MT5 multi-symbol export: read-only rates only."
Write-Host "No orders. No credentials. No broker execution."
Write-Host "Symbols: $Symbols"
Write-Host "Timeframes: $Timeframes"
Write-Host "Bars: $Bars"
Write-Host "OutputDir: $outputRoot"

New-Item -ItemType Directory -Force -Path $outputRoot | Out-Null

$exported = @()
$skipped = @()
$symbolList = $Symbols.Split(",") | ForEach-Object { $_.Trim().ToUpperInvariant() } | Where-Object { $_ }
$timeframeList = $Timeframes.Split(",") | ForEach-Object { $_.Trim().ToUpperInvariant() } | Where-Object { $_ }

foreach ($symbol in $symbolList) {
    foreach ($timeframe in $timeframeList) {
        $output = Join-Path $outputRoot "$($symbol)_$($timeframe)_$($Bars).csv"
        Write-Host ""
        Write-Host "Exporting $symbol $timeframe -> $output"
        & $Python $exportScript --symbol $symbol --timeframe $timeframe --bars $Bars --output $output
        if ($LASTEXITCODE -eq 0 -and (Test-Path $output)) {
            $exported += [pscustomobject]@{
                Symbol = $symbol
                Timeframe = $timeframe
                Output = $output
            }
        } else {
            $skipped += [pscustomobject]@{
                Symbol = $symbol
                Timeframe = $timeframe
                Reason = "export_failed_or_symbol_unavailable"
            }
            Write-Warning "Skipped $symbol $timeframe; MT5 did not return usable rates."
        }
    }
}

Write-Host ""
Write-Host "Export complete."
Write-Host "Exported: $($exported.Count)"
Write-Host "Skipped: $($skipped.Count)"
if ($exported.Count -gt 0) {
    $exported | Format-Table -AutoSize
}
if ($skipped.Count -gt 0) {
    Write-Host "Skipped symbols/timeframes:"
    $skipped | Format-Table -AutoSize
}

exit 0
