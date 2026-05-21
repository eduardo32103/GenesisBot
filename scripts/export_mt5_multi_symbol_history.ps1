param(
    [string]$Symbols = "BTCUSD,ETHUSD,XAUUSD,NAS100,US500,EURUSD,GBPUSD",
    [string]$Timeframes = "M15,M30,H1",
    [int]$Bars = 20000,
    [string]$OutputDir = "data/backtests/multisymbol",
    [string]$Python = "python",
    [switch]$DiscoverAliases
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$exportScript = Join-Path $repoRoot "scripts/export_mt5_history.py"
$symbolDiscoveryScript = Join-Path $repoRoot "scripts/list_mt5_available_symbols.py"
$outputRoot = Join-Path $repoRoot $OutputDir
$aliasReportJson = Join-Path $outputRoot "mt5_symbol_alias_discovery.json"
$aliasReportCsv = Join-Path $outputRoot "mt5_symbol_alias_discovery.csv"
$exportReportCsv = Join-Path $outputRoot "mt5_multi_symbol_export_report.csv"

Write-Host "Genesis MT5 multi-symbol export: read-only rates only."
Write-Host "No orders. No credentials. No broker execution."
Write-Host "Symbols: $Symbols"
Write-Host "Timeframes: $Timeframes"
Write-Host "Bars: $Bars"
Write-Host "OutputDir: $outputRoot"

New-Item -ItemType Directory -Force -Path $outputRoot | Out-Null

$exported = @()
$skipped = @()
$discoveredAliases = @{}
$symbolList = $Symbols.Split(",") | ForEach-Object { $_.Trim().ToUpperInvariant() } | Where-Object { $_ }
$timeframeList = $Timeframes.Split(",") | ForEach-Object { $_.Trim().ToUpperInvariant() } | Where-Object { $_ }

if ($DiscoverAliases) {
    Write-Host ""
    Write-Host "Discovering MT5 aliases..."
    & $Python $symbolDiscoveryScript --symbols $Symbols --output-json $aliasReportJson --output-csv $aliasReportCsv
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Alias discovery failed; falling back to local alias patterns."
    } elseif (Test-Path $aliasReportJson) {
        $aliasReport = Get-Content -Path $aliasReportJson -Raw | ConvertFrom-Json
        foreach ($row in @($aliasReport.rows)) {
            if ($row.requested_symbol -and $row.resolved_symbol) {
                $discoveredAliases[$row.requested_symbol.ToUpperInvariant()] = [string]$row.resolved_symbol
            }
        }
    }
}

function Get-AliasCandidates {
    param([string]$RequestedSymbol)
    switch ($RequestedSymbol.ToUpperInvariant()) {
        "BTCUSD" { return @("BTCUSD", "BTCUSDm", "BTCUSD.r", "BTCUSD.b", "BTCUSD#", "BTCUSD.a", "BTCUSD.raw") }
        "ETHUSD" { return @("ETHUSD", "ETHUSDm", "ETHUSD.r", "ETHUSD.b", "ETHUSD#", "ETHUSD.a", "ETHUSD.raw") }
        "XAUUSD" { return @("XAUUSD", "XAUUSDm", "XAUUSD.r", "XAUUSD.b", "GOLD", "GOLDm", "GOLD.r", "GOLD.b", "XAUUSD#") }
        "NAS100" { return @("NAS100", "NAS100m", "NAS100.r", "NAS100.b", "US100", "US100m", "US100.b", "USTEC", "USTECm", "USTEC.b", "NASDAQ", "NASDAQm") }
        "US500" { return @("US500", "US500m", "US500.r", "US500.b", "SPX500", "SPX500m", "SPX500.b", "SP500", "SP500m", "USSPX500") }
        "EURUSD" { return @("EURUSD", "EURUSDm", "EURUSD.r", "EURUSD.b", "EURUSD#", "EURUSD.a", "EURUSD.raw") }
        "GBPUSD" { return @("GBPUSD", "GBPUSDm", "GBPUSD.r", "GBPUSD.b", "GBPUSD#", "GBPUSD.a", "GBPUSD.raw") }
        default { return @($RequestedSymbol) }
    }
}

foreach ($symbol in $symbolList) {
    foreach ($timeframe in $timeframeList) {
        $exportedThisPair = $false
        $lastReason = ""
        $resolved = ""
        $candidates = @(Get-AliasCandidates -RequestedSymbol $symbol)
        if ($discoveredAliases.ContainsKey($symbol)) {
            $candidates = @($discoveredAliases[$symbol]) + $candidates
        }
        foreach ($candidate in ($candidates | Select-Object -Unique)) {
            $output = Join-Path $outputRoot "$($symbol)_$($timeframe)_$($Bars).csv"
            $resolvedOutput = Join-Path $outputRoot "$($candidate)_$($timeframe)_$($Bars).csv"
            if ($candidate -ne $symbol) {
                $output = $resolvedOutput
            }
            Write-Host ""
            Write-Host "Exporting requested=$symbol resolved=$candidate $timeframe -> $output"
            & $Python $exportScript --symbol $candidate --timeframe $timeframe --bars $Bars --output $output
            if ($LASTEXITCODE -eq 0 -and (Test-Path $output)) {
                $resolved = $candidate
                $exportedThisPair = $true
                $barsExported = [Math]::Max(0, ((Get-Content -Path $output | Measure-Object -Line).Lines - 1))
                $exported += [pscustomobject]@{
                    RequestedSymbol = $symbol
                    ResolvedSymbol = $candidate
                    Timeframe = $timeframe
                    Status = "exported"
                    Reason = ""
                    CsvPath = $output
                    BarsRequested = $Bars
                    BarsExported = $barsExported
                }
                break
            }
            $lastReason = "candidate_failed:$candidate"
        }
        if ($exportedThisPair) {
            continue
        }
        $skipped += [pscustomobject]@{
            RequestedSymbol = $symbol
            ResolvedSymbol = $resolved
            Timeframe = $timeframe
            Status = "skipped"
            Reason = if ($lastReason) { $lastReason } else { "symbol_unavailable_or_no_rates" }
            CsvPath = ""
            BarsRequested = $Bars
            BarsExported = 0
        }
        Write-Warning "Skipped requested=$symbol $timeframe; no alias returned usable rates."
        <#
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
        #>
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

@($exported + $skipped) | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $exportReportCsv
Write-Host "Export report: $exportReportCsv"

exit 0
