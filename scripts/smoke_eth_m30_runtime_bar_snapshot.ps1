param(
    [string]$BaseUrl = "http://127.0.0.1:8020",
    [int]$TimeoutSec = 20,
    [int]$Bars = 100
)

$ErrorActionPreference = "Stop"

function Invoke-GenesisGet {
    param([string]$Path)
    $endpoint = "$BaseUrl$Path"
    Write-Host "GET $endpoint"
    return Invoke-RestMethod -Uri $endpoint -Method Get -TimeoutSec $TimeoutSec -ErrorAction Stop
}

function Invoke-GenesisPost {
    param(
        [string]$Path,
        [object]$Body
    )
    $endpoint = "$BaseUrl$Path"
    Write-Host "POST $endpoint"
    return Invoke-RestMethod -Uri $endpoint -Method Post -Body ($Body | ConvertTo-Json -Depth 12) -ContentType "application/json" -TimeoutSec $TimeoutSec -ErrorAction Stop
}

function Assert-PaperSafety {
    param(
        [string]$Name,
        [object]$Payload
    )
    if ($Payload.broker_touched -ne $false) {
        throw "$Name broker_touched was not false"
    }
    if ($Payload.order_executed -ne $false) {
        throw "$Name order_executed was not false"
    }
    if ($Payload.order_policy -ne "journal_only_no_broker") {
        throw "$Name order_policy was not journal_only_no_broker"
    }
}

Write-Host "BaseUrl=$BaseUrl"
Write-Host "TimeoutSec=$TimeoutSec"
Write-Host "Bars=$Bars"

$health = Invoke-GenesisGet "/api/genesis/mt5/health"
Assert-PaperSafety "health" $health

$rows = @()
$start = [datetime]::Parse("2026-05-01T00:00:00Z")
for ($i = 0; $i -lt $Bars; $i++) {
    $price = 3200.0 + (($i % 6) * 0.15)
    $rows += [pscustomobject]@{
        time = $start.AddMinutes(30 * $i).ToString("o")
        open = [math]::Round($price, 4)
        high = [math]::Round($price + 1.0, 4)
        low = [math]::Round($price - 1.0, 4)
        close = [math]::Round($price + 0.05, 4)
        volume = 10 + $i
    }
}

$barsPayload = @{
    symbol = "ETHUSD"
    timeframe = "M30"
    bars_data = $rows
    bid = 3200.10
    ask = 3200.40
    last = 3200.25
    spread = 0.30
    source = "smoke_eth_m30_runtime_bar_snapshot"
    broker_touched = $false
    order_executed = $false
    order_policy = "journal_only_no_broker"
}

$barsIngest = Invoke-GenesisPost "/api/genesis/mt5/bars" $barsPayload
$forward = Invoke-GenesisGet "/api/genesis/mt5/forward-profile-state?symbol=ETHUSD&timeframe=M30"
$risk = Invoke-GenesisGet "/api/genesis/mt5/risk-state?symbol=ETHUSD&timeframe=M30"
$decision = Invoke-GenesisGet "/api/genesis/mt5/decision?symbol=ETHUSD&timeframe=M30"
$open = Invoke-GenesisGet "/api/genesis/mt5/shadow-trades/open?symbol=ETHUSD"

Assert-PaperSafety "bars" $barsIngest
Assert-PaperSafety "forward-profile-state" $forward
Assert-PaperSafety "risk-state" $risk
Assert-PaperSafety "decision" $decision
Assert-PaperSafety "shadow-trades/open" $open

[pscustomobject]@{
    HealthStatus = $health.status
    BarsStatus = $barsIngest.status
    BarsLoaded = $barsIngest.bars_loaded
    RuntimeSnapshotAvailable = $barsIngest.runtime_snapshot_available
    RuntimeSnapshotRecent = $barsIngest.runtime_snapshot_recent
    RuntimeSnapshotComplete = $barsIngest.runtime_snapshot_complete
    RuntimeSnapshotContext = $barsIngest.runtime_snapshot_context
    TrendScore = $barsIngest.trend_score
    MomentumScore = $barsIngest.momentum_score
    VolatilityScore = $barsIngest.volatility_score
    MarketRegime = $barsIngest.market_regime
    ForwardStatus = $forward.status
    ForwardProfile = $forward.profile
    ForwardActive = $forward.active
    ForwardReason = $forward.reason
    ForwardAppliesToPaperShadow = $forward.applies_to_paper_shadow
    ForwardAppliesToRealTrading = $forward.applies_to_real_trading
    Decision = $decision.decision
    DecisionReason = $decision.reason
    DecisionRuntimeSnapshotComplete = $decision.runtime_snapshot_complete
    RiskState = $risk.risk_state
    RiskAllowed = $risk.allowed
    OpenShadowTrades = $open.open_count
    BrokerTouched = $decision.broker_touched
    OrderExecuted = $decision.order_executed
    OrderPolicy = $decision.order_policy
} | Format-List
