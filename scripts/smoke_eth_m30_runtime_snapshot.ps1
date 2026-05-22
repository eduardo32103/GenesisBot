param(
    [string]$BaseUrl = "http://127.0.0.1:8020",
    [int]$TimeoutSec = 20,
    [switch]$SkipTickPost
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
    return Invoke-RestMethod -Uri $endpoint -Method Post -Body ($Body | ConvertTo-Json -Depth 8) -ContentType "application/json" -TimeoutSec $TimeoutSec -ErrorAction Stop
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
Write-Host "SkipTickPost=$SkipTickPost"

$health = Invoke-GenesisGet "/api/genesis/mt5/health"
Assert-PaperSafety "health" $health

if (-not $SkipTickPost) {
    $tickBody = @{
        symbol = "ETHUSD"
        bid = 3200.10
        ask = 3200.40
        last = 3200.25
        spread = 0.30
        timeframe = "M30"
        source = "smoke_eth_m30_runtime_snapshot"
        broker_touched = $false
        order_executed = $false
        order_policy = "journal_only_no_broker"
    }
    $tick = Invoke-GenesisPost "/api/genesis/mt5/tick" $tickBody
    Assert-PaperSafety "tick" $tick
} else {
    $tick = [pscustomobject]@{ status = "tick_post_skipped"; broker_touched = $false; order_executed = $false; order_policy = "journal_only_no_broker" }
}

$forward = Invoke-GenesisGet "/api/genesis/mt5/forward-profile-state?symbol=ETHUSD&timeframe=M30"
$risk = Invoke-GenesisGet "/api/genesis/mt5/risk-state?symbol=ETHUSD&timeframe=M30"
$decision = Invoke-GenesisGet "/api/genesis/mt5/decision?symbol=ETHUSD&timeframe=M30"
$open = Invoke-GenesisGet "/api/genesis/mt5/shadow-trades/open?symbol=ETHUSD"

Assert-PaperSafety "forward-profile-state" $forward
Assert-PaperSafety "risk-state" $risk
Assert-PaperSafety "decision" $decision
Assert-PaperSafety "shadow-trades/open" $open

[pscustomobject]@{
    HealthStatus = $health.status
    TickStatus = $tick.status
    ForwardStatus = $forward.status
    ForwardProfile = $forward.profile
    ForwardActive = $forward.active
    ForwardReason = $forward.reason
    RuntimeSnapshotAvailable = $forward.runtime_snapshot_available
    RuntimeSnapshotRecent = $forward.runtime_snapshot_recent
    RuntimeSnapshotComplete = $forward.runtime_snapshot_complete
    RuntimeSnapshotContext = $forward.runtime_snapshot_context
    RiskState = $risk.risk_state
    RiskAllowed = $risk.allowed
    Decision = $decision.decision
    DecisionReason = $decision.reason
    DecisionRuntimeSnapshotAvailable = $decision.runtime_snapshot_available
    DecisionRuntimeSnapshotRecent = $decision.runtime_snapshot_recent
    PaperForwardCandidateProfile = $decision.paper_forward_candidate_profile
    OpenShadowTrades = $open.open_count
    BrokerTouched = $decision.broker_touched
    OrderExecuted = $decision.order_executed
    OrderPolicy = $decision.order_policy
} | Format-List
