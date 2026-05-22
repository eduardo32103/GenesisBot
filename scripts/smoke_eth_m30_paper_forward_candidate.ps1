param(
    [string]$BaseUrl = "https://genesisbot-production.up.railway.app",
    [int]$TimeoutSec = 20
)

$ErrorActionPreference = "Stop"

function Invoke-GenesisGet {
    param([string]$Path)
    $endpoint = "$BaseUrl$Path"
    Write-Host "GET $endpoint"
    return Invoke-RestMethod -Uri $endpoint -Method Get -TimeoutSec $TimeoutSec -ErrorAction Stop
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

$health = Invoke-GenesisGet "/api/genesis/mt5/health"
$forward = Invoke-GenesisGet "/api/genesis/mt5/forward-profile-state?symbol=ETHUSD&timeframe=M30"
$risk = Invoke-GenesisGet "/api/genesis/mt5/risk-state?symbol=ETHUSD&timeframe=M30"
$decision = Invoke-GenesisGet "/api/genesis/mt5/decision?symbol=ETHUSD&timeframe=M30"
$open = Invoke-GenesisGet "/api/genesis/mt5/shadow-trades/open?symbol=ETHUSD"

Assert-PaperSafety "health" $health
Assert-PaperSafety "forward-profile-state" $forward
Assert-PaperSafety "risk-state" $risk
Assert-PaperSafety "decision" $decision
Assert-PaperSafety "shadow-trades/open" $open

[pscustomobject]@{
    HealthStatus = $health.status
    ForwardStatus = $forward.status
    ForwardProfile = $forward.profile
    ForwardActive = $forward.active
    AppliesToPaperShadow = $forward.applies_to_paper_shadow
    AppliesToRealTrading = $forward.applies_to_real_trading
    RiskState = $risk.risk_state
    RiskAllowed = $risk.allowed
    Decision = $decision.decision
    DecisionReason = $decision.reason
    PaperForwardCandidateProfile = $decision.paper_forward_candidate_profile
    OpenShadowTrades = $open.open_count
    BrokerTouched = $decision.broker_touched
    OrderExecuted = $decision.order_executed
    OrderPolicy = $decision.order_policy
} | Format-List
