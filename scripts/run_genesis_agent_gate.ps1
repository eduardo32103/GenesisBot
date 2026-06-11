param()

$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $RepoRoot

function Invoke-GateStep {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][scriptblock]$Command
    )

    Write-Host ""
    Write-Host "== $Name =="
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "Gate step failed: $Name"
    }
}

function Test-AllowedForbiddenMention {
    param([Parameter(Mandatory = $true)][string]$Line)

    $lower = $Line.ToLowerInvariant()
    $allowedFragments = @(
        "forbidden",
        "no order_send",
        "notin(`"order_send`"",
        "assertnotin(`"order_send`"",
        "martingale_enabled`": false",
        "averaging_down_enabled`": false",
        "self.assertfalse(result[`"martingale_enabled`"])",
        "self.assertfalse(result[`"averaging_down_enabled`"])",
        "martingale_or_loss_scaling_blocked",
        "def _martingale_detected",
        "no martingale",
        "no grid",
        "no averaging down",
        "no increasing lot after loss",
        "journal_only_no_broker"
    )

    foreach ($fragment in $allowedFragments) {
        if ($lower.Contains($fragment)) {
            return $true
        }
    }

    if ($lower -match "martingale[_a-z0-9]*\s*=\s*false") {
        return $true
    }
    if ($lower -match "averaging_down[_a-z0-9]*\s*=\s*false") {
        return $true
    }
    if ($lower -match "live_trading_enabled\s*=\s*false") {
        return $true
    }
    return $false
}

function Invoke-ForbiddenTextScan {
    $runtimeRoots = @("api", "app", "core", "integrations", "mt5", "services", "workers", "scripts")
    $extensions = @(".py", ".js", ".jsx", ".ts", ".tsx", ".ps1", ".yml", ".yaml")
    $terms = @("order_send", "martingale", "averaging_down", "grid_strategy", "live_trading_enabled=true", "increase_lot_after_loss")
    $suspicious = New-Object System.Collections.Generic.List[string]
    $safeMentions = 0

    foreach ($root in $runtimeRoots) {
        if (-not (Test-Path $root)) {
            continue
        }
        Get-ChildItem -Path $root -Recurse -File | Where-Object {
            $extensions -contains $_.Extension.ToLowerInvariant()
        } | ForEach-Object {
            $path = $_.FullName
            if ($path -eq $PSCommandPath) {
                continue
            }
            $relative = Resolve-Path -Relative $path
            $lineNumber = 0
            foreach ($line in Get-Content -LiteralPath $path) {
                $lineNumber += 1
                $lower = $line.ToLowerInvariant()
                $matched = $false
                foreach ($term in $terms) {
                    if ($lower.Contains($term)) {
                        $matched = $true
                        break
                    }
                }
                if (-not $matched) {
                    continue
                }
                if (Test-AllowedForbiddenMention -Line $line) {
                    $safeMentions += 1
                    continue
                }
                $suspicious.Add("${relative}:${lineNumber}: $line")
            }
        }
    }

    Write-Host "safe_forbidden_mentions=$safeMentions"
    if ($suspicious.Count -gt 0) {
        Write-Host "Suspicious forbidden text found:"
        foreach ($item in $suspicious) {
            Write-Host $item
        }
        throw "Forbidden text scan failed"
    }
    Write-Host "forbidden_text_scan=pass"
}

Invoke-GateStep -Name "Persistent Intelligence tests" -Command {
    python -m unittest tests.unit.test_mt5_persistent_intelligence_store
}
Invoke-GateStep -Name "Persistent DB Doctor tests" -Command {
    python -m unittest tests.unit.test_mt5_persistent_db_doctor
}
Invoke-GateStep -Name "Autonomous Learning Orchestrator tests" -Command {
    python -m unittest tests.unit.test_mt5_autonomous_learning_orchestrator
}
Invoke-GateStep -Name "Capital Protection tests" -Command {
    python -m unittest tests.unit.test_mt5_capital_protection_governor
}
Invoke-GateStep -Name "Strategy Tournament tests" -Command {
    python -m unittest tests.unit.test_mt5_strategy_tournament
}
Invoke-GateStep -Name "Adaptive Strategy Governor tests" -Command {
    python -m unittest tests.unit.test_mt5_adaptive_strategy_governor
}
Invoke-GateStep -Name "Adaptive Strategy Governor enforcement tests" -Command {
    python -m unittest tests.unit.test_mt5_adaptive_strategy_governor_enforcement
}
Invoke-GateStep -Name "Shadow Trade Hygiene tests" -Command {
    python -m unittest tests.unit.test_mt5_shadow_trade_hygiene
}
Invoke-GateStep -Name "Risk Recovery tests" -Command {
    python -m unittest tests.unit.test_mt5_risk_recovery
}
Invoke-GateStep -Name "Dashboard syntax" -Command {
    node --check app/dashboard/app.js
}
Invoke-GateStep -Name "Whitespace diff check" -Command {
    git diff --check
}
Invoke-GateStep -Name "Forbidden activation scan" -Command {
    Invoke-ForbiddenTextScan
}

Write-Host ""
Write-Host "genesis_agent_gate=pass"
Write-Host "broker_touched=false"
Write-Host "order_executed=false"
Write-Host "order_policy=journal_only_no_broker"
