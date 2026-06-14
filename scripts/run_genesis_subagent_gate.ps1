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

function Test-SafeForbiddenMention {
    param([Parameter(Mandatory = $true)][string]$Line)

    $lower = $Line.ToLowerInvariant()
    $safeFragments = @(
        "forbidden",
        "no order_send",
        "notin(`"order_send`"",
        "assertnotin(`"order_send`"",
        "never touches broker execution",
        "no broker execution",
        "no martingale",
        "no grid",
        "no averaging down",
        "no increasing lot after loss",
        "no size increase after loss",
        "journal_only_no_broker",
        "martingale_enabled`": false",
        "grid_enabled`": false",
        "averaging_down_enabled`": false",
        "self.assertfalse(result[`"martingale_enabled`"])",
        "self.assertfalse(result[`"averaging_down_enabled`"])",
        "martingale_or_loss_scaling_blocked",
        "def _martingale_detected",
        "$terms = @("
    )

    foreach ($fragment in $safeFragments) {
        if ($lower.Contains($fragment)) {
            return $true
        }
    }
    return $false
}

function Invoke-ForbiddenActivationScan {
    $runtimeRoots = @("api", "core", "integrations", "mt5", "services", "workers", "scripts")
    $extensions = @(".py", ".js", ".jsx", ".ts", ".tsx", ".ps1", ".yml", ".yaml")
    $forbiddenPatterns = @(
        @{ ForbiddenTerm = "order_send"; Pattern = "order_send" },
        @{ ForbiddenTerm = "live_trading_enabled=true"; Pattern = "live_trading_enabled\s*[:=]\s*true" },
        @{ ForbiddenTerm = "live trading enabled"; Pattern = "live trading enabled" },
        @{ ForbiddenTerm = "broker execution"; Pattern = "broker execution" },
        @{ ForbiddenTerm = "martingale"; Pattern = "martingale" },
        @{ ForbiddenTerm = "grid_strategy"; Pattern = "grid_strategy" },
        @{ ForbiddenTerm = "averaging_down"; Pattern = "averaging_down" },
        @{ ForbiddenTerm = "averaging down"; Pattern = "averaging down" },
        @{ ForbiddenTerm = "increase lot after loss"; Pattern = "increase lot after loss" },
        @{ ForbiddenTerm = "increase_lot_after_loss"; Pattern = "increase_lot_after_loss" }
    )
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
            $name = $_.Name.ToLowerInvariant()
            if ($name -in @("run_genesis_agent_gate.ps1", "run_genesis_subagent_gate.ps1")) {
                return
            }
            $relative = Resolve-Path -Relative $path
            $lineNumber = 0
            foreach ($line in Get-Content -LiteralPath $path) {
                $lineNumber += 1
                $lower = $line.ToLowerInvariant()
                $matched = $false
                foreach ($item in $forbiddenPatterns) {
                    if ($lower -match $item.Pattern) {
                        $matched = $true
                        break
                    }
                }
                if (-not $matched) {
                    continue
                }
                if (Test-SafeForbiddenMention -Line $line) {
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
        throw "Forbidden activation scan failed"
    }
    Write-Host "forbidden_activation_scan=pass"
}

Invoke-GateStep -Name "Subagent mission control tests" -Command {
    python -m unittest tests.unit.test_mt5_agent_mission_control
}
Invoke-GateStep -Name "Subagent safety contract tests" -Command {
    python -m unittest tests.unit.test_genesis_subagent_safety_contract
}
Invoke-GateStep -Name "Persistent Intelligence tests" -Command {
    python -m unittest tests.unit.test_mt5_persistent_intelligence_store
}
Invoke-GateStep -Name "Paper observation readiness tests" -Command {
    python -m unittest tests.unit.test_mt5_xau_m15_paper_observation_readiness
}
Invoke-GateStep -Name "Shadow lifecycle tests" -Command {
    python -m unittest tests.unit.test_mt5_xau_m15_paper_shadow_monitor
}
Invoke-GateStep -Name "Strategy Tournament tests" -Command {
    python -m unittest tests.unit.test_mt5_strategy_tournament
}
Invoke-GateStep -Name "Capital Protection tests" -Command {
    python -m unittest tests.unit.test_mt5_capital_protection_governor
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
    Invoke-ForbiddenActivationScan
}

Write-Host ""
Write-Host "genesis_subagent_gate=pass"
Write-Host "broker_touched=false"
Write-Host "order_executed=false"
Write-Host "order_policy=journal_only_no_broker"
