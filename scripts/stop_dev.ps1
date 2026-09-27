# stop_dev.ps1 — Stop all fraud detection services, processes and containers
# Usage: .\scripts\stop_dev.ps1
# Optional: .\scripts\stop_dev.ps1 -KeepDocker   (leaves Docker Desktop running)
#           .\scripts\stop_dev.ps1 -Prune         (also deletes volumes + images)

param(
    [switch]$KeepDocker,
    [switch]$Prune
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "SilentlyContinue"   # keep going even if one step fails

Write-Host @"

========================================
  Fraud Detection — Stopping All Services
========================================
"@ -ForegroundColor Cyan


# ── 1. Kill Spark job (spark-submit) ─────────────────────────────────────────
Write-Host "[1/5] Stopping Spark job (spark-submit)..." -ForegroundColor Yellow

$sparkProcs = Get-Process -Name "java" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -like "*spark-submit*" -or
                   $_.CommandLine -like "*SparkSubmit*" -or
                   $_.CommandLine -like "*fraud_detection*" }

if ($sparkProcs) {
    $sparkProcs | ForEach-Object {
        Write-Host "      Killing Spark PID $($_.Id)" -ForegroundColor DarkGray
        Stop-Process -Id $_.Id -Force
    }
    Write-Host "      Spark job stopped." -ForegroundColor Green
} else {
    # Fallback: stop all java processes (Spark runs on JVM)
    $javaProcs = Get-Process -Name "java" -ErrorAction SilentlyContinue
    if ($javaProcs) {
        Write-Host "      Stopping all Java/Spark processes..." -ForegroundColor DarkGray
        $javaProcs | Stop-Process -Force
        Write-Host "      Done." -ForegroundColor Green
    } else {
        Write-Host "      No Spark/Java process found." -ForegroundColor DarkGray
    }
}


# ── 2. Kill transaction generator (Python) ────────────────────────────────────
Write-Host "`n[2/5] Stopping transaction generator (Python)..." -ForegroundColor Yellow

$pyProcs = Get-Process -Name "python", "python3", "uv" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -like "*transaction_generator*" -or
                   $_.CommandLine -like "*fraud_detection*" }

if ($pyProcs) {
    $pyProcs | ForEach-Object {
        Write-Host "      Killing Python PID $($_.Id)" -ForegroundColor DarkGray
        Stop-Process -Id $_.Id -Force
    }
    Write-Host "      Generator stopped." -ForegroundColor Green
} else {
    Write-Host "      No generator process found." -ForegroundColor DarkGray
}


# ── 3. Stop Docker containers (docker-compose down) ───────────────────────────
Write-Host "`n[3/5] Stopping Docker containers..." -ForegroundColor Yellow

# Confirm docker-compose.yml exists
if (-not (Test-Path "docker-compose.yml")) {
    Write-Host "      docker-compose.yml not found in $PWD" -ForegroundColor Red
    Write-Host "      Run from the project root: cd C:\GitHub\fraud-detection-project" -ForegroundColor Yellow
} else {
    try {
        docker info *>$null 2>&1
        $dockerUp = $true
    } catch {
        $dockerUp = $false
    }

    if ($dockerUp) {
        if ($Prune) {
            Write-Host "      Running docker-compose down --volumes --rmi local ..." -ForegroundColor DarkGray
            docker-compose down --volumes --rmi local 2>&1
        } else {
            Write-Host "      Running docker-compose down ..." -ForegroundColor DarkGray
            docker-compose down 2>&1
        }
        Write-Host "      Containers stopped." -ForegroundColor Green
    } else {
        Write-Host "      Docker engine not running — skipping." -ForegroundColor DarkGray
    }
}


# ── 4. List any remaining containers (sanity check) ───────────────────────────
Write-Host "`n[4/5] Verifying no containers still running..." -ForegroundColor Yellow

try {
    $running = docker ps --format "{{.Names}}" 2>$null
    $relevant = $running | Where-Object { $_ -match "kafka|zookeeper|spark" }
    if ($relevant) {
        Write-Host "      WARNING — these containers are still up:" -ForegroundColor Red
        $relevant | ForEach-Object { Write-Host "        - $_" -ForegroundColor Red }
        Write-Host "      Force-stop with: docker stop $($relevant -join ' ')" -ForegroundColor Yellow
    } else {
        Write-Host "      All project containers stopped." -ForegroundColor Green
    }
} catch {
    Write-Host "      Could not verify (Docker not reachable)." -ForegroundColor DarkGray
}


# ── 5. Optionally quit Docker Desktop ─────────────────────────────────────────
Write-Host "`n[5/5] Docker Desktop..." -ForegroundColor Yellow

if ($KeepDocker) {
    Write-Host "      -KeepDocker flag set — leaving Docker Desktop running." -ForegroundColor DarkGray
} else {
    $dockerDesktop = Get-Process -Name "Docker Desktop" -ErrorAction SilentlyContinue
    if ($dockerDesktop) {
        Write-Host "      Quitting Docker Desktop..." -ForegroundColor DarkGray
        # Graceful quit via the system tray API (avoids data corruption vs. kill)
        $dockerDesktop | ForEach-Object {
            $_.CloseMainWindow() | Out-Null
        }
        Start-Sleep -Seconds 3
        # Force if still alive
        $still = Get-Process -Name "Docker Desktop" -ErrorAction SilentlyContinue
        if ($still) { $still | Stop-Process -Force }
        Write-Host "      Docker Desktop closed." -ForegroundColor Green
    } else {
        Write-Host "      Docker Desktop was not running." -ForegroundColor DarkGray
    }
}


# ── Summary ───────────────────────────────────────────────────────────────────
Write-Host @"

========================================
  All services stopped.

  To restart:
    .\scripts\run_dev.ps1

  Flags used this run:
    -KeepDocker : $KeepDocker
    -Prune      : $Prune   (deleted volumes + local images)
========================================
"@ -ForegroundColor Cyan
