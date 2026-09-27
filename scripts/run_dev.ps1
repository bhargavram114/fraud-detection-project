# run_dev.ps1 — Daily dev workflow
# Usage: .\scripts\run_dev.ps1
param(
    [string]$Mode = "mixed",
    [int]$Tps = 10
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ── 1. Confirm running from project root ─────────────────────────────────────
if (-not (Test-Path "docker-compose.yml")) {
    Write-Host "[ERROR] Run this from the project root (where docker-compose.yml lives)." -ForegroundColor Red
    Write-Host "        cd C:\GitHub\fraud-detection-project" -ForegroundColor Yellow
    exit 1
}

# ── 2. Start Docker Desktop if engine is not responding ──────────────────────
Write-Host "`n[1/3] Checking Docker engine..." -ForegroundColor Cyan

$dockerReady = $false
try { docker info *>$null 2>&1; $dockerReady = $true } catch {}

if (-not $dockerReady) {
    $dockerExe = "C:\Program Files\Docker\Docker\Docker Desktop.exe"
    if (-not (Test-Path $dockerExe)) {
        Write-Host "[ERROR] Docker Desktop not found. Download from:" -ForegroundColor Red
        Write-Host "        https://www.docker.com/products/docker-desktop/" -ForegroundColor White
        exit 1
    }

    Write-Host "        Docker Desktop not running — launching it now..." -ForegroundColor Yellow
    Start-Process $dockerExe

    Write-Host "        Waiting for engine (up to 90s) ..." -ForegroundColor Yellow
    $waited = 0
    while (-not $dockerReady -and $waited -lt 90) {
        Start-Sleep -Seconds 5
        $waited += 5
        try { docker info *>$null 2>&1; $dockerReady = $true } catch {}
        Write-Host "        ... ${waited}s" -ForegroundColor DarkGray
    }

    if (-not $dockerReady) {
        Write-Host "[ERROR] Docker engine did not start after 90s." -ForegroundColor Red
        Write-Host "        Open Docker Desktop manually, wait for the whale icon to stop animating, then re-run." -ForegroundColor Yellow
        exit 1
    }
}
Write-Host "        Docker engine is ready." -ForegroundColor Green

# ── 3. Start Kafka + Spark ───────────────────────────────────────────────────
Write-Host "`n[2/3] Starting Kafka + Spark..." -ForegroundColor Cyan
docker-compose up -d

Write-Host "        Waiting 12s for Kafka to be fully ready..." -ForegroundColor DarkGray
Start-Sleep -Seconds 12

# ── 4. Create Kafka topics ───────────────────────────────────────────────────
Write-Host "`n[3/3] Creating Kafka topics..." -ForegroundColor Cyan

docker exec kafka kafka-topics `
    --bootstrap-server localhost:9092 `
    --create --if-not-exists `
    --topic transactions `
    --partitions 3 --replication-factor 1

docker exec kafka kafka-topics `
    --bootstrap-server localhost:9092 `
    --create --if-not-exists `
    --topic fraud-alerts `
    --partitions 3 --replication-factor 1

docker exec kafka kafka-topics `
    --bootstrap-server localhost:9092 `
    --create --if-not-exists `
    --topic fraud-detection-dlq `
    --partitions 1 --replication-factor 1

Write-Host "        Topics: transactions, fraud-alerts, fraud-detection-dlq" -ForegroundColor Green

# ── Done ─────────────────────────────────────────────────────────────────────
Write-Host @"

========================================
  All services running. Open 2 terminals:
========================================

  Terminal 1 — Spark fraud detection job:
    uv run spark-submit ``
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 ``
      jobs\fraud_detection.py

  Terminal 2 — Transaction generator (mode=$Mode, tps=$Tps):
    uv run python producer\transaction_generator.py --mode $Mode --tps $Tps

  Run tests:
    uv run pytest tests\ -v --cov=jobs --cov-report=term-missing

  Kafka UI  :  http://localhost:8080
  Spark UI  :  http://localhost:8081
"@ -ForegroundColor Cyan
