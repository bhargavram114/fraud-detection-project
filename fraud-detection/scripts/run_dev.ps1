# run_dev.ps1 — Daily dev workflow (after setup_windows.ps1 done once)
# Usage: .\scripts\run_dev.ps1

param(
    [string]$Mode = "mixed",   # mixed | normal
    [int]$Tps = 10
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Host "`n[dev] Starting Kafka + Spark..." -ForegroundColor Yellow
docker-compose up -d
Start-Sleep -Seconds 8

Write-Host "[dev] Creating Kafka topics..." -ForegroundColor Yellow
docker exec kafka kafka-topics --bootstrap-server localhost:9092 `
    --create --if-not-exists --topic transactions --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 `
    --create --if-not-exists --topic fraud-alerts --partitions 3 --replication-factor 1

Write-Host @"

[dev] Ready. Open 2 more terminals:

  Spark job:
    uv run spark-submit ``
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 ``
      jobs\fraud_detection.py

  Generator (mode=$Mode, tps=$Tps):
    uv run python producer\transaction_generator.py --mode $Mode --tps $Tps

  Tests:
    uv run pytest tests\ -v --cov=jobs --cov-report=term-missing

  Kafka UI : http://localhost:8080
  Spark UI : http://localhost:8081
"@ -ForegroundColor Cyan
