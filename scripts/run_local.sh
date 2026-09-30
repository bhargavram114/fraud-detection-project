#!/bin/bash
# run_local.sh — One-command local development setup (macOS / Linux / WSL)
# Run this from the project root: bash scripts/run_local.sh
#
# Windows users: use scripts\run_dev.ps1 instead (same steps, PowerShell).
# Dependencies are managed by uv + pyproject.toml — there is no requirements.txt.

set -e
echo "========================================"
echo " Fraud Detection Pipeline — Local Setup"
echo "========================================"

# Prerequisites: docker (with the compose v2 plugin), uv, Java 11+
command -v docker >/dev/null || { echo "docker not found"; exit 1; }
command -v uv     >/dev/null || { echo "uv not found — https://docs.astral.sh/uv/"; exit 1; }
command -v java   >/dev/null || { echo "Java 11+ not found (required by Spark)"; exit 1; }

# Step 1: Start infrastructure
echo ""
echo "[1/4] Starting Kafka + Spark..."
docker compose up -d
echo "       Waiting for Kafka to become healthy..."
for i in $(seq 1 30); do
  if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

# Step 2: Create Kafka topics explicitly (auto-create is on, but explicit is better)
echo ""
echo "[2/4] Creating Kafka topics..."
for topic in transactions fraud-alerts fraud-detection-dlq; do
  docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$topic" \
    --partitions 3 --replication-factor 1
done
echo "       Topics created: transactions, fraud-alerts, fraud-detection-dlq"

# Step 3: Install Python dependencies from pyproject.toml / uv.lock
echo ""
echo "[3/4] Installing Python dependencies with uv..."
uv sync --extra dev

echo ""
echo "[4/4] Ready! Open 3 terminals and run:"
echo ""
echo "  Terminal 1 — Start fraud detection job (dev, console output):"
echo "    uv run spark-submit \\"
echo "      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\"
echo "      jobs/fraud_detection.py"
echo ""
echo "  Terminal 2 — Start transaction generator (injects fraud bursts):"
echo "    uv run python producer/transaction_generator.py --mode mixed --tps 10"
echo ""
echo "  Terminal 3 — Watch the fraud alerts topic:"
echo "    docker exec kafka kafka-console-consumer \\"
echo "      --bootstrap-server localhost:9092 \\"
echo "      --topic fraud-alerts --from-beginning"
echo ""
echo "  Kafka UI (visual):  http://localhost:8080"
echo "  Spark Master UI:    http://localhost:8081"
echo ""
