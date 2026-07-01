#!/bin/bash
# run_local.sh — One-command local development setup
# Run this from the project root: bash scripts/run_local.sh

set -e
echo "========================================"
echo " Fraud Detection Pipeline — Local Setup"
echo "========================================"

# Step 1: Start infrastructure
echo ""
echo "[1/4] Starting Kafka + Spark..."
docker-compose up -d
sleep 10   # wait for Kafka to be ready

# Step 2: Create Kafka topics explicitly (auto-create is on, but explicit is better)
echo ""
echo "[2/4] Creating Kafka topics..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic transactions \
  --partitions 3 --replication-factor 1

docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic fraud-alerts \
  --partitions 3 --replication-factor 1

echo "       Topics created: transactions, fraud-alerts"

# Step 3: Install Python dependencies
echo ""
echo "[3/4] Installing Python dependencies..."
pip install -r requirements.txt -q

echo ""
echo "[4/4] Ready! Open 3 terminals and run:"
echo ""
echo "  Terminal 1 — Start fraud detection job:"
echo "    spark-submit \\"
echo "      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\"
echo "      jobs/fraud_detection.py"
echo ""
echo "  Terminal 2 — Start transaction generator (injects fraud bursts):"
echo "    python producer/transaction_generator.py --mode mixed --tps 10"
echo ""
echo "  Terminal 3 — Watch the fraud alerts topic:"
echo "    docker exec kafka kafka-console-consumer \\"
echo "      --bootstrap-server localhost:9092 \\"
echo "      --topic fraud-alerts --from-beginning"
echo ""
echo "  Kafka UI (visual):  http://localhost:8080"
echo "  Spark Master UI:    http://localhost:8081"
echo ""
