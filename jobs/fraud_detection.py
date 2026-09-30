"""
fraud_detection.py — Development / local version of the streaming job.

USE THIS FILE FOR:
  - Local development and debugging (console output, easy to read)
  - Learning and experimenting with the pipeline
  - Demonstrating the pipeline in interviews

USE fraud_detection_prod.py FOR:
  - Staging and production deployments
  - Any environment that needs monitoring, DLQ, health checks

DIFFERENCES FROM PROD VERSION:
  ┌─────────────────────┬─────────────────────────────────────────────┐
  │ Concern             │ Dev approach                                │
  ├─────────────────────┼─────────────────────────────────────────────┤
  │ Config              │ Hardcoded strings (easy to change)          │
  │ Logging             │ Spark console output (human-readable)       │
  │ Bad messages        │ Silently dropped (filter isNotNull)         │
  │ Output sink         │ Console only (no Kafka, no Parquet)         │
  │ Monitoring          │ None                                        │
  │ Shutdown            │ Ctrl+C                                      │
  └─────────────────────┴─────────────────────────────────────────────┘

RUN:
  uv run spark-submit `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 `
    jobs\fraud_detection.py
"""

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType

# Ensure project root is on the path so relative imports work
sys.path.insert(0, str(Path(__file__).parent.parent))

from jobs.schemas import TRANSACTION_SCHEMA
from jobs.rules.fraud_rules import RULE_REGISTRY

# ── Local config ──────────────────────────────────────────────────────────────
KAFKA_BROKER  = "localhost:9092"
INPUT_TOPIC   = "transactions"
CHECKPOINT    = "/tmp/fraud-dev-checkpoints"   # lost on restart — fine for dev


def main():
    # ── Spark session ─────────────────────────────────────────────────────────
    # shuffle.partitions=4 — default is 200 which causes 200 tiny tasks per
    # micro-batch; catastrophic for streaming performance on a local machine.
    # Match this to your Kafka partition count (3) or CPU core count.
    spark = (
        SparkSession.builder
        .appName("FraudDetection-Dev")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")   # suppress INFO noise

    # ── Kafka source ──────────────────────────────────────────────────────────
    # Kafka delivers each message as raw bytes in a (key, value) struct.
    # We cast value → string → parse with TRANSACTION_SCHEMA.
    # Rows where txn_id is null = JSON parse failed → silently dropped in dev.
    # In prod, these go to the Dead Letter Queue instead.
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "latest")    # don't replay old messages on start
        .option("maxOffsetsPerTrigger", 1000)   # backpressure — limit per micro-batch
        .load()
        .withColumn("raw_value", col("value").cast(StringType()))
    )

    parsed = (
        raw
        .withColumn("data", from_json(col("raw_value"), TRANSACTION_SCHEMA))
        .select("data.*")
        .filter(col("txn_id").isNotNull())   # drop unparseable messages
    )

    # ── Apply all 11 stateless rules + union results ──────────────────────────
    # Each rule_fn adds withWatermark internally (see fraud_rules.py).
    # Note: rule_dormant_card (Rule 10) needs a reference DataFrame and is
    # omitted in the dev job for simplicity. It runs in the prod job.
    alert_dfs = []
    for rule_fn in RULE_REGISTRY:
        try:
            alert_dfs.append(rule_fn(parsed))
        except Exception as e:
            print(f"[WARN] Rule {rule_fn.__name__} failed to register: {e}")

    # union() combines all rule outputs into one alerts stream.
    # All rules must produce the same schema (enforced by _to_alert in transforms.py).
    merged = alert_dfs[0]
    for adf in alert_dfs[1:]:
        merged = merged.union(adf)

    # ── Console sink ──────────────────────────────────────────────────────────
    # outputMode("update") — emit only rows that changed since the last trigger.
    # Suitable for aggregations. Do NOT use "complete" at scale — it emits
    # the full result table every trigger regardless of what changed.
    query = (
        merged.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .option("checkpointLocation", f"{CHECKPOINT}/console")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("\n" + "="*60)
    print("  Fraud Detection Pipeline — DEV MODE")
    print("  Listening on topic:", INPUT_TOPIC)
    print("  Alerts will appear below every 10 seconds...")
    print("="*60 + "\n")

    query.awaitTermination()


if __name__ == "__main__":
    main()
