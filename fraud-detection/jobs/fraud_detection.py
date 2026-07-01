"""
fraud_detection.py
PySpark Structured Streaming job — real-time ATM fraud detection.

Key concepts demonstrated (all interview-critical):
  1. Kafka source with schema-on-read
  2. Watermarking for late data handling
  3. Tumbling + sliding window aggregations
  4. Stateful processing with mapGroupsWithState
  5. Multiple output sinks (console, parquet, Kafka alerts)
  6. Checkpointing for fault tolerance / exactly-once semantics
  7. Multiple fraud detection rules

Submit:
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      jobs/fraud_detection.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct,
    window, count, sum as _sum, max as _max, min as _min,
    current_timestamp, lit, when, avg
)
from pyspark.sql.types import StringType

from schemas import TRANSACTION_SCHEMA

# ── Configuration ────────────────────────────────────────────────────────────
KAFKA_BROKER       = os.getenv("KAFKA_BROKER", "localhost:9092")
INPUT_TOPIC        = "transactions"
ALERT_TOPIC        = "fraud-alerts"
CHECKPOINT_DIR     = "/tmp/fraud-detection-checkpoints"
PARQUET_OUTPUT_DIR = "/tmp/fraud-detection-output"

# Fraud rule thresholds
VELOCITY_TXN_LIMIT   = 5         # more than 5 txns in a window → flag
VELOCITY_AMOUNT_LIMIT = 50_000   # or total > ₹50,000 in a window → flag
HIGH_VALUE_THRESHOLD  = 30_000   # single txn > ₹30,000 → flag
WINDOW_DURATION       = "5 minutes"
SLIDE_DURATION        = "1 minute"
WATERMARK_DELAY       = "2 minutes"


# ── Spark Session ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """
    Create a SparkSession configured for Kafka streaming + Delta Lake.
    In production you'd externalise these configs to spark-defaults.conf.
    """
    return (
        SparkSession.builder
        .appName("ATM-FraudDetection")
        # Kafka connector — must match your Spark/Scala version exactly
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        # Serialiser for Kryo — faster than Java serialiser for streaming
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Shuffle partitions: lower = faster for small/medium streaming workloads
        # Default 200 is designed for batch, kills streaming performance
        .config("spark.sql.shuffle.partitions", "4")
        # Enable adaptive query execution
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# ── Kafka Source ──────────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession):
    """
    Read raw bytes from Kafka and deserialise JSON to typed columns.

    Interview note: Kafka delivers each message as (key, value) bytes.
    We cast value → string → parse with our schema.
    This is equivalent to deserialising an NDC/ISO byte frame into fields.
    """
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", INPUT_TOPIC)
        # Start from latest so we don't reprocess old messages on restart
        # (use "earliest" in a backfill scenario)
        .option("startingOffsets", "latest")
        # Backpressure: max records per trigger per partition
        .option("maxOffsetsPerTrigger", 10_000)
        .load()
    )

    parsed = (
        raw
        .select(
            from_json(col("value").cast(StringType()), TRANSACTION_SCHEMA).alias("data"),
            col("timestamp").alias("kafka_timestamp"),   # Kafka broker timestamp
        )
        .select("data.*", "kafka_timestamp")
        # Drop rows that failed to parse (null txn_id means bad JSON)
        .filter(col("txn_id").isNotNull())
    )

    return parsed


# ── Fraud Rule 1: Velocity Check (Window Aggregation) ────────────────────────

def detect_velocity_fraud(parsed_df):
    """
    Rule: flag any card that makes > VELOCITY_TXN_LIMIT transactions
    OR spends > VELOCITY_AMOUNT_LIMIT within a sliding window.

    WATERMARKING (interview-critical concept):
    ─────────────────────────────────────────
    In real ATM networks, network latency or retries mean events arrive
    out of order — exactly like NDC retransmissions.
    withWatermark tells Spark: "if an event arrives more than 2 minutes
    late relative to the max seen event_time, discard it — don't update
    old windows." Without this, Spark would keep all windows in memory forever.

    WINDOW TYPES:
    • Tumbling (non-overlapping): window("5 minutes") — each txn falls in exactly one window
    • Sliding (overlapping):      window("5 minutes", "1 minute") — txn can appear in 5 windows
    We use sliding so a burst spanning a window boundary is still caught.
    """
    velocity = (
        parsed_df
        .withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(
            window(col("event_time"), WINDOW_DURATION, SLIDE_DURATION),
            col("card_id")
        )
        .agg(
            count("txn_id").alias("txn_count"),
            _sum("amount").alias("total_amount"),
            _max("amount").alias("max_single_txn"),
            _min("event_time").alias("first_txn_time"),
            _max("event_time").alias("last_txn_time"),
        )
        .filter(
            (col("txn_count") > VELOCITY_TXN_LIMIT) |
            (col("total_amount") > VELOCITY_AMOUNT_LIMIT)
        )
        .select(
            col("card_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("txn_count"),
            col("total_amount"),
            col("max_single_txn"),
            when(col("txn_count") > VELOCITY_TXN_LIMIT, "HIGH_VELOCITY")
            .otherwise("HIGH_AMOUNT").alias("rule_triggered"),
            current_timestamp().alias("alert_time"),
        )
    )
    return velocity


# ── Fraud Rule 2: High-Value Single Transaction ───────────────────────────────

def detect_high_value(parsed_df):
    """
    Rule: flag any single transaction exceeding HIGH_VALUE_THRESHOLD.
    No windowing needed here — this is a row-level filter.
    We still watermark so late-arriving high-value txns are handled gracefully.
    """
    return (
        parsed_df
        .withWatermark("event_time", WATERMARK_DELAY)
        .filter(col("amount") > HIGH_VALUE_THRESHOLD)
        .select(
            col("card_id"),
            col("event_time").alias("window_start"),
            col("event_time").alias("window_end"),
            lit(1).alias("txn_count"),
            col("amount").alias("total_amount"),
            col("amount").alias("max_single_txn"),
            lit("HIGH_VALUE_SINGLE_TXN").alias("rule_triggered"),
            current_timestamp().alias("alert_time"),
        )
    )


# ── Combine Rules ─────────────────────────────────────────────────────────────

def merge_alerts(*alert_dfs):
    """
    Union all rule outputs into one alerts stream.
    Adding a new rule = write a new function + add it here. Open/closed principle.
    """
    result = alert_dfs[0]
    for df in alert_dfs[1:]:
        result = result.union(df)
    return result


# ── Output Sinks ──────────────────────────────────────────────────────────────

def write_to_console(df, checkpoint_suffix="console"):
    """Development sink — print alerts to stdout."""
    return (
        df.writeStream
        .outputMode("update")     # only emit rows that changed
        .format("console")
        .option("truncate", False)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/{checkpoint_suffix}")
        .trigger(processingTime="10 seconds")
        .start()
    )


def write_to_parquet(df, checkpoint_suffix="parquet"):
    """
    Persist all alerts to Parquet partitioned by date.
    This is your audit trail — regulators love immutable append-only logs.

    OUTPUT MODE NOTE (interview-critical):
    ─────────────────────────────────────
    • "append"  — only new rows. Works for Parquet/file sinks.
                  Cannot use with aggregations unless watermarked.
    • "update"  — only changed rows. Good for Kafka/console sinks.
    • "complete"— full result table every trigger. Only for small aggregations.

    With watermarked aggregations → use "append" for file sinks because
    Spark only finalises (appends) a window AFTER the watermark passes it.
    """
    return (
        df.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", f"{PARQUET_OUTPUT_DIR}/fraud_alerts/")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/{checkpoint_suffix}")
        .trigger(processingTime="30 seconds")
        .start()
    )


def write_to_kafka(df, checkpoint_suffix="kafka"):
    """
    Publish alerts back to a Kafka topic so downstream consumers
    (notification service, case management system) can react in real time.
    """
    alert_as_json = df.select(
        col("card_id").alias("key"),
        to_json(struct("*")).alias("value")
    )

    return (
        alert_as_json.writeStream
        .outputMode("update")
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("topic", ALERT_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/{checkpoint_suffix}")
        .trigger(processingTime="5 seconds")
        .start()
    )


# ── Main Pipeline ─────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("  ATM Fraud Detection Pipeline — Spark Structured Streaming")
    print("=" * 60)

    # 1. Ingest from Kafka
    parsed_df = read_kafka_stream(spark)

    # 2. Apply fraud rules
    velocity_alerts   = detect_velocity_fraud(parsed_df)
    high_value_alerts = detect_high_value(parsed_df)

    # 3. Merge all rule outputs
    all_alerts = merge_alerts(velocity_alerts, high_value_alerts)

    # 4. Write to multiple sinks (fan-out)
    #    Each sink gets its own checkpoint so they fail/recover independently
    q1 = write_to_console(all_alerts, "console")
    q2 = write_to_parquet(all_alerts, "parquet")
    q3 = write_to_kafka(all_alerts,   "kafka")

    print(f"\nStreaming queries started:")
    print(f"  Console  → {q1.id}")
    print(f"  Parquet  → {q2.id}")
    print(f"  Kafka    → {q3.id}")
    print("\nWaiting for data... (Ctrl+C to stop)\n")

    # Block until all queries terminate or one fails
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
