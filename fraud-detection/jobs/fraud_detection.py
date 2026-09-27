"""
fraud_detection.py — Dev/local version of the streaming job.
Simpler than prod: console sink only, no DLQ, no health server.
Run:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    jobs/fraud_detection.py
"""
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType

sys.path.insert(0, str(Path(__file__).parent.parent))
from jobs.schemas import TRANSACTION_SCHEMA
from jobs.rules.fraud_rules import RULE_REGISTRY

KAFKA_BROKER = "localhost:9092"
CHECKPOINT   = "/tmp/fraud-dev-checkpoints"


def main():
    spark = (
        SparkSession.builder.appName("FraudDetection-Dev")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", "transactions")
        .option("startingOffsets", "latest")
        .load()
        .withColumn("raw", col("value").cast(StringType()))
    )

    parsed = (
        raw.withColumn("data", from_json(col("raw"), TRANSACTION_SCHEMA))
        .select("data.*")
        .filter(col("txn_id").isNotNull())
    )

    alert_dfs = [rule_fn(parsed) for rule_fn in RULE_REGISTRY]
    merged = alert_dfs[0]
    for adf in alert_dfs[1:]:
        merged = merged.union(adf)

    query = (
        merged.writeStream.outputMode("update").format("console")
        .option("truncate", False)
        .option("checkpointLocation", f"{CHECKPOINT}/console")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("\n[DEV] Fraud Detection running — waiting for transactions...\n")
    query.awaitTermination()


if __name__ == "__main__":
    main()
