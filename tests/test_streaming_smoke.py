"""
test_streaming_smoke.py — Proves the streaming wrappers (withWatermark + transform
+ current_timestamp) actually plan and run on a real streaming DataFrame.

The pure transforms are unit-tested on batch data; this covers the thin wrapper
layer in fraud_rules.py. A rate source stands in for Kafka (no broker needed).
"""
import time
from pyspark.sql.functions import col, lit, expr
from jobs.schemas import TRANSACTION_SCHEMA


def _stream_txns(spark, rows_per_second=20):
    """Rate source -> a streaming DataFrame with TRANSACTION_SCHEMA's columns."""
    base = (spark.readStream.format("rate")
            .option("rowsPerSecond", rows_per_second).load())
    return (base
            .withColumn("txn_id",       col("value").cast("string"))
            .withColumn("card_id",      lit("CARD_S"))
            .withColumn("account_id",   lit("ACC"))
            .withColumn("amount",       lit(100.0))
            .withColumn("currency",     lit("INR"))
            .withColumn("merchant_id",  lit("MER"))
            .withColumn("merchant_cat", lit("5411"))
            .withColumn("terminal_id",  lit("TRM"))
            .withColumn("country",      lit("IN"))
            .withColumn("city",         lit("Bengaluru"))
            .withColumn("txn_type",     lit("WITHDRAWAL"))
            .withColumn("channel",      lit("ATM"))
            .withColumn("txn_status",   lit("APPROVED"))
            .withColumnRenamed("timestamp", "event_time")
            .select(*[f.name for f in TRANSACTION_SCHEMA.fields]))


def test_velocity_wrapper_runs_on_a_real_stream(spark):
    from jobs.rules.fraud_rules import rule_velocity_check
    stream = _stream_txns(spark)
    assert stream.isStreaming
    alerts = rule_velocity_check(stream)
    assert alerts.isStreaming
    assert "alert_time" in alerts.columns and "rule_triggered" in alerts.columns

    q = (alerts.writeStream.format("memory").queryName("smoke_velocity")
         .outputMode("update").trigger(processingTime="1 second").start())
    try:
        deadline = time.time() + 40
        n = 0
        while time.time() < deadline and n == 0:
            time.sleep(2)
            n = spark.sql("select count(*) from smoke_velocity").collect()[0][0]
        assert q.exception() is None
        assert n > 0, "20 txns/sec on one card should exceed the 5-per-window limit"
    finally:
        q.stop()


def test_all_single_input_wrappers_plan_on_a_stream(spark):
    """Every registered wrapper must accept a streaming DataFrame and keep it streaming."""
    from jobs.rules.fraud_rules import build_rules
    stream = _stream_txns(spark, rows_per_second=1)
    for rule in build_rules():
        out = rule(stream)
        assert out.isStreaming, rule.__name__
