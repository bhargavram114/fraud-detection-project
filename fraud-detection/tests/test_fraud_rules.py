"""
test_fraud_rules.py
Unit tests for fraud detection logic using PySpark's test utilities.

Interview tip: always show you test your Spark code.
Use a local SparkSession — no cluster needed for unit tests.

Run: pytest tests/ -v
"""

import pytest
from datetime import datetime, timezone, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
sys.path.insert(0, "../jobs")

from jobs.fraud_detection import detect_high_value
from jobs.schemas import TRANSACTION_SCHEMA


@pytest.fixture(scope="session")
def spark():
    """One SparkSession shared across all tests in the session."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("FraudDetectionTests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def make_txn_row(card_id="C001", amount=100.0, seconds_offset=0):
    """Helper: build a transaction dict with a controllable timestamp."""
    base_time = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    return {
        "txn_id":       f"T-{seconds_offset}",
        "card_id":      card_id,
        "account_id":   "ACC-1",
        "amount":       float(amount),
        "currency":     "INR",
        "merchant_id":  "MER-1",
        "merchant_cat": "ATM_WITHDRAWAL",
        "terminal_id":  "TRM-1",
        "country":      "IN",
        "city":         "Hyderabad",
        "txn_type":     "WITHDRAWAL",
        "channel":      "ATM",
        "event_time":   base_time + timedelta(seconds=seconds_offset),
    }


class TestHighValueRule:

    def test_high_value_transaction_is_flagged(self, spark):
        data = [make_txn_row(amount=35_000)]  # exceeds ₹30,000 threshold
        df = spark.createDataFrame(data, schema=TRANSACTION_SCHEMA)
        result = detect_high_value(df).collect()
        assert len(result) == 1
        assert result[0]["rule_triggered"] == "HIGH_VALUE_SINGLE_TXN"

    def test_normal_transaction_not_flagged(self, spark):
        data = [make_txn_row(amount=500)]   # well below threshold
        df = spark.createDataFrame(data, schema=TRANSACTION_SCHEMA)
        result = detect_high_value(df).collect()
        assert len(result) == 0

    def test_boundary_at_threshold_not_flagged(self, spark):
        data = [make_txn_row(amount=30_000)]  # exactly at threshold, not above
        df = spark.createDataFrame(data, schema=TRANSACTION_SCHEMA)
        result = detect_high_value(df).collect()
        assert len(result) == 0

    def test_multiple_mixed_transactions(self, spark):
        data = [
            make_txn_row(card_id="C001", amount=500,    seconds_offset=0),
            make_txn_row(card_id="C002", amount=45_000, seconds_offset=1),
            make_txn_row(card_id="C003", amount=200,    seconds_offset=2),
            make_txn_row(card_id="C004", amount=31_000, seconds_offset=3),
        ]
        df = spark.createDataFrame(data, schema=TRANSACTION_SCHEMA)
        result = detect_high_value(df).collect()
        # Only C002 and C004 should be flagged
        assert len(result) == 2
        flagged_cards = {r["card_id"] for r in result}
        assert flagged_cards == {"C002", "C004"}
