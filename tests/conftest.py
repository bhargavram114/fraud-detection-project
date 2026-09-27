"""
conftest.py — Shared fixtures and test-data factory for all test modules.

Design principle:
  All rule transforms are PURE FUNCTIONS that accept a plain batch DataFrame.
  withWatermark() is applied only in the streaming wrapper layer (fraud_rules.py).
  This makes every transform fully testable without a streaming context.
"""
import pytest
from datetime import datetime, timezone, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jobs.schemas import TRANSACTION_SCHEMA

BASE_TIME = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)   # noon UTC


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("FraudDetectionTests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def make_txn(
    card_id:      str   = "CARD_001",
    amount:       float = 500.0,
    channel:      str   = "ATM",
    txn_status:   str   = "APPROVED",
    merchant_id:  str   = "MER_001",
    terminal_id:  str   = "TRM_001",
    country:      str   = "IN",
    event_time:   datetime = None,
    txn_id:       str   = None,
    seconds_offset: int = 0,
) -> dict:
    """Build one transaction row. event_time defaults to BASE_TIME + offset."""
    import uuid
    t = event_time or (BASE_TIME + timedelta(seconds=seconds_offset))
    return {
        "txn_id":       txn_id or str(uuid.uuid4()),
        "card_id":      card_id,
        "account_id":   f"ACC_{card_id}",
        "amount":       float(amount),
        "currency":     "INR",
        "merchant_id":  merchant_id,
        "merchant_cat": "ATM_WITHDRAWAL",
        "terminal_id":  terminal_id,
        "country":      country,
        "city":         "Hyderabad",
        "txn_type":     "WITHDRAWAL",
        "channel":      channel,
        "txn_status":   txn_status,
        "event_time":   t,
    }


def make_df(spark, rows: list) -> "DataFrame":
    return spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)
