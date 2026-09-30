"""
conftest.py — Shared fixtures and test-data factory for all test modules.

═══════════════════════════════════════════════════════════════════════════════
DESIGN PRINCIPLE — WHY PURE FUNCTION TESTING WORKS:

  All 12 fraud rule transforms in transforms.py accept a plain batch
  DataFrame — no streaming context, no withWatermark(), no Kafka.
  This means we can test them using spark.createDataFrame() in a
  standard pytest session. No Kafka cluster, no Docker, no mocking.

  The pattern:
    1. Create a small DataFrame with make_df(spark, [make_txn(...), ...])
    2. Call the transform function directly
    3. Assert on the returned DataFrame

  This gives us:
    - Fast tests (no network, no I/O)
    - Deterministic tests (controlled input → controlled output)
    - Full coverage of business logic
    - Tests that run on any machine with Python 3.11 + Java

SESSION-SCOPED SPARKSESSION:
  scope="session" means one SparkSession is created for the entire pytest run
  and shared across all test classes and functions. Starting and stopping
  Spark is expensive (~5s per instance). Session scope reduces total test
  time from ~60s to ~15s.
═══════════════════════════════════════════════════════════════════════════════
"""

import sys
import os
import pytest
from datetime import datetime, timezone, timedelta

# Ensure project root is importable regardless of where pytest is invoked from
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jobs.schemas import TRANSACTION_SCHEMA

# Base timestamp used across all tests — noon UTC on 2026-01-01.
# Using a fixed time makes tests deterministic and ensures all events
# in a test fall within expected window boundaries.
BASE_TIME = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="session")
def spark():
    """
    Shared SparkSession for the entire test suite.
    local[2] — 2 local threads simulating 2 Spark executors.
    shuffle.partitions=2 — default 200 creates unnecessary overhead in tests.
    """
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("FraudDetectionTests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        # Suppress Spark's verbose startup logging in test output
        .config("spark.driver.extraJavaOptions",
                "-Dlog4j.logLevel=ERROR -Dlog4j.logger.org=ERROR")
        .getOrCreate()
    )


def make_txn(
    card_id:        str      = "CARD_001",
    amount:         float    = 500.0,
    channel:        str      = "ATM",
    txn_status:     str      = "APPROVED",
    merchant_id:    str      = "MER_001",
    terminal_id:    str      = "TRM_001",
    country:        str      = "IN",
    event_time:     datetime = None,
    txn_id:         str      = None,
    seconds_offset: int      = 0,
) -> dict:
    """
    Build one transaction row dictionary for use in tests.

    USAGE PATTERNS:

      # Single default transaction
      make_txn()

      # Override specific fields
      make_txn(card_id="FRAUD_CARD", amount=35000.0)

      # Time-offset transactions for window tests
      # 6 transactions spread over 2.5 minutes — triggers velocity rule
      rows = [make_txn(card_id="C1", seconds_offset=i*30) for i in range(6)]

      # Specific timestamp (e.g. 2AM for unusual-hour test)
      from datetime import datetime, timezone
      t = datetime(2026, 1, 1, 2, 0, 0, tzinfo=timezone.utc)
      make_txn(amount=9000.0, event_time=t)

    NOTE: seconds_offset is relative to BASE_TIME (noon UTC 2026-01-01).
    All window tests use offsets to control which window events fall into.
    """
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


def make_df(spark, rows: list):
    """
    Create a typed batch DataFrame from a list of transaction dicts.
    Uses TRANSACTION_SCHEMA to ensure types match what the streaming
    pipeline expects — catches type mismatches at test time, not runtime.
    """
    return spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)
