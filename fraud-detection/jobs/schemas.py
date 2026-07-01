"""
schemas.py
Centralised schema definitions for the fraud detection pipeline.
Keeping schemas in one place means the producer and consumer
always agree on the contract — same idea as your NDC/ISO message
format definitions in ATM software.
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType, BooleanType
)

# ── Raw transaction event coming off Kafka ──────────────────────────────────
TRANSACTION_SCHEMA = StructType([
    StructField("txn_id",       StringType(),    nullable=False),
    StructField("card_id",      StringType(),    nullable=False),
    StructField("account_id",   StringType(),    nullable=False),
    StructField("amount",       DoubleType(),    nullable=False),
    StructField("currency",     StringType(),    nullable=True),
    StructField("merchant_id",  StringType(),    nullable=True),
    StructField("merchant_cat", StringType(),    nullable=True),  # MCC code equivalent
    StructField("terminal_id",  StringType(),    nullable=True),  # ATM / POS terminal
    StructField("country",      StringType(),    nullable=True),
    StructField("city",         StringType(),    nullable=True),
    StructField("txn_type",     StringType(),    nullable=True),  # withdrawal, purchase, transfer
    StructField("channel",      StringType(),    nullable=True),  # ATM, online, POS
    StructField("event_time",   TimestampType(), nullable=False),
])

# ── Fraud alert schema written to the output Kafka topic ────────────────────
FRAUD_ALERT_SCHEMA = StructType([
    StructField("card_id",       StringType(),   nullable=False),
    StructField("window_start",  TimestampType(),nullable=False),
    StructField("window_end",    TimestampType(),nullable=False),
    StructField("txn_count",     IntegerType(),  nullable=False),
    StructField("total_amount",  DoubleType(),   nullable=False),
    StructField("rule_triggered",StringType(),   nullable=False),
    StructField("alert_time",    TimestampType(),nullable=False),
])
