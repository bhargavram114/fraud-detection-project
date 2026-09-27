"""
schemas.py — Single source of truth for all data contracts.
Every field any fraud rule references must be declared here.
"""
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType
)

TRANSACTION_SCHEMA = StructType([
    StructField("txn_id",       StringType(),    False),
    StructField("card_id",      StringType(),    False),
    StructField("account_id",   StringType(),    True),
    StructField("amount",       DoubleType(),    False),
    StructField("currency",     StringType(),    True),
    StructField("merchant_id",  StringType(),    True),
    StructField("merchant_cat", StringType(),    True),
    StructField("terminal_id",  StringType(),    True),
    StructField("country",      StringType(),    True),
    StructField("city",         StringType(),    True),
    StructField("txn_type",     StringType(),    True),
    StructField("channel",      StringType(),    True),
    StructField("txn_status",   StringType(),    True),   # APPROVED | DECLINED
    StructField("event_time",   TimestampType(), False),
])

ALERT_SCHEMA = StructType([
    StructField("card_id",        StringType(),    False),
    StructField("window_start",   TimestampType(), False),
    StructField("window_end",     TimestampType(), False),
    StructField("txn_count",      IntegerType(),   False),
    StructField("total_amount",   DoubleType(),    False),
    StructField("rule_triggered", StringType(),    False),
    StructField("risk_score",     IntegerType(),   False),
    StructField("alert_time",     TimestampType(), False),
])

REQUIRED_ALERT_COLUMNS = [f.name for f in ALERT_SCHEMA.fields
                           if f.name != "alert_time"]  # alert_time is generated
