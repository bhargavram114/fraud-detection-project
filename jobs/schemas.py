"""
schemas.py — Single source of truth for all data contracts.

WHY A SEPARATE SCHEMAS FILE?
  In ATM software (NDC/ISO), every message type has a fixed byte layout
  defined in one place — both sender and receiver import the same spec.
  We follow the same principle here: every field any fraud rule references
  must be declared in this file. If producer and consumer disagree on a
  field name or type, Spark silently returns nulls — very hard to debug.

DESIGN RULE:
  Never define a StructType inline inside a rule or job file.
  Always import from here. This ensures:
    - Producer (transaction_generator.py) and consumer (fraud rules) agree
    - Schema changes are a one-line edit in one file
    - Tests validate the schema contract independently of rule logic
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType
)

# ── Transaction event schema ─────────────────────────────────────────────────
# Mirrors the fields a real ATM transaction carries:
#   card_id      → PAN (Primary Account Number), hashed for privacy
#   terminal_id  → ATM/POS terminal identifier (maps to physical device)
#   merchant_cat → Merchant Category Code (MCC) — equivalent to ATM type code
#   txn_status   → APPROVED | DECLINED — needed for Rule 12 (declined-then-approved)
#   event_time   → when the transaction occurred at the terminal (not server receipt time)
#                  Using event_time (not processing_time) is critical for correct
#                  window semantics — late-arriving events land in the right window

TRANSACTION_SCHEMA = StructType([
    StructField("txn_id",       StringType(),    False),  # unique transaction ID
    StructField("card_id",      StringType(),    False),  # hashed PAN
    StructField("account_id",   StringType(),    True),   # linked account
    StructField("amount",       DoubleType(),    False),  # transaction amount in INR
    StructField("currency",     StringType(),    True),   # ISO 4217 currency code
    StructField("merchant_id",  StringType(),    True),   # merchant / ATM operator ID
    StructField("merchant_cat", StringType(),    True),   # MCC equivalent
    StructField("terminal_id",  StringType(),    True),   # physical terminal ID
    StructField("country",      StringType(),    True),   # ISO 3166 country code
    StructField("city",         StringType(),    True),   # city of transaction
    StructField("txn_type",     StringType(),    True),   # WITHDRAWAL | PURCHASE | TRANSFER
    StructField("channel",      StringType(),    True),   # ATM | POS | ONLINE | MOBILE
    StructField("txn_status",   StringType(),    True),   # APPROVED | DECLINED
    StructField("event_time",   TimestampType(), False),  # terminal timestamp (event time)
])

# ── Alert output schema ───────────────────────────────────────────────────────
# Every fraud rule must produce a DataFrame conforming to this schema.
# The _to_alert() helper in transforms.py enforces this at runtime.
# alert_time is excluded from REQUIRED_ALERT_COLUMNS because it is generated
# at write time by the streaming wrapper (fraud_rules.py), not by the transform.

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

# Columns that every rule transform must produce (alert_time added later)
REQUIRED_ALERT_COLUMNS = [
    f.name for f in ALERT_SCHEMA.fields if f.name != "alert_time"
]
