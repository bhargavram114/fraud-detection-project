"""
transforms.py — Pure transformation functions for all 12 fraud rules.

KEY DESIGN DECISION (testability):
  withWatermark() only works on streaming DataFrames — calling it on a
  batch DataFrame raises AnalysisException. So concerns are separated:

    transforms.py   — pure functions, batch-safe, fully unit-testable
    fraud_rules.py  — streaming wrappers that add withWatermark() then
                      delegate to these transforms

Output contract (every function returns these columns):
  card_id, window_start, window_end, txn_count,
  total_amount, rule_triggered, risk_score
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, sum as _sum, max as _max,
    countDistinct, lit, when, hour, window,
)

# ── Window durations ──────────────────────────────────────────────────────────
W5M  = ("5 minutes",  "1 minute")
W10M = ("10 minutes", "2 minutes")
W30M = ("30 minutes", "5 minutes")
W1H  = ("1 hour",     "5 minutes")

# ── Risk scores (0–100) ───────────────────────────────────────────────────────
RISK = {
    "HIGH_VELOCITY":            85,
    "HIGH_VALUE_SINGLE_TXN":   80,
    "HIGH_WINDOW_AMOUNT":       90,
    "GEO_VELOCITY":             95,
    "UNUSUAL_HOUR":             50,
    "ROUND_AMOUNT_STRUCTURING": 75,
    "RAPID_MERCHANT_SWITCH":    70,
    "CNP_SPIKE":                65,
    "MULTI_CARD_TERMINAL":      80,
    "DORMANT_CARD":             60,
    "SUB_THRESHOLD_STRUCT":     85,
    "DECLINED_THEN_APPROVED":   90,
}


def _to_alert(df: DataFrame, rule_name: str) -> DataFrame:
    """Standardise alert columns so union() across rules is safe."""
    return df.select(
        col("card_id"),
        col("window_start"),
        col("window_end"),
        col("txn_count").cast("int"),
        col("total_amount").cast("double"),
        lit(rule_name).alias("rule_triggered"),
        lit(RISK[rule_name]).alias("risk_score"),
    )


# ── Rule 1: Velocity Check ────────────────────────────────────────────────────
def velocity_check(df: DataFrame) -> DataFrame:
    """Flag card with >5 transactions in any 5-minute sliding window."""
    agg = (
        df.groupBy(window(col("event_time"), *W5M), col("card_id"))
        .agg(
            count("txn_id").alias("txn_count"),
            _sum("amount").alias("total_amount"),
        )
        .filter(col("txn_count") > 5)
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
    )
    return _to_alert(agg, "HIGH_VELOCITY")


# ── Rule 2: High Value Single Transaction ─────────────────────────────────────
def high_value_single(df: DataFrame) -> DataFrame:
    """Flag any single transaction exceeding ₹30,000."""
    filtered = (
        df.filter(col("amount") > 30_000)
        .withColumn("window_start", col("event_time"))
        .withColumn("window_end",   col("event_time"))
        .withColumn("txn_count",    lit(1))
        .withColumn("total_amount", col("amount"))
    )
    return _to_alert(filtered, "HIGH_VALUE_SINGLE_TXN")


# ── Rule 3: High Window Amount ────────────────────────────────────────────────
def high_window_amount(df: DataFrame) -> DataFrame:
    """Flag when total spend per card exceeds ₹50,000 in any 5-min window."""
    agg = (
        df.groupBy(window(col("event_time"), *W5M), col("card_id"))
        .agg(
            count("txn_id").alias("txn_count"),
            _sum("amount").alias("total_amount"),
        )
        .filter(col("total_amount") > 50_000)
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
    )
    return _to_alert(agg, "HIGH_WINDOW_AMOUNT")


# ── Rule 4: Geographic Velocity ───────────────────────────────────────────────
def geo_velocity(df: DataFrame) -> DataFrame:
    """Flag same card used in 2+ countries within 30 minutes."""
    agg = (
        df.groupBy(window(col("event_time"), *W30M), col("card_id"))
        .agg(
            countDistinct("country").alias("country_count"),
            count("txn_id").alias("txn_count"),
            _sum("amount").alias("total_amount"),
        )
        .filter(col("country_count") > 1)
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
    )
    return _to_alert(agg, "GEO_VELOCITY")


# ── Rule 5: Unusual Hour ──────────────────────────────────────────────────────
def unusual_hour(df: DataFrame) -> DataFrame:
    """Flag transactions between 01:00–04:00 UTC with amount > ₹5,000."""
    filtered = (
        df.filter(
            hour(col("event_time")).between(1, 4) &
            (col("amount") > 5_000)
        )
        .withColumn("window_start", col("event_time"))
        .withColumn("window_end",   col("event_time"))
        .withColumn("txn_count",    lit(1))
        .withColumn("total_amount", col("amount"))
    )
    return _to_alert(filtered, "UNUSUAL_HOUR")


# ── Rule 6: Round Amount Structuring ─────────────────────────────────────────
def round_amount_structuring(df: DataFrame) -> DataFrame:
    """Flag 3+ exact-round-amount txns (amount % 1000 == 0) in 10 minutes."""
    agg = (
        df.filter(col("amount") % 1000 == 0)
        .groupBy(window(col("event_time"), *W10M), col("card_id"))
        .agg(
            count("txn_id").alias("txn_count"),
            _sum("amount").alias("total_amount"),
        )
        .filter(col("txn_count") >= 3)
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
    )
    return _to_alert(agg, "ROUND_AMOUNT_STRUCTURING")


# ── Rule 7: Rapid Merchant Switching ─────────────────────────────────────────
def rapid_merchant_switch(df: DataFrame) -> DataFrame:
    """Flag card used at 4+ distinct merchants in any 5-minute window."""
    agg = (
        df.groupBy(window(col("event_time"), *W5M), col("card_id"))
        .agg(
            countDistinct("merchant_id").alias("merchant_count"),
            count("txn_id").alias("txn_count"),
            _sum("amount").alias("total_amount"),
        )
        .filter(col("merchant_count") > 4)
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
    )
    return _to_alert(agg, "RAPID_MERCHANT_SWITCH")


# ── Rule 8: Card-Not-Present Spike ───────────────────────────────────────────
def cnp_spike(df: DataFrame) -> DataFrame:
    """Flag 3+ ONLINE transactions from one card in 10 minutes."""
    agg = (
        df.filter(col("channel") == "ONLINE")
        .groupBy(window(col("event_time"), *W10M), col("card_id"))
        .agg(
            count("txn_id").alias("txn_count"),
            _sum("amount").alias("total_amount"),
        )
        .filter(col("txn_count") > 3)
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
    )
    return _to_alert(agg, "CNP_SPIKE")


# ── Rule 9: Multiple Cards at Same Terminal ───────────────────────────────────
def multi_card_terminal(df: DataFrame) -> DataFrame:
    """Flag ATM terminal with 5+ distinct cards in 10 minutes (skimmer)."""
    agg = (
        df.groupBy(window(col("event_time"), *W10M), col("terminal_id"))
        .agg(
            countDistinct("card_id").alias("card_count"),
            count("txn_id").alias("txn_count"),
            _sum("amount").alias("total_amount"),
            _max("card_id").alias("card_id"),
        )
        .filter(col("card_count") > 5)
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
    )
    return _to_alert(agg, "MULTI_CARD_TERMINAL")


# ── Rule 10: Dormant Card Activation ─────────────────────────────────────────
def dormant_card(df: DataFrame, dormant_ref_df: DataFrame) -> DataFrame:
    """
    Flag transactions by cards inactive for 30+ days.
    dormant_ref_df: DataFrame with a single 'card_id' column.
    In the streaming job this is a static DataFrame — Spark automatically
    broadcasts the smaller side of the join.
    """
    filtered = (
        df.join(dormant_ref_df.select("card_id"), on="card_id", how="inner")
        .withColumn("window_start", col("event_time"))
        .withColumn("window_end",   col("event_time"))
        .withColumn("txn_count",    lit(1))
        .withColumn("total_amount", col("amount"))
    )
    return _to_alert(filtered, "DORMANT_CARD")


# ── Rule 11: Sub-Threshold Structuring ───────────────────────────────────────
def sub_threshold_structuring(df: DataFrame) -> DataFrame:
    """
    Flag 5+ txns between ₹9,000–₹9,999 in 1 hour.
    Detects structuring to avoid the ₹10,000 RBI/FIU-IND reporting threshold.
    """
    agg = (
        df.filter((col("amount") >= 9_000) & (col("amount") < 10_000))
        .groupBy(window(col("event_time"), *W1H), col("card_id"))
        .agg(
            count("txn_id").alias("txn_count"),
            _sum("amount").alias("total_amount"),
        )
        .filter(col("txn_count") >= 5)
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
    )
    return _to_alert(agg, "SUB_THRESHOLD_STRUCT")


# ── Rule 12: Declined then Approved ──────────────────────────────────────────
def declined_then_approved(df: DataFrame) -> DataFrame:
    """
    Flag card with 2+ DECLINED txns followed by an APPROVED txn
    in the same 10-minute window. Detects PIN brute-force / card testing.

    Uses conditional aggregation (single groupBy) instead of a
    stream-stream join — simpler state management, fully testable in batch.
    """
    agg = (
        df.groupBy(window(col("event_time"), *W10M), col("card_id"))
        .agg(
            count(when(col("txn_status") == "DECLINED", 1)).alias("decline_count"),
            count(when(col("txn_status") == "APPROVED", 1)).alias("approve_count"),
            count("txn_id").alias("txn_count"),
            _sum("amount").alias("total_amount"),
        )
        .filter(
            (col("decline_count") >= 2) &
            (col("approve_count") >= 1)
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
    )
    return _to_alert(agg, "DECLINED_THEN_APPROVED")


# ── Registry ──────────────────────────────────────────────────────────────────
# dormant_card excluded — needs a second argument (reference DataFrame)
STATELESS_TRANSFORMS = [
    velocity_check,
    high_value_single,
    high_window_amount,
    geo_velocity,
    unusual_hour,
    round_amount_structuring,
    rapid_merchant_switch,
    cnp_spike,
    multi_card_terminal,
    sub_threshold_structuring,
    declined_then_approved,
]
