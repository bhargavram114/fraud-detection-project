"""
fraud_rules.py — Streaming wrappers for all 12 fraud rule transforms.

Each function here:
  1. Applies withWatermark() to the streaming DataFrame
  2. Delegates to the matching pure transform in transforms.py
  3. Adds alert_time (generated at write time, not testable in batch)

Adding a new rule:
  - Write the transform in transforms.py (and its tests first)
  - Add a thin wrapper here
  - Register it in RULE_REGISTRY
  Nothing else changes.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp
from jobs.rules.transforms import (
    velocity_check, high_value_single, high_window_amount,
    geo_velocity, unusual_hour, round_amount_structuring,
    rapid_merchant_switch, cnp_spike, multi_card_terminal,
    sub_threshold_structuring, declined_then_approved,
    dormant_card as _dormant_card_transform,
)

WATERMARK = "2 minutes"


def _with_alert_time(df: DataFrame) -> DataFrame:
    return df.withColumn("alert_time", current_timestamp())


def rule_velocity_check(df: DataFrame) -> DataFrame:
    return _with_alert_time(velocity_check(df.withWatermark("event_time", WATERMARK)))

def rule_high_value_single(df: DataFrame) -> DataFrame:
    return _with_alert_time(high_value_single(df.withWatermark("event_time", WATERMARK)))

def rule_high_window_amount(df: DataFrame) -> DataFrame:
    return _with_alert_time(high_window_amount(df.withWatermark("event_time", WATERMARK)))

def rule_geo_velocity(df: DataFrame) -> DataFrame:
    return _with_alert_time(geo_velocity(df.withWatermark("event_time", WATERMARK)))

def rule_unusual_hour(df: DataFrame) -> DataFrame:
    return _with_alert_time(unusual_hour(df.withWatermark("event_time", WATERMARK)))

def rule_round_amount_structuring(df: DataFrame) -> DataFrame:
    return _with_alert_time(round_amount_structuring(df.withWatermark("event_time", WATERMARK)))

def rule_rapid_merchant_switch(df: DataFrame) -> DataFrame:
    return _with_alert_time(rapid_merchant_switch(df.withWatermark("event_time", WATERMARK)))

def rule_cnp_spike(df: DataFrame) -> DataFrame:
    return _with_alert_time(cnp_spike(df.withWatermark("event_time", WATERMARK)))

def rule_multi_card_terminal(df: DataFrame) -> DataFrame:
    return _with_alert_time(multi_card_terminal(df.withWatermark("event_time", WATERMARK)))

def rule_sub_threshold_structuring(df: DataFrame) -> DataFrame:
    return _with_alert_time(sub_threshold_structuring(df.withWatermark("event_time", WATERMARK)))

def rule_declined_then_approved(df: DataFrame) -> DataFrame:
    return _with_alert_time(declined_then_approved(df.withWatermark("event_time", WATERMARK)))

def rule_dormant_card(df: DataFrame, dormant_ref_df: DataFrame) -> DataFrame:
    """Stream-static join. Spark automatically broadcasts the static side."""
    return _with_alert_time(
        _dormant_card_transform(df.withWatermark("event_time", WATERMARK), dormant_ref_df)
    )


# ── Registry ──────────────────────────────────────────────────────────────────
RULE_REGISTRY = [
    rule_velocity_check,
    rule_high_value_single,
    rule_high_window_amount,
    rule_geo_velocity,
    rule_unusual_hour,
    rule_round_amount_structuring,
    rule_rapid_merchant_switch,
    rule_cnp_spike,
    rule_multi_card_terminal,
    rule_sub_threshold_structuring,
    rule_declined_then_approved,
    # rule_dormant_card excluded — needs dormant_ref_df argument
]
