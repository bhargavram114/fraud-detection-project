"""
fraud_rules.py — Streaming wrappers for all 12 fraud rule transforms.

═══════════════════════════════════════════════════════════════════════════════
RESPONSIBILITY OF THIS FILE:
  1. Apply withWatermark() to the incoming streaming DataFrame
  2. Delegate to the matching pure transform in transforms.py
  3. Add alert_time (current timestamp at the moment of detection)

WHAT THIS FILE DOES NOT DO:
  - No business logic (all in transforms.py)
  - No output sinks (all in fraud_detection.py / fraud_detection_prod.py)
  - No configuration (all in config/app.yaml)

WATERMARKING — WHY IT LIVES HERE AND NOT IN TRANSFORMS:
  withWatermark() raises AnalysisException on a batch DataFrame.
  Keeping it here means transforms.py stays batch-safe and fully testable.
  The streaming wrapper is not unit-tested directly — it is thin enough
  that correctness is verified through integration tests.

WATERMARK VALUE (2 minutes):
  Tells Spark: "if an event arrives more than 2 minutes late relative to
  the maximum event_time seen so far, drop it and do not update old windows."
  In ATM networks, network retries and switch failovers can delay events
  by seconds to minutes — 2 minutes covers >99% of realistic late arrivals
  without keeping too much state in memory.

  INTERVIEW TIP: "What happens if you set the watermark too short?"
    Late events are dropped → missed fraud detections.
  "What if you set it too long?"
    Windows stay in the state store longer → higher memory usage.
    Balance between completeness and memory cost.

ADDING A NEW RULE:
  1. Write tests in test_transforms.py (TDD Red phase)
  2. Implement the transform function in transforms.py (TDD Green phase)
  3. Add a one-liner wrapper here (same pattern as all functions below)
  4. Register it in RULE_REGISTRY
  That's it — no other files need changing.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp

from jobs.rules.transforms import (
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
    dormant_card as _dormant_card_transform,
)

# Watermark delay applied to ALL streaming rules.
# Centralised here — change once to affect every rule.
WATERMARK_DELAY = "2 minutes"


def _with_alert_time(df: DataFrame) -> DataFrame:
    """
    Append the detection timestamp to the alert.
    alert_time = when Spark detected the fraud pattern, NOT when the
    transaction occurred (that's window_start / event_time).
    Useful for SLA monitoring: how quickly did the pipeline detect the alert?
    """
    return df.withColumn("alert_time", current_timestamp())


# ── Rule wrappers ─────────────────────────────────────────────────────────────
# Each function:
#   1. Applies withWatermark to the streaming DataFrame
#   2. Passes it to the pure transform
#   3. Appends alert_time
# All wrappers follow the same 3-line pattern intentionally — consistency
# makes them easy to scan and verify at a glance.

def rule_velocity_check(df: DataFrame) -> DataFrame:
    return _with_alert_time(velocity_check(df.withWatermark("event_time", WATERMARK_DELAY)))

def rule_high_value_single(df: DataFrame) -> DataFrame:
    return _with_alert_time(high_value_single(df.withWatermark("event_time", WATERMARK_DELAY)))

def rule_high_window_amount(df: DataFrame) -> DataFrame:
    return _with_alert_time(high_window_amount(df.withWatermark("event_time", WATERMARK_DELAY)))

def rule_geo_velocity(df: DataFrame) -> DataFrame:
    return _with_alert_time(geo_velocity(df.withWatermark("event_time", WATERMARK_DELAY)))

def rule_unusual_hour(df: DataFrame) -> DataFrame:
    return _with_alert_time(unusual_hour(df.withWatermark("event_time", WATERMARK_DELAY)))

def rule_round_amount_structuring(df: DataFrame) -> DataFrame:
    return _with_alert_time(round_amount_structuring(df.withWatermark("event_time", WATERMARK_DELAY)))

def rule_rapid_merchant_switch(df: DataFrame) -> DataFrame:
    return _with_alert_time(rapid_merchant_switch(df.withWatermark("event_time", WATERMARK_DELAY)))

def rule_cnp_spike(df: DataFrame) -> DataFrame:
    return _with_alert_time(cnp_spike(df.withWatermark("event_time", WATERMARK_DELAY)))

def rule_multi_card_terminal(df: DataFrame) -> DataFrame:
    return _with_alert_time(multi_card_terminal(df.withWatermark("event_time", WATERMARK_DELAY)))

def rule_sub_threshold_structuring(df: DataFrame) -> DataFrame:
    return _with_alert_time(sub_threshold_structuring(df.withWatermark("event_time", WATERMARK_DELAY)))

def rule_declined_then_approved(df: DataFrame) -> DataFrame:
    return _with_alert_time(declined_then_approved(df.withWatermark("event_time", WATERMARK_DELAY)))

def rule_dormant_card(df: DataFrame, dormant_ref_df: DataFrame) -> DataFrame:
    """
    Stream-static join: streaming transactions against static dormant card list.
    Spark automatically broadcasts the static side (small Parquet file)
    to every executor — no explicit broadcast() call needed for stream-static.
    The static DataFrame is read once at job startup in fraud_detection_prod.py
    and reused for every micro-batch — it is NOT re-read per batch.
    """
    return _with_alert_time(
        _dormant_card_transform(df.withWatermark("event_time", WATERMARK_DELAY), dormant_ref_df)
    )


# ── Rule registry ─────────────────────────────────────────────────────────────
# All rules that take a single streaming DataFrame as input.
# rule_dormant_card is excluded — it requires a second argument
# (dormant_ref_df) and is called separately in the job files.
#
# The registry is iterated in apply_all_rules() — a failed rule logs an error
# and is skipped; it does NOT bring down the whole pipeline.

RULE_REGISTRY = [
    rule_velocity_check,           # Rule 1  — risk 85
    rule_high_value_single,        # Rule 2  — risk 80
    rule_high_window_amount,       # Rule 3  — risk 90
    rule_geo_velocity,             # Rule 4  — risk 95 (highest)
    rule_unusual_hour,             # Rule 5  — risk 50 (contextual)
    rule_round_amount_structuring, # Rule 6  — risk 75
    rule_rapid_merchant_switch,    # Rule 7  — risk 70
    rule_cnp_spike,                # Rule 8  — risk 65
    rule_multi_card_terminal,      # Rule 9  — risk 80
    rule_sub_threshold_structuring,# Rule 11 — risk 85
    rule_declined_then_approved,   # Rule 12 — risk 90
    # Rule 10 (rule_dormant_card) handled separately — needs dormant_ref_df
]
