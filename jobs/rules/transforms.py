"""
transforms.py — Pure transformation functions for all 12 fraud rules.

═══════════════════════════════════════════════════════════════════════════════
KEY DESIGN DECISION — WHY SEPARATE TRANSFORMS FROM STREAMING WRAPPERS?
═══════════════════════════════════════════════════════════════════════════════
  withWatermark() only matters for streaming queries. On a batch DataFrame it
  is accepted and has no effect (verified on Spark 3.5.0 — it does NOT raise
  AnalysisException). We still keep it out of the rule logic because the
  watermark delay is a streaming/operational setting, not business logic:
  one delay is applied to every rule in one place, and the rules stay pure.

  Two layers:

    transforms.py  (THIS FILE)
      Pure functions. Accept any DataFrame — batch or streaming.
      No withWatermark(). No writeStream. Just transformation logic.
      → Fully unit-testable with spark.createDataFrame() in pytest.

    fraud_rules.py (STREAMING WRAPPERS)
      Thin wrappers. Call df.withWatermark(...) then delegate here.
      → Covered by tests/test_streaming_smoke.py (runs a real streaming query).

  This mirrors how you'd design it in C#:
    Business logic in a service class (testable, no I/O).
    Infrastructure concerns in a wrapper/adapter (hard to test, minimal code).

═══════════════════════════════════════════════════════════════════════════════
OUTPUT CONTRACT — every function must return a DataFrame with these columns:
  card_id, window_start, window_end, txn_count, total_amount,
  rule_triggered, risk_score
  (alert_time is added by the streaming wrapper at write time)
═══════════════════════════════════════════════════════════════════════════════

ADDING A NEW RULE:
  1. Write tests in tests/test_transforms.py first (TDD Red)
  2. Add a function here following the same pattern (TDD Green)
  3. Add a thin wrapper in fraud_rules.py
  4. Register it in STATELESS_TRANSFORMS (or handle separately if it needs
     extra args like dormant_card does)
  Nothing else changes.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, sum as _sum, max as _max,
    countDistinct, lit, when, hour, window,
    collect_list, sort_array, from_utc_timestamp,
    md5, concat_ws, unix_timestamp, floor,
)

from jobs.rules.thresholds import resolve

# Every rule takes an optional `thresholds` dict (see thresholds.py). None means
# the defaults, which mirror config/app.yaml. The prod job passes the values
# loaded from app.yaml so tuning the YAML really changes behaviour.

# ── Window size constants ─────────────────────────────────────────────────────
# Format: (duration, slide_interval)
# Sliding windows overlap — a transaction can fall into multiple windows.
# This ensures a fraud burst that straddles a boundary is still caught.
#
# INTERVIEW TIP: "Why sliding and not tumbling?"
#   Tumbling = non-overlapping. A burst of 6 txns split across two 5-min
#   tumbling windows (3 in each) would not trigger the velocity rule.
#   Sliding with 1-min slide means the burst appears in up to 5 windows —
#   at least one of them will contain all 6 txns and fire the alert.

W5M  = ("5 minutes",  "1 minute")   # velocity, high-amount, merchant-switch
W10M = ("10 minutes", "2 minutes")  # CNP spike, multi-card, declined-then-approved
W30M = ("30 minutes", "5 minutes")  # geo-velocity (impossible travel window)
W1H  = ("1 hour",     "5 minutes")  # sub-threshold structuring (regulatory)

# ── Risk scores (0–100) ───────────────────────────────────────────────────────
# Used downstream for alert prioritisation — case management systems triage
# by risk score. 90+ = automatic block; 70–89 = human review; <70 = monitor.
RISK = {
    "HIGH_VELOCITY":            85,
    "HIGH_VALUE_SINGLE_TXN":   80,
    "HIGH_WINDOW_AMOUNT":       90,
    "GEO_VELOCITY":             95,   # highest — physically impossible
    "UNUSUAL_HOUR":             50,   # contextual signal only
    "ROUND_AMOUNT_STRUCTURING": 75,
    "RAPID_MERCHANT_SWITCH":    70,
    "CNP_SPIKE":                65,
    "MULTI_CARD_TERMINAL":      80,
    "DORMANT_CARD":             60,
    "SUB_THRESHOLD_STRUCT":     85,   # regulatory compliance rule
    "DECLINED_THEN_APPROVED":   90,   # PIN brute-force indicator
}


def _to_alert(df: DataFrame, rule_name: str) -> DataFrame:
    """
    Standardise output columns so union() across all 12 rules is type-safe.

    Every rule produces different intermediate columns (e.g. merchant_count,
    country_count, decline_count). This function projects them all down to
    the common alert contract, casting types explicitly to avoid schema
    mismatches that would silently produce nulls in Parquet output.
    """
    return df.select(
        col("card_id"),
        col("window_start"),
        col("window_end"),
        col("txn_count").cast("int"),
        col("total_amount").cast("double"),
        lit(rule_name).alias("rule_triggered"),
        lit(RISK[rule_name]).alias("risk_score"),
    )


def with_alert_id(df: DataFrame, bucket_seconds: int = 3600) -> DataFrame:
    """
    Add a deterministic `alert_id` used downstream as an idempotency key.

    THE PROBLEM:
      With sliding windows one fraud burst lands in several overlapping windows
      (a 5-min window sliding every 1 min -> up to 5 alerts, each with a
      different window_start). Hashing window_start directly gave one ID per
      window, i.e. several IDs for one incident. Bucketing by the window's own
      length does not help: the windows that contain a burst span several
      minutes of start times, so they often straddle a bucket edge anyway.

    THE FIX — alert throttling per (card, rule, time bucket):
      - Aggregated rules: hash (card_id, rule, floor(window_start / bucket)),
        bucket = 1 hour by default. All overlapping windows of one burst share
        an ID unless the burst straddles a clock-hour boundary (then 2 IDs).
        TRADE-OFF: two separate bursts on the same card+rule inside one bucket
        also share an ID, so an idempotent consumer keeps only the first.
        That is deliberate ("one open alert per card per rule per hour") but is
        a policy choice; lower bucket_seconds for finer alerts.
      - Row-level rules (window_start == window_end, i.e. rules 2, 5, 10):
        hash the exact event timestamp, so two large withdrawals seconds apart
        stay two distinct alerts.

    KNOWN LIMIT: Rule 9 groups by terminal and reports a representative card
    (max card_id), which can differ between overlapping windows.
    """
    is_row_level = col("window_end") <= col("window_start")
    slot = when(is_row_level, col("window_start").cast("string")).otherwise(
        floor(unix_timestamp(col("window_start")) / bucket_seconds).cast("string")
    )
    return df.withColumn(
        "alert_id",
        md5(concat_ws("|", col("card_id"), col("rule_triggered"), slot)),
    )


# ══════════════════════════════════════════════════════════════════════════════
# RULE 1 — Velocity Check
# ══════════════════════════════════════════════════════════════════════════════
def velocity_check(df: DataFrame, thresholds: dict | None = None) -> DataFrame:
    """
    Flag any card with more than 5 transactions in a 5-minute sliding window.

    WHY THIS CATCHES FRAUD:
      A stolen card or cloned card is typically used in rapid succession —
      the attacker runs as many transactions as possible before the card
      is blocked. Legitimate cardholders rarely make 6+ transactions
      in any 5-minute period.

    WINDOW AGGREGATION:
      groupBy(window(...), card_id) creates a stateful operator.
      Spark maintains a state store (RocksDB by default in production)
      keyed by (window_start, window_end, card_id).
      Each incoming event updates the count in the relevant window(s).

    INTERVIEW TIP:
      "What happens to windows in memory?"
      Without watermarking (applied in the streaming wrapper), windows
      accumulate in the state store forever → OOM. The watermark tells
      Spark it's safe to evict windows older than (max_event_time - delay).
    """
    t = resolve(thresholds)
    agg = (
        df.groupBy(window(col("event_time"), *W5M), col("card_id"))
        .agg(
            count("txn_id").alias("txn_count"),
            _sum("amount").alias("total_amount"),
        )
        .filter(col("txn_count") > t["velocity_txn_limit"])
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
    )
    return _to_alert(agg, "HIGH_VELOCITY")


# ══════════════════════════════════════════════════════════════════════════════
# RULE 2 — High Value Single Transaction
# ══════════════════════════════════════════════════════════════════════════════
def high_value_single(df: DataFrame, thresholds: dict | None = None) -> DataFrame:
    """
    Flag any single transaction exceeding ₹30,000.

    WHY NO WINDOW?
      This is a stateless row-level filter — no aggregation needed.
      Each transaction is evaluated independently. This makes it the
      cheapest rule to execute (no state store, no shuffle).

    NOTE ON window_start / window_end:
      We set both to event_time so the output schema matches all other
      rules, allowing clean union() in apply_all_rules(). The values
      are semantically "the transaction happened at this instant".

    INTERVIEW TIP:
      "Why is outputMode('append') required for file sinks with watermarked
      aggregations, but this rule could technically use any output mode?"
      Row-level filters with no aggregation produce append-only output
      naturally — each event either matches or doesn't, with no updates.
    """
    t = resolve(thresholds)
    filtered = (
        df.filter(col("amount") > t["high_value_single"])
        .withColumn("window_start", col("event_time"))
        .withColumn("window_end",   col("event_time"))
        .withColumn("txn_count",    lit(1))
        .withColumn("total_amount", col("amount"))
    )
    return _to_alert(filtered, "HIGH_VALUE_SINGLE_TXN")


# ══════════════════════════════════════════════════════════════════════════════
# RULE 3 — High Window Amount
# ══════════════════════════════════════════════════════════════════════════════
def high_window_amount(df: DataFrame, thresholds: dict | None = None) -> DataFrame:
    """
    Flag when a card's total spend exceeds ₹50,000 in any 5-minute window.

    DIFFERENCE FROM RULE 1:
      Rule 1 catches high-frequency attacks (many small transactions).
      Rule 3 catches high-value attacks (few large transactions that
      individually don't cross Rule 2's single-txn threshold).
      Example: 3 × ₹18,000 = ₹54,000 — misses Rule 1 and Rule 2,
      but correctly triggers Rule 3.

    Together, Rules 1, 2, and 3 cover the full fraud surface for
    amount-based card abuse.
    """
    t = resolve(thresholds)
    agg = (
        df.groupBy(window(col("event_time"), *W5M), col("card_id"))
        .agg(
            count("txn_id").alias("txn_count"),
            _sum("amount").alias("total_amount"),
        )
        .filter(col("total_amount") > t["velocity_window_amount"])
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
    )
    return _to_alert(agg, "HIGH_WINDOW_AMOUNT")


# ══════════════════════════════════════════════════════════════════════════════
# RULE 4 — Geographic Velocity (Impossible Travel)
# ══════════════════════════════════════════════════════════════════════════════
def geo_velocity(df: DataFrame, thresholds: dict | None = None) -> DataFrame:
    """
    Flag the same card used in 2+ different countries within 30 minutes.

    WHY THIS IS HIGH CONFIDENCE (risk score 95):
      It is physically impossible to be in India and the UAE within 30
      minutes. If the same card fires transactions in both countries in
      that window, one of them is fraudulent — card data was stolen and
      used remotely while the cardholder is still present in one location.

    PRODUCTION ENHANCEMENT:
      Country-level detection is a coarse proxy. In a full implementation
      you would use lat/long coordinates and calculate Haversine distance,
      then divide by time delta to get implied travel speed.
      Flag if speed > 900 km/h (speed of sound — faster than any aircraft).
      Spark UDF or a pre-computed distance lookup table handles this.

    INTERVIEW TIP:
      "How would you handle the case where the cardholder is genuinely
      travelling between nearby countries (India → Sri Lanka)?"
      Combine with velocity and amount rules — a single low-value
      cross-border txn is likely legitimate; it's the burst pattern
      that confirms fraud. Risk-score weighting helps here.
    """
    t = resolve(thresholds)
    agg = (
        df.groupBy(window(col("event_time"), *W30M), col("card_id"))
        .agg(
            countDistinct("country").alias("country_count"),
            count("txn_id").alias("txn_count"),
            _sum("amount").alias("total_amount"),
        )
        .filter(col("country_count") >= t["geo_velocity_countries"])
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
    )
    return _to_alert(agg, "GEO_VELOCITY")


# ══════════════════════════════════════════════════════════════════════════════
# RULE 5 — Unusual Hour Transaction
# ══════════════════════════════════════════════════════════════════════════════
def unusual_hour(df: DataFrame, thresholds: dict | None = None) -> DataFrame:
    """
    Flag transactions in [01:00, 04:00) LOCAL time with amount > ₹5,000.

    WHY BOTH CONDITIONS?
      Late-night ATM activity is statistically anomalous but not impossible.
      Requiring amount > ₹5,000 filters out legitimate small purchases
      (petrol, convenience store) that happen at odd hours, and focuses
      the rule on significant withdrawals/transfers where risk is higher.

    TIMEZONE HANDLING:
      event_time is stored in UTC, but "unusual" is a local-time notion
      (2AM UTC = 7:30AM IST is a normal morning). The hour is computed with
      from_utc_timestamp(event_time, unusual_hour_timezone). app.yaml sets
      Asia/Kolkata for this INR pipeline; the code default is UTC so unit
      tests are timezone-independent. One timezone per pipeline is still a
      simplification — a multi-country deployment would join a per-terminal
      timezone lookup instead.

    BOUNDARIES:
      Half-open interval [start, end): 01:00:00 is flagged, 04:00:00 is not.
      (The old between(1, 4) also flagged everything up to 04:59.)

    LOWEST RISK SCORE (50):
      This rule is a contextual signal, not a standalone fraud indicator.
      It should be combined with other rules in a risk-scoring engine
      rather than used to block transactions outright.
    """
    t = resolve(thresholds)
    local_hour = hour(from_utc_timestamp(col("event_time"), t["unusual_hour_timezone"]))
    filtered = (
        df.filter(
            (local_hour >= t["unusual_hour_start"]) &
            (local_hour <  t["unusual_hour_end"]) &
            (col("amount") > t["unusual_hour_min_amount"])
        )
        .withColumn("window_start", col("event_time"))
        .withColumn("window_end",   col("event_time"))
        .withColumn("txn_count",    lit(1))
        .withColumn("total_amount", col("amount"))
    )
    return _to_alert(filtered, "UNUSUAL_HOUR")


# ══════════════════════════════════════════════════════════════════════════════
# RULE 6 — Round Amount Structuring
# ══════════════════════════════════════════════════════════════════════════════
def round_amount_structuring(df: DataFrame, thresholds: dict | None = None) -> DataFrame:
    """
    Flag 3+ transactions of exact round amounts (divisible by 1000) in 10 min.

    WHAT IS STRUCTURING?
      'Structuring' (also called 'smurfing') is the practice of breaking
      large transactions into smaller ones to avoid regulatory reporting
      thresholds or fraud detection rules. Money mules and fraudsters
      often transact in psychologically round amounts (₹5,000, ₹10,000)
      because they are working from a script, not making organic purchases.
      Legitimate consumers almost never spend exactly ₹5,000 repeatedly.

    REGULATORY CONTEXT (India):
      RBI / FIU-IND require banks to file Currency Transaction Reports (CTRs)
      for cash transactions above ₹10 lakh. Structuring to avoid this
      threshold violates the Prevention of Money Laundering Act (PMLA) 2002.

    INTERVIEW TIP:
      This rule demonstrates domain knowledge beyond pure engineering.
      Mention PMLA and FIU-IND in interviews — it shows you understand
      why the rule exists, not just how to implement it.
    """
    t = resolve(thresholds)
    agg = (
        df.filter(col("amount") % t["round_amount_modulo"] == 0)     # exact round thousands only
        .groupBy(window(col("event_time"), *W10M), col("card_id"))
        .agg(
            count("txn_id").alias("txn_count"),
            _sum("amount").alias("total_amount"),
        )
        .filter(col("txn_count") >= t["round_amount_min_txns"])
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
    )
    return _to_alert(agg, "ROUND_AMOUNT_STRUCTURING")


# ══════════════════════════════════════════════════════════════════════════════
# RULE 7 — Rapid Merchant Switching
# ══════════════════════════════════════════════════════════════════════════════
def rapid_merchant_switch(df: DataFrame, thresholds: dict | None = None) -> DataFrame:
    """
    Flag a card used at 4+ distinct merchants within a 5-minute window.

    WHY THIS PATTERN INDICATES FRAUD:
      Legitimate cardholders visit one merchant at a time and take minutes
      to browse, select, and complete a purchase. A stolen card or cloned
      card is 'tested' across multiple merchants rapidly — the attacker
      is verifying the card works before attempting a large purchase.
      This is also called 'card testing' or 'carding'.

    countDistinct vs count:
      We use countDistinct("merchant_id") not count("txn_id").
      Multiple transactions at the same merchant (e.g. a split bill)
      are fine — it's visiting many different merchants that's suspicious.

    INTERVIEW TIP:
      "When would you use approxCountDistinct instead of countDistinct?"
      For very high cardinality at massive scale, HyperLogLog
      (approxCountDistinct) is much cheaper — O(1) memory vs O(n).
      Here, merchant_count per card per 5 min is tiny, so exact is fine.
    """
    t = resolve(thresholds)
    agg = (
        df.groupBy(window(col("event_time"), *W5M), col("card_id"))
        .agg(
            countDistinct("merchant_id").alias("merchant_count"),
            count("txn_id").alias("txn_count"),
            _sum("amount").alias("total_amount"),
        )
        .filter(col("merchant_count") > t["rapid_merchant_distinct"])
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
    )
    return _to_alert(agg, "RAPID_MERCHANT_SWITCH")


# ══════════════════════════════════════════════════════════════════════════════
# RULE 8 — Card-Not-Present (CNP) Spike
# ══════════════════════════════════════════════════════════════════════════════
def cnp_spike(df: DataFrame, thresholds: dict | None = None) -> DataFrame:
    """
    Flag 3+ ONLINE (Card-Not-Present) transactions from one card in 10 minutes.

    WHAT IS A CNP TRANSACTION?
      Card-Not-Present means the physical card is not swiped/tapped —
      the cardholder (or fraudster) enters card details online or by phone.
      CNP fraud is the dominant form of digital card fraud because stolen
      card details (number, expiry, CVV) are sufficient — no physical card needed.

    WHY PRE-FILTER BEFORE WINDOWING?
      df.filter(col("channel") == "ONLINE") reduces the input size before
      the expensive groupBy + window operation. The state store only holds
      ONLINE transactions — typically 20–30% of total volume.
      Pre-filtering before aggregation is a key PySpark performance pattern.

    INTERVIEW TIP:
      "Card details sold on dark web marketplaces (like carding forums)
      are used exclusively online. The buyer has the numbers but not the
      physical card. This rule specifically targets that attack vector."
    """
    t = resolve(thresholds)
    agg = (
        df.filter(col("channel") == "ONLINE")    # pre-filter before windowing
        .groupBy(window(col("event_time"), *W10M), col("card_id"))
        .agg(
            count("txn_id").alias("txn_count"),
            _sum("amount").alias("total_amount"),
        )
        .filter(col("txn_count") > t["cnp_spike_txns"])
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
    )
    return _to_alert(agg, "CNP_SPIKE")


# ══════════════════════════════════════════════════════════════════════════════
# RULE 9 — Multiple Cards at Same Terminal
# ══════════════════════════════════════════════════════════════════════════════
def multi_card_terminal(df: DataFrame, thresholds: dict | None = None) -> DataFrame:
    """
    Flag an ATM terminal with 5+ distinct cards transacting within 10 minutes.

    WHY THIS DETECTS SKIMMERS:
      A hardware skimmer attached to an ATM captures card data for every
      card inserted while it's installed. The fraudster then clones those
      cards and uses them simultaneously (or in quick succession) at other
      locations. The indicator is: many different cards flowing through
      the SAME compromised terminal in a short window.

    KEY DIFFERENCE FROM OTHER RULES:
      This rule groups by terminal_id, not card_id.
      The victim is the terminal (and indirectly, all its users).
      The card_id in the output is a representative card for alert routing —
      in production you'd alert on the terminal and notify all cards that
      transacted at it within the detected window.

    INTERVIEW TIP:
      "This rule inverts the grouping key. Most rules ask 'is this card
      behaving strangely?' — this one asks 'is this terminal behaving
      strangely?' It's a supply-side vs. demand-side perspective on fraud."
    """
    t = resolve(thresholds)
    agg = (
        df.groupBy(window(col("event_time"), *W10M), col("terminal_id"))
        .agg(
            countDistinct("card_id").alias("card_count"),
            count("txn_id").alias("txn_count"),
            _sum("amount").alias("total_amount"),
            _max("card_id").alias("card_id"),   # representative card for output schema
        )
        .filter(col("card_count") > t["multi_card_terminal"])
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
    )
    return _to_alert(agg, "MULTI_CARD_TERMINAL")


# ══════════════════════════════════════════════════════════════════════════════
# RULE 10 — Dormant Card Sudden Activation
# ══════════════════════════════════════════════════════════════════════════════
def dormant_card(df: DataFrame, dormant_ref_df: DataFrame) -> DataFrame:
    """
    Flag transactions from cards that have been inactive for 30+ days.

    HOW THE DORMANT LIST IS BUILT:
      A nightly Airflow DAG (batch job) queries the transaction history,
      finds all card_ids with no activity in the last 30 days, and writes
      that list to a Parquet file (the dormant reference dataset).
      At streaming job startup, fraud_detection_prod.py reads this file
      as a static DataFrame and passes it here.

    STREAM-STATIC JOIN PATTERN (behaviour verified on Spark 3.5.0):
      One side is the live event stream, the other a static lookup table.
      - The static side is broadcast (BroadcastHashJoin) as long as its
        estimated size is under spark.sql.autoBroadcastJoinThreshold (10 MB
        by default). A large dormant list would fall back to a shuffle join,
        so keep the list small or raise the threshold / wrap it in broadcast().
      - The static DataFrame is scanned again in every micro-batch, but its
        FILE LIST is fixed when the DataFrame is created. Files the nightly
        job adds afterwards are NOT seen until the streaming job restarts.
      - Stream-stream joins, by contrast, need both sides watermarked and keep
        join state.

    WHY DORMANT CARD FRAUD HAPPENS:
      Fraudsters sometimes obtain card details but wait weeks or months
      before using them — waiting for the cardholder to forget about the
      card or for fraud monitoring to relax. A sudden activation after
      a long dormancy period is a strong signal.

    INTERVIEW TIP:
      This is the Lambda Architecture pattern in practice:
        Batch layer  → nightly job computes dormant cards
        Speed layer  → streaming job joins against the batch output
      Mention Kappa Architecture as an alternative (pure streaming,
      no separate batch) for bonus points.
    """
    filtered = (
        # Inner join — only rows where card_id appears in the dormant list
        df.join(dormant_ref_df.select("card_id"), on="card_id", how="inner")
        .withColumn("window_start", col("event_time"))
        .withColumn("window_end",   col("event_time"))
        .withColumn("txn_count",    lit(1))
        .withColumn("total_amount", col("amount"))
    )
    return _to_alert(filtered, "DORMANT_CARD")


# ══════════════════════════════════════════════════════════════════════════════
# RULE 11 — Sub-Threshold Structuring
# ══════════════════════════════════════════════════════════════════════════════
def sub_threshold_structuring(df: DataFrame, thresholds: dict | None = None) -> DataFrame:
    """
    Flag 5+ transactions each between ₹9,000–₹9,999 within a 1-hour window.

    THE ₹10,000 REGULATORY THRESHOLD:
      Banks in India are required to report individual cash transactions
      above ₹10,000 to RBI / FIU-IND under the Prevention of Money
      Laundering Act (PMLA) 2002. Fraudsters and money launderers
      deliberately keep each transaction just below ₹10,000 to avoid
      triggering these reports. Repeated sub-threshold transactions that
      collectively represent a large transfer are the tell.

    WHY A 1-HOUR WINDOW (not 5 or 10 minutes)?
      Sophisticated structurers space transactions to avoid velocity rules.
      A 1-hour window with a 5-minute slide catches the pattern even when
      transactions are spread across the hour rather than bunched together.

    DISTINCTION FROM RULE 6 (Round Amount Structuring):
      Rule 6 catches psychologically round amounts (₹5,000, ₹10,000).
      Rule 11 catches amounts just BELOW a regulatory threshold (₹9,800,
      ₹9,950, ₹9,999) — a different and more deliberate structuring pattern.
      Both rules can fire on the same card simultaneously, which increases
      the combined risk signal.
    """
    t = resolve(thresholds)
    agg = (
        df.filter((col("amount") >= t["sub_threshold_low"]) & (col("amount") < t["sub_threshold_high"]))
        .groupBy(window(col("event_time"), *W1H), col("card_id"))
        .agg(
            count("txn_id").alias("txn_count"),
            _sum("amount").alias("total_amount"),
        )
        .filter(col("txn_count") >= t["sub_threshold_min_txns"])
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
    )
    return _to_alert(agg, "SUB_THRESHOLD_STRUCT")


# ══════════════════════════════════════════════════════════════════════════════
# RULE 12 — Declined then Approved
# ══════════════════════════════════════════════════════════════════════════════
def declined_then_approved(df: DataFrame, thresholds: dict | None = None) -> DataFrame:
    """
    Flag a card with 2+ DECLINED transactions followed by an APPROVED one
    within the same 10-minute window. ORDER MATTERS: the approval must come
    after the Nth decline (N = decline_min_count).

    WHY THIS DETECTS PIN BRUTE-FORCE / CARD TESTING:
      When a stolen card is used at an ATM with an unknown PIN, the attacker
      tries multiple PINs until one works (or the card is swallowed after 3
      attempts). In online fraud, card details may be incomplete (missing CVV
      or billing address) — the fraudster probes multiple merchants until one
      approves. This rule detects the pattern of multiple failures followed
      by a success.

    YOUR ATM BACKGROUND ADVANTAGE:
      In NDC protocol, response code 'Unable to Process' and ISO response
      code '05 (Do Not Honour)' are decline codes you've handled in
      Activate Enterprise. This rule detects exactly the pattern you'd see
      in ATM switch logs before a successful authorisation — now at the
      analytics layer instead of the protocol layer.

    IMPLEMENTATION CHOICE — Conditional Aggregation vs Stream-Stream Join:
      Option A (original): stream-stream join between a declines stream and
        an approvals stream. Requires both sides to be watermarked, complex
        state management, harder to test.
      Option B (this implementation): single groupBy with conditional
        aggregation using when(). One pass, no join, minimal extra state,
        fully testable in batch mode.

        count(when(col("txn_status") == "DECLINED", 1))
          → counts only DECLINED rows within each window group
        count(when(col("txn_status") == "APPROVED", 1))
          → counts only APPROVED rows within each window group

      Option B needs no join state and is simpler to test.

    HOW ORDER IS ENFORCED (single aggregation, still no join):
      decline_times      = sorted list of DECLINED event_times in the window
      last_approval_time = latest APPROVED event_time in the window
      Alert only if last_approval_time > decline_times[N-1], i.e. some approval
      happened after at least N declines. "approvals first, declines later" and
      "1 decline, approval, 1 decline" no longer fire.
      collect_list only holds DECLINED timestamps for one card in one window,
      which is small, so the extra state is negligible.

    INTERVIEW TIP:
      If asked "why not a stream-stream join?", explain the trade-offs:
      stream-stream joins require both sides watermarked, create two
      separate state stores, and complicate checkpoint recovery. The
      conditional aggregation achieves the same logic in one operator.
    """
    t = resolve(thresholds)
    n = t["decline_min_count"]
    agg = (
        df.groupBy(window(col("event_time"), *W10M), col("card_id"))
        .agg(
            # Conditional counts — count only rows matching each status
            count(when(col("txn_status") == "DECLINED", 1)).alias("decline_count"),
            count(when(col("txn_status") == "APPROVED", 1)).alias("approve_count"),
            # Ordering evidence: when did declines happen, when was the last approval?
            sort_array(
                collect_list(when(col("txn_status") == "DECLINED", col("event_time")))
            ).alias("decline_times"),
            _max(
                when(col("txn_status") == "APPROVED", col("event_time"))
            ).alias("last_approval_time"),
            count("txn_id").alias("txn_count"),
            _sum("amount").alias("total_amount"),
        )
        .filter(
            (col("decline_count") >= n) &                       # at least N failures
            (col("approve_count") >= 1) &                       # at least 1 success
            # ...and a success AFTER the Nth failure (null if fewer than N declines)
            (col("last_approval_time") > col("decline_times")[n - 1])
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
    )
    return _to_alert(agg, "DECLINED_THEN_APPROVED")


# ── Rule registry ─────────────────────────────────────────────────────────────
# All stateless transforms (single DataFrame input).
# dormant_card is excluded because it requires a second argument
# (the reference DataFrame) — it is handled separately in fraud_rules.py.
# To add a new rule: implement the function above, append it here.

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
