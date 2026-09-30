"""
test_transforms.py — TDD tests for all 12 fraud rule transforms.

═══════════════════════════════════════════════════════════════════════════════
TDD WORKFLOW USED TO BUILD THIS FILE:
  1. RED   — write these tests first; all fail (ModuleNotFoundError)
  2. GREEN — implement transforms.py to make them pass
  3. REFACTOR — clean up, add edge cases, improve assertions

WHY TEST TRANSFORMS NOT STREAMING WRAPPERS?
  The streaming wrappers in fraud_rules.py add withWatermark() and
  current_timestamp() — both require a streaming context and are
  non-deterministic (timestamp changes every run). Testing them would
  require a running Kafka cluster and produce flaky results.

  The transforms in transforms.py contain 100% of the business logic.
  Testing them with batch DataFrames gives us:
    ✓ Fast (< 30s for all 51 tests)
    ✓ Deterministic (fixed timestamps, controlled data)
    ✓ No infrastructure (no Kafka, no Docker)
    ✓ Full business logic coverage

SLIDING WINDOW NOTE:
  Several tests assert len(result) >= 1 (not == 1) because sliding windows
  overlap. 6 transactions spread over 2.5 minutes will appear in multiple
  5-minute windows — each window that contains > 5 txns fires an alert.
  This is correct behaviour, not a bug. Asserting >= 1 instead of == 1
  makes tests robust to different window placements without losing coverage.

RUN SPECIFIC TEST:
  uv run pytest tests/test_transforms.py::TestGeoVelocity -v
  uv run pytest tests/test_transforms.py -v -k "declined"
"""

import pytest
from datetime import datetime, timezone, timedelta
from tests.conftest import BASE_TIME, make_txn, make_df


# ══════════════════════════════════════════════════════════════════════════════
# RULE 1 — Velocity Check
# ══════════════════════════════════════════════════════════════════════════════
class TestVelocityCheck:
    """
    Rule: >5 transactions from the same card in a 5-minute sliding window.
    Risk score: 85
    """

    def test_flags_card_with_6_txns_in_window(self, spark):
        """6 txns in 2.5 minutes must trigger the velocity rule."""
        from jobs.rules.transforms import velocity_check
        rows   = [make_txn(card_id="FRAUD", seconds_offset=i*30) for i in range(6)]
        result = velocity_check(make_df(spark, rows)).collect()
        # Sliding windows overlap — burst appears in multiple windows
        assert len(result) >= 1
        assert all(r["card_id"]        == "FRAUD"         for r in result)
        assert all(r["rule_triggered"] == "HIGH_VELOCITY" for r in result)

    def test_does_not_flag_exactly_5_txns(self, spark):
        """Threshold is >5, so exactly 5 transactions must NOT be flagged."""
        from jobs.rules.transforms import velocity_check
        rows   = [make_txn(card_id="LEGIT", seconds_offset=i*30) for i in range(5)]
        result = velocity_check(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_flags_only_the_fraudulent_card(self, spark):
        """When fraud and legit cards coexist, only the fraud card is flagged."""
        from jobs.rules.transforms import velocity_check
        fraud = [make_txn(card_id="FRAUD", seconds_offset=i*20) for i in range(7)]
        legit = [make_txn(card_id="LEGIT", seconds_offset=i*20) for i in range(3)]
        result = velocity_check(make_df(spark, fraud + legit)).collect()
        assert all(r["card_id"] == "FRAUD" for r in result)

    def test_result_has_all_required_alert_columns(self, spark):
        """Output schema must match REQUIRED_ALERT_COLUMNS contract."""
        from jobs.rules.transforms import velocity_check
        from jobs.schemas import REQUIRED_ALERT_COLUMNS
        rows   = [make_txn(card_id="C1", seconds_offset=i*20) for i in range(6)]
        result = velocity_check(make_df(spark, rows))
        for col_name in REQUIRED_ALERT_COLUMNS:
            assert col_name in result.columns, f"Missing column: {col_name}"

    def test_risk_score_is_85(self, spark):
        from jobs.rules.transforms import velocity_check
        rows   = [make_txn(card_id="C1", seconds_offset=i*20) for i in range(6)]
        result = velocity_check(make_df(spark, rows)).collect()
        assert result[0]["risk_score"] == 85


# ══════════════════════════════════════════════════════════════════════════════
# RULE 2 — High Value Single Transaction
# ══════════════════════════════════════════════════════════════════════════════
class TestHighValueSingle:
    """
    Rule: any single transaction exceeding ₹30,000.
    Risk score: 80
    Row-level filter — no windowing, cheapest rule to execute.
    """

    def test_flags_txn_above_30000(self, spark):
        from jobs.rules.transforms import high_value_single
        result = high_value_single(make_df(spark, [make_txn(amount=35000.0)])).collect()
        assert len(result) == 1
        assert result[0]["rule_triggered"] == "HIGH_VALUE_SINGLE_TXN"
        assert result[0]["total_amount"]   == 35000.0

    def test_does_not_flag_exactly_30000(self, spark):
        """Boundary: ₹30,000 is NOT above threshold — should not be flagged."""
        from jobs.rules.transforms import high_value_single
        result = high_value_single(make_df(spark, [make_txn(amount=30000.0)])).collect()
        assert len(result) == 0

    def test_does_not_flag_normal_amount(self, spark):
        from jobs.rules.transforms import high_value_single
        result = high_value_single(make_df(spark, [make_txn(amount=500.0)])).collect()
        assert len(result) == 0

    def test_flags_only_high_value_among_mixed_amounts(self, spark):
        from jobs.rules.transforms import high_value_single
        rows   = [make_txn(amount=500.0), make_txn(amount=40000.0), make_txn(amount=200.0)]
        result = high_value_single(make_df(spark, rows)).collect()
        assert len(result) == 1
        assert result[0]["total_amount"] == 40000.0

    def test_risk_score_is_80(self, spark):
        from jobs.rules.transforms import high_value_single
        result = high_value_single(make_df(spark, [make_txn(amount=31000.0)])).collect()
        assert result[0]["risk_score"] == 80


# ══════════════════════════════════════════════════════════════════════════════
# RULE 3 — High Window Amount
# ══════════════════════════════════════════════════════════════════════════════
class TestHighWindowAmount:
    """
    Rule: total spend by one card exceeds ₹50,000 in a 5-minute window.
    Risk score: 90
    Complements Rule 1 (which counts transactions) and Rule 2 (single large txn).
    """

    def test_flags_when_total_exceeds_50000(self, spark):
        """4 × ₹15,000 = ₹60,000 — must trigger high-amount rule."""
        from jobs.rules.transforms import high_window_amount
        rows   = [make_txn(card_id="C1", amount=15000.0, seconds_offset=i*30)
                  for i in range(4)]
        result = high_window_amount(make_df(spark, rows)).collect()
        assert len(result) >= 1
        assert result[0]["rule_triggered"] == "HIGH_WINDOW_AMOUNT"

    def test_does_not_flag_under_50000(self, spark):
        """4 × ₹10,000 = ₹40,000 — should NOT trigger."""
        from jobs.rules.transforms import high_window_amount
        rows   = [make_txn(card_id="C1", amount=10000.0, seconds_offset=i*30)
                  for i in range(4)]
        result = high_window_amount(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_risk_score_is_90(self, spark):
        from jobs.rules.transforms import high_window_amount
        rows   = [make_txn(card_id="C1", amount=15000.0, seconds_offset=i*30)
                  for i in range(4)]
        result = high_window_amount(make_df(spark, rows)).collect()
        assert result[0]["risk_score"] == 90


# ══════════════════════════════════════════════════════════════════════════════
# RULE 4 — Geographic Velocity (Impossible Travel)
# ══════════════════════════════════════════════════════════════════════════════
class TestGeoVelocity:
    """
    Rule: same card in 2+ countries within 30 minutes.
    Risk score: 95 (highest — physically impossible)
    """

    def test_flags_two_countries_in_window(self, spark):
        from jobs.rules.transforms import geo_velocity
        rows   = [
            make_txn(card_id="C1", country="IN", seconds_offset=0),
            make_txn(card_id="C1", country="US", seconds_offset=300),  # 5 min later
        ]
        result = geo_velocity(make_df(spark, rows)).collect()
        assert len(result) >= 1
        assert result[0]["rule_triggered"] == "GEO_VELOCITY"

    def test_does_not_flag_same_country_multiple_txns(self, spark):
        """Multiple transactions in the same country are fine."""
        from jobs.rules.transforms import geo_velocity
        rows   = [make_txn(card_id="C1", country="IN", seconds_offset=i*60)
                  for i in range(4)]
        result = geo_velocity(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_flags_three_countries(self, spark):
        """Three different countries in the window — definitely suspicious."""
        from jobs.rules.transforms import geo_velocity
        rows   = [
            make_txn(card_id="C1", country="IN", seconds_offset=0),
            make_txn(card_id="C1", country="AE", seconds_offset=300),
            make_txn(card_id="C1", country="GB", seconds_offset=600),
        ]
        result = geo_velocity(make_df(spark, rows)).collect()
        assert len(result) >= 1

    def test_risk_score_is_95(self, spark):
        from jobs.rules.transforms import geo_velocity
        rows   = [
            make_txn(card_id="C1", country="IN", seconds_offset=0),
            make_txn(card_id="C1", country="AE", seconds_offset=300),
        ]
        result = geo_velocity(make_df(spark, rows)).collect()
        assert result[0]["risk_score"] == 95


# ══════════════════════════════════════════════════════════════════════════════
# RULE 5 — Unusual Hour
# ══════════════════════════════════════════════════════════════════════════════
class TestUnusualHour:
    """
    Rule: transaction between 01:00–04:00 UTC with amount > ₹5,000.
    Risk score: 50 (contextual signal, not standalone blocker)
    """

    def test_flags_2am_high_value_transaction(self, spark):
        from jobs.rules.transforms import unusual_hour
        t      = datetime(2026, 1, 1, 2, 30, 0, tzinfo=timezone.utc)
        result = unusual_hour(make_df(spark, [make_txn(amount=10000.0, event_time=t)])).collect()
        assert len(result) == 1
        assert result[0]["rule_triggered"] == "UNUSUAL_HOUR"

    def test_does_not_flag_noon_transaction(self, spark):
        """BASE_TIME is noon — must not be flagged regardless of amount."""
        from jobs.rules.transforms import unusual_hour
        result = unusual_hour(make_df(spark, [make_txn(amount=10000.0)])).collect()
        assert len(result) == 0

    def test_does_not_flag_low_amount_at_odd_hour(self, spark):
        """Odd hour but amount ≤ ₹5,000 — below the amount threshold."""
        from jobs.rules.transforms import unusual_hour
        t      = datetime(2026, 1, 1, 2, 0, 0, tzinfo=timezone.utc)
        result = unusual_hour(make_df(spark, [make_txn(amount=100.0, event_time=t)])).collect()
        assert len(result) == 0

    def test_boundary_exactly_5000_not_flagged(self, spark):
        """Boundary: ₹5,000 is NOT above threshold — should not be flagged."""
        from jobs.rules.transforms import unusual_hour
        t      = datetime(2026, 1, 1, 2, 0, 0, tzinfo=timezone.utc)
        result = unusual_hour(make_df(spark, [make_txn(amount=5000.0, event_time=t)])).collect()
        assert len(result) == 0

    def test_risk_score_is_50(self, spark):
        from jobs.rules.transforms import unusual_hour
        t      = datetime(2026, 1, 1, 3, 0, 0, tzinfo=timezone.utc)
        result = unusual_hour(make_df(spark, [make_txn(amount=9000.0, event_time=t)])).collect()
        assert result[0]["risk_score"] == 50


# ══════════════════════════════════════════════════════════════════════════════
# RULE 6 — Round Amount Structuring
# ══════════════════════════════════════════════════════════════════════════════
class TestRoundAmountStructuring:
    """
    Rule: 3+ transactions of exact round amounts (amount % 1000 == 0) in 10 min.
    Risk score: 75
    Regulatory: PMLA 2002, FIU-IND reporting requirements.
    """

    def test_flags_3_round_amount_txns(self, spark):
        from jobs.rules.transforms import round_amount_structuring
        rows   = [make_txn(card_id="C1", amount=5000.0, seconds_offset=i*60)
                  for i in range(3)]
        result = round_amount_structuring(make_df(spark, rows)).collect()
        assert len(result) >= 1
        assert result[0]["rule_triggered"] == "ROUND_AMOUNT_STRUCTURING"

    def test_does_not_flag_non_round_amounts(self, spark):
        """₹5,432 is not a round thousand — should not trigger."""
        from jobs.rules.transforms import round_amount_structuring
        rows   = [make_txn(card_id="C1", amount=5432.0, seconds_offset=i*60)
                  for i in range(5)]
        result = round_amount_structuring(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_does_not_flag_only_2_round_txns(self, spark):
        """Threshold is >=3 — exactly 2 round txns must not trigger."""
        from jobs.rules.transforms import round_amount_structuring
        rows   = [make_txn(card_id="C1", amount=5000.0, seconds_offset=i*60)
                  for i in range(2)]
        result = round_amount_structuring(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_risk_score_is_75(self, spark):
        from jobs.rules.transforms import round_amount_structuring
        rows   = [make_txn(card_id="C1", amount=10000.0, seconds_offset=i*60)
                  for i in range(3)]
        result = round_amount_structuring(make_df(spark, rows)).collect()
        assert result[0]["risk_score"] == 75


# ══════════════════════════════════════════════════════════════════════════════
# RULE 7 — Rapid Merchant Switching
# ══════════════════════════════════════════════════════════════════════════════
class TestRapidMerchantSwitch:
    """
    Rule: same card at 4+ distinct merchants in 5 minutes ('card testing').
    Risk score: 70
    """

    def test_flags_5_distinct_merchants(self, spark):
        from jobs.rules.transforms import rapid_merchant_switch
        rows   = [make_txn(card_id="C1", merchant_id=f"MER_{i}", seconds_offset=i*30)
                  for i in range(5)]
        result = rapid_merchant_switch(make_df(spark, rows)).collect()
        assert len(result) >= 1
        assert result[0]["rule_triggered"] == "RAPID_MERCHANT_SWITCH"

    def test_does_not_flag_4_or_fewer_merchants(self, spark):
        """Threshold is >4, so exactly 4 distinct merchants must not trigger."""
        from jobs.rules.transforms import rapid_merchant_switch
        rows   = [make_txn(card_id="C1", merchant_id=f"MER_{i}", seconds_offset=i*30)
                  for i in range(4)]
        result = rapid_merchant_switch(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_does_not_flag_same_merchant_multiple_txns(self, spark):
        """10 transactions at the same merchant is not suspicious."""
        from jobs.rules.transforms import rapid_merchant_switch
        rows   = [make_txn(card_id="C1", merchant_id="MER_001", seconds_offset=i*20)
                  for i in range(10)]
        result = rapid_merchant_switch(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_risk_score_is_70(self, spark):
        from jobs.rules.transforms import rapid_merchant_switch
        rows   = [make_txn(card_id="C1", merchant_id=f"MER_{i}", seconds_offset=i*30)
                  for i in range(5)]
        result = rapid_merchant_switch(make_df(spark, rows)).collect()
        assert result[0]["risk_score"] == 70


# ══════════════════════════════════════════════════════════════════════════════
# RULE 8 — Card-Not-Present (CNP) Spike
# ══════════════════════════════════════════════════════════════════════════════
class TestCNPSpike:
    """
    Rule: 3+ ONLINE channel transactions in 10 minutes.
    Risk score: 65
    Targets stolen card details used for online fraud.
    """

    def test_flags_4_online_txns(self, spark):
        from jobs.rules.transforms import cnp_spike
        rows   = [make_txn(card_id="C1", channel="ONLINE", seconds_offset=i*60)
                  for i in range(4)]
        result = cnp_spike(make_df(spark, rows)).collect()
        assert len(result) >= 1
        assert result[0]["rule_triggered"] == "CNP_SPIKE"

    def test_does_not_flag_atm_channel(self, spark):
        """ATM channel (physical card present) is not a CNP transaction."""
        from jobs.rules.transforms import cnp_spike
        rows   = [make_txn(card_id="C1", channel="ATM", seconds_offset=i*60)
                  for i in range(5)]
        result = cnp_spike(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_does_not_flag_3_or_fewer_online_txns(self, spark):
        """Threshold is >3 — exactly 3 online txns must not trigger."""
        from jobs.rules.transforms import cnp_spike
        rows   = [make_txn(card_id="C1", channel="ONLINE", seconds_offset=i*60)
                  for i in range(3)]
        result = cnp_spike(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_risk_score_is_65(self, spark):
        from jobs.rules.transforms import cnp_spike
        rows   = [make_txn(card_id="C1", channel="ONLINE", seconds_offset=i*60)
                  for i in range(4)]
        result = cnp_spike(make_df(spark, rows)).collect()
        assert result[0]["risk_score"] == 65


# ══════════════════════════════════════════════════════════════════════════════
# RULE 9 — Multiple Cards at Same Terminal
# ══════════════════════════════════════════════════════════════════════════════
class TestMultiCardTerminal:
    """
    Rule: 5+ distinct cards at one ATM terminal in 10 minutes (skimmer indicator).
    Risk score: 80
    Groups by terminal_id — detects compromised terminals, not compromised cards.
    """

    def test_flags_6_distinct_cards_at_same_terminal(self, spark):
        from jobs.rules.transforms import multi_card_terminal
        rows   = [make_txn(card_id=f"CARD_{i}", terminal_id="TRM_SKIMMER",
                           seconds_offset=i*60) for i in range(6)]
        result = multi_card_terminal(make_df(spark, rows)).collect()
        assert len(result) >= 1
        assert result[0]["rule_triggered"] == "MULTI_CARD_TERMINAL"

    def test_does_not_flag_5_or_fewer_cards(self, spark):
        """Threshold is >5 — exactly 5 distinct cards must not trigger."""
        from jobs.rules.transforms import multi_card_terminal
        rows   = [make_txn(card_id=f"CARD_{i}", terminal_id="TRM_GOOD",
                           seconds_offset=i*60) for i in range(5)]
        result = multi_card_terminal(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_does_not_flag_same_card_multiple_times(self, spark):
        """Same card using the same terminal 10 times is not a skimmer pattern."""
        from jobs.rules.transforms import multi_card_terminal
        rows   = [make_txn(card_id="CARD_001", terminal_id="TRM_001",
                           seconds_offset=i*60) for i in range(10)]
        result = multi_card_terminal(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_risk_score_is_80(self, spark):
        from jobs.rules.transforms import multi_card_terminal
        rows   = [make_txn(card_id=f"CARD_{i}", terminal_id="TRM_X",
                           seconds_offset=i*60) for i in range(6)]
        result = multi_card_terminal(make_df(spark, rows)).collect()
        assert result[0]["risk_score"] == 80


# ══════════════════════════════════════════════════════════════════════════════
# RULE 10 — Dormant Card Activation
# ══════════════════════════════════════════════════════════════════════════════
class TestDormantCard:
    """
    Rule: card inactive for 30+ days suddenly transacts.
    Risk score: 60
    Stream-static join — transaction stream vs. nightly batch dormant card list.
    """

    def test_flags_card_in_dormant_list(self, spark):
        """A dormant card transacting must be flagged."""
        from jobs.rules.transforms import dormant_card
        txn_df     = make_df(spark, [make_txn(card_id="DORMANT_001")])
        dormant_df = spark.createDataFrame([("DORMANT_001",)], ["card_id"])
        result     = dormant_card(txn_df, dormant_df).collect()
        assert len(result) == 1
        assert result[0]["rule_triggered"] == "DORMANT_CARD"

    def test_does_not_flag_active_card(self, spark):
        """An active card (not in the dormant list) must not be flagged."""
        from jobs.rules.transforms import dormant_card
        txn_df     = make_df(spark, [make_txn(card_id="ACTIVE_001")])
        dormant_df = spark.createDataFrame([("DORMANT_999",)], ["card_id"])
        result     = dormant_card(txn_df, dormant_df).collect()
        assert len(result) == 0

    def test_empty_dormant_list_flags_nothing(self, spark):
        """An empty dormant reference list — no alerts expected."""
        from jobs.rules.transforms import dormant_card
        txn_df     = make_df(spark, [make_txn(card_id="ANY_CARD")])
        dormant_df = spark.createDataFrame([], spark.createDataFrame([("x",)], ["card_id"]).schema)
        result     = dormant_card(txn_df, dormant_df).collect()
        assert len(result) == 0

    def test_risk_score_is_60(self, spark):
        from jobs.rules.transforms import dormant_card
        txn_df     = make_df(spark, [make_txn(card_id="D1")])
        dormant_df = spark.createDataFrame([("D1",)], ["card_id"])
        result     = dormant_card(txn_df, dormant_df).collect()
        assert result[0]["risk_score"] == 60


# ══════════════════════════════════════════════════════════════════════════════
# RULE 11 — Sub-Threshold Structuring
# ══════════════════════════════════════════════════════════════════════════════
class TestSubThresholdStructuring:
    """
    Rule: 5+ transactions between ₹9,000–₹9,999 in 1 hour.
    Risk score: 85
    Regulatory: PMLA 2002 — deliberate avoidance of ₹10,000 reporting threshold.
    """

    def test_flags_5_sub_threshold_txns(self, spark):
        from jobs.rules.transforms import sub_threshold_structuring
        rows   = [make_txn(card_id="C1", amount=9500.0, seconds_offset=i*120)
                  for i in range(5)]
        result = sub_threshold_structuring(make_df(spark, rows)).collect()
        assert len(result) >= 1
        assert result[0]["rule_triggered"] == "SUB_THRESHOLD_STRUCT"

    def test_does_not_flag_amounts_above_10000(self, spark):
        """₹10,500 is above the threshold band — different rule territory."""
        from jobs.rules.transforms import sub_threshold_structuring
        rows   = [make_txn(card_id="C1", amount=10500.0, seconds_offset=i*120)
                  for i in range(5)]
        result = sub_threshold_structuring(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_does_not_flag_amounts_below_9000(self, spark):
        """₹8,000 is below the band — not sub-threshold structuring."""
        from jobs.rules.transforms import sub_threshold_structuring
        rows   = [make_txn(card_id="C1", amount=8000.0, seconds_offset=i*120)
                  for i in range(5)]
        result = sub_threshold_structuring(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_does_not_flag_fewer_than_5_txns(self, spark):
        """4 sub-threshold txns — does not meet the >=5 count threshold."""
        from jobs.rules.transforms import sub_threshold_structuring
        rows   = [make_txn(card_id="C1", amount=9500.0, seconds_offset=i*120)
                  for i in range(4)]
        result = sub_threshold_structuring(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_risk_score_is_85(self, spark):
        from jobs.rules.transforms import sub_threshold_structuring
        rows   = [make_txn(card_id="C1", amount=9800.0, seconds_offset=i*120)
                  for i in range(5)]
        result = sub_threshold_structuring(make_df(spark, rows)).collect()
        assert result[0]["risk_score"] == 85


# ══════════════════════════════════════════════════════════════════════════════
# RULE 12 — Declined then Approved
# ══════════════════════════════════════════════════════════════════════════════
class TestDeclinedThenApproved:
    """
    Rule: 2+ DECLINED transactions followed by 1+ APPROVED in same 10-min window.
    Risk score: 90
    Detects PIN brute-force, card testing, and credential stuffing.
    Implementation: conditional aggregation (not stream-stream join).
    """

    def test_flags_2_declines_then_approval(self, spark):
        from jobs.rules.transforms import declined_then_approved
        rows   = (
            [make_txn(card_id="C1", txn_status="DECLINED", seconds_offset=i*60)
             for i in range(2)] +
            [make_txn(card_id="C1", txn_status="APPROVED", seconds_offset=300)]
        )
        result = declined_then_approved(make_df(spark, rows)).collect()
        assert len(result) >= 1
        assert result[0]["rule_triggered"] == "DECLINED_THEN_APPROVED"

    def test_does_not_flag_only_declines_no_approval(self, spark):
        """Declines without a final approval — pattern incomplete, no alert."""
        from jobs.rules.transforms import declined_then_approved
        rows   = [make_txn(card_id="C1", txn_status="DECLINED", seconds_offset=i*60)
                  for i in range(3)]
        result = declined_then_approved(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_does_not_flag_only_approvals(self, spark):
        """All approved — legitimate usage, no pattern to detect."""
        from jobs.rules.transforms import declined_then_approved
        rows   = [make_txn(card_id="C1", txn_status="APPROVED", seconds_offset=i*60)
                  for i in range(3)]
        result = declined_then_approved(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_does_not_flag_single_decline_then_approval(self, spark):
        """1 decline + 1 approval = normal usage (wrong PIN once). Not fraud."""
        from jobs.rules.transforms import declined_then_approved
        rows   = [
            make_txn(card_id="C1", txn_status="DECLINED", seconds_offset=0),
            make_txn(card_id="C1", txn_status="APPROVED", seconds_offset=120),
        ]
        result = declined_then_approved(make_df(spark, rows)).collect()
        assert len(result) == 0   # threshold is >= 2 declines

    def test_risk_score_is_90(self, spark):
        from jobs.rules.transforms import declined_then_approved
        rows   = (
            [make_txn(card_id="C1", txn_status="DECLINED", seconds_offset=i*60)
             for i in range(2)] +
            [make_txn(card_id="C1", txn_status="APPROVED", seconds_offset=300)]
        )
        result = declined_then_approved(make_df(spark, rows)).collect()
        assert result[0]["risk_score"] == 90
