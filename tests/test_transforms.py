"""
test_transforms.py — TDD tests for all 12 fraud rule transforms.

RED phase: these all fail until transforms.py is implemented correctly.

Rule: each transform function accepts a plain batch DataFrame (no watermark).
      withWatermark lives only in the streaming wrapper layer.
"""
import pytest
from datetime import timedelta
from tests.conftest import BASE_TIME, make_txn, make_df


# ── Rule 1: Velocity Check ────────────────────────────────────────────────────
class TestVelocityCheck:

    def test_flags_card_with_6_txns_in_window(self, spark):
        from jobs.rules.transforms import velocity_check
        rows = [make_txn(card_id="FRAUD", seconds_offset=i*30) for i in range(6)]
        result = velocity_check(make_df(spark, rows)).collect()
        # Sliding windows overlap — 6 txns in 2.5 min appear in multiple windows
        assert len(result) >= 1
        assert all(r["card_id"] == "FRAUD" for r in result)
        assert all(r["rule_triggered"] == "HIGH_VELOCITY" for r in result)

    def test_does_not_flag_card_with_5_txns(self, spark):
        from jobs.rules.transforms import velocity_check
        rows = [make_txn(card_id="LEGIT", seconds_offset=i*30) for i in range(5)]
        result = velocity_check(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_flags_correct_card_among_multiple(self, spark):
        from jobs.rules.transforms import velocity_check
        fraud = [make_txn(card_id="FRAUD", seconds_offset=i*20) for i in range(7)]
        legit = [make_txn(card_id="LEGIT", seconds_offset=i*20) for i in range(3)]
        result = velocity_check(make_df(spark, fraud + legit)).collect()
        assert all(r["card_id"] == "FRAUD" for r in result)

    def test_result_has_required_columns(self, spark):
        from jobs.rules.transforms import velocity_check
        from jobs.schemas import REQUIRED_ALERT_COLUMNS
        rows = [make_txn(card_id="C1", seconds_offset=i*20) for i in range(6)]
        result = velocity_check(make_df(spark, rows))
        for col in REQUIRED_ALERT_COLUMNS:
            assert col in result.columns, f"Missing column: {col}"

    def test_risk_score_is_85(self, spark):
        from jobs.rules.transforms import velocity_check
        rows = [make_txn(card_id="C1", seconds_offset=i*20) for i in range(6)]
        result = velocity_check(make_df(spark, rows)).collect()
        assert result[0]["risk_score"] == 85


# ── Rule 2: High Value Single Transaction ─────────────────────────────────────
class TestHighValueSingle:

    def test_flags_txn_above_30000(self, spark):
        from jobs.rules.transforms import high_value_single
        rows = [make_txn(amount=35000.0)]
        result = high_value_single(make_df(spark, rows)).collect()
        assert len(result) == 1
        assert result[0]["rule_triggered"] == "HIGH_VALUE_SINGLE_TXN"

    def test_does_not_flag_at_exact_threshold(self, spark):
        from jobs.rules.transforms import high_value_single
        rows = [make_txn(amount=30000.0)]
        result = high_value_single(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_does_not_flag_below_threshold(self, spark):
        from jobs.rules.transforms import high_value_single
        rows = [make_txn(amount=500.0)]
        result = high_value_single(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_flags_only_high_value_among_mixed(self, spark):
        from jobs.rules.transforms import high_value_single
        rows = [make_txn(amount=500.0), make_txn(amount=40000.0), make_txn(amount=200.0)]
        result = high_value_single(make_df(spark, rows)).collect()
        assert len(result) == 1
        assert result[0]["total_amount"] == 40000.0

    def test_risk_score_is_80(self, spark):
        from jobs.rules.transforms import high_value_single
        result = high_value_single(make_df(spark, [make_txn(amount=31000.0)])).collect()
        assert result[0]["risk_score"] == 80


# ── Rule 3: High Window Amount ────────────────────────────────────────────────
class TestHighWindowAmount:

    def test_flags_when_total_exceeds_50000(self, spark):
        from jobs.rules.transforms import high_window_amount
        rows = [make_txn(card_id="C1", amount=15000.0, seconds_offset=i*30)
                for i in range(4)]   # 4 × 15000 = 60000
        result = high_window_amount(make_df(spark, rows)).collect()
        assert len(result) >= 1
        assert result[0]["rule_triggered"] == "HIGH_WINDOW_AMOUNT"

    def test_does_not_flag_under_50000(self, spark):
        from jobs.rules.transforms import high_window_amount
        rows = [make_txn(card_id="C1", amount=10000.0, seconds_offset=i*30)
                for i in range(4)]   # 4 × 10000 = 40000
        result = high_window_amount(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_risk_score_is_90(self, spark):
        from jobs.rules.transforms import high_window_amount
        rows = [make_txn(card_id="C1", amount=15000.0, seconds_offset=i*30)
                for i in range(4)]
        result = high_window_amount(make_df(spark, rows)).collect()
        assert result[0]["risk_score"] == 90


# ── Rule 4: Geographic Velocity ───────────────────────────────────────────────
class TestGeoVelocity:

    def test_flags_two_countries_in_window(self, spark):
        from jobs.rules.transforms import geo_velocity
        rows = [
            make_txn(card_id="C1", country="IN", seconds_offset=0),
            make_txn(card_id="C1", country="US", seconds_offset=300),
        ]
        result = geo_velocity(make_df(spark, rows)).collect()
        assert len(result) >= 1
        assert result[0]["rule_triggered"] == "GEO_VELOCITY"

    def test_does_not_flag_same_country(self, spark):
        from jobs.rules.transforms import geo_velocity
        rows = [make_txn(card_id="C1", country="IN", seconds_offset=i*60)
                for i in range(4)]
        result = geo_velocity(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_risk_score_is_95(self, spark):
        from jobs.rules.transforms import geo_velocity
        rows = [
            make_txn(card_id="C1", country="IN", seconds_offset=0),
            make_txn(card_id="C1", country="AE", seconds_offset=300),
        ]
        result = geo_velocity(make_df(spark, rows)).collect()
        assert result[0]["risk_score"] == 95


# ── Rule 5: Unusual Hour ──────────────────────────────────────────────────────
class TestUnusualHour:

    def test_flags_2am_high_value(self, spark):
        from jobs.rules.transforms import unusual_hour
        from datetime import datetime, timezone
        t = datetime(2026, 1, 1, 2, 30, 0, tzinfo=timezone.utc)
        rows = [make_txn(amount=10000.0, event_time=t)]
        result = unusual_hour(make_df(spark, rows)).collect()
        assert len(result) == 1
        assert result[0]["rule_triggered"] == "UNUSUAL_HOUR"

    def test_does_not_flag_noon_transaction(self, spark):
        from jobs.rules.transforms import unusual_hour
        rows = [make_txn(amount=10000.0)]   # BASE_TIME = noon
        result = unusual_hour(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_does_not_flag_low_amount_at_odd_hour(self, spark):
        from jobs.rules.transforms import unusual_hour
        from datetime import datetime, timezone
        t = datetime(2026, 1, 1, 2, 0, 0, tzinfo=timezone.utc)
        rows = [make_txn(amount=100.0, event_time=t)]   # below 5000 threshold
        result = unusual_hour(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_risk_score_is_50(self, spark):
        from jobs.rules.transforms import unusual_hour
        from datetime import datetime, timezone
        t = datetime(2026, 1, 1, 3, 0, 0, tzinfo=timezone.utc)
        result = unusual_hour(make_df(spark, [make_txn(amount=9000.0, event_time=t)])).collect()
        assert result[0]["risk_score"] == 50


# ── Rule 6: Round Amount Structuring ──────────────────────────────────────────
class TestRoundAmountStructuring:

    def test_flags_3_round_amount_txns(self, spark):
        from jobs.rules.transforms import round_amount_structuring
        rows = [make_txn(card_id="C1", amount=5000.0, seconds_offset=i*60)
                for i in range(3)]
        result = round_amount_structuring(make_df(spark, rows)).collect()
        assert len(result) >= 1
        assert result[0]["rule_triggered"] == "ROUND_AMOUNT_STRUCTURING"

    def test_does_not_flag_non_round_amounts(self, spark):
        from jobs.rules.transforms import round_amount_structuring
        rows = [make_txn(card_id="C1", amount=5432.0, seconds_offset=i*60)
                for i in range(5)]
        result = round_amount_structuring(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_does_not_flag_only_2_round_txns(self, spark):
        from jobs.rules.transforms import round_amount_structuring
        rows = [make_txn(card_id="C1", amount=5000.0, seconds_offset=i*60)
                for i in range(2)]
        result = round_amount_structuring(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_risk_score_is_75(self, spark):
        from jobs.rules.transforms import round_amount_structuring
        rows = [make_txn(card_id="C1", amount=10000.0, seconds_offset=i*60)
                for i in range(3)]
        result = round_amount_structuring(make_df(spark, rows)).collect()
        assert result[0]["risk_score"] == 75


# ── Rule 7: Rapid Merchant Switching ─────────────────────────────────────────
class TestRapidMerchantSwitch:

    def test_flags_5_distinct_merchants(self, spark):
        from jobs.rules.transforms import rapid_merchant_switch
        rows = [make_txn(card_id="C1", merchant_id=f"MER_{i}", seconds_offset=i*30)
                for i in range(5)]
        result = rapid_merchant_switch(make_df(spark, rows)).collect()
        assert len(result) >= 1
        assert result[0]["rule_triggered"] == "RAPID_MERCHANT_SWITCH"

    def test_does_not_flag_4_or_fewer_merchants(self, spark):
        from jobs.rules.transforms import rapid_merchant_switch
        rows = [make_txn(card_id="C1", merchant_id=f"MER_{i}", seconds_offset=i*30)
                for i in range(4)]
        result = rapid_merchant_switch(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_risk_score_is_70(self, spark):
        from jobs.rules.transforms import rapid_merchant_switch
        rows = [make_txn(card_id="C1", merchant_id=f"MER_{i}", seconds_offset=i*30)
                for i in range(5)]
        result = rapid_merchant_switch(make_df(spark, rows)).collect()
        assert result[0]["risk_score"] == 70


# ── Rule 8: Card-Not-Present Spike ───────────────────────────────────────────
class TestCNPSpike:

    def test_flags_4_online_txns_in_window(self, spark):
        from jobs.rules.transforms import cnp_spike
        rows = [make_txn(card_id="C1", channel="ONLINE", seconds_offset=i*60)
                for i in range(4)]
        result = cnp_spike(make_df(spark, rows)).collect()
        assert len(result) >= 1
        assert result[0]["rule_triggered"] == "CNP_SPIKE"

    def test_does_not_flag_atm_channel(self, spark):
        from jobs.rules.transforms import cnp_spike
        rows = [make_txn(card_id="C1", channel="ATM", seconds_offset=i*60)
                for i in range(5)]
        result = cnp_spike(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_does_not_flag_3_or_fewer_online(self, spark):
        from jobs.rules.transforms import cnp_spike
        rows = [make_txn(card_id="C1", channel="ONLINE", seconds_offset=i*60)
                for i in range(3)]
        result = cnp_spike(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_risk_score_is_65(self, spark):
        from jobs.rules.transforms import cnp_spike
        rows = [make_txn(card_id="C1", channel="ONLINE", seconds_offset=i*60)
                for i in range(4)]
        result = cnp_spike(make_df(spark, rows)).collect()
        assert result[0]["risk_score"] == 65


# ── Rule 9: Multiple Cards at Same Terminal ───────────────────────────────────
class TestMultiCardTerminal:

    def test_flags_6_cards_at_same_terminal(self, spark):
        from jobs.rules.transforms import multi_card_terminal
        rows = [make_txn(card_id=f"CARD_{i}", terminal_id="TRM_SKIMMER",
                         seconds_offset=i*60) for i in range(6)]
        result = multi_card_terminal(make_df(spark, rows)).collect()
        assert len(result) >= 1
        assert result[0]["rule_triggered"] == "MULTI_CARD_TERMINAL"

    def test_does_not_flag_5_or_fewer_cards(self, spark):
        from jobs.rules.transforms import multi_card_terminal
        rows = [make_txn(card_id=f"CARD_{i}", terminal_id="TRM_GOOD",
                         seconds_offset=i*60) for i in range(5)]
        result = multi_card_terminal(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_risk_score_is_80(self, spark):
        from jobs.rules.transforms import multi_card_terminal
        rows = [make_txn(card_id=f"CARD_{i}", terminal_id="TRM_X",
                         seconds_offset=i*60) for i in range(6)]
        result = multi_card_terminal(make_df(spark, rows)).collect()
        assert result[0]["risk_score"] == 80


# ── Rule 10: Dormant Card ─────────────────────────────────────────────────────
class TestDormantCard:

    def test_flags_card_in_dormant_list(self, spark):
        from jobs.rules.transforms import dormant_card
        txn_df     = make_df(spark, [make_txn(card_id="DORMANT_001")])
        dormant_df = spark.createDataFrame([("DORMANT_001",)], ["card_id"])
        result = dormant_card(txn_df, dormant_df).collect()
        assert len(result) == 1
        assert result[0]["rule_triggered"] == "DORMANT_CARD"

    def test_does_not_flag_active_card(self, spark):
        from jobs.rules.transforms import dormant_card
        txn_df     = make_df(spark, [make_txn(card_id="ACTIVE_001")])
        dormant_df = spark.createDataFrame([("DORMANT_999",)], ["card_id"])
        result = dormant_card(txn_df, dormant_df).collect()
        assert len(result) == 0

    def test_risk_score_is_60(self, spark):
        from jobs.rules.transforms import dormant_card
        txn_df     = make_df(spark, [make_txn(card_id="D1")])
        dormant_df = spark.createDataFrame([("D1",)], ["card_id"])
        result = dormant_card(txn_df, dormant_df).collect()
        assert result[0]["risk_score"] == 60


# ── Rule 11: Sub-Threshold Structuring ───────────────────────────────────────
class TestSubThresholdStructuring:

    def test_flags_5_sub_threshold_txns(self, spark):
        from jobs.rules.transforms import sub_threshold_structuring
        rows = [make_txn(card_id="C1", amount=9500.0,
                         seconds_offset=i*120) for i in range(5)]
        result = sub_threshold_structuring(make_df(spark, rows)).collect()
        assert len(result) >= 1
        assert result[0]["rule_triggered"] == "SUB_THRESHOLD_STRUCT"

    def test_does_not_flag_amounts_above_10000(self, spark):
        from jobs.rules.transforms import sub_threshold_structuring
        rows = [make_txn(card_id="C1", amount=10500.0,
                         seconds_offset=i*120) for i in range(5)]
        result = sub_threshold_structuring(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_does_not_flag_fewer_than_5_txns(self, spark):
        from jobs.rules.transforms import sub_threshold_structuring
        rows = [make_txn(card_id="C1", amount=9500.0,
                         seconds_offset=i*120) for i in range(4)]
        result = sub_threshold_structuring(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_risk_score_is_85(self, spark):
        from jobs.rules.transforms import sub_threshold_structuring
        rows = [make_txn(card_id="C1", amount=9800.0,
                         seconds_offset=i*120) for i in range(5)]
        result = sub_threshold_structuring(make_df(spark, rows)).collect()
        assert result[0]["risk_score"] == 85


# ── Rule 12: Declined then Approved ──────────────────────────────────────────
class TestDeclinedThenApproved:

    def test_flags_declines_followed_by_approval(self, spark):
        from jobs.rules.transforms import declined_then_approved
        rows = (
            [make_txn(card_id="C1", txn_status="DECLINED",
                      seconds_offset=i*60) for i in range(2)] +
            [make_txn(card_id="C1", txn_status="APPROVED",
                      seconds_offset=300)]
        )
        result = declined_then_approved(make_df(spark, rows)).collect()
        assert len(result) >= 1
        assert result[0]["rule_triggered"] == "DECLINED_THEN_APPROVED"

    def test_does_not_flag_only_declines(self, spark):
        from jobs.rules.transforms import declined_then_approved
        rows = [make_txn(card_id="C1", txn_status="DECLINED",
                         seconds_offset=i*60) for i in range(3)]
        result = declined_then_approved(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_does_not_flag_only_approvals(self, spark):
        from jobs.rules.transforms import declined_then_approved
        rows = [make_txn(card_id="C1", txn_status="APPROVED",
                         seconds_offset=i*60) for i in range(3)]
        result = declined_then_approved(make_df(spark, rows)).collect()
        assert len(result) == 0

    def test_does_not_flag_single_decline_then_approval(self, spark):
        from jobs.rules.transforms import declined_then_approved
        rows = [
            make_txn(card_id="C1", txn_status="DECLINED", seconds_offset=0),
            make_txn(card_id="C1", txn_status="APPROVED", seconds_offset=120),
        ]
        result = declined_then_approved(make_df(spark, rows)).collect()
        assert len(result) == 0   # needs >= 2 declines

    def test_risk_score_is_90(self, spark):
        from jobs.rules.transforms import declined_then_approved
        rows = (
            [make_txn(card_id="C1", txn_status="DECLINED",
                      seconds_offset=i*60) for i in range(2)] +
            [make_txn(card_id="C1", txn_status="APPROVED", seconds_offset=300)]
        )
        result = declined_then_approved(make_df(spark, rows)).collect()
        assert result[0]["risk_score"] == 90
