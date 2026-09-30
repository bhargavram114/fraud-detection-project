"""
test_fixes.py — Regression tests for the ten review findings.

Each class maps to one finding so a future regression points straight at it:
  1  Rule 12 ordering            -> TestDeclinedThenApprovedOrdering
  2  Thresholds from config      -> TestThresholds, TestYamlMatchesDefaults
  3  alert_id dedupe             -> TestAlertId
  4  Unusual hour local time     -> TestUnusualHourLocalTime
  5  Producer Kafka key          -> TestProducerKey
  9  Image consistency           -> TestRepoConsistency
"""
import os
import re
import yaml
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from tests.conftest import make_txn, make_df, BASE_TIME

ROOT = os.path.join(os.path.dirname(__file__), "..")


# ── Finding 1 ─────────────────────────────────────────────────────────────────
class TestDeclinedThenApprovedOrdering:
    """The approval must come AFTER the Nth decline, not merely exist in the window."""

    def _run(self, spark, statuses):
        from jobs.rules.transforms import declined_then_approved
        rows = [make_txn(card_id="C1", txn_status=s, seconds_offset=i * 60)
                for i, s in enumerate(statuses)]
        return declined_then_approved(make_df(spark, rows)).collect()

    def test_declines_then_approval_fires(self, spark):
        assert len(self._run(spark, ["DECLINED", "DECLINED", "APPROVED"])) >= 1

    def test_approval_first_then_declines_does_not_fire(self, spark):
        assert self._run(spark, ["APPROVED", "DECLINED", "DECLINED"]) == []

    def test_decline_approval_decline_does_not_fire(self, spark):
        """Only one decline precedes the approval -> not the brute-force pattern."""
        assert self._run(spark, ["DECLINED", "APPROVED", "DECLINED"]) == []

    def test_approval_before_and_after_declines_fires(self, spark):
        assert len(self._run(spark, ["APPROVED", "DECLINED", "DECLINED", "APPROVED"])) >= 1

    def test_decline_min_count_is_configurable(self, spark):
        from jobs.rules.transforms import declined_then_approved
        rows = [make_txn(card_id="C1", txn_status=s, seconds_offset=i * 60)
                for i, s in enumerate(["DECLINED", "APPROVED"])]
        df = make_df(spark, rows)
        assert declined_then_approved(df).collect() == []
        assert len(declined_then_approved(df, {"decline_min_count": 1}).collect()) >= 1


# ── Finding 2 ─────────────────────────────────────────────────────────────────
class TestThresholds:
    def test_default_velocity_does_not_fire_on_5(self, spark):
        from jobs.rules.transforms import velocity_check
        rows = [make_txn(seconds_offset=i * 10) for i in range(5)]
        assert velocity_check(make_df(spark, rows)).collect() == []

    def test_override_lowers_velocity_limit(self, spark):
        from jobs.rules.transforms import velocity_check
        rows = [make_txn(seconds_offset=i * 10) for i in range(3)]
        res = velocity_check(make_df(spark, rows), {"velocity_txn_limit": 2}).collect()
        assert len(res) >= 1

    def test_override_raises_high_value(self, spark):
        from jobs.rules.transforms import high_value_single
        df = make_df(spark, [make_txn(amount=40_000.0)])
        assert len(high_value_single(df).collect()) == 1
        assert high_value_single(df, {"high_value_single": 50_000}).collect() == []

    def test_unknown_key_rejected(self):
        from jobs.rules.thresholds import resolve
        with pytest.raises(ValueError, match="velocity_txn_limt"):
            resolve({"velocity_txn_limt": 3})

    def test_build_rules_binds_thresholds(self, spark):
        from jobs.rules.fraud_rules import build_rules, RULE_REGISTRY
        rules = build_rules({"velocity_txn_limit": 2})
        assert len(rules) == len(RULE_REGISTRY) == 11
        assert [r.__name__ for r in rules] == [r.__name__ for r in RULE_REGISTRY]


class TestYamlMatchesDefaults:
    """app.yaml and thresholds.py must not drift apart (except the deliberate timezone)."""

    def test_yaml_thresholds_are_valid_and_match_defaults(self):
        from jobs.rules.thresholds import load_thresholds, DEFAULT_THRESHOLDS
        with open(os.path.join(ROOT, "config", "app.yaml"), encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        loaded = load_thresholds(cfg)          # raises on unknown keys
        for k, v in DEFAULT_THRESHOLDS.items():
            if k == "unusual_hour_timezone":
                assert loaded[k] == "Asia/Kolkata"   # deliberate: INR pipeline
            else:
                assert loaded[k] == v, f"{k}: yaml={loaded[k]} default={v}"


# ── Finding 3 ─────────────────────────────────────────────────────────────────
class TestAlertId:
    # BASE_TIME is exactly 12:00, the edge of the 1-hour alert bucket. Start the
    # burst 30 minutes in so its sliding windows stay inside one bucket.
    MID = 1800

    def _alerts(self, spark, rows):
        from jobs.rules.transforms import velocity_check, with_alert_id
        return with_alert_id(velocity_check(make_df(spark, rows)))

    def test_burst_across_overlapping_windows_shares_one_id(self, spark):
        rows = [make_txn(card_id="C1", seconds_offset=self.MID + i * 10) for i in range(8)]
        out = self._alerts(spark, rows)
        assert out.select("window_start").distinct().count() > 1   # several sliding windows
        assert out.select("alert_id").distinct().count() == 1      # ...one alert_id

    def test_burst_straddling_bucket_edge_can_split_known_limit(self, spark):
        """Documents the stated limitation: a burst on the hour edge yields 2 IDs."""
        rows = [make_txn(card_id="C1", seconds_offset=i * 10) for i in range(8)]  # starts 12:00:00
        out = self._alerts(spark, rows)
        assert out.select("alert_id").distinct().count() == 2

    def test_different_cards_get_different_ids(self, spark):
        rows = ([make_txn(card_id="C1", seconds_offset=self.MID + i * 10) for i in range(8)] +
                [make_txn(card_id="C2", seconds_offset=self.MID + i * 10) for i in range(8)])
        assert self._alerts(spark, rows).select("alert_id").distinct().count() == 2

    def test_row_level_alerts_keep_distinct_ids(self, spark):
        from jobs.rules.transforms import high_value_single, with_alert_id
        rows = [make_txn(card_id="C1", amount=40_000.0, seconds_offset=0),
                make_txn(card_id="C1", amount=41_000.0, seconds_offset=5)]
        out = with_alert_id(high_value_single(make_df(spark, rows)))
        assert out.select("alert_id").distinct().count() == 2

    def test_same_input_gives_same_id(self, spark):
        rows = [make_txn(card_id="C1", seconds_offset=self.MID + i * 10) for i in range(8)]
        a = {r[0] for r in self._alerts(spark, rows).select("alert_id").collect()}
        b = {r[0] for r in self._alerts(spark, rows).select("alert_id").collect()}
        assert a == b


# ── Finding 4 ─────────────────────────────────────────────────────────────────
class TestUnusualHourLocalTime:
    def _n(self, spark, hh, mm, ss=0, thresholds=None):
        from jobs.rules.transforms import unusual_hour
        t = datetime(2026, 1, 1, hh, mm, ss, tzinfo=timezone.utc)
        df = make_df(spark, [make_txn(amount=10_000.0, event_time=t)])
        return len(unusual_hour(df, thresholds).collect())

    def test_start_boundary_inclusive(self, spark):
        assert self._n(spark, 1, 0, 0) == 1
        assert self._n(spark, 0, 59, 59) == 0

    def test_end_boundary_exclusive(self, spark):
        assert self._n(spark, 3, 59, 59) == 1
        assert self._n(spark, 4, 0, 0) == 0     # old between(1, 4) wrongly flagged this
        assert self._n(spark, 4, 30, 0) == 0

    def test_ist_converts_before_taking_hour(self, spark):
        ist = {"unusual_hour_timezone": "Asia/Kolkata"}
        assert self._n(spark, 21, 0, thresholds=ist) == 1   # 02:30 IST -> flagged
        assert self._n(spark, 2, 30, thresholds=ist) == 0   # 08:00 IST -> normal morning


# ── Finding 5 ─────────────────────────────────────────────────────────────────
class TestProducerKey:
    def test_send_txn_keys_by_card_id(self):
        pytest.importorskip("faker")
        pytest.importorskip("kafka")
        from producer.transaction_generator import send_txn, TOPIC
        producer = MagicMock()
        send_txn(producer, {"card_id": "CARD_9", "txn_id": "T1"})
        producer.send.assert_called_once_with(TOPIC, key="CARD_9",
                                              value={"card_id": "CARD_9", "txn_id": "T1"})

    def test_no_keyless_send_left_in_producer(self):
        src = open(os.path.join(ROOT, "producer", "transaction_generator.py"), encoding="utf-8").read()
        assert "producer.send(TOPIC, value=" not in src


# ── Finding 9 (and 8) ─────────────────────────────────────────────────────────
class TestRepoConsistency:
    def _images(self, path):
        txt = open(os.path.join(ROOT, path), encoding="utf-8").read()
        return set(re.findall(r"(?:image:|FROM)\s+(\S*spark\S*)", txt))

    def test_spark_image_identical_everywhere(self):
        found = {f: self._images(f) for f in
                 ("Dockerfile", "docker-compose.yml", "docker-compose.prod.yml")}
        flat = set().union(*found.values())
        assert len(flat) == 1, found

    def test_run_local_does_not_reference_missing_requirements(self):
        script = open(os.path.join(ROOT, "scripts", "run_local.sh"), encoding="utf-8").read()
        code = [l for l in script.splitlines() if not l.strip().startswith("#")]
        if not os.path.exists(os.path.join(ROOT, "requirements.txt")):
            assert not any("requirements.txt" in l for l in code)
