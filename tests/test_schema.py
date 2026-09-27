"""
test_schema.py — Schema contract tests.
These must pass before any rule logic is tested.
"""
import pytest
from tests.conftest import make_txn, make_df


class TestTransactionSchema:

    def test_schema_has_txn_status(self, spark):
        from jobs.schemas import TRANSACTION_SCHEMA
        names = [f.name for f in TRANSACTION_SCHEMA.fields]
        assert "txn_status" in names, "txn_status required for rule 12"

    def test_schema_has_event_time(self, spark):
        from jobs.schemas import TRANSACTION_SCHEMA
        names = [f.name for f in TRANSACTION_SCHEMA.fields]
        assert "event_time" in names

    def test_valid_row_loads(self, spark):
        row = make_txn()
        df = make_df(spark, [row])
        assert df.count() == 1

    def test_alert_schema_columns(self):
        from jobs.schemas import REQUIRED_ALERT_COLUMNS
        expected = {"card_id","window_start","window_end",
                    "txn_count","total_amount","rule_triggered","risk_score"}
        assert expected.issubset(set(REQUIRED_ALERT_COLUMNS))

    def test_alert_has_risk_score(self):
        from jobs.schemas import REQUIRED_ALERT_COLUMNS
        assert "risk_score" in REQUIRED_ALERT_COLUMNS
