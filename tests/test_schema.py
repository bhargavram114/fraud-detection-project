"""
test_schema.py — Schema contract tests.

These tests validate the data contract defined in schemas.py.
They run first (alphabetically) and must pass before any rule logic
is tested — a schema bug would cause silent null propagation in every rule.

WHAT WE TEST:
  - Required fields exist (including recently added txn_status for Rule 12)
  - A valid transaction row can be created with the schema
  - The alert schema has the required output columns
  - risk_score is included in the alert contract

WHY THESE TESTS MATTER:
  If txn_status were missing from TRANSACTION_SCHEMA, rule_declined_then_approved
  would silently return all nulls for that column, and the rule would never fire.
  This would be very hard to debug in a live streaming job.
  A schema test catches it immediately.
"""

import pytest
from tests.conftest import make_txn, make_df


class TestTransactionSchema:

    def test_schema_has_txn_status(self, spark):
        """txn_status is required by Rule 12 (declined-then-approved)."""
        from jobs.schemas import TRANSACTION_SCHEMA
        field_names = [f.name for f in TRANSACTION_SCHEMA.fields]
        assert "txn_status" in field_names, \
            "txn_status missing from schema — Rule 12 will silently fail"

    def test_schema_has_event_time(self, spark):
        """event_time is required by every windowed rule."""
        from jobs.schemas import TRANSACTION_SCHEMA
        field_names = [f.name for f in TRANSACTION_SCHEMA.fields]
        assert "event_time" in field_names

    def test_schema_has_terminal_id(self, spark):
        """terminal_id is required by Rule 9 (multi-card terminal)."""
        from jobs.schemas import TRANSACTION_SCHEMA
        field_names = [f.name for f in TRANSACTION_SCHEMA.fields]
        assert "terminal_id" in field_names

    def test_valid_row_loads_without_error(self, spark):
        """A complete valid transaction row must load without null coercion."""
        row = make_txn()
        df  = make_df(spark, [row])
        assert df.count() == 1
        # Verify no fields were silently coerced to null
        first = df.first()
        assert first["txn_id"]     is not None
        assert first["card_id"]    is not None
        assert first["event_time"] is not None

    def test_alert_schema_has_required_columns(self):
        """Alert output contract must include all expected columns."""
        from jobs.schemas import REQUIRED_ALERT_COLUMNS
        expected = {
            "card_id", "window_start", "window_end",
            "txn_count", "total_amount", "rule_triggered", "risk_score"
        }
        missing = expected - set(REQUIRED_ALERT_COLUMNS)
        assert not missing, f"Missing alert columns: {missing}"

    def test_alert_schema_has_risk_score(self):
        """risk_score must be in the contract — used for downstream triage."""
        from jobs.schemas import REQUIRED_ALERT_COLUMNS
        assert "risk_score" in REQUIRED_ALERT_COLUMNS
