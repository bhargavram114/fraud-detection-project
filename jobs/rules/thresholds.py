"""
thresholds.py — Single place where fraud-rule thresholds are defined and resolved.

WHY THIS FILE EXISTS:
  config/app.yaml documents every threshold as the tuning point, but the rules
  in transforms.py used to hardcode the same numbers (e.g. `> 5`, `30_000`),
  so editing the YAML changed nothing. Now:

      app.yaml `thresholds:` section  ->  load_thresholds(cfg)  ->  transforms

  DEFAULT_THRESHOLDS mirrors app.yaml so unit tests (which pass no config)
  and the dev job keep working without reading any file. Unknown keys are
  rejected so a typo in YAML fails fast instead of being silently ignored.

COMPARISON SEMANTICS (kept identical to the original hardcoded rules):
  >  : velocity_txn_limit, velocity_window_amount, high_value_single,
       unusual_hour_min_amount, rapid_merchant_distinct, cnp_spike_txns,
       multi_card_terminal
  >= : round_amount_min_txns, sub_threshold_min_txns, decline_min_count,
       geo_velocity_countries
  unusual hour is [unusual_hour_start, unusual_hour_end) in unusual_hour_timezone.
  sub-threshold band is [sub_threshold_low, sub_threshold_high).
"""

DEFAULT_THRESHOLDS = {
    "velocity_txn_limit":        5,
    "velocity_window_amount":    50_000,
    "high_value_single":         30_000,
    "geo_velocity_countries":    2,
    "unusual_hour_start":        1,
    "unusual_hour_end":          4,
    "unusual_hour_min_amount":   5_000,
    "unusual_hour_timezone":     "UTC",
    "round_amount_modulo":       1_000,
    "round_amount_min_txns":     3,
    "rapid_merchant_distinct":   4,
    "cnp_spike_txns":            3,
    "multi_card_terminal":       5,
    "sub_threshold_low":         9_000,
    "sub_threshold_high":        10_000,
    "sub_threshold_min_txns":    5,
    "decline_min_count":         2,
}


def resolve(overrides: dict | None = None) -> dict:
    """Return DEFAULT_THRESHOLDS updated with `overrides`. Unknown keys raise."""
    resolved = dict(DEFAULT_THRESHOLDS)
    if overrides:
        unknown = set(overrides) - set(DEFAULT_THRESHOLDS)
        if unknown:
            raise ValueError(f"Unknown threshold keys: {sorted(unknown)}")
        resolved.update(overrides)
    return resolved


def load_thresholds(cfg: dict) -> dict:
    """Extract and validate the `thresholds:` section of the loaded app.yaml."""
    return resolve((cfg or {}).get("thresholds"))
