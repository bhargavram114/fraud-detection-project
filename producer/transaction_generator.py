"""
transaction_generator.py — Simulates a stream of ATM / card transactions.

WHAT THIS DOES:
  Generates realistic ATM transaction events and publishes them to Kafka.
  Two modes:
    normal — randomised legitimate transactions only
    mixed  — normal transactions + periodic fraud burst injections

FRAUD BURST INJECTION (mixed mode):
  Every 50 normal transactions, the generator picks one card from the pool
  and fires 8 transactions from it in rapid succession. This triggers the
  velocity rule (Rule 1) and potentially the high-window-amount rule (Rule 3).
  Watch the console sink in fraud_detection.py light up with alerts.

WHY THIS IS VALUABLE FOR DEMOS:
  In interviews, run this in one terminal and the Spark job in another.
  Point out when alerts appear and explain which rule fired and why.
  Having working end-to-end demo code is a strong differentiator.

RUN:
  uv run python producer\\transaction_generator.py --mode mixed --tps 10
  uv run python producer\\transaction_generator.py --mode normal --tps 5
"""

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

fake = Faker()

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC          = "transactions"

# Merchant Category Codes (MCC equivalents — same concept as ATM type codes)
MERCHANT_CATS  = [
    "ATM_WITHDRAWAL", "GROCERY", "FUEL", "ONLINE_RETAIL",
    "RESTAURANT", "TRAVEL", "ELECTRONICS", "PHARMACY",
]
CHANNELS   = ["ATM", "POS", "ONLINE", "MOBILE"]
TXN_TYPES  = ["WITHDRAWAL", "PURCHASE", "TRANSFER"]
COUNTRIES  = ["IN", "US", "GB", "SG", "AE"]
STATUSES   = ["APPROVED"] * 9 + ["DECLINED"]   # 90% approved, 10% declined (realistic)

# Pre-generate 200 card IDs so bursts always target the same pool
# This means the same cards appear across multiple bursts — realistic
# (a skimmer captures the same cards repeatedly)
CARD_POOL = [str(uuid.uuid4()) for _ in range(200)]


# ── Transaction factory ───────────────────────────────────────────────────────

def make_transaction(card_id: str = None, amount: float = None,
                     txn_status: str = None) -> dict:
    """
    Build one realistic ATM/card transaction event.
    All fields match TRANSACTION_SCHEMA in schemas.py.
    event_time is always UTC — the Spark job expects UTC timestamps.
    """
    return {
        "txn_id":       str(uuid.uuid4()),
        "card_id":      card_id or random.choice(CARD_POOL),
        "account_id":   f"ACC-{random.randint(10000, 99999)}",
        "amount":       amount  or round(random.uniform(10, 5000), 2),
        "currency":     "INR",
        "merchant_id":  f"MER-{random.randint(1000, 9999)}",
        "merchant_cat": random.choice(MERCHANT_CATS),
        "terminal_id":  f"TRM-{random.randint(100, 999)}",
        "country":      random.choice(COUNTRIES),
        "city":         fake.city(),
        "txn_type":     random.choice(TXN_TYPES),
        "channel":      random.choice(CHANNELS),
        "txn_status":   txn_status or random.choice(STATUSES),
        # ISO 8601 timestamp — consistent with how ATM hosts timestamp events
        "event_time":   datetime.now(timezone.utc).isoformat(),
    }


def make_fraud_burst(card_id: str, count: int = 8) -> list[dict]:
    """
    Generate a burst of rapid high-value transactions on a single card.
    This triggers:
      Rule 1 (velocity) — count > 5
      Rule 3 (high window amount) — 8 × ~₹1,750 avg = ~₹14,000
      Potentially Rule 7 (rapid merchant switch) — random merchants
    """
    return [
        make_transaction(
            card_id=card_id,
            amount=round(random.uniform(500, 3000), 2),
            txn_status="APPROVED",  # all approved — card is actively being drained
        )
        for _ in range(count)
    ]


# ── Kafka producer ─────────────────────────────────────────────────────────────

def build_producer() -> KafkaProducer:
    """
    Create a Kafka producer with settings appropriate for a simulation.
    acks="all" — strongest durability guarantee (equivalent to ATM ISO auth
    where both primary and backup host must acknowledge before confirming).
    linger_ms=5 — small batching window for slightly better throughput.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        # Key = card_id: Kafka hashes the key to pick a partition, so every event
        # for one card lands in the SAME partition and stays in order.
        # Without a key, events are spread round-robin and one card's events can
        # be consumed out of order across partitions.
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
        acks="all",
        retries=3,
    )


def send_txn(producer, txn: dict) -> None:
    """Publish one transaction, keyed by card_id (see build_producer)."""
    producer.send(TOPIC, key=txn["card_id"], value=txn)


# ── Main loop ─────────────────────────────────────────────────────────────────

def run(mode: str, tps: int):
    producer = build_producer()
    print(f"\n[Producer] Started | mode={mode} | target={tps} TPS | topic={TOPIC}")
    print("[Producer] Ctrl+C to stop\n")

    sent       = 0
    fraud_card = random.choice(CARD_POOL)   # one 'compromised' card

    try:
        while True:
            batch_start = time.time()

            if mode == "mixed" and sent > 0 and sent % 50 == 0:
                # Every 50 normal txns, inject a fraud burst on one card
                fraud_card = random.choice(CARD_POOL)
                bursts     = make_fraud_burst(fraud_card)
                for txn in bursts:
                    send_txn(producer, txn)
                sent += len(bursts)
                print(f"  ⚠️  [FRAUD BURST] card={fraud_card[:8]}... "
                      f"count={len(bursts)} — watch for alerts in Spark!")
            else:
                txn = make_transaction()
                send_txn(producer, txn)
                sent += 1

            # Throttle to target TPS
            elapsed    = time.time() - batch_start
            sleep_time = max(0, (1 / tps) - elapsed)
            time.sleep(sleep_time)

            if sent % 100 == 0:
                print(f"[Producer] {sent:,} transactions sent")

    except KeyboardInterrupt:
        print(f"\n[Producer] Stopped. Total sent: {sent:,}")
    finally:
        producer.flush()
        producer.close()


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ATM Transaction Generator")
    parser.add_argument(
        "--mode", choices=["normal", "mixed"], default="mixed",
        help="normal = legitimate txns only | mixed = injects fraud bursts every 50 txns"
    )
    parser.add_argument(
        "--tps", type=int, default=10,
        help="Target transactions per second (default: 10)"
    )
    args = parser.parse_args()
    run(args.mode, args.tps)
