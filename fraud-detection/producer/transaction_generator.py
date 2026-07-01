"""
transaction_generator.py
Simulates a stream of ATM / card transactions sent to Kafka.

Two modes:
  normal  — randomised legitimate transactions (80% of volume)
  burst   — simulates a card being used many times in a short window (fraud)

Run:
    python producer/transaction_generator.py --mode mixed --tps 10
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

fake = Faker()

# ── Config ──────────────────────────────────────────────────────────────────
KAFKA_BROKER   = "localhost:9092"
TOPIC          = "transactions"

# Merchant category codes (like MCC in card payment networks)
MERCHANT_CATS  = ["ATM_WITHDRAWAL", "GROCERY", "FUEL", "ONLINE_RETAIL",
                  "RESTAURANT", "TRAVEL", "ELECTRONICS", "PHARMACY"]
CHANNELS       = ["ATM", "POS", "ONLINE", "MOBILE"]
TXN_TYPES      = ["WITHDRAWAL", "PURCHASE", "TRANSFER"]
COUNTRIES      = ["IN", "US", "GB", "SG", "AE"]

# Pre-generated card IDs so we can target specific cards for fraud bursts
CARD_POOL = [str(uuid.uuid4()) for _ in range(200)]


# ── Helpers ─────────────────────────────────────────────────────────────────

def make_transaction(card_id: str = None, amount: float = None) -> dict:
    """Build one realistic transaction event."""
    return {
        "txn_id":       str(uuid.uuid4()),
        "card_id":      card_id or random.choice(CARD_POOL),
        "account_id":   f"ACC-{random.randint(10000, 99999)}",
        "amount":       amount or round(random.uniform(10, 5000), 2),
        "currency":     "INR",
        "merchant_id":  f"MER-{random.randint(1000, 9999)}",
        "merchant_cat": random.choice(MERCHANT_CATS),
        "terminal_id":  f"TRM-{random.randint(100, 999)}",
        "country":      random.choice(COUNTRIES),
        "city":         fake.city(),
        "txn_type":     random.choice(TXN_TYPES),
        "channel":      random.choice(CHANNELS),
        # ISO 8601 — consistent with how ATM hosts timestamp events
        "event_time":   datetime.now(timezone.utc).isoformat(),
    }


def make_fraud_burst(card_id: str, count: int = 8) -> list[dict]:
    """
    Generate a burst of rapid transactions on a single card.
    This will trigger the velocity rule in the Spark job
    (>5 transactions in a 5-minute window).
    """
    return [
        make_transaction(card_id=card_id, amount=round(random.uniform(500, 3000), 2))
        for _ in range(count)
    ]


# ── Producer ────────────────────────────────────────────────────────────────

def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # Tuned for low-latency single-message sends (suits a simulation)
        linger_ms=5,
        acks="all",          # strongest durability — equivalent to ATM ISO auth
        retries=3,
    )


def run(mode: str, tps: int):
    producer = build_producer()
    print(f"[Producer] Started | mode={mode} | target TPS={tps} | topic={TOPIC}")

    sent = 0
    fraud_card = random.choice(CARD_POOL)   # pick one card to "compromise"

    try:
        while True:
            batch_start = time.time()

            if mode == "mixed" and sent % 50 == 0:
                # Every 50 normal transactions, inject a fraud burst on one card
                fraud_card = random.choice(CARD_POOL)
                bursts = make_fraud_burst(fraud_card)
                for txn in bursts:
                    producer.send(TOPIC, value=txn)
                    print(f"  [FRAUD BURST] card={fraud_card[:8]}... amount={txn['amount']}")
                sent += len(bursts)

            else:
                txn = make_transaction()
                producer.send(TOPIC, value=txn)
                sent += 1

            # Throttle to target TPS
            elapsed = time.time() - batch_start
            sleep_time = max(0, (1 / tps) - elapsed)
            time.sleep(sleep_time)

            if sent % 100 == 0:
                print(f"[Producer] Sent {sent} transactions")

    except KeyboardInterrupt:
        print(f"\n[Producer] Stopped. Total sent: {sent}")
    finally:
        producer.flush()
        producer.close()


# ── Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ATM Transaction Generator")
    parser.add_argument("--mode", choices=["normal", "mixed"], default="mixed",
                        help="normal = clean data only | mixed = injects fraud bursts")
    parser.add_argument("--tps", type=int, default=10,
                        help="Transactions per second to generate")
    args = parser.parse_args()
    run(args.mode, args.tps)
