# Real-Time ATM Fraud Detection Pipeline

PySpark Structured Streaming pipeline that detects fraudulent ATM/card
transactions in real time using Kafka as the event backbone.

## Tech Stack
| Layer | Technology |
|---|---|
| Event streaming | Apache Kafka 3.5 |
| Stream processing | PySpark Structured Streaming 3.5 |
| Orchestration | Docker Compose (local) |
| Storage | Parquet (Data Lake sink) |
| Language | Python 3.11 |

## Architecture
```
[Transaction Generator]
        │  JSON events @ ~10 TPS
        ▼
[Kafka: transactions topic]  ◄─── 3 partitions
        │
        ▼
[Spark Structured Streaming]
   ├── Watermark: 2 minutes (handles late data)
   ├── Sliding windows: 5 min / 1 min slide
   │
   ├── Rule 1: Velocity (>5 txns OR >₹50k in window)
   └── Rule 2: High-value (single txn >₹30k)
        │
        ├──► [Kafka: fraud-alerts topic]  → downstream consumers
        ├──► [Parquet: /data/fraud_alerts/]  → audit trail / batch
        └──► [Console]  → development monitoring
```

## Fraud Detection Rules
| Rule | Condition | Output Mode |
|---|---|---|
| Velocity | >5 transactions per card in 5-min window | `update` |
| High Amount | Total spend >₹50,000 in 5-min window | `update` |
| High Value | Single transaction >₹30,000 | `append` |

## Quick Start
```bash
bash scripts/run_local.sh
```

## Key Concepts & Interview Answers

### Why watermarking?
In ATM networks, events arrive out of order due to network retries, timeouts,
and switch failovers — exactly like NDC retransmission scenarios.
Without `withWatermark("event_time", "2 minutes")`, Spark would keep every
window's state in memory forever, causing OOM. The watermark tells Spark:
"any event arriving more than 2 minutes late can be safely dropped."

### Why sliding windows vs tumbling?
A fraud burst that straddles a window boundary (e.g. 4 txns at 11:59,
4 txns at 12:01) would escape a tumbling window (5 txns max per window).
Sliding windows overlap, so the burst appears in multiple windows and is caught.

### Why `outputMode("update")` for Kafka but `outputMode("append")` for Parquet?
- `append` only emits finalised (watermark-passed) windows. Safe for files.
- `update` emits every time a window changes. Good for real-time downstream consumers.
- `complete` emits the full result table every trigger. Too expensive at scale.

### Exactly-once semantics
Checkpointing + Kafka's idempotent producer gives end-to-end exactly-once:
Spark tracks Kafka offsets in the checkpoint dir. On restart, it replays
from the last committed offset, not from scratch.

### Parallelism
Kafka partitions (3) = Spark task parallelism for that stage.
`spark.sql.shuffle.partitions=4` avoids the default-200 shuffle overhead.
