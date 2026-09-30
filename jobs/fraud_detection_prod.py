"""
fraud_detection_prod.py — Production-grade streaming job.

Wires together: Kafka source → 12 fraud rules → 3 output sinks.
All business logic lives in transforms.py. This file is pure infrastructure.

═══════════════════════════════════════════════════════════════════════════════
WHAT THIS FILE ADDS OVER THE DEV VERSION:
  ┌────────────────────────────────┬──────────────────────────────────────────┐
  │ Concern                        │ Production solution                      │
  ├────────────────────────────────┼──────────────────────────────────────────┤
  │ Configuration                  │ YAML config + env var overrides          │
  │ Logging                        │ Structured JSON logs (ELK/Datadog-ready) │
  │ Bad message handling           │ Dead Letter Queue (DLQ) Kafka topic      │
  │ Monitoring                     │ /health HTTP endpoint (K8s liveness)     │
  │ Graceful shutdown              │ SIGTERM handler flushes checkpoints first │
  │ Dormant card lookup            │ Stream-static join (broadcast, batch ref) │
  │ Alert deduplication            │ MD5 hash as alert_id for idempotency     │
  │ Checkpoints                    │ Durable path (S3/ADLS) — survives restart│
  │ Deployment                     │ Kubernetes + CI/CD (see deploy/)         │
  └────────────────────────────────┴──────────────────────────────────────────┘

DEPLOY:
  APP_ENV=prod spark-submit \\
    --master k8s://https://<cluster>:443 \\
    --deploy-mode cluster \\
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
    --conf spark.kubernetes.container.image=fraud-detection:1.0.0 \\
    jobs/fraud_detection_prod.py
"""

import os
import sys
import signal
import json
import logging
import yaml
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, to_json, struct, lit, md5, concat_ws
from pyspark.sql.types import StringType

sys.path.insert(0, str(Path(__file__).parent.parent))
from jobs.schemas import TRANSACTION_SCHEMA
from jobs.rules.fraud_rules import RULE_REGISTRY, rule_dormant_card


# ── Structured JSON Logging ───────────────────────────────────────────────────
# JSON logs are parseable by Datadog, ELK, GCP Cloud Logging automatically.
# Plain text logs require brittle regex parsing — avoid in production.
# Every log line is a complete JSON object with standard fields that log
# aggregation platforms can index and alert on without custom parsers.

class JSONFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "ts":      datetime.utcnow().isoformat(),
            "level":   record.levelname,
            "msg":     record.getMessage(),
            "service": "fraud-detection",
        })

_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(JSONFormatter())
logging.basicConfig(level=logging.INFO, handlers=[_handler])
log = logging.getLogger("fraud-detection")


# ── Config Loader ─────────────────────────────────────────────────────────────
# Reads config/app.yaml as the base, then overlays environment variables.
# Priority: env vars > app.yaml defaults.
# RULE: secrets (Kafka credentials, PagerDuty keys) NEVER go in app.yaml.
#       They come in as env vars, injected by Kubernetes Secrets at runtime.

def load_config() -> dict:
    path = Path(__file__).parent.parent / "config" / "app.yaml"
    with open(path) as f:
        cfg = yaml.safe_load(f)

    # Env var overrides — each tuple maps (yaml_section, yaml_key) to an env var name
    overrides = {
        ("kafka",   "broker"):              "KAFKA_BROKER",
        ("spark",   "checkpoint_dir"):      "CHECKPOINT_DIR",
        ("storage", "parquet_output"):      "PARQUET_OUTPUT",
        ("storage", "dormant_cards_path"):  "DORMANT_CARDS_PATH",
    }
    for (section, key), env_var in overrides.items():
        if val := os.getenv(env_var):
            cfg[section][key] = val

    log.info(f"config_loaded env={cfg['app']['env']}")
    return cfg


# ── Spark Session ─────────────────────────────────────────────────────────────
def build_spark(cfg: dict) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"{cfg['app']['name']}-{cfg['app']['env']}")
        # Kryo serialiser is faster than default Java serialiser for streaming
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Match shuffle partitions to Kafka partition count × parallelism factor
        # Default 200 creates 200 tiny tasks per micro-batch — very slow
        .config("spark.sql.shuffle.partitions", cfg["spark"]["shuffle_partitions"])
        # AQE (Adaptive Query Execution) — let Spark optimise joins at runtime
        .config("spark.sql.adaptive.enabled",   cfg["spark"]["adaptive_enabled"])
        .config("spark.sql.session.timeZone",   "UTC")
        .getOrCreate()
    )


# ── Kafka Source with DLQ routing ─────────────────────────────────────────────
# Returns two DataFrames:
#   good_df — rows that parsed successfully (txn_id is not null)
#   bad_df  — rows that failed to parse (sent to Dead Letter Queue)
#
# WHY A DLQ?
#   Without it, bad messages are silently dropped. In production, a spike
#   in DLQ messages means the upstream schema changed — critical to know.
#   Ops teams monitor DLQ consumer lag as a data quality signal.

def read_kafka(spark: SparkSession, cfg: dict):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", cfg["kafka"]["broker"])
        .option("subscribe",               cfg["kafka"]["input_topic"])
        .option("startingOffsets",         cfg["kafka"]["starting_offsets"])
        .option("maxOffsetsPerTrigger",    cfg["kafka"]["max_offsets_per_trigger"])
        # Production Kafka auth (MSK / Confluent Cloud) — uncomment and set secret:
        # .option("kafka.security.protocol", "SASL_SSL")
        # .option("kafka.sasl.mechanism",    "PLAIN")
        # .option("kafka.sasl.jaas.config",  os.getenv("KAFKA_SASL_CONFIG"))
        .load()
        .withColumn("raw_value", col("value").cast(StringType()))
    )

    parsed = raw.withColumn(
        "data", from_json(col("raw_value"), TRANSACTION_SCHEMA)
    ).select("data.*", "raw_value")

    good_df = parsed.filter(col("txn_id").isNotNull())
    bad_df  = parsed.filter(col("txn_id").isNull())
    return good_df, bad_df


# ── Apply all 12 rules ────────────────────────────────────────────────────────
# Iterates the RULE_REGISTRY, applies each rule, unions all alert DataFrames.
# A rule that raises an exception is logged and skipped — one broken rule
# does NOT bring down the entire pipeline (fault isolation).
#
# ALERT DEDUPLICATION:
#   Without dedup, every micro-batch that re-evaluates the same window
#   would re-emit the same alert. We add a deterministic alert_id:
#   MD5(card_id | rule_triggered | window_start)
#   Downstream consumers (case management systems) use alert_id as an
#   idempotency key — processing the same alert_id twice has no effect.

def apply_all_rules(df: DataFrame, dormant_ref_df=None) -> DataFrame:
    alert_dfs = []

    # Rules 1–9, 11, 12 (stateless — single DataFrame input)
    for rule_fn in RULE_REGISTRY:
        try:
            alert_dfs.append(rule_fn(df))
            log.info(f"rule_registered fn={rule_fn.__name__}")
        except Exception as e:
            log.error(f"rule_failed fn={rule_fn.__name__} error={e}")

    # Rule 10 — Dormant Card (needs static reference DataFrame)
    if dormant_ref_df is not None:
        try:
            alert_dfs.append(rule_dormant_card(df, dormant_ref_df))
            log.info("rule_registered fn=rule_dormant_card")
        except Exception as e:
            log.error(f"rule_failed fn=rule_dormant_card error={e}")
    else:
        log.warning("rule_dormant_card_disabled dormant_ref_df=None")

    # Union all rule outputs (all share the same schema via _to_alert)
    merged = alert_dfs[0]
    for adf in alert_dfs[1:]:
        merged = merged.union(adf)

    # Add deterministic dedup ID
    return merged.withColumn(
        "alert_id",
        md5(concat_ws("|",
            col("card_id"),
            col("rule_triggered"),
            col("window_start").cast("string"),
        ))
    )


# ── Output Sinks ──────────────────────────────────────────────────────────────
# Three independent sinks — each with its own checkpoint directory.
# Independent checkpoints mean: if the Kafka sink fails, Parquet and DLQ
# continue unaffected. Recovery restarts only the failed sink's offset.

def write_kafka_alerts(df: DataFrame, cfg: dict):
    """
    Publish alerts to Kafka fraud-alerts topic.
    Real-time consumers: notification service, case management, card blocking.
    outputMode("update") — emit on every change, not just finalised windows.
    Suitable for Kafka since it can handle out-of-order updates.
    """
    return (
        df.select(
            col("card_id").alias("key"),
            to_json(struct(
                "alert_id", "card_id", "rule_triggered", "risk_score",
                "txn_count", "total_amount", "window_start", "window_end", "alert_time"
            )).alias("value")
        )
        .writeStream
        .outputMode("update")
        .format("kafka")
        .option("kafka.bootstrap.servers", cfg["kafka"]["broker"])
        .option("topic", cfg["kafka"]["alert_topic"])
        .option("checkpointLocation", f"{cfg['spark']['checkpoint_dir']}/kafka-alerts")
        .trigger(processingTime=cfg["spark"]["trigger_interval"])
        .start()
    )


def write_parquet(df: DataFrame, cfg: dict):
    """
    Persist all alerts to Parquet for audit trail and ML training data.
    Partitioned by rule_triggered — cheap partition pruning for queries like
    "show me all GEO_VELOCITY alerts in the last 7 days".

    CRITICAL: outputMode must be "append" for file sinks with watermarked
    aggregations. Spark only finalises (appends) a window AFTER the watermark
    passes it — guaranteeing the window is complete before writing.
    Using "update" with a file sink would raise AnalysisException.
    """
    return (
        df.writeStream
        .outputMode("append")          # MUST be append for Parquet + watermarked agg
        .format("parquet")
        .option("path", f"{cfg['storage']['parquet_output']}/fraud_alerts/")
        .option("checkpointLocation", f"{cfg['spark']['checkpoint_dir']}/parquet")
        .partitionBy("rule_triggered") # partition pruning for downstream analytics
        .trigger(processingTime="60 seconds")   # batch writes, not per micro-batch
        .start()
    )


def write_dlq(bad_df: DataFrame, cfg: dict):
    """
    Route unparseable messages to a Dead Letter Queue Kafka topic.
    Preserves the original raw bytes — nothing is lost.
    Ops team monitors DLQ consumer lag in Grafana:
      - Normal: 0 messages / minute
      - Spike: upstream schema changed — needs investigation
    """
    return (
        bad_df.select(
            lit("parse_failure").alias("key"),
            col("raw_value").alias("value"),
        )
        .writeStream
        .outputMode("append")
        .format("kafka")
        .option("kafka.bootstrap.servers", cfg["kafka"]["broker"])
        .option("topic", cfg["kafka"]["dlq_topic"])
        .option("checkpointLocation", f"{cfg['spark']['checkpoint_dir']}/dlq")
        .trigger(processingTime="30 seconds")
        .start()
    )


# ── Graceful Shutdown ─────────────────────────────────────────────────────────
# Kubernetes sends SIGTERM before SIGKILL (default grace period: 30s).
# We catch SIGTERM to flush all stream checkpoints before the pod dies.
# Without this, the next pod restart may reprocess events already handled,
# causing duplicate alerts. Catching SIGTERM = clean offset commit = no dupes.

def setup_shutdown(queries: list):
    def _handle(sig, _frame):
        log.info(f"shutdown_signal_received sig={sig}")
        for q in queries:
            try:
                q.stop()
                log.info(f"query_stopped id={q.id}")
            except Exception as e:
                log.error(f"query_stop_failed id={q.id} error={e}")
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT,  _handle)


# ── Health Check HTTP Server ──────────────────────────────────────────────────
# Kubernetes liveness probe hits GET /health every 30 seconds.
# Returns 200 if all streaming queries are active, 503 if any have stopped.
# The probe failure threshold is 3 — 3 consecutive failures trigger a pod restart.
# This gives the pipeline 90 seconds to recover a transient error before restart.

def start_health_server(port: int, queries: list):
    import threading
    from http.server import HTTPServer, BaseHTTPRequestHandler

    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/health":
                all_active = all(q.isActive for q in queries)
                status     = 200 if all_active else 503
                body       = json.dumps({
                    "status":  "ok" if all_active else "degraded",
                    "queries": [{"id": str(q.id), "active": q.isActive} for q in queries],
                }).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)
        def log_message(self, *args): pass   # suppress HTTP access logs

    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    log.info(f"health_server_started port={port}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    cfg   = load_config()
    spark = build_spark(cfg)
    spark.sparkContext.setLogLevel(cfg["spark"]["log_level"])
    log.info(f"pipeline_starting env={cfg['app']['env']}")

    # Load dormant card reference (stream-static join for Rule 10)
    # This is read ONCE at startup — not re-read per micro-batch.
    # The nightly Airflow job refreshes the Parquet file; restart the
    # Spark job to pick up the latest dormant card list.
    dormant_ref_df = None
    try:
        dormant_ref_df = (
            spark.read
            .parquet(cfg["storage"]["dormant_cards_path"])
            .select("card_id")
            .distinct()
        )
        log.info("dormant_cards_loaded")
    except Exception as e:
        log.warning(f"dormant_cards_unavailable rule_10=disabled error={e}")

    # Ingest — split into good (parsed) and bad (DLQ) streams
    good_df, bad_df = read_kafka(spark, cfg)

    # Apply all 12 fraud rules
    alerts = apply_all_rules(good_df, dormant_ref_df)

    # Fan-out to 3 independent sinks
    queries = [
        write_kafka_alerts(alerts,  cfg),   # real-time alert consumers
        write_parquet(alerts,       cfg),   # audit trail + ML training
        write_dlq(bad_df,           cfg),   # unparseable message recovery
    ]

    # Start health server and wire shutdown handler
    start_health_server(cfg["monitoring"]["health_check_port"], queries)
    setup_shutdown(queries)

    log.info(f"all_queries_active count={len(queries)}")
    spark.streams.awaitAnyTermination()   # block until any query fails or stops


if __name__ == "__main__":
    main()
