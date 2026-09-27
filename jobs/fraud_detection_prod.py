"""
fraud_detection_prod.py — Production streaming job.

Wires together: Kafka source → 12 fraud rules → 3 sinks (Kafka / Parquet / DLQ).
All business logic lives in transforms.py. This file is infrastructure only.
"""

import os, sys, signal, json, logging, yaml
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, to_json, struct, lit, md5, concat_ws
from pyspark.sql.types import StringType

sys.path.insert(0, str(Path(__file__).parent.parent))
from jobs.schemas import TRANSACTION_SCHEMA
from jobs.rules.fraud_rules import RULE_REGISTRY, rule_dormant_card


# ── Structured JSON logging ───────────────────────────────────────────────────
class JSONFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "ts": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "msg": record.getMessage(),
            "service": "fraud-detection",
        })

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JSONFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])
log = logging.getLogger("fraud-detection")


# ── Config ────────────────────────────────────────────────────────────────────
def load_config() -> dict:
    path = Path(__file__).parent.parent / "config" / "app.yaml"
    with open(path) as f:
        cfg = yaml.safe_load(f)
    overrides = {
        ("kafka",   "broker"):         "KAFKA_BROKER",
        ("spark",   "checkpoint_dir"): "CHECKPOINT_DIR",
        ("storage", "parquet_output"): "PARQUET_OUTPUT",
        ("storage", "dormant_cards_path"): "DORMANT_CARDS_PATH",
    }
    for (section, key), env_var in overrides.items():
        if val := os.getenv(env_var):
            cfg[section][key] = val
    return cfg


# ── Spark session ─────────────────────────────────────────────────────────────
def build_spark(cfg: dict) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"{cfg['app']['name']}-{cfg['app']['env']}")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", cfg["spark"]["shuffle_partitions"])
        .config("spark.sql.adaptive.enabled",   cfg["spark"]["adaptive_enabled"])
        .getOrCreate()
    )


# ── Kafka source ──────────────────────────────────────────────────────────────
def read_kafka(spark: SparkSession, cfg: dict):
    """Returns (good_df, bad_df) — bad rows go to DLQ."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg["kafka"]["broker"])
        .option("subscribe", cfg["kafka"]["input_topic"])
        .option("startingOffsets", cfg["kafka"]["starting_offsets"])
        .option("maxOffsetsPerTrigger", cfg["kafka"]["max_offsets_per_trigger"])
        .load()
        .withColumn("raw_value", col("value").cast(StringType()))
    )
    parsed = raw.withColumn(
        "data", from_json(col("raw_value"), TRANSACTION_SCHEMA)
    ).select("data.*", "raw_value")

    return (
        parsed.filter(col("txn_id").isNotNull()),
        parsed.filter(col("txn_id").isNull()),
    )


# ── Apply all rules ───────────────────────────────────────────────────────────
def apply_all_rules(df: DataFrame, cfg: dict, dormant_ref_df=None) -> DataFrame:
    alert_dfs = []
    for rule_fn in RULE_REGISTRY:
        try:
            alert_dfs.append(rule_fn(df))
            log.info(f"rule_registered fn={rule_fn.__name__}")
        except Exception as e:
            log.error(f"rule_failed fn={rule_fn.__name__} error={e}")

    if dormant_ref_df is not None:
        try:
            alert_dfs.append(rule_dormant_card(df, dormant_ref_df))
        except Exception as e:
            log.error(f"rule_failed fn=rule_dormant_card error={e}")

    merged = alert_dfs[0]
    for adf in alert_dfs[1:]:
        merged = merged.union(adf)

    # Deterministic dedup ID — downstream consumers key on this
    return merged.withColumn(
        "alert_id",
        md5(concat_ws("|", col("card_id"), col("rule_triggered"),
                      col("window_start").cast("string")))
    )


# ── Sinks ─────────────────────────────────────────────────────────────────────
def write_kafka_alerts(df, cfg):
    chk = f"{cfg['spark']['checkpoint_dir']}/kafka-alerts"
    return (
        df.select(col("card_id").alias("key"), to_json(struct("*")).alias("value"))
        .writeStream.outputMode("update").format("kafka")
        .option("kafka.bootstrap.servers", cfg["kafka"]["broker"])
        .option("topic", cfg["kafka"]["alert_topic"])
        .option("checkpointLocation", chk)
        .trigger(processingTime=cfg["spark"]["trigger_interval"])
        .start()
    )


def write_parquet(df, cfg):
    chk = f"{cfg['spark']['checkpoint_dir']}/parquet"
    return (
        df.writeStream.outputMode("append").format("parquet")
        .option("path", f"{cfg['storage']['parquet_output']}/fraud_alerts/")
        .option("checkpointLocation", chk)
        .partitionBy("rule_triggered")
        .trigger(processingTime="60 seconds")
        .start()
    )


def write_dlq(bad_df, cfg):
    chk = f"{cfg['spark']['checkpoint_dir']}/dlq"
    return (
        bad_df.select(
            lit("parse_failure").alias("key"),
            col("raw_value").alias("value")
        )
        .writeStream.outputMode("append").format("kafka")
        .option("kafka.bootstrap.servers", cfg["kafka"]["broker"])
        .option("topic", cfg["kafka"]["dlq_topic"])
        .option("checkpointLocation", chk)
        .trigger(processingTime="30 seconds")
        .start()
    )


# ── Graceful shutdown ─────────────────────────────────────────────────────────
def setup_shutdown(queries: list):
    def _handle(sig, _):
        log.info(f"shutdown_signal sig={sig}")
        for q in queries:
            try:
                q.stop()
            except Exception as e:
                log.error(f"stop_failed id={q.id} error={e}")
        sys.exit(0)
    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT,  _handle)


# ── Health server ─────────────────────────────────────────────────────────────
def start_health_server(port: int, queries: list):
    import threading
    from http.server import HTTPServer, BaseHTTPRequestHandler

    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            ok = all(q.isActive for q in queries)
            body = json.dumps({"status": "ok" if ok else "degraded"}).encode()
            self.send_response(200 if ok else 503)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)
        def log_message(self, *a): pass

    threading.Thread(target=HTTPServer(("0.0.0.0", port), H).serve_forever,
                     daemon=True).start()
    log.info(f"health_server_started port={port}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    cfg   = load_config()
    spark = build_spark(cfg)
    spark.sparkContext.setLogLevel(cfg["spark"]["log_level"])

    log.info(f"pipeline_starting env={cfg['app']['env']}")

    # Load static dormant-card reference (stream-static join)
    dormant_ref_df = None
    try:
        dormant_ref_df = spark.read.parquet(cfg["storage"]["dormant_cards_path"]).select("card_id")
        log.info("dormant_cards_loaded")
    except Exception as e:
        log.warning(f"dormant_cards_unavailable error={e} rule_10=disabled")

    good_df, bad_df = read_kafka(spark, cfg)
    alerts = apply_all_rules(good_df, cfg, dormant_ref_df)

    queries = [
        write_kafka_alerts(alerts,  cfg),
        write_parquet(alerts,       cfg),
        write_dlq(bad_df,           cfg),
    ]

    start_health_server(cfg["monitoring"]["health_check_port"], queries)
    setup_shutdown(queries)

    log.info("all_queries_active")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
