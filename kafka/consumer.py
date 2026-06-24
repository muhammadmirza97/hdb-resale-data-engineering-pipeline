"""
Kafka Consumer — HDB Resale Transaction Ingester
==================================================
Reads events from the 'hdb-resale-transactions' Kafka topic and
writes them into the DuckDB raw staging table in micro-batches.

NOTE: This is a demonstration of a streaming ingestion pattern.
      Real HDB data is batch-only; this simulates what would happen
      if transactions were available in real time.

Requirements:
  pip install kafka-python

Usage:
  python -m kafka.consumer              # runs until interrupted (Ctrl+C)
  python -m kafka.consumer --max 200    # stop after 200 messages
"""

import argparse
import json
import signal
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

TOPIC = "hdb-resale-transactions"
BATCH_SIZE = 50
_running = True


def _handle_signal(sig, frame):
    global _running
    print("\n[KAFKA CONSUMER] Interrupt received — flushing and exiting...")
    _running = False


def run_consumer(max_messages: int = 0) -> None:
    try:
        from kafka import KafkaConsumer
    except ImportError:
        print("[KAFKA] kafka-python not installed. Run: pip install kafka-python")
        sys.exit(1)

    import pandas as pd
    from src.storage import get_connection
    from src.config_loader import load_config

    config = load_config()
    raw_table = config["tables"]["raw_table"]

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=["localhost:9092"],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="hdb-resale-consumer-group",
        consumer_timeout_ms=10000,
    )

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    buffer = []
    total = 0

    print(f"[KAFKA CONSUMER] Listening on topic '{TOPIC}' ...")

    for message in consumer:
        if not _running:
            break
        buffer.append(message.value)
        total += 1

        if len(buffer) >= BATCH_SIZE:
            _flush(buffer, raw_table)
            buffer.clear()
            print(f"[KAFKA CONSUMER] Total received: {total:,}")

        if max_messages and total >= max_messages:
            break

    if buffer:
        _flush(buffer, raw_table)

    consumer.close()
    print(f"[KAFKA CONSUMER] Finished. Total messages processed: {total:,}")


def _flush(records: list, raw_table: str) -> None:
    import pandas as pd
    from src.storage import get_connection

    df = pd.DataFrame(records)
    conn = get_connection()
    conn.execute(f"INSERT INTO {raw_table} SELECT * FROM df")
    conn.close()
    print(f"[KAFKA CONSUMER] Flushed {len(records)} records → {raw_table}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max", type=int, default=0, dest="max_messages",
                        help="Stop after N messages (0 = run until interrupted)")
    args = parser.parse_args()
    run_consumer(max_messages=args.max_messages)
