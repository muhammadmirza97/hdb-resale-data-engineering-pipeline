"""
Kafka Producer — HDB Resale Transaction Simulator
===================================================
Demonstrates a streaming ingestion pattern for HDB resale data.

NOTE: Real HDB resale data is published monthly in batch form on data.gov.sg.
      This Kafka component simulates what a streaming architecture would look
      like IF the data were available in real time (e.g. via a transactional
      system or a webhook).

Requirements:
  pip install kafka-python

Usage:
  python -m kafka.producer              # simulate 100 events at 1 event/sec
  python -m kafka.producer --count 50 --delay 0.5

Requires:
  A running Kafka broker on localhost:9092 (use docker-compose.yml kafka service).
"""

import argparse
import json
import random
import sys
import time
from datetime import datetime
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

TOWNS = [
    "ANG MO KIO", "BEDOK", "BISHAN", "BUKIT BATOK", "BUKIT MERAH",
    "BUKIT PANJANG", "BUKIT TIMAH", "CENTRAL AREA", "CHOA CHU KANG",
    "CLEMENTI", "GEYLANG", "HOUGANG", "JURONG EAST", "JURONG WEST",
    "KALLANG/WHAMPOA", "MARINE PARADE", "PASIR RIS", "PUNGGOL",
    "QUEENSTOWN", "SEMBAWANG", "SENGKANG", "SERANGOON", "TAMPINES",
    "TOA PAYOH", "WOODLANDS", "YISHUN",
]

FLAT_TYPES = ["2 ROOM", "3 ROOM", "4 ROOM", "5 ROOM", "EXECUTIVE"]
TOPIC = "hdb-resale-transactions"


def generate_event() -> dict:
    town = random.choice(TOWNS)
    flat_type = random.choice(FLAT_TYPES)
    floor_area = round(random.uniform(45, 160), 1)
    storey_low = random.randint(1, 40)
    storey_high = storey_low + 2
    lease_commence = random.randint(1975, 2020)
    sale_year = random.randint(2020, 2024)
    remaining_years = 99 - (sale_year - lease_commence)
    base_price = floor_area * random.uniform(4000, 12000)

    return {
        "event_id":       f"EVT-{random.randint(100000, 999999)}",
        "month":          f"{sale_year}-{random.randint(1,12):02d}",
        "town":           town,
        "flat_type":      flat_type,
        "block":          str(random.randint(1, 999)),
        "street_name":    f"{town} STREET {random.randint(1, 99)}",
        "storey_range":   f"{storey_low:02d} TO {storey_high:02d}",
        "floor_area_sqm": floor_area,
        "flat_model":     random.choice(["Improved", "New Generation", "Model A", "Standard", "Premium Apartment"]),
        "lease_commence_date": str(lease_commence),
        "remaining_lease": f"{remaining_years} years",
        "resale_price":   round(base_price / 1000) * 1000,
        "produced_at":    datetime.utcnow().isoformat(),
    }


def run_producer(count: int = 100, delay: float = 1.0) -> None:
    try:
        from kafka import KafkaProducer
    except ImportError:
        print("[KAFKA] kafka-python not installed. Run: pip install kafka-python")
        sys.exit(1)

    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"[KAFKA PRODUCER] Sending {count} events to topic '{TOPIC}' ...")
    for i in range(count):
        event = generate_event()
        producer.send(TOPIC, value=event)
        print(f"  [{i+1:>4}/{count}] {event['town']:20s} {event['flat_type']:12s} ${event['resale_price']:>9,.0f}")
        time.sleep(delay)

    producer.flush()
    producer.close()
    print(f"[KAFKA PRODUCER] Done. {count} events sent.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--delay", type=float, default=1.0)
    args = parser.parse_args()
    run_producer(count=args.count, delay=args.delay)
