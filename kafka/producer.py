"""
Kafka Producer — World Happiness Streaming ETL
Reads the unified processed dataset and streams records one by one
to the 'happiness-predictions' topic as JSON events.
"""
import json
import time
import argparse
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

TOPIC = "happiness-predictions"
BOOTSTRAP_SERVERS = ["localhost:9092"]
DATA_PATH = "../data/processed/happiness_unified.csv"


def make_event(row: pd.Series) -> dict:
    return {
        "country": str(row["country"]),
        "year": int(row["year"]),
        "gdp": round(float(row["gdp"]), 4),
        "family": round(float(row["family"]), 4),
        "health": round(float(row["health"]), 4),
        "freedom": round(float(row["freedom"]), 4),
        "generosity": round(float(row["generosity"]), 4),
        "corruption": round(float(row["corruption"]), 4),
        "actual_happiness_score": round(float(row["happiness_score"]), 4),
    }


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def stream(delay: float = 0.5, limit: int | None = None) -> None:
    df = pd.read_csv(DATA_PATH)
    print(f"Loaded {len(df)} records from {DATA_PATH}")

    try:
        producer = create_producer()
    except NoBrokersAvailable:
        print("ERROR: Kafka broker not reachable. Start docker-compose first.")
        return

    sent = 0
    for _, row in df.iterrows():
        if limit and sent >= limit:
            break
        event = make_event(row)
        key = f"{event['country']}_{event['year']}"
        future = producer.send(TOPIC, key=key, value=event)
        future.get(timeout=10)
        print(f"[SENT] {key} → happiness={event['actual_happiness_score']}")
        sent += 1
        time.sleep(delay)

    producer.flush()
    producer.close()
    print(f"\nDone. Sent {sent} events to topic '{TOPIC}'.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Happiness Kafka Producer")
    parser.add_argument("--delay", type=float, default=0.5, help="Seconds between events")
    parser.add_argument("--limit", type=int, default=None, help="Max events to send")
    args = parser.parse_args()
    stream(delay=args.delay, limit=args.limit)
