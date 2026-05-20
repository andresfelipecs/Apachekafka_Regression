"""
Kafka Consumer — World Happiness Streaming ETL
Consumes events from 'happiness-predictions' topic and:
  1. Stores raw event in raw_happiness_events (before any processing)
  2. Validates event schema
  3. Loads pre-trained model
  4. Generates happiness score prediction
  5. Stores prediction result in fact_predictions (with dimension linkage)
"""
import json
import sys
import traceback
from datetime import datetime, timezone

import joblib
import numpy as np
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

TOPIC = "happiness-predictions"
BOOTSTRAP_SERVERS = ["localhost:9092"]
GROUP_ID = "happiness-consumer-group"
MODEL_PATH = "../models/model.pkl"
FEATURE_META_PATH = "../models/feature_meta.pkl"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "happiness_db",
    "user": "etl_user",
    "password": "etl_pass",
}

REQUIRED_FIELDS = {"country", "year", "gdp", "family", "health", "freedom", "generosity", "corruption"}
NUMERIC_FIELDS = {"gdp", "family", "health", "freedom", "generosity", "corruption", "year"}


# ─────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────
def validate_event(event: dict) -> tuple[bool, str]:
    missing = REQUIRED_FIELDS - set(event.keys())
    if missing:
        return False, f"INVALID_SCHEMA: missing fields {missing}"

    for field in NUMERIC_FIELDS:
        try:
            val = float(event[field])
            if np.isnan(val) or np.isinf(val):
                return False, f"INVALID_VALUES: {field} is NaN/Inf"
        except (TypeError, ValueError):
            return False, f"INVALID_VALUES: {field}={event[field]!r} is not numeric"

    return True, "VALID"


# ─────────────────────────────────────────────
# Database helpers
# ─────────────────────────────────────────────
def get_or_create_country(cur, country_name: str) -> int:
    cur.execute("SELECT country_id FROM dim_country WHERE country_name = %s", (country_name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "INSERT INTO dim_country (country_name) VALUES (%s) RETURNING country_id",
        (country_name,),
    )
    return cur.fetchone()[0]


def get_or_create_date(cur, year: int) -> int:
    cur.execute("SELECT date_id FROM dim_date WHERE year = %s", (year,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO dim_date (year) VALUES (%s) RETURNING date_id", (year,))
    return cur.fetchone()[0]


def insert_raw_event(cur, payload: dict, status: str) -> int:
    cur.execute(
        "INSERT INTO raw_happiness_events (raw_payload, processing_status) VALUES (%s, %s) RETURNING raw_event_id",
        (json.dumps(payload), status),
    )
    return cur.fetchone()[0]


def insert_dim_raw_event(cur, raw_event_id: int, event: dict) -> None:
    cur.execute(
        """INSERT INTO dim_raw_event
           (raw_event_id, country, year, gdp, family, health, freedom, generosity, corruption)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (raw_event_id) DO NOTHING""",
        (
            raw_event_id,
            event.get("country"),
            event.get("year"),
            event.get("gdp"),
            event.get("family"),
            event.get("health"),
            event.get("freedom"),
            event.get("generosity"),
            event.get("corruption"),
        ),
    )


def insert_prediction(cur, raw_event_id: int, country_id: int, date_id: int,
                      actual: float, predicted: float) -> None:
    error = actual - predicted
    cur.execute(
        """INSERT INTO fact_predictions
           (raw_event_id, country_id, date_id, actual_score, predicted_score, prediction_error)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        (raw_event_id, country_id, date_id, round(actual, 4), round(predicted, 4), round(error, 4)),
    )


# ─────────────────────────────────────────────
# Main consumer loop
# ─────────────────────────────────────────────
def consume() -> None:
    print("Loading model...")
    model = joblib.load(MODEL_PATH)
    feature_meta = joblib.load(FEATURE_META_PATH)
    features = feature_meta["features"]
    print(f"Model loaded. Features: {features}")

    print("Connecting to database...")
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False

    print("Connecting to Kafka...")
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=GROUP_ID,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
    except NoBrokersAvailable:
        print("ERROR: Kafka broker not reachable. Start docker-compose first.")
        conn.close()
        return

    print(f"Listening on topic '{TOPIC}'... (Ctrl+C to stop)\n")

    for message in consumer:
        event = message.value
        status = "VALID"
        raw_event_id = None

        try:
            cur = conn.cursor()

            # 1. Store raw event BEFORE any processing
            is_valid, status = validate_event(event)
            raw_event_id = insert_raw_event(cur, event, status)
            conn.commit()

            if not is_valid:
                print(f"[INVALID] raw_id={raw_event_id} status={status} payload={event}")
                cur.close()
                continue

            # 2. Store dim_raw_event
            insert_dim_raw_event(cur, raw_event_id, event)

            # 3. Ensure feature ordering consistency
            row = {}
            for feat in features:
                if feat == "social_wellbeing":
                    row[feat] = float(event["family"]) + float(event["health"])
                elif feat == "positive_factors":
                    row[feat] = (float(event["gdp"]) + float(event["freedom"])
                                 + float(event["generosity"]) - float(event["corruption"]))
                else:
                    row[feat] = float(event[feat])

            X = np.array([[row[f] for f in features]])

            # 4. Generate prediction
            predicted_score = float(model.predict(X)[0])
            actual_score = float(event.get("actual_happiness_score", 0.0))

            # 5. Lookup/create dimension keys
            country_id = get_or_create_country(cur, event["country"])
            date_id = get_or_create_date(cur, int(event["year"]))

            # 6. Store prediction result
            insert_prediction(cur, raw_event_id, country_id, date_id, actual_score, predicted_score)
            conn.commit()

            error = actual_score - predicted_score
            print(
                f"[OK] raw_id={raw_event_id} | {event['country']} {event['year']} "
                f"| actual={actual_score:.3f} predicted={predicted_score:.3f} error={error:.3f}"
            )
            cur.close()

        except Exception as exc:
            conn.rollback()
            print(f"[ERROR] {exc}")
            traceback.print_exc()
            # Update raw event status to PREDICTION_ERROR if we have an id
            if raw_event_id:
                try:
                    cur2 = conn.cursor()
                    cur2.execute(
                        "UPDATE raw_happiness_events SET processing_status='PREDICTION_ERROR' WHERE raw_event_id=%s",
                        (raw_event_id,),
                    )
                    conn.commit()
                    cur2.close()
                except Exception:
                    conn.rollback()

    consumer.close()
    conn.close()


if __name__ == "__main__":
    consume()
