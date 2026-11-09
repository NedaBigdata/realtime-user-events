import json
import logging
import os
import time

import psycopg2
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", 5432))
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "redpanda:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "user_events")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", 10))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 100))


def delivery_report(err, msg):
    if err is not None:
        log.error("Delivery failed: %s", err)


def extract_event(row):
    user = row["payload"]["results"][0]
    return {
        "raw_id": row["id"],
        "fetched_at": row["fetched_at"].isoformat(),
        "user_id": user["login"]["uuid"],
        "gender": user["gender"],
        "age": int(user["dob"]["age"]),
        "city": user["location"]["city"],
        "country": user["location"]["country"],
        "registered_date": user["registered"]["date"],
        "email": user["email"],
    }


def fetch_unprocessed(conn, limit):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.id, r.fetched_at, r.payload
            FROM users_raw r
            LEFT JOIN users_processed_log p ON p.raw_id = r.id
            WHERE p.raw_id IS NULL
            ORDER BY r.id ASC
            LIMIT %s
            """,
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def mark_processed(conn, raw_ids):
    if not raw_ids:
        return
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO users_processed_log (raw_id) VALUES (%s) ON CONFLICT DO NOTHING",
            [(rid,) for rid in raw_ids],
        )
    conn.commit()


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    log.info("Producer connected to %s", KAFKA_BROKER)

    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )

    while True:
        try:
            rows = fetch_unprocessed(conn, BATCH_SIZE)
            if not rows:
                time.sleep(POLL_INTERVAL)
                continue

            log.info("Sending %d rows to Kafka", len(rows))
            sent_ids = []
            for row in rows:
                event = extract_event(row)
                producer.produce(
                    KAFKA_TOPIC,
                    key=str(event["user_id"]).encode(),
                    value=json.dumps(event).encode(),
                    callback=delivery_report,
                )
                producer.poll(0)
                sent_ids.append(row["id"])

            producer.flush(timeout=30)
            mark_processed(conn, sent_ids)
            log.info("Marked %d rows as processed", len(sent_ids))
        except Exception as exc:
            log.exception("Producer loop error: %s", exc)
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()