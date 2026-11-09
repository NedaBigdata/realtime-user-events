import json
import logging
import os
from datetime import datetime

import clickhouse_connect
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "redpanda:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "user_events")
KAFKA_GROUP = os.environ.get("KAFKA_GROUP", "clickhouse-consumer")

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.environ["CLICKHOUSE_USER"]
CLICKHOUSE_PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]
CLICKHOUSE_DB = os.environ["CLICKHOUSE_DB"]

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 100))


def parse_event(raw):
    e = json.loads(raw.decode("utf-8"))
    return (
        datetime.fromisoformat(e["fetched_at"].replace("Z", "+00:00")),
        str(e["user_id"]),
        str(e["gender"]),
        int(e["age"]),
        str(e["city"]),
        str(e["country"]),
        datetime.fromisoformat(e["registered_date"].replace("Z", "+00:00")),
        json.dumps(e),
    )


def flush(client, rows):
    if not rows:
        return
    try:
        client.insert(
            "user_events",
            rows,
            column_names=[
                "event_time", "user_id", "gender", "age",
                "city", "country", "registered_date", "raw",
            ],
        )
        log.info("Inserted %d rows into ClickHouse", len(rows))
    except Exception as exc:
        log.exception("ClickHouse insert failed: %s", exc)


def main():
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )
    log.info("Connected to ClickHouse %s:%s", CLICKHOUSE_HOST, CLICKHOUSE_PORT)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": KAFKA_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([KAFKA_TOPIC])
    log.info("Subscribed to %s as group %s", KAFKA_TOPIC, KAFKA_GROUP)

    buffer = []
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if buffer:
                    flush(client, buffer)
                    consumer.commit(asynchronous=False)
                    buffer = []
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("Kafka error: %s", msg.error())
                continue

            try:
                buffer.append(parse_event(msg.value()))
            except Exception as exc:
                log.exception("Bad message dropped: %s", exc)
                continue

            if len(buffer) >= BATCH_SIZE:
                flush(client, buffer)
                consumer.commit(asynchronous=False)
                buffer = []
    except KeyboardInterrupt:
        pass
    finally:
        if buffer:
            flush(client, buffer)
            consumer.commit(asynchronous=False)
        consumer.close()


if __name__ == "__main__":
    main()