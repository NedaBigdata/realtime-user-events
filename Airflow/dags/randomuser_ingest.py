from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

import psycopg2
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", 5432))
POSTGRES_DB = os.environ.get("POSTGRES_DB", "user_events")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "rte_user")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "rte_pass")
RANDOMUSER_URL = "https://randomuser.me/api/?results=20&nat=us,gb,de,fr,ir"

default_args = {
    "owner": "rte",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def fetch_and_store() -> None:
    """از randomuser.me می‌خواند و پاسخ خام JSON را در users_raw درج می‌کند."""
    log.info("Fetching data from %s", RANDOMUSER_URL)
    response = requests.get(RANDOMUSER_URL, params={"results": 1}, timeout=15)
    response.raise_for_status()
    payload = response.json()

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users_raw (source, payload)
                VALUES (%s, %s::jsonb)
                """,
                ("randomuser.me", json.dumps(payload)),
            )
        log.info("Inserted one row into users_raw")
    finally:
        conn.close()


with DAG(
    dag_id="randomuser_ingest",
    description="Fetch random users from randomuser.me and store raw JSON in Postgres",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/1 * * * *",  # هر ۵ دقیقه
    catchup=False,
    tags=["ingest", "randomuser"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_and_store",
        python_callable=fetch_and_store,
    )