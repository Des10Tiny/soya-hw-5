import json
import os
import time
import uuid
from datetime import datetime, timezone

import boto3
import clickhouse_connect
import psycopg2
import pytest
import requests
from botocore.client import Config

PRODUCER_URL = os.getenv("PRODUCER_URL", "http://producer:8000")
AGGREGATOR_URL = os.getenv("AGGREGATOR_URL", "http://aggregator:8001")
CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
PG_DB = os.getenv("POSTGRES_DB", "analytics")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "movie-analytics")


@pytest.fixture(scope="session")
def ch_client():
    client = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username="default",
        password="",
        database="default"
    )
    return client


@pytest.fixture(scope="session")
def pg_conn():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB
    )
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def s3_client():
    client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
        config=Config(signature_version="s3v4")
    )
    return client


def test_producer_health():
    res = requests.get(f"{PRODUCER_URL}/health", timeout=5)
    assert res.status_code == 200
    assert res.json().get("status") == "ok"


def test_producer_validation_error():
    invalid_event = {
        "user_id": "test_user",
    }
    res = requests.post(f"{PRODUCER_URL}/events", json=invalid_event, timeout=5)
    assert res.status_code in (400, 422)


def test_end_to_end_pipeline(ch_client):
    test_event_id = str(uuid.uuid4())
    test_user_id = f"test_user_{uuid.uuid4().hex[:8]}"
    test_movie_id = "m-test-movie-42"
    test_session_id = str(uuid.uuid4())
    now_iso = datetime.now(timezone.utc).isoformat()
    progress_seconds = 750

    event_payload = {
        "event_id": test_event_id,
        "user_id": test_user_id,
        "movie_id": test_movie_id,
        "event_type": "VIEW_PAUSED",
        "timestamp": now_iso,
        "device_type": "TV",
        "session_id": test_session_id,
        "progress_seconds": progress_seconds
    }

    post_res = requests.post(f"{PRODUCER_URL}/events", json=event_payload, timeout=10)
    assert post_res.status_code == 201, f"Failed to publish event: {post_res.text}"
    body = post_res.json()
    assert body["event_id"] == test_event_id

    max_wait_seconds = 20
    start_time = time.time()
    found_row = None

    query = """
    SELECT
        toString(event_id),
        user_id,
        movie_id,
        event_type,
        device_type,
        session_id,
        progress_seconds
    FROM default.movie_events
    WHERE event_id = {event_id:UUID}
    """

    while time.time() - start_time < max_wait_seconds:
        res = ch_client.query(query, parameters={"event_id": test_event_id})
        if res.result_rows:
            found_row = res.result_rows[0]
            break
        time.sleep(1)

    assert found_row is not None, f"Event {test_event_id} was not found in ClickHouse within {max_wait_seconds}s"

    assert found_row[0] == test_event_id
    assert found_row[1] == test_user_id
    assert found_row[2] == test_movie_id
    assert found_row[3] == "VIEW_PAUSED"
    assert found_row[4] == "TV"
    assert found_row[5] == test_session_id
    assert found_row[6] == progress_seconds


def test_aggregation_and_export_pipeline(pg_conn, s3_client):
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    agg_res = requests.post(f"{AGGREGATOR_URL}/aggregate?date={today_str}", timeout=20)
    assert agg_res.status_code == 200, f"Aggregation failed: {agg_res.text}"
    data = agg_res.json()
    assert data["status"] == "success"
    assert "metrics" in data

    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT metric_name, metric_value FROM daily_metrics WHERE metric_date = %s",
            (today_str,)
        )
        rows = dict(cur.fetchall())

    assert "dau" in rows
    assert "avg_watch_time" in rows
    assert "conversion_rate" in rows

    exp_res = requests.post(f"{AGGREGATOR_URL}/export?date={today_str}", timeout=20)
    assert exp_res.status_code == 200
    export_body = exp_res.json()
    assert export_body["status"] == "success"

    s3_key = f"daily/{today_str}/aggregates.json"
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
    body = obj["Body"].read().decode("utf-8")
    parsed = json.loads(body)
    assert parsed["date"] == today_str
    assert "metrics" in parsed
