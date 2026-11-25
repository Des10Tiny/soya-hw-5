import io
import json
import logging
import os
import struct
import time
from typing import Any, Dict

import fastavro
import requests
from confluent_kafka import KafkaException, Producer

logger = logging.getLogger("producer")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker1:29092,broker2:29092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "movie-events")
SCHEMA_FILE = os.getenv("SCHEMA_FILE", "/schemas/movie_event.avsc")


class MovieEventProducer:
    def __init__(self):
        self.topic = TOPIC_NAME
        self.schema_registry_url = SCHEMA_REGISTRY_URL
        self.schema_id = None
        self.avro_schema = None
        self.producer = None

    def initialize(self):
        logger.info(
            "Initializing Kafka Producer connecting to %s ...", BOOTSTRAP_SERVERS
        )
        producer_conf = {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 200,
            "enable.idempotence": True,
            "linger.ms": 10,
        }
        self.producer = Producer(producer_conf)

        if not os.path.exists(SCHEMA_FILE):
            raise FileNotFoundError(f"Schema file not found at {SCHEMA_FILE}")

        with open(SCHEMA_FILE, "r", encoding="utf-8") as f:
            raw_schema = json.load(f)
        self.avro_schema = fastavro.parse_schema(raw_schema)
        self.raw_schema_json = json.dumps(raw_schema)

        self._get_or_register_schema()

    def _get_or_register_schema(self, max_retries: int = 30):
        subject = f"{self.topic}-value"
        url = f"{self.schema_registry_url}/subjects/{subject}/versions"
        latest_url = f"{self.schema_registry_url}/subjects/{subject}/versions/latest"

        for attempt in range(max_retries):
            try:
                r = requests.get(latest_url, timeout=3)
                if r.status_code == 200:
                    self.schema_id = r.json()["id"]
                    logger.info(
                        "Using existing Schema ID: %d for subject %s",
                        self.schema_id,
                        subject,
                    )
                    return

                headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
                r = requests.post(
                    url,
                    headers=headers,
                    json={"schema": self.raw_schema_json},
                    timeout=3,
                )
                if r.status_code in (200, 201):
                    self.schema_id = r.json()["id"]
                    logger.info("Registered new schema! Schema ID: %d", self.schema_id)
                    return
            except Exception as e:
                logger.warning("Waiting for Schema Registry... (%s)", e)
            time.sleep(2)

        raise RuntimeError("Failed to obtain schema ID from Schema Registry")

    def serialize(self, event_dict: Dict[str, Any]) -> bytes:
        buffer = io.BytesIO()
        buffer.write(struct.pack(">bI", 0, self.schema_id))
        fastavro.schemaless_writer(buffer, self.avro_schema, event_dict)
        return buffer.getvalue()

    def publish(self, event_dict: Dict[str, Any], max_retries: int = 3) -> str:
        user_id = str(event_dict["user_id"])
        event_id = str(event_dict["event_id"])
        event_type = str(event_dict["event_type"])
        timestamp = event_dict["timestamp"]

        payload = self.serialize(event_dict)
        key = user_id.encode("utf-8")

        delivery_error = []

        def on_delivery(err, msg):
            if err:
                delivery_error.append(err)
            else:
                logger.info(
                    "Published event: event_id=%s, event_type=%s, timestamp=%s, partition=%d, offset=%d",
                    event_id,
                    event_type,
                    timestamp,
                    msg.partition(),
                    msg.offset(),
                )

        backoff = 0.2
        for attempt in range(1, max_retries + 1):
            try:
                delivery_error.clear()
                self.producer.produce(
                    topic=self.topic, key=key, value=payload, callback=on_delivery
                )
                self.producer.poll(0)
                remaining = self.producer.flush(timeout=5)
                if remaining > 0 or delivery_error:
                    err_msg = delivery_error[0] if delivery_error else "Flush timeout"
                    raise KafkaException(err_msg)

                return event_id
            except Exception as e:
                logger.warning(
                    "Attempt %d/%d failed to publish event %s: %s. Retrying in %.2fs...",
                    attempt,
                    max_retries,
                    event_id,
                    e,
                    backoff,
                )
                if attempt == max_retries:
                    logger.error("Exhausted retries publishing event %s", event_id)
                    raise
                time.sleep(backoff)
                backoff *= 2

        return event_id
