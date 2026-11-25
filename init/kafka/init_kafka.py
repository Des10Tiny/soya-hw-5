import json
import logging
import os
import sys
import time

import requests
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("kafka-init")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker1:29092,broker2:29092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "movie-events")
NUM_PARTITIONS = int(os.getenv("KAFKA_PARTITIONS", "3"))
REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "2"))
MIN_ISR = os.getenv("KAFKA_MIN_ISR", "1")
SCHEMA_FILE = os.getenv("SCHEMA_FILE", "/schemas/movie_event.avsc")


def wait_for_services():
    logger.info("Waiting for Schema Registry at %s ...", SCHEMA_REGISTRY_URL)
    for _ in range(60):
        try:
            r = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=3)
            if r.status_code == 200:
                logger.info("Schema Registry is ready!")
                break
        except Exception:
            pass
        time.sleep(2)
    else:
        logger.error("Timed out waiting for Schema Registry")
        sys.exit(1)

    logger.info("Checking Kafka brokers at %s ...", BOOTSTRAP_SERVERS)
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    for _ in range(60):
        try:
            md = admin.list_topics(timeout=5)
            if len(md.brokers) >= REPLICATION_FACTOR:
                logger.info("Kafka cluster is ready with %d brokers!", len(md.brokers))
                return admin
        except Exception as e:
            logger.info("Waiting for Kafka brokers... (%s)", e)
        time.sleep(2)
    logger.error("Timed out waiting for Kafka brokers")
    sys.exit(1)


def create_topic(admin: AdminClient):
    md = admin.list_topics(timeout=10)
    if TOPIC_NAME in md.topics and len(md.topics[TOPIC_NAME].partitions) > 0:
        logger.info(
            "Topic '%s' already exists with %d partitions.",
            TOPIC_NAME,
            len(md.topics[TOPIC_NAME].partitions),
        )
        return

    logger.info(
        "Creating topic '%s' with %d partitions, replication factor %d, min.insync.replicas=%s...",
        TOPIC_NAME,
        NUM_PARTITIONS,
        REPLICATION_FACTOR,
        MIN_ISR,
    )
    topic = NewTopic(
        topic=TOPIC_NAME,
        num_partitions=NUM_PARTITIONS,
        replication_factor=REPLICATION_FACTOR,
        config={"min.insync.replicas": MIN_ISR},
    )
    futures = admin.create_topics([topic])
    for topic_name, future in futures.items():
        try:
            future.result()
            logger.info("Topic '%s' created successfully!", topic_name)
        except Exception as e:
            logger.error("Failed to create topic '%s': %s", topic_name, e)
            sys.exit(1)


def register_schema():
    with open(SCHEMA_FILE, "r", encoding="utf-8") as f:
        schema_dict = json.load(f)

    subject = f"{TOPIC_NAME}-value"
    logger.info("Registering schema under subject '%s'...", subject)
    payload = {"schema": json.dumps(schema_dict)}
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    response = requests.post(url, headers=headers, json=payload, timeout=10)
    if response.status_code in (200, 201):
        data = response.json()
        logger.info("Schema registered successfully! Schema ID: %s", data.get("id"))
    else:
        check = requests.get(
            f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest", timeout=5
        )
        if check.status_code == 200:
            logger.info("Schema already registered. Latest info: %s", check.json())
        else:
            logger.error("Failed to register schema: %s", response.text)
            sys.exit(1)


def main():
    admin = wait_for_services()
    create_topic(admin)
    register_schema()
    logger.info("Kafka and Schema Registry initialization complete.")


if __name__ == "__main__":
    main()
