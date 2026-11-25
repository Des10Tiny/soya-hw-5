import json
import logging
import os
import time
from typing import Any, Dict

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

logger = logging.getLogger("s3-exporter")

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "movie-analytics")
S3_REGION = os.getenv("S3_REGION", "us-east-1")


class S3Exporter:
    def __init__(self, pg_writer):
        self.pg_writer = pg_writer
        self.s3_client = None

    def get_client(self):
        if self.s3_client:
            return self.s3_client
        for attempt in range(30):
            try:
                client = boto3.client(
                    "s3",
                    endpoint_url=S3_ENDPOINT,
                    aws_access_key_id=S3_ACCESS_KEY,
                    aws_secret_access_key=S3_SECRET_KEY,
                    region_name=S3_REGION,
                    config=Config(signature_version="s3v4"),
                )
                try:
                    client.head_bucket(Bucket=S3_BUCKET)
                except ClientError:
                    client.create_bucket(Bucket=S3_BUCKET)
                    logger.info("Created S3 bucket '%s'", S3_BUCKET)

                self.s3_client = client
                logger.info("Connected to MinIO/S3 successfully!")
                return self.s3_client
            except Exception as e:
                logger.warning("Waiting for MinIO/S3... (%s)", e)
                time.sleep(2)
        raise RuntimeError("Could not connect to MinIO/S3 after retries")

    def export_daily_aggregates(
        self, target_date: str, max_retries: int = 3
    ) -> Dict[str, Any]:
        logger.info("Starting S3 export for date %s ...", target_date)
        s3_key = f"daily/{target_date}/aggregates.json"
        s3_uri = f"s3://{S3_BUCKET}/{s3_key}"

        daily_data = self.pg_writer.fetch_daily_metrics(target_date)
        if not daily_data:
            logger.warning(
                "No metrics found in PostgreSQL for date %s. Nothing to export.",
                target_date,
            )
            return {
                "status": "skipped",
                "reason": "no_metrics_found",
                "date": target_date,
            }

        json_bytes = json.dumps(daily_data, indent=2, ensure_ascii=False).encode(
            "utf-8"
        )

        backoff = 0.5
        for attempt in range(1, max_retries + 1):
            try:
                client = self.get_client()
                client.put_object(
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                    Body=json_bytes,
                    ContentType="application/json",
                )
                logger.info(
                    "Successfully exported aggregates to %s (%d bytes)",
                    s3_uri,
                    len(json_bytes),
                )
                self.pg_writer.log_export(target_date, s3_uri, "SUCCESS")
                return {
                    "status": "success",
                    "s3_path": s3_uri,
                    "date": target_date,
                    "size_bytes": len(json_bytes),
                }
            except Exception as e:
                logger.error(
                    "Attempt %d/%d: S3 upload failed for %s: %s",
                    attempt,
                    max_retries,
                    s3_uri,
                    e,
                )
                if attempt == max_retries:
                    self.pg_writer.log_export(target_date, s3_uri, "FAILED", str(e))
                    raise
                time.sleep(backoff)
                backoff *= 2
