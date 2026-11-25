import json
import logging
import os
import time
from typing import Any, Dict

import psycopg2
from psycopg2.extras import execute_batch

logger = logging.getLogger("pg-writer")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
PG_DB = os.getenv("POSTGRES_DB", "analytics")


class PostgresWriter:
    def __init__(self):
        self.conn = None

    def get_connection(self):
        if self.conn and not self.conn.closed:
            return self.conn
        for attempt in range(30):
            try:
                self.conn = psycopg2.connect(
                    host=PG_HOST,
                    port=PG_PORT,
                    user=PG_USER,
                    password=PG_PASSWORD,
                    dbname=PG_DB,
                    connect_timeout=5,
                )
                self.conn.autocommit = True
                logger.info("Connected to PostgreSQL successfully!")
                return self.conn
            except Exception as e:
                logger.warning("Waiting for PostgreSQL... (%s)", e)
                time.sleep(2)
        raise RuntimeError("Could not connect to PostgreSQL after retries")

    def save_metrics(self, calculation_result: Dict[str, Any], max_retries: int = 3):
        target_date = calculation_result["date"]
        calc_at = calculation_result["calculated_at"]
        metrics = calculation_result["metrics"]

        rows_to_upsert = [
            (target_date, "dau", float(metrics["dau"]), json.dumps({}), calc_at),
            (
                target_date,
                "avg_watch_time",
                float(metrics["avg_watch_time"]),
                json.dumps({}),
                calc_at,
            ),
            (
                target_date,
                "views_started",
                float(metrics["views_started"]),
                json.dumps({}),
                calc_at,
            ),
            (
                target_date,
                "views_finished",
                float(metrics["views_finished"]),
                json.dumps({}),
                calc_at,
            ),
            (
                target_date,
                "conversion_rate",
                float(metrics["conversion_rate"]),
                json.dumps({}),
                calc_at,
            ),
            (
                target_date,
                "retention_d1",
                float(metrics["retention_d1"]),
                json.dumps({}),
                calc_at,
            ),
            (
                target_date,
                "retention_d7",
                float(metrics["retention_d7"]),
                json.dumps({}),
                calc_at,
            ),
            (
                target_date,
                "top_movies",
                float(len(metrics["top_movies"])),
                json.dumps({"ranking": metrics["top_movies"]}),
                calc_at,
            ),
        ]

        upsert_sql = """
        INSERT INTO daily_metrics (metric_date, metric_name, metric_value, dimensions, calculated_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (metric_date, metric_name)
        DO UPDATE SET
            metric_value = EXCLUDED.metric_value,
            dimensions = EXCLUDED.dimensions,
            calculated_at = EXCLUDED.calculated_at;
        """

        backoff = 0.5
        for attempt in range(1, max_retries + 1):
            try:
                conn = self.get_connection()
                with conn.cursor() as cur:
                    execute_batch(cur, upsert_sql, rows_to_upsert)
                logger.info(
                    "Idempotently saved %d metrics into PostgreSQL for date %s",
                    len(rows_to_upsert),
                    target_date,
                )
                return
            except Exception as e:
                logger.error(
                    "Attempt %d/%d: Failed to write metrics to PostgreSQL: %s",
                    attempt,
                    max_retries,
                    e,
                )
                if self.conn:
                    try:
                        self.conn.close()
                    except Exception:
                        pass
                    self.conn = None
                if attempt == max_retries:
                    raise
                time.sleep(backoff)
                backoff *= 2

    def fetch_daily_metrics(self, target_date: str) -> Dict[str, Any]:
        conn = self.get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT metric_name, metric_value, dimensions, calculated_at
                FROM daily_metrics
                WHERE metric_date = %s
                """,
                (target_date,),
            )
            rows = cur.fetchall()

        if not rows:
            return {}

        result = {
            "date": target_date,
            "calculated_at": rows[0][3].isoformat() if rows[0][3] else None,
            "metrics": {},
        }
        for name, value, dimensions, _ in rows:
            if name == "top_movies" and dimensions:
                result["metrics"][name] = dimensions.get("ranking", [])
            else:
                result["metrics"][name] = value

        return result

    def log_export(
        self, export_date: str, s3_path: str, status: str, error_message: str = None
    ):
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO export_log (export_date, s3_path, status, error_message)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (export_date, s3_path, status, error_message),
                )
        except Exception as e:
            logger.warning("Failed to log export to PostgreSQL: %s", e)
