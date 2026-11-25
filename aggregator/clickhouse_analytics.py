import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List

import clickhouse_connect

logger = logging.getLogger("clickhouse-analytics")

CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_DB = os.getenv("CLICKHOUSE_DB", "default")


class ClickHouseAnalytics:
    def __init__(self):
        self.client = None

    def connect(self):
        logger.info("Connecting to ClickHouse at %s:%d ...", CH_HOST, CH_PORT)
        for attempt in range(30):
            try:
                self.client = clickhouse_connect.get_client(
                    host=CH_HOST,
                    port=CH_PORT,
                    username=CH_USER,
                    password=CH_PASSWORD,
                    database=CH_DB,
                    connect_timeout=10,
                    send_receive_timeout=30,
                )
                logger.info("Connected to ClickHouse successfully!")
                return
            except Exception as e:
                logger.warning("Waiting for ClickHouse... (%s)", e)
                time.sleep(2)
        raise RuntimeError("Could not connect to ClickHouse after retries")

    def calculate_metrics_for_date(self, target_date: date) -> Dict[str, Any]:
        if not self.client:
            self.connect()

        target_str = target_date.strftime("%Y-%m-%d")
        d1_cohort_str = (target_date - timedelta(days=1)).strftime("%Y-%m-%d")
        d7_cohort_str = (target_date - timedelta(days=7)).strftime("%Y-%m-%d")

        core_query = """
        SELECT
            count(*) as total_events,
            uniqExact(user_id) as dau,
            countIf(event_type = 'VIEW_STARTED') as views_started,
            countIf(event_type = 'VIEW_FINISHED') as views_finished,
            coalesce(round(avgIf(progress_seconds, event_type = 'VIEW_FINISHED'), 2), 0.0) as avg_watch_time,
            if(countIf(event_type = 'VIEW_STARTED') = 0, 0.0,
               round(countIf(event_type = 'VIEW_FINISHED') * 100.0 / countIf(event_type = 'VIEW_STARTED'), 2)
            ) as conversion_rate
        FROM default.movie_events
        WHERE toDate(timestamp) = {target_date:Date}
        """
        res = self.client.query(core_query, parameters={"target_date": target_str})
        row = res.first_row or [0, 0, 0, 0, 0.0, 0.0]
        (
            total_events,
            dau,
            views_started,
            views_finished,
            avg_watch_time,
            conversion_rate,
        ) = row

        retention_d1_query = """
        WITH user_first_seen AS (
            SELECT user_id, min(toDate(timestamp)) AS cohort_date
            FROM default.movie_events
            WHERE event_type IN ('VIEW_STARTED', 'VIEW_FINISHED')
            GROUP BY user_id
        ),
        cohort_users AS (
            SELECT user_id FROM user_first_seen WHERE cohort_date = {cohort_date:Date}
        ),
        active_on_target AS (
            SELECT DISTINCT user_id FROM default.movie_events
            WHERE toDate(timestamp) = {target_date:Date}
              AND event_type IN ('VIEW_STARTED', 'VIEW_FINISHED')
              AND user_id IN (SELECT user_id FROM cohort_users)
        )
        SELECT
            (SELECT count(*) FROM cohort_users) AS cohort_size,
            (SELECT count(*) FROM active_on_target) AS returned_count,
            if((SELECT count(*) FROM cohort_users) = 0, 0.0,
               round((SELECT count(*) FROM active_on_target) * 100.0 / (SELECT count(*) FROM cohort_users), 2)
            ) AS retention_d1
        """
        r1_res = self.client.query(
            retention_d1_query,
            parameters={"cohort_date": d1_cohort_str, "target_date": target_str},
        )
        retention_d1 = float(r1_res.first_row[2]) if r1_res.first_row else 0.0

        retention_d7_query = """
        WITH user_first_seen AS (
            SELECT user_id, min(toDate(timestamp)) AS cohort_date
            FROM default.movie_events
            WHERE event_type IN ('VIEW_STARTED', 'VIEW_FINISHED')
            GROUP BY user_id
        ),
        cohort_users AS (
            SELECT user_id FROM user_first_seen WHERE cohort_date = {cohort_date:Date}
        ),
        active_on_target AS (
            SELECT DISTINCT user_id FROM default.movie_events
            WHERE toDate(timestamp) = {target_date:Date}
              AND event_type IN ('VIEW_STARTED', 'VIEW_FINISHED')
              AND user_id IN (SELECT user_id FROM cohort_users)
        )
        SELECT
            (SELECT count(*) FROM cohort_users) AS cohort_size,
            (SELECT count(*) FROM active_on_target) AS returned_count,
            if((SELECT count(*) FROM cohort_users) = 0, 0.0,
               round((SELECT count(*) FROM active_on_target) * 100.0 / (SELECT count(*) FROM cohort_users), 2)
            ) AS retention_d7
        """
        r7_res = self.client.query(
            retention_d7_query,
            parameters={"cohort_date": d7_cohort_str, "target_date": target_str},
        )
        retention_d7 = float(r7_res.first_row[2]) if r7_res.first_row else 0.0

        top_movies_query = """
        SELECT movie_id, count(*) as views_count
        FROM default.movie_events
        WHERE toDate(timestamp) = {target_date:Date}
          AND event_type = 'VIEW_STARTED'
        GROUP BY movie_id
        ORDER BY views_count DESC, movie_id ASC
        LIMIT 10
        """
        tm_res = self.client.query(
            top_movies_query, parameters={"target_date": target_str}
        )
        top_movies = [
            {"movie_id": r[0], "views_count": int(r[1])} for r in tm_res.result_rows
        ]

        now_dt = datetime.now(timezone.utc)
        self._materialize_clickhouse(
            target_date=target_date,
            dau=dau,
            avg_watch_time=avg_watch_time,
            views_started=views_started,
            views_finished=views_finished,
            conversion_rate=conversion_rate,
            retention_d1=retention_d1,
            retention_d7=retention_d7,
            top_movies=top_movies,
            calculated_at=now_dt,
        )

        return {
            "date": target_str,
            "calculated_at": now_dt.isoformat(),
            "raw_events_processed": total_events,
            "metrics": {
                "dau": int(dau),
                "avg_watch_time": float(avg_watch_time),
                "views_started": int(views_started),
                "views_finished": int(views_finished),
                "conversion_rate": float(conversion_rate),
                "retention_d1": float(retention_d1),
                "retention_d7": float(retention_d7),
                "top_movies": top_movies,
            },
        }

    def _materialize_clickhouse(
        self,
        target_date: date,
        dau: int,
        avg_watch_time: float,
        views_started: int,
        views_finished: int,
        conversion_rate: float,
        retention_d1: float,
        retention_d7: float,
        top_movies: List[Dict[str, Any]],
        calculated_at: datetime,
    ):
        try:
            self.client.insert(
                table="daily_aggregates",
                data=[
                    (
                        target_date,
                        dau,
                        avg_watch_time,
                        views_started,
                        views_finished,
                        conversion_rate,
                        retention_d1,
                        retention_d7,
                        calculated_at,
                    )
                ],
                column_names=[
                    "metric_date",
                    "dau",
                    "avg_watch_time",
                    "views_started",
                    "views_finished",
                    "conversion_rate",
                    "retention_d1",
                    "retention_d7",
                    "calculated_at",
                ],
            )

            if top_movies:
                tm_data = [
                    (target_date, m["movie_id"], m["views_count"], calculated_at)
                    for m in top_movies
                ]
                self.client.insert(
                    table="top_movies_daily",
                    data=tm_data,
                    column_names=[
                        "metric_date",
                        "movie_id",
                        "views_count",
                        "calculated_at",
                    ],
                )

            cohort_matrix_query = """
            INSERT INTO default.cohort_retention_daily
            WITH
              user_cohort AS (
                SELECT user_id, min(toDate(timestamp)) AS cohort_date
                FROM default.movie_events
                WHERE event_type IN ('VIEW_STARTED', 'VIEW_FINISHED')
                GROUP BY user_id
              ),
              cohort_sizes AS (
                SELECT cohort_date, count(DISTINCT user_id) AS cohort_size
                FROM user_cohort
                GROUP BY cohort_date
              ),
              user_activity AS (
                SELECT DISTINCT user_id, toDate(timestamp) AS activity_date
                FROM default.movie_events
                WHERE event_type IN ('VIEW_STARTED', 'VIEW_FINISHED')
              )
            SELECT
              uc.cohort_date,
              toUInt8(ua.activity_date - uc.cohort_date) AS day_offset,
              cs.cohort_size,
              count(DISTINCT uc.user_id) AS active_users,
              round(count(DISTINCT uc.user_id) * 100.0 / cs.cohort_size, 2) AS retention_rate,
              now() AS calculated_at
            FROM user_cohort uc
            JOIN user_activity ua ON uc.user_id = ua.user_id
            JOIN cohort_sizes cs ON uc.cohort_date = cs.cohort_date
            WHERE ua.activity_date >= uc.cohort_date AND (ua.activity_date - uc.cohort_date) <= 7
            GROUP BY uc.cohort_date, day_offset, cs.cohort_size;
            """
            self.client.command(cohort_matrix_query)

            logger.info(
                "Successfully materialized metrics into ClickHouse tables for date %s",
                target_date,
            )
        except Exception as e:
            logger.error("Failed to materialize ClickHouse aggregates: %s", e)
            raise
