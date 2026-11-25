import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler
from clickhouse_analytics import ClickHouseAnalytics
from dateutil import parser
from fastapi import FastAPI, HTTPException, Query, status
from pg_writer import PostgresWriter
from s3_exporter import S3Exporter

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
)
logger = logging.getLogger("aggregation-service")

CALC_INTERVAL_MINUTES = int(os.getenv("CALCULATION_INTERVAL_MINUTES", "5"))
AUTO_EXPORT_TO_S3 = os.getenv("AUTO_EXPORT_TO_S3", "true").lower() == "true"
RUN_ON_STARTUP = os.getenv("RUN_ON_STARTUP", "true").lower() == "true"

ch_analytics = ClickHouseAnalytics()
pg_writer = PostgresWriter()
s3_exporter = S3Exporter(pg_writer)
scheduler = BackgroundScheduler()


def run_pipeline_for_date(target_date: date):
    cycle_start = time.perf_counter()
    date_str = target_date.strftime("%Y-%m-%d")
    logger.info("==================================================================")
    logger.info("[CYCLE START] Running aggregation cycle for date %s ...", date_str)

    try:
        calc_result = ch_analytics.calculate_metrics_for_date(target_date)
        records_processed = calc_result.get("raw_events_processed", 0)
        pg_writer.save_metrics(calc_result)

        s3_res = None
        if AUTO_EXPORT_TO_S3:
            s3_res = s3_exporter.export_daily_aggregates(date_str)

        elapsed = time.perf_counter() - cycle_start
        logger.info(
            "[CYCLE FINISH] Completed aggregation cycle for date %s. "
            "Raw records processed: %d. Elapsed time: %.3f seconds (%.1f ms).",
            date_str,
            records_processed,
            elapsed,
            elapsed * 1000.0,
        )
        logger.info(
            "=================================================================="
        )
        return {
            "status": "success",
            "date": date_str,
            "records_processed": records_processed,
            "elapsed_seconds": round(elapsed, 3),
            "metrics": calc_result["metrics"],
            "s3_export": s3_res,
        }
    except Exception as e:
        elapsed = time.perf_counter() - cycle_start
        logger.error(
            "[CYCLE ERROR] Failed aggregation cycle for date %s after %.3f seconds: %s",
            date_str,
            elapsed,
            e,
            exc_info=True,
        )
        raise


def scheduled_job():
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    logger.info(
        "Scheduled job triggered: updating yesterday (%s) and today (%s)",
        yesterday,
        today,
    )
    try:
        run_pipeline_for_date(yesterday)
    except Exception as e:
        logger.error("Error in scheduled run for yesterday: %s", e)
    try:
        run_pipeline_for_date(today)
    except Exception as e:
        logger.error("Error in scheduled run for today: %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Aggregation Service...")
    ch_analytics.connect()
    pg_writer.get_connection()
    s3_exporter.get_client()

    if RUN_ON_STARTUP:
        logger.info(
            "RUN_ON_STARTUP is enabled. Running initial aggregation for past 10 days..."
        )
        today = datetime.now(timezone.utc).date()
        for offset in range(10, -1, -1):
            past_date = today - timedelta(days=offset)
            try:
                run_pipeline_for_date(past_date)
            except Exception as e:
                logger.warning("Initial run for %s skipped or failed: %s", past_date, e)

    logger.info(
        "Starting APScheduler with interval %d minutes...", CALC_INTERVAL_MINUTES
    )
    scheduler.add_job(
        scheduled_job,
        trigger="interval",
        minutes=CALC_INTERVAL_MINUTES,
        id="daily_metrics_aggregator",
        replace_existing=True,
    )
    scheduler.start()

    yield

    logger.info("Shutting down Aggregation Service scheduler...")
    scheduler.shutdown(wait=False)


app = FastAPI(title="Online Cinema Aggregation Service", lifespan=lifespan)


@app.get("/health", status_code=status.HTTP_200_OK)
def health():
    return {"status": "ok", "service": "aggregation-service"}


@app.post("/aggregate")
def manual_aggregate(date_str: Optional[str] = Query(None, alias="date")):
    try:
        if date_str:
            target_date = parser.isoparse(date_str).date()
        else:
            target_date = datetime.now(timezone.utc).date()

        result = run_pipeline_for_date(target_date)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Aggregation calculation failed: {str(e)}",
        )


@app.post("/export")
def manual_export(date_str: Optional[str] = Query(None, alias="date")):
    try:
        if date_str:
            target_date = parser.isoparse(date_str).date()
        else:
            target_date = datetime.now(timezone.utc).date()

        s3_res = s3_exporter.export_daily_aggregates(target_date.strftime("%Y-%m-%d"))
        return s3_res
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"S3 export failed: {str(e)}",
        )


@app.get("/metrics")
def get_metrics(date_str: str = Query(..., alias="date")):
    metrics = pg_writer.fetch_daily_metrics(date_str)
    if not metrics:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No metrics found for {date_str}",
        )
    return metrics
