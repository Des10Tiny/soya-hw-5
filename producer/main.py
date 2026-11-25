import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from enum import Enum
from typing import Optional

from dateutil import parser
from fastapi import FastAPI, HTTPException, Query, status
from generator import EventGenerator
from pydantic import BaseModel, Field, field_validator

from producer import MovieEventProducer

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
)
logger = logging.getLogger("movie-service")


class EventTypeEnum(str, Enum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceTypeEnum(str, Enum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


class MovieEventRequest(BaseModel):
    event_id: Optional[str] = Field(default=None)
    user_id: str = Field(..., min_length=1)
    movie_id: str = Field(..., min_length=1)
    event_type: EventTypeEnum = Field(...)
    timestamp: str = Field(...)
    device_type: DeviceTypeEnum = Field(...)
    session_id: str = Field(..., min_length=1)
    progress_seconds: Optional[int] = Field(default=None, ge=0)

    @field_validator("event_id", mode="before")
    def validate_event_id(cls, v):
        if not v:
            return str(uuid.uuid4())
        try:
            uuid.UUID(str(v))
            return str(v)
        except ValueError:
            raise ValueError("event_id must be a valid UUID")

    @field_validator("timestamp")
    def validate_timestamp(cls, v):
        try:
            dt = parser.isoparse(v)
            return dt.isoformat()
        except Exception:
            raise ValueError("timestamp must be a valid ISO 8601 datetime")


producer_service = MovieEventProducer()
event_generator = EventGenerator(producer_service)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Movie Service...")
    producer_service.initialize()

    auto_bootstrap = os.getenv("AUTO_BOOTSTRAP", "false").lower() == "true"
    if auto_bootstrap:
        days = int(os.getenv("BOOTSTRAP_DAYS", "10"))
        logger.info("AUTO_BOOTSTRAP enabled. Generating %d days of data...", days)
        event_generator.generate_historical_dataset(days=days)

    generator_enabled = os.getenv("GENERATOR_ENABLED", "true").lower() == "true"
    interval = float(os.getenv("GENERATOR_INTERVAL", "2.0"))
    if generator_enabled:
        logger.info(
            "GENERATOR_ENABLED is true. Starting live event generator with interval %.1fs...",
            interval,
        )
        event_generator.start(interval_seconds=interval)

    yield

    logger.info("Shutting down Movie Service...")
    event_generator.stop()


app = FastAPI(title="Online Cinema Movie Service", lifespan=lifespan)


@app.get("/health", status_code=status.HTTP_200_OK)
def health():
    return {"status": "ok", "service": "producer"}


@app.post("/events", status_code=status.HTTP_201_CREATED)
def publish_event(event: MovieEventRequest):
    try:
        dt = parser.isoparse(event.timestamp)
        ts_millis = int(dt.timestamp() * 1000)
        evt_id = event.event_id if event.event_id else str(uuid.uuid4())

        payload = {
            "event_id": evt_id,
            "user_id": event.user_id,
            "movie_id": event.movie_id,
            "event_type": event.event_type.value,
            "timestamp": ts_millis,
            "device_type": event.device_type.value,
            "session_id": event.session_id,
            "progress_seconds": event.progress_seconds,
        }

        published_id = producer_service.publish(payload)
        return {
            "status": "success",
            "message": "Event published to Kafka",
            "event_id": published_id,
            "partition_key": event.user_id,
            "timestamp": event.timestamp,
        }
    except Exception as e:
        logger.error("Failed to publish event %s: %s", event.event_id, e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Kafka publication error: {str(e)}",
        )


@app.post("/generator/start")
def start_generator(interval: float = Query(2.0, ge=0.1, le=60.0)):
    event_generator.start(interval_seconds=interval)
    return {"status": "started", "interval_seconds": interval}


@app.post("/generator/stop")
def stop_generator():
    event_generator.stop()
    return {"status": "stopped"}


@app.get("/generator/status")
def generator_status():
    return {"running": event_generator.is_running()}


@app.post("/generator/bootstrap")
def bootstrap_dataset(
    days: int = Query(10, ge=1, le=30), cohort_size: int = Query(40, ge=5, le=100)
):
    total = event_generator.generate_historical_dataset(
        days=days, base_cohort_size=cohort_size
    )
    return {
        "status": "completed",
        "days": days,
        "cohort_size": cohort_size,
        "total_events": total,
    }
