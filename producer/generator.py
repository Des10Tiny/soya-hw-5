import logging
import random
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Optional

logger = logging.getLogger("generator")

MOVIES = [
    "m-interstellar",
    "m-inception",
    "m-matrix",
    "m-pulp-fiction",
    "fight-club",
    "m-shawshank",
    "m-dark-knight",
    "m-forrest-gump",
    "m-gladiator",
    "m-oppenheimer",
]

DEVICES = ["MOBILE", "DESKTOP", "TV", "TABLET"]
SEARCH_QUERIES = [
    "sci-fi",
    "nolan",
    "action 2024",
    "comedy",
    "thriller",
    "drama",
    "top movies",
]


class EventGenerator:
    def __init__(self, producer):
        self.producer = producer
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self, interval_seconds: float = 1.0):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop, args=(interval_seconds,), daemon=True
        )
        self._thread.start()
        logger.info("Real-time event generator started.")

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=3)
        logger.info("Real-time event generator stopped.")

    def is_running(self) -> bool:
        return self._running

    def _run_loop(self, interval_seconds: float):
        while self._running:
            try:
                self.simulate_single_user_journey()
            except Exception as e:
                logger.error("Error in generator loop: %s", e)
            time.sleep(interval_seconds)

    def simulate_single_user_journey(
        self, user_id: Optional[str] = None, base_time: Optional[datetime] = None
    ) -> List[dict]:
        if not user_id:
            user_id = f"user_{random.randint(1000, 9999)}"
        if not base_time:
            base_time = datetime.now(timezone.utc)

        movie_id = random.choice(MOVIES)
        device_type = random.choice(DEVICES)
        session_id = str(uuid.uuid4())
        movie_duration = random.randint(3600, 7200)

        events = []
        current_time = base_time

        if random.random() < 0.5:
            search_event = {
                "event_id": str(uuid.uuid4()),
                "user_id": user_id,
                "movie_id": movie_id,
                "event_type": "SEARCHED",
                "timestamp": int(current_time.timestamp() * 1000),
                "device_type": device_type,
                "session_id": session_id,
                "progress_seconds": None,
            }
            events.append(search_event)
            self.producer.publish(search_event)
            current_time += timedelta(seconds=random.randint(10, 60))

        start_event = {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "movie_id": movie_id,
            "event_type": "VIEW_STARTED",
            "timestamp": int(current_time.timestamp() * 1000),
            "device_type": device_type,
            "session_id": session_id,
            "progress_seconds": 0,
        }
        events.append(start_event)
        self.producer.publish(start_event)
        current_progress = 0

        if random.random() < 0.4:
            current_progress += random.randint(300, 1800)
            current_time += timedelta(seconds=current_progress)

            pause_event = {
                "event_id": str(uuid.uuid4()),
                "user_id": user_id,
                "movie_id": movie_id,
                "event_type": "VIEW_PAUSED",
                "timestamp": int(current_time.timestamp() * 1000),
                "device_type": device_type,
                "session_id": session_id,
                "progress_seconds": current_progress,
            }
            events.append(pause_event)
            self.producer.publish(pause_event)

            pause_duration = random.randint(30, 300)
            current_time += timedelta(seconds=pause_duration)

            resume_event = {
                "event_id": str(uuid.uuid4()),
                "user_id": user_id,
                "movie_id": movie_id,
                "event_type": "VIEW_RESUMED",
                "timestamp": int(current_time.timestamp() * 1000),
                "device_type": device_type,
                "session_id": session_id,
                "progress_seconds": current_progress,
            }
            events.append(resume_event)
            self.producer.publish(resume_event)

        if random.random() < 0.70:
            remaining = movie_duration - current_progress
            current_progress += remaining
            current_time += timedelta(seconds=remaining)

            finish_event = {
                "event_id": str(uuid.uuid4()),
                "user_id": user_id,
                "movie_id": movie_id,
                "event_type": "VIEW_FINISHED",
                "timestamp": int(current_time.timestamp() * 1000),
                "device_type": device_type,
                "session_id": session_id,
                "progress_seconds": current_progress,
            }
            events.append(finish_event)
            self.producer.publish(finish_event)

            if random.random() < 0.30:
                current_time += timedelta(seconds=random.randint(5, 30))
                like_event = {
                    "event_id": str(uuid.uuid4()),
                    "user_id": user_id,
                    "movie_id": movie_id,
                    "event_type": "LIKED",
                    "timestamp": int(current_time.timestamp() * 1000),
                    "device_type": device_type,
                    "session_id": session_id,
                    "progress_seconds": None,
                }
                events.append(like_event)
                self.producer.publish(like_event)

        return events

    def generate_historical_dataset(self, days: int = 10, base_cohort_size: int = 40):
        logger.info("Generating historical dataset for %d days...", days)
        now = datetime.now(timezone.utc).replace(
            hour=12, minute=0, second=0, microsecond=0
        )
        retention_probs = {
            0: 1.0,
            1: 0.45,
            2: 0.32,
            3: 0.26,
            4: 0.22,
            5: 0.19,
            6: 0.16,
            7: 0.14,
        }

        cohorts = {}
        for d in range(days, -1, -1):
            cohort_date = now - timedelta(days=d)
            cohort_name = f"c_{cohort_date.strftime('%Y%m%d')}"
            users = [f"{cohort_name}_u{i}" for i in range(base_cohort_size)]
            cohorts[d] = (cohort_date, users)

        total_events = 0
        for cohort_day_idx, (cohort_date, users) in cohorts.items():
            for user_id in users:
                event_time = cohort_date + timedelta(minutes=random.randint(0, 720))
                events = self.simulate_single_user_journey(
                    user_id=user_id, base_time=event_time
                )
                total_events += len(events)

                for offset in range(1, 8):
                    if cohort_day_idx - offset < 0:
                        continue
                    prob = retention_probs.get(offset, 0.1)
                    if random.random() < prob:
                        return_date = cohort_date + timedelta(
                            days=offset, minutes=random.randint(0, 720)
                        )
                        ret_events = self.simulate_single_user_journey(
                            user_id=user_id, base_time=return_date
                        )
                        total_events += len(ret_events)

        logger.info(
            "Historical data generation complete! Total events produced: %d",
            total_events,
        )
        return total_events
