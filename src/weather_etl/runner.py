"""Run the DAG's tasks locally, in the same order and with the same retry policy as Airflow.

This is what `make demo` uses: no scheduler or metadata database needed. One logical date = one
DAG run; each city is a mapped task instance with up to ``retries`` extra tries. Retry delays and
HTTP backoff are recorded rather than slept, so a week of backfill runs in seconds.
"""
from __future__ import annotations

import json
import logging
import random
from datetime import date, timedelta
from pathlib import Path

from .client import OpenMeteoClient
from .config import Settings
from .db import Database
from .fixtures import FixtureSession
from .tasks import TaskRecorder, extract, transform_load

log = logging.getLogger("weather_etl")

TASK_RETRIES = 3          # mirrors default_args in dags/weather_etl_dag.py


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def run_city_day(settings: Settings, db: Database, city_id: str, day: date, raw_dir: Path, source: str,
                 attempts_log: list, run_id: str) -> str:
    """One mapped task instance through all its tries. Returns the final state."""
    for try_number in range(1, TASK_RETRIES + 2):
        if source == "fixture":
            session = FixtureSession(settings.start_date).register(settings.cities)
            session.task_try = try_number
            client = OpenMeteoClient(settings.source, session=session, sleep=lambda s: None,
                                     rng=random.Random(f"{city_id}-{day}-{try_number}"))
        else:
            client = OpenMeteoClient(settings.source)
        try:
            with TaskRecorder(db, run_id, "extract", city_id, day, try_number) as rec:
                out = extract(settings, client, city_id, day, raw_dir)
                rec.extra["http_calls"] = len(out["attempts"])
                attempts_log += [{"run_id": run_id, "city_id": city_id, "day": day.isoformat(),
                                  "try_number": try_number, **a} for a in out["attempts"]]
            with TaskRecorder(db, run_id, "transform_load", city_id, day, try_number) as rec:
                res = transform_load(settings, db, city_id, day, out["path"], run_id, try_number)
                rec.extra["rows"] = res["rows"]
                if res["flagged"]:
                    rec.extra["detail"] = f"{res['flagged']} hour(s) with QC flags"
            return "success"
        except Exception as exc:  # noqa: BLE001 - the task failed; Airflow would schedule a retry
            log.warning("%s %s try %d failed: %s", day, city_id, try_number, exc)
    return "failed"


def run(settings: Settings, db: Database, start: date, end: date, raw_dir: Path, source: str = "fixture",
        attempts_file: Path | None = None) -> dict:
    db.create_schema()
    attempts: list[dict] = []
    states: dict[str, str] = {}
    for day in daterange(start, end):
        run_id = f"scheduled__{day.isoformat()}T00:00:00+00:00"
        for city in settings.cities:
            states[f"{day} {city.id}"] = run_city_day(settings, db, city.id, day, raw_dir, source, attempts, run_id)
        log.info("%s: %s", day, {c.id: states[f"{day} {c.id}"] for c in settings.cities})
    if attempts_file:
        attempts_file.parent.mkdir(parents=True, exist_ok=True)
        attempts_file.write_text("".join(json.dumps(a) + "\n" for a in attempts))
    failed = [k for k, v in states.items() if v != "success"]
    return {"task_instances": len(states), "failed": failed, "http_calls": len(attempts)}
