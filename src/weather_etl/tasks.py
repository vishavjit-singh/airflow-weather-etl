"""The task bodies. The Airflow DAG and the local runner both call these, so the DAG file stays thin."""
from __future__ import annotations

import json
import time
from dataclasses import asdict
from datetime import date, datetime, timezone
from pathlib import Path

from .client import OpenMeteoClient
from .config import Settings
from .db import Database
from .load import load_day, log_task, save_dq
from .quality import check_day
from .transform import to_rows


def raw_path(raw_dir: Path, city_id: str, day: date) -> Path:
    return Path(raw_dir) / f"day={day.isoformat()}" / f"{city_id}.json"


def extract(settings: Settings, client: OpenMeteoClient, city_id: str, day: date, raw_dir: Path) -> dict:
    """Fetch one city-day and land the raw response (overwrites: idempotent per data interval)."""
    result = client.fetch_day(settings.city(city_id), day)
    path = raw_path(raw_dir, city_id, day)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result.payload))
    return {"path": str(path), "attempts": [asdict(a) for a in result.attempts]}


def transform_load(settings: Settings, db: Database, city_id: str, day: date, raw_file: str, run_id: str,
                   try_number: int = 1) -> dict:
    """Validate, run the DQ checks (raise → task retry) and load. Returns row counts and check results."""
    payload = json.loads(Path(raw_file).read_text())
    rows = to_rows(payload, city_id, day, settings.source.hourly, settings.quality)
    try:
        results = check_day(rows, settings.source.hourly, settings.quality)
    except Exception as exc:
        save_dq(db, getattr(exc, "results", []), run_id, city_id, day, try_number)
        raise
    save_dq(db, results, run_id, city_id, day, try_number)
    loaded = load_day(db, rows, city_id, day, run_id)
    return {"rows": loaded, "flagged": sum(1 for r in rows if r["qc_flags"]), "checks": results}


class TaskRecorder:
    """Times a task body and writes one row per try to etl_task_runs (the local runner's 'Gantt')."""

    def __init__(self, db: Database, run_id: str, task_id: str, city_id: str, day: date, try_number: int):
        self.db, self.base = db, dict(run_id=run_id, task_id=task_id, city_id=city_id, day=day.isoformat(),
                                      try_number=try_number)

    def __enter__(self):
        self.t0 = time.perf_counter()
        self.started = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        self.extra: dict = {}
        return self

    def __exit__(self, exc_type, exc, tb):
        log_task(self.db, **self.base, state="failed" if exc else "success", started_at=self.started,
                 duration_s=round(time.perf_counter() - self.t0, 4),
                 detail=f"{exc_type.__name__}: {exc}" if exc else self.extra.pop("detail", None), **self.extra)
        return False
