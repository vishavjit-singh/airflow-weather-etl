"""Idempotent loads: re-running a day (retry, backfill, `airflow tasks clear`) gives the same tables."""
from __future__ import annotations

from datetime import date, timedelta

from .db import Database

MEASURES = ("temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m")

UPSERT_HOURLY = """
INSERT INTO hourly_observations (city_id, observed_at, temperature_2m, relative_humidity_2m, precipitation,
                                 wind_speed_10m, qc_flags, run_id)
VALUES (:city_id, :observed_at, :temperature_2m, :relative_humidity_2m, :precipitation, :wind_speed_10m,
        :qc_flags, :run_id)
ON CONFLICT (city_id, observed_at) DO UPDATE SET
    temperature_2m = excluded.temperature_2m,
    relative_humidity_2m = excluded.relative_humidity_2m,
    precipitation = excluded.precipitation,
    wind_speed_10m = excluded.wind_speed_10m,
    qc_flags = excluded.qc_flags,
    run_id = excluded.run_id
"""

DAILY = """
INSERT INTO daily_summary (city_id, day, hours, temp_min, temp_max, temp_mean, humidity_mean, precip_total,
                           wind_max, flagged_hours, run_id)
SELECT city_id, :day, COUNT(*), MIN(temperature_2m), MAX(temperature_2m), ROUND(AVG(temperature_2m) * 10) / 10,
       ROUND(AVG(relative_humidity_2m)), ROUND(SUM(precipitation) * 10) / 10, MAX(wind_speed_10m),
       SUM(CASE WHEN qc_flags IS NULL THEN 0 ELSE 1 END), :run_id
FROM hourly_observations
WHERE city_id = :city_id AND observed_at >= :start AND observed_at < :end
GROUP BY city_id
"""


def interval(day: date) -> dict:
    return {"start": f"{day.isoformat()}T00:00:00Z", "end": f"{(day + timedelta(days=1)).isoformat()}T00:00:00Z"}


def load_day(db: Database, rows: list[dict], city_id: str, day: date, run_id: str) -> int:
    """Upsert the hours and rebuild that city-day's summary in one transaction."""
    try:
        db.executemany(UPSERT_HOURLY, [{**r, "run_id": run_id} for r in rows])
        db.execute("DELETE FROM daily_summary WHERE city_id = :city_id AND day = :day",
                   {"city_id": city_id, "day": day.isoformat()})
        db.execute(DAILY, {"city_id": city_id, "day": day.isoformat(), "run_id": run_id, **interval(day)})
        db.commit()
    except Exception:
        db.rollback()
        raise
    return len(rows)


def save_dq(db: Database, results: list[dict], run_id: str, city_id: str, day: date, try_number: int) -> None:
    db.execute("DELETE FROM dq_results WHERE run_id = :run_id AND city_id = :city_id AND day = :day "
               "AND try_number = :try_number",
               {"run_id": run_id, "city_id": city_id, "day": day.isoformat(), "try_number": try_number})
    db.executemany(
        "INSERT INTO dq_results (run_id, city_id, day, try_number, check_name, value, threshold, status) "
        "VALUES (:run_id, :city_id, :day, :try_number, :check, :value, :threshold, :status)",
        [{**r, "run_id": run_id, "city_id": city_id, "day": day.isoformat(), "try_number": try_number}
         for r in results])
    db.commit()


def log_task(db: Database, **row) -> None:
    row.setdefault("http_calls", 0)
    row.setdefault("rows", 0)
    row.setdefault("detail", None)
    db.execute("DELETE FROM etl_task_runs WHERE run_id = :run_id AND task_id = :task_id AND city_id = :city_id "
               "AND day = :day AND try_number = :try_number", row)
    db.execute("INSERT INTO etl_task_runs (run_id, task_id, city_id, day, try_number, state, started_at, duration_s, "
               "http_calls, rows, detail) VALUES (:run_id, :task_id, :city_id, :day, :try_number, :state, "
               ":started_at, :duration_s, :http_calls, :rows, :detail)", row)
    db.commit()
