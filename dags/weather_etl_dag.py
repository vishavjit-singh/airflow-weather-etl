"""Daily weather ETL: Open-Meteo archive → PostgreSQL, one mapped task per city.

Orchestration features on show:

* TaskFlow API with dynamic task mapping (one `extract`/`transform_load` pair per city)
* retries with exponential backoff, a max retry delay and an execution timeout per task
* a pool (`open_meteo_api`) that caps concurrent calls to the free API during backfills
* an SLA on the final `report` step (Airflow 2 has no SLAs on mapped tasks), plus failure and SLA-miss
  callbacks that log a structured alert
* `catchup=True` + idempotent, data-interval-based loads, so backfills and `tasks clear` are safe
* a Dataset outlet so downstream DAGs can be scheduled on fresh data instead of on a clock

The task bodies live in the `weather_etl` package (unit-tested without Airflow). This file only wires them.
Set `WEATHER_SOURCE=fixture` to run against the deterministic fixture instead of the live API (CI does).
"""
from __future__ import annotations

import logging
import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from weather_etl.config import load_settings

SETTINGS = load_settings(os.environ.get("WEATHER_ETL_CONFIG"))
CONN_ID = os.environ.get("WEATHER_DB_CONN_ID", "weather_db")
RAW_DIR = Path(os.environ.get("WEATHER_RAW_DIR", "/opt/airflow/data/raw"))
SOURCE = os.environ.get("WEATHER_SOURCE", "api")
DAILY_SUMMARY = Dataset("postgres://weather_db/public/daily_summary")

log = logging.getLogger("weather_etl.alerts")


def _alert(context) -> None:
    ti = context["task_instance"]
    log.error("ALERT task failed dag=%s task=%s map_index=%s run=%s try=%s error=%s", ti.dag_id, ti.task_id,
              ti.map_index, context["run_id"], ti.try_number, context.get("exception"))


def _sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    log.error("ALERT SLA missed dag=%s tasks=%s blocking=%s", dag.dag_id, task_list, blocking_task_list)


def _db():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    from weather_etl.db import Database

    return Database(PostgresHook(postgres_conn_id=CONN_ID).get_conn(), "postgres")


def _client():
    from weather_etl.client import OpenMeteoClient
    from weather_etl.fixtures import FixtureSession

    if SOURCE == "fixture":
        session = FixtureSession(SETTINGS.start_date).register(SETTINGS.cities)
        session.task_try = get_current_context()["ti"].try_number
        return OpenMeteoClient(SETTINGS.source, session=session)
    return OpenMeteoClient(SETTINGS.source)


default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": _alert,
}


@dag(
    dag_id="weather_etl",
    schedule="@daily",
    start_date=pendulum.datetime(SETTINGS.start_date.year, SETTINGS.start_date.month, SETTINGS.start_date.day,
                                 tz="UTC"),
    catchup=True,
    max_active_runs=3,
    default_args=default_args,
    sla_miss_callback=_sla_miss,
    tags=["etl", "weather", "postgres"],
    doc_md=__doc__,
)
def weather_etl():
    @task
    def create_tables() -> None:
        _db().create_schema()

    @task
    def cities() -> list[str]:
        return [c.id for c in SETTINGS.cities]

    @task(pool="open_meteo_api")
    def extract(city_id: str, data_interval_start=None) -> dict:
        from weather_etl.tasks import extract as extract_day

        out = extract_day(SETTINGS, _client(), city_id, data_interval_start.date(), RAW_DIR)
        retried = [a for a in out["attempts"] if a["status"] != 200]
        if retried:
            log.warning("%s: %d HTTP retries: %s", city_id, len(retried), retried)
        return {"city_id": city_id, "path": out["path"]}

    @task(outlets=[DAILY_SUMMARY])
    def transform_load(extracted: dict, data_interval_start=None, run_id=None, ti=None) -> dict:
        from weather_etl.tasks import transform_load as load_day

        res = load_day(SETTINGS, _db(), extracted["city_id"], data_interval_start.date(), extracted["path"], run_id,
                       ti.try_number)
        return {"city_id": extracted["city_id"], "rows": res["rows"], "flagged": res["flagged"]}

    # The SLA sits here because Airflow rejects SLAs on mapped tasks. `report` runs once every city has
    # loaded, so it measures the same thing: the whole day landed within the SLA.
    @task(trigger_rule="all_done", sla=timedelta(minutes=SETTINGS.sla_minutes))
    def report(results: list[dict], data_interval_start=None) -> None:
        loaded = [r for r in results if r]
        log.info("%s: loaded %d cities, %d rows, %d flagged hours", data_interval_start.date(), len(loaded),
                 sum(r["rows"] for r in loaded), sum(r["flagged"] for r in loaded))

    city_ids = cities()
    create_tables() >> city_ids
    extracted = extract.expand(city_id=city_ids)
    report(transform_load.expand(extracted=extracted))


weather_etl()
