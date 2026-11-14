PY ?= python3
export PYTHONPATH := src
START ?= 2026-10-01
END ?= 2026-10-07

.PHONY: install demo run report summary schema test lint up down clean

install:
	$(PY) -m pip install -r requirements-dev.txt

## The DAG's tasks for one backfilled week, locally (SQLite, deterministic fixture source) + charts + summary
demo: clean run report summary

run:
	$(PY) -m weather_etl run --start $(START) --end $(END)

report:
	$(PY) -m weather_etl report

summary:
	$(PY) -m weather_etl summary

schema:
	$(PY) -m weather_etl schema --dialect postgres > sql/schema_postgres.sql

test:
	$(PY) -m pytest -q

lint:
	$(PY) -m ruff check .

## Airflow + PostgreSQL in Docker (UI on :8080)
up:
	docker compose up --build

down:
	docker compose down -v

clean:
	rm -rf data
