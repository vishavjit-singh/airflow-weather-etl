"""A thin DB-API wrapper so the same SQL runs on PostgreSQL (Airflow, docker compose) and SQLite (demo, tests).

SQL is written with ``:name`` parameters and converted to ``%(name)s`` for psycopg2. Upserts use
``INSERT … ON CONFLICT … DO UPDATE``, which both databases support.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

_PARAM = re.compile(r"(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)")

DDL = {
    "postgres": {"ts": "TIMESTAMPTZ", "real": "DOUBLE PRECISION", "now": "now()", "serial": "BIGSERIAL"},
    "sqlite": {"ts": "TEXT", "real": "REAL", "now": "CURRENT_TIMESTAMP", "serial": "INTEGER"},
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS hourly_observations (
    city_id               TEXT NOT NULL,
    observed_at           {ts} NOT NULL,
    temperature_2m        {real},
    relative_humidity_2m  {real},
    precipitation         {real},
    wind_speed_10m        {real},
    qc_flags              TEXT,
    run_id                TEXT NOT NULL,
    loaded_at             {ts} NOT NULL DEFAULT {now},
    PRIMARY KEY (city_id, observed_at)
);
CREATE TABLE IF NOT EXISTS daily_summary (
    city_id         TEXT NOT NULL,
    day             TEXT NOT NULL,
    hours           INTEGER NOT NULL,
    temp_min        {real},
    temp_max        {real},
    temp_mean       {real},
    humidity_mean   {real},
    precip_total    {real},
    wind_max        {real},
    flagged_hours   INTEGER NOT NULL,
    run_id          TEXT NOT NULL,
    PRIMARY KEY (city_id, day)
);
CREATE TABLE IF NOT EXISTS dq_results (
    run_id     TEXT NOT NULL,
    city_id    TEXT NOT NULL,
    day        TEXT NOT NULL,
    try_number INTEGER NOT NULL,
    check_name TEXT NOT NULL,
    value      {real},
    threshold  {real},
    status     TEXT NOT NULL,
    PRIMARY KEY (run_id, city_id, day, try_number, check_name)
);
CREATE TABLE IF NOT EXISTS etl_task_runs (
    run_id      TEXT NOT NULL,
    task_id     TEXT NOT NULL,
    city_id     TEXT NOT NULL,
    day         TEXT NOT NULL,
    try_number  INTEGER NOT NULL,
    state       TEXT NOT NULL,
    started_at  TEXT NOT NULL,
    duration_s  {real} NOT NULL,
    http_calls  INTEGER NOT NULL DEFAULT 0,
    rows        INTEGER NOT NULL DEFAULT 0,
    detail      TEXT,
    PRIMARY KEY (run_id, task_id, city_id, day, try_number)
);
"""


class Database:
    def __init__(self, conn, dialect: str):
        if dialect not in DDL:
            raise ValueError(dialect)
        self.conn, self.dialect = conn, dialect

    @classmethod
    def from_url(cls, url: str) -> Database:
        if url.startswith("sqlite:///"):
            path = Path(url[len("sqlite:///"):])
            path.parent.mkdir(parents=True, exist_ok=True)
            return cls(sqlite3.connect(path), "sqlite")
        if url.startswith(("postgresql://", "postgres://")):
            import psycopg2  # only needed for PostgreSQL targets

            return cls(psycopg2.connect(url), "postgres")
        raise ValueError(f"unsupported database url {url!r}")

    def sql(self, statement: str) -> str:
        return _PARAM.sub(r"%(\1)s", statement) if self.dialect == "postgres" else statement

    def execute(self, statement: str, params: dict | None = None) -> None:
        cur = self.conn.cursor()
        cur.execute(self.sql(statement), params or {})
        cur.close()

    def executemany(self, statement: str, rows: list[dict]) -> None:
        if rows:
            cur = self.conn.cursor()
            cur.executemany(self.sql(statement), rows)
            cur.close()

    def query(self, statement: str, params: dict | None = None) -> list[dict]:
        cur = self.conn.cursor()
        cur.execute(self.sql(statement), params or {})
        cols = [d[0] for d in cur.description]
        out = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
        return out

    def create_schema(self) -> None:
        for stmt in SCHEMA.format(**DDL[self.dialect]).split(";"):
            if stmt.strip():
                self.execute(stmt)
        self.conn.commit()

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def close(self) -> None:
        self.conn.close()


def schema_sql(dialect: str) -> str:
    return SCHEMA.format(**DDL[dialect]).strip() + "\n"
