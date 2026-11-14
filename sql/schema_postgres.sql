CREATE TABLE IF NOT EXISTS hourly_observations (
    city_id               TEXT NOT NULL,
    observed_at           TIMESTAMPTZ NOT NULL,
    temperature_2m        DOUBLE PRECISION,
    relative_humidity_2m  DOUBLE PRECISION,
    precipitation         DOUBLE PRECISION,
    wind_speed_10m        DOUBLE PRECISION,
    qc_flags              TEXT,
    run_id                TEXT NOT NULL,
    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (city_id, observed_at)
);
CREATE TABLE IF NOT EXISTS daily_summary (
    city_id         TEXT NOT NULL,
    day             TEXT NOT NULL,
    hours           INTEGER NOT NULL,
    temp_min        DOUBLE PRECISION,
    temp_max        DOUBLE PRECISION,
    temp_mean       DOUBLE PRECISION,
    humidity_mean   DOUBLE PRECISION,
    precip_total    DOUBLE PRECISION,
    wind_max        DOUBLE PRECISION,
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
    value      DOUBLE PRECISION,
    threshold  DOUBLE PRECISION,
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
    duration_s  DOUBLE PRECISION NOT NULL,
    http_calls  INTEGER NOT NULL DEFAULT 0,
    rows        INTEGER NOT NULL DEFAULT 0,
    detail      TEXT,
    PRIMARY KEY (run_id, task_id, city_id, day, try_number)
);
