from weather_etl.db import Database, schema_sql
from weather_etl.fixtures import series
from weather_etl.load import load_day
from weather_etl.transform import to_rows

M = ("temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m")


def rows_for(settings, city, d):
    return to_rows({"hourly": series(city, d)}, city, d, M, settings.quality)


def test_load_is_idempotent(settings, db, day):
    rows = rows_for(settings, "delhi", day(0))
    load_day(db, rows, "delhi", day(0), "run-1")
    load_day(db, rows, "delhi", day(0), "run-2")          # retry / backfill / tasks clear
    assert db.query("SELECT COUNT(*) AS n FROM hourly_observations")[0]["n"] == 24
    d = db.query("SELECT * FROM daily_summary")
    assert len(d) == 1 and d[0]["hours"] == 24 and d[0]["run_id"] == "run-2"


def test_daily_summary_values(settings, db, day):
    rows = rows_for(settings, "mumbai", day(2))
    load_day(db, rows, "mumbai", day(2), "r")
    s = db.query("SELECT * FROM daily_summary")[0]
    temps = [r["temperature_2m"] for r in rows]
    assert s["temp_min"] == min(temps) and s["temp_max"] == max(temps)
    assert s["temp_mean"] == round(sum(temps) / 24, 1)
    assert s["precip_total"] == round(sum(r["precipitation"] for r in rows), 1)


def test_corrected_values_replace_old_ones(settings, db, day):
    rows = rows_for(settings, "delhi", day(0))
    load_day(db, rows, "delhi", day(0), "r1")
    rows[0]["temperature_2m"] = 1.5
    load_day(db, rows, "delhi", day(0), "r2")
    got = db.query("SELECT temperature_2m FROM hourly_observations WHERE observed_at = '2026-10-01T00:00:00Z'")
    assert got[0]["temperature_2m"] == 1.5


def test_days_do_not_touch_each_other(settings, db, day):
    load_day(db, rows_for(settings, "delhi", day(0)), "delhi", day(0), "r")
    load_day(db, rows_for(settings, "delhi", day(1)), "delhi", day(1), "r")
    load_day(db, rows_for(settings, "delhi", day(0)), "delhi", day(0), "again")
    days = {r["day"]: r["run_id"] for r in db.query("SELECT day, run_id FROM daily_summary")}
    assert days == {"2026-10-01": "again", "2026-10-02": "r"}


def test_postgres_parameter_style(tmp_path):
    db = Database(None, "postgres")
    assert db.sql("WHERE a = :a AND b::text = :b_2") == "WHERE a = %(a)s AND b::text = %(b_2)s"
    assert "TIMESTAMPTZ" in schema_sql("postgres") and "TIMESTAMPTZ" not in schema_sql("sqlite")
