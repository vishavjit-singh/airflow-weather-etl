"""The same load path against a real PostgreSQL (CI service container). Set PG_DSN to run locally."""
import os

import pytest

from weather_etl.db import Database
from weather_etl.runner import run

pytestmark = pytest.mark.postgres
DSN = os.environ.get("PG_DSN")


@pytest.fixture
def pg():
    if not DSN:
        pytest.skip("PG_DSN not set")
    pytest.importorskip("psycopg2")
    db = Database.from_url(DSN)
    db.create_schema()
    for t in ("hourly_observations", "daily_summary", "dq_results", "etl_task_runs"):
        db.execute(f"TRUNCATE {t}")
    db.commit()
    yield db
    db.close()


def test_backfill_into_postgres_is_idempotent(settings, pg, tmp_path, day):
    out = run(settings, pg, day(0), day(6), tmp_path / "raw")
    assert out["failed"] == []
    run(settings, pg, day(0), day(6), tmp_path / "raw")
    assert pg.query("SELECT COUNT(*) AS n FROM hourly_observations")[0]["n"] == 42 * 24
    assert pg.query("SELECT COUNT(*) AS n FROM daily_summary")[0]["n"] == 42
    flagged = pg.query("SELECT city_id, observed_at FROM hourly_observations WHERE qc_flags IS NOT NULL")
    assert [f["city_id"] for f in flagged] == ["chandigarh"]
