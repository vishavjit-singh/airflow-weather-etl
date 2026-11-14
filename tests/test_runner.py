import json

from weather_etl.runner import run


def test_backfill_week(settings, db, tmp_path, day):
    out = run(settings, db, day(0), day(6), tmp_path / "raw", attempts_file=tmp_path / "a.jsonl")
    assert out["task_instances"] == 42 and out["failed"] == []
    assert db.query("SELECT COUNT(*) AS n FROM hourly_observations")[0]["n"] == 42 * 24
    retried = db.query("SELECT city_id, day, try_number, state, detail FROM etl_task_runs WHERE try_number > 1 "
                       "OR state = 'failed' ORDER BY try_number")
    assert [(r["city_id"], r["day"], r["try_number"], r["state"]) for r in retried] == [
        ("ludhiana", "2026-10-07", 1, "failed"), ("ludhiana", "2026-10-07", 2, "success"),
        ("ludhiana", "2026-10-07", 2, "success")]
    attempts = [json.loads(x) for x in (tmp_path / "a.jsonl").read_text().splitlines()]
    assert sorted(a["error"] for a in attempts if a["status"] != 200) == [
        "HTTP 429", "HTTP 502", "HTTP 503", "HTTP 503", "ReadTimeout"]
    assert (tmp_path / "raw" / "day=2026-10-03" / "chandigarh.json").exists()


def test_rerunning_the_backfill_changes_nothing(settings, db, tmp_path, day):
    run(settings, db, day(0), day(2), tmp_path / "raw")
    before = db.query("SELECT * FROM daily_summary ORDER BY city_id, day")
    run(settings, db, day(0), day(2), tmp_path / "raw")
    after = db.query("SELECT * FROM daily_summary ORDER BY city_id, day")
    assert before == after
    assert db.query("SELECT COUNT(*) AS n FROM hourly_observations")[0]["n"] == 18 * 24


def test_a_day_that_never_completes_fails_after_all_retries(settings, db, tmp_path, day, monkeypatch):
    from weather_etl import fixtures

    monkeypatch.setattr(fixtures, "INCOMPLETE_FIRST_TRY", set())
    real = fixtures.series
    monkeypatch.setattr(fixtures, "series", lambda c, d, seed=7: {k: v[:20] for k, v in real(c, d, seed).items()}
                        if c == "jaipur" else real(c, d, seed))
    out = run(settings, db, day(0), day(0), tmp_path / "raw")
    assert out["failed"] == ["2026-10-01 jaipur"]
    tries = db.query("SELECT try_number FROM etl_task_runs WHERE city_id = 'jaipur' AND task_id = 'transform_load'")
    assert len(tries) == 4                                  # 1 try + 3 retries, like default_args
    assert not db.query("SELECT * FROM daily_summary WHERE city_id = 'jaipur'")
