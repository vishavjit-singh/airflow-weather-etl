"""DAG checks. They need Airflow, so they run in the CI `airflow` job (pytest -m airflow)."""
import pytest

pytest.importorskip("airflow")
pytestmark = pytest.mark.airflow


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder="dags", include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_structure_and_policy(dagbag):
    dag = dagbag.get_dag("weather_etl")
    assert dag.catchup and dag.max_active_runs == 3 and dag.schedule_interval == "@daily"
    assert {t.task_id for t in dag.tasks} == {"create_tables", "cities", "extract", "transform_load", "report"}
    extract, load = dag.get_task("extract"), dag.get_task("transform_load")
    assert extract.pool == "open_meteo_api"
    assert load.retries == 3 and load.retry_exponential_backoff
    assert load.sla is not None and load.outlets
    assert dag.get_task("report").trigger_rule == "all_done"
