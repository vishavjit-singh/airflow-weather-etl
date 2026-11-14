from datetime import date

import pytest

from weather_etl.client import OpenMeteoClient
from weather_etl.config import load_settings
from weather_etl.db import Database
from weather_etl.fixtures import FixtureSession


@pytest.fixture(scope="session")
def settings():
    return load_settings()


@pytest.fixture
def db(tmp_path):
    d = Database.from_url(f"sqlite:///{tmp_path / 'w.db'}")
    d.create_schema()
    yield d
    d.close()


@pytest.fixture
def fixture_client(settings):
    def make(task_try=1):
        session = FixtureSession(settings.start_date).register(settings.cities)
        session.task_try = task_try
        sleeps = []
        client = OpenMeteoClient(settings.source, session=session, sleep=sleeps.append)
        client.sleeps = sleeps
        return client
    return make


@pytest.fixture
def day(settings):
    return lambda i: date.fromordinal(settings.start_date.toordinal() + i)
