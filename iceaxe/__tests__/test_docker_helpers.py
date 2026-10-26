from unittest.mock import Mock

import pytest
from docker.errors import APIError

from iceaxe.__tests__ import docker_helpers


@pytest.fixture(autouse=True)
def clear_table():
    """These unit tests do not use a database."""


@pytest.mark.parametrize(
    "probe_results, expected_ready",
    [
        ([Mock(exit_code=2), Mock(exit_code=1), Mock(exit_code=0)], True),
        ([Mock(exit_code=1)] * 30, False),
        ([APIError("Container is starting"), Mock(exit_code=0)], True),
    ],
    ids=["waits-for-ready", "times-out", "retries-errors"],
)
def test_wait_for_postgres_ready(monkeypatch, probe_results, expected_ready):
    monkeypatch.setattr(docker_helpers, "get_free_port", lambda: 5432)
    monkeypatch.setattr(docker_helpers.docker, "from_env", Mock())
    monkeypatch.setattr(docker_helpers.time, "sleep", Mock())
    monkeypatch.setattr(docker_helpers.socket, "create_connection", Mock())
    postgres = docker_helpers.PostgresContainer()
    container = Mock(status="running")
    container.exec_run.side_effect = probe_results
    postgres.container = container

    assert postgres._wait_for_container_ready() is expected_ready
    assert container.exec_run.call_count == len(probe_results)
    container.exec_run.assert_called_with(
        [
            "pg_isready",
            "-h",
            "127.0.0.1",
            "-U",
            postgres.pg_user,
            "-d",
            postgres.pg_db,
        ]
    )
