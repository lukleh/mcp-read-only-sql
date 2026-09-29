"""The CLI connectors stop their client process when the caller gives up.

``execute_query_with_timeout`` cancels the connector call at the hard
timeout. psql and clickhouse-client are child processes: unless the
cancellation kills them, they keep running the statement on the server
after the caller has already returned an error.
"""

import asyncio

import pytest

from mcp_read_only_sql.connectors.clickhouse.cli import ClickHouseCLIConnector
from mcp_read_only_sql.connectors.postgresql.cli import PostgreSQLCLIConnector
from mcp_read_only_sql.utils.timeout_wrapper import HardTimeoutError
from tests.conftest import FakeCLIStdin, make_connection
from tests.docker_test_config import docker_test_server


class _HangingStdout:
    """A client that has started and never prints anything."""

    def __init__(self, released: asyncio.Event):
        self._released = released

    async def readline(self) -> bytes:
        await self._released.wait()
        return b""


class _Stderr:
    async def read(self) -> bytes:
        return b""


class HangingProcess:
    def __init__(self, log: list):
        self.log = log
        self.released = asyncio.Event()
        self.stdout = _HangingStdout(self.released)
        self.stderr = _Stderr()
        self.stdin = FakeCLIStdin()
        self.returncode = None

    def kill(self) -> None:
        self.log.append("kill")
        self.returncode = -9
        self.released.set()

    async def wait(self) -> int:
        self.log.append("wait")
        return self.returncode if self.returncode is not None else 0


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("connector_class", "config_name"),
    [(PostgreSQLCLIConnector, "postgres_config"), (ClickHouseCLIConnector, "clickhouse_config")],
)
async def test_hard_timeout_kills_the_client_process(
    connector_class, config_name, request, monkeypatch
):
    log: list = []
    processes: list = []

    async def fake_exec(*cmd, **kwargs):
        process = HangingProcess(log)
        processes.append(process)
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    connector = connector_class(request.getfixturevalue(config_name))
    connector.query_timeout = 30
    connector.hard_timeout = 0.2

    with pytest.raises(HardTimeoutError, match="hard timeout"):
        await connector.execute_query_with_timeout("SELECT 1")

    assert log[:2] == ["kill", "wait"], "the client was not killed and reaped"
    assert all(process.returncode == -9 for process in processes)


def _cli_connector(db_type: str, statement_timeout_hard: float):
    connection = make_connection(
        {
            "connection_name": f"{db_type}_cli_hard_timeout",
            "type": db_type,
            "servers": [docker_test_server(db_type)],
            "db": "testdb",
            "username": "testuser",
            "password": "testpass",
            "implementation": "cli",
            "query_timeout": 30,
        }
    )
    connector = (
        PostgreSQLCLIConnector(connection)
        if db_type == "postgresql"
        else ClickHouseCLIConnector(connection)
    )
    connector.hard_timeout = statement_timeout_hard
    return connector


@pytest.mark.docker
@pytest.mark.usefixtures("docker_check")
@pytest.mark.anyio
async def test_hard_timeout_ends_the_statement_on_the_postgres_server():
    """psql is killed at the hard timeout and the capped statement_timeout
    ends the statement on the server, so nothing keeps running."""
    connector = _cli_connector("postgresql", 1)
    marker = "SELECT pg_sleep(20) AS cli_hard_timeout_marker"

    with pytest.raises(HardTimeoutError):
        await connector.execute_query_with_timeout(marker)

    checker = PostgreSQLCLIConnector(connector.connection)
    for _ in range(20):
        await asyncio.sleep(0.25)
        active = await checker.execute_query(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE state = 'active' AND query LIKE '%cli_hard_timeout_marker%' "
            "AND pid <> pg_backend_pid()"
        )
        if active.split("\n")[1] == "0":
            break
    assert active.split("\n")[1] == "0", "the statement outlived the hard timeout"
