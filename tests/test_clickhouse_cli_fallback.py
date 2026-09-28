"""The Python connectors tunnel through system ssh, like the CLI connectors."""

from typing import ClassVar

import pytest

from mcp_read_only_sql.connectors.clickhouse.python import ClickHousePythonConnector
from mcp_read_only_sql.connectors.postgresql.python import PostgreSQLPythonConnector
from tests.conftest import make_connection


def _config(db_type: str, port: int):
    return make_connection(
        {
            "connection_name": f"{db_type}_ssh",
            "type": db_type,
            "servers": [f"example.com:{port}"],
            "db": "default",
            "username": "user",
            "password": "pass",
            "implementation": "python",
            "ssh_tunnel": {
                "host": "bastion.example.com",
                "user": "alice",
                "private_key": "/tmp/key",
            },
        }
    )


class FakeCLITunnel:
    started: ClassVar[list[tuple[str, int]]] = []
    stopped: ClassVar[int] = 0

    def __init__(self, ssh_config, remote_host, remote_port):
        self.target = (remote_host, remote_port)

    async def start(self):
        FakeCLITunnel.started.append(self.target)
        return 60000

    async def stop(self):
        FakeCLITunnel.stopped += 1


@pytest.fixture(autouse=True)
def _fake_tunnel(monkeypatch):
    FakeCLITunnel.started.clear()
    FakeCLITunnel.stopped = 0
    monkeypatch.setattr("mcp_read_only_sql.connectors.base.CLISSHTunnel", FakeCLITunnel)
    monkeypatch.setattr(
        "mcp_read_only_sql.connectors.clickhouse.python.CLISSHTunnel", FakeCLITunnel
    )


@pytest.mark.anyio
async def test_postgresql_python_uses_the_system_ssh_tunnel(monkeypatch):
    seen = {}

    def fake_sync_query(self, host, port, database, query, *args, **kwargs):
        seen["endpoint"] = (host, port)
        return "col\n1"

    monkeypatch.setattr(PostgreSQLPythonConnector, "_execute_sync_query", fake_sync_query)

    result = await PostgreSQLPythonConnector(_config("postgresql", 5432)).execute_query(
        "SELECT 1"
    )

    assert result == "col\n1"
    assert FakeCLITunnel.started == [("example.com", 5432)]
    assert seen["endpoint"] == ("127.0.0.1", 60000)
    assert FakeCLITunnel.stopped == 1


@pytest.mark.anyio
async def test_clickhouse_python_tunnels_to_the_http_port(monkeypatch):
    """The native port in the config maps to the HTTP port the driver speaks."""

    def fake_sync_query(self, host, port, database, query, *args, **kwargs):
        return "col\n1"

    monkeypatch.setattr(ClickHousePythonConnector, "_execute_sync_query", fake_sync_query)

    await ClickHousePythonConnector(_config("clickhouse", 9000)).execute_query("SELECT 1")

    assert FakeCLITunnel.started == [("example.com", 8123)]
    assert FakeCLITunnel.stopped == 1
