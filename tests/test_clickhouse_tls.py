"""TLS follows the remote endpoint even when SSH changes the local port."""

import asyncio
from contextlib import asynccontextmanager

import pytest

from mcp_read_only_sql.connectors.clickhouse.cli import ClickHouseCLIConnector
from tests.conftest import FakeCLIProcess, make_connection


@pytest.mark.anyio
@pytest.mark.parametrize("port", [8123, 8443, 9000, 9440])
@pytest.mark.parametrize("tunneled", [False, True])
async def test_cli_tls_follows_original_endpoint(port, tunneled, monkeypatch):
    connector = ClickHouseCLIConnector(
        make_connection(
            {
                "connection_name": "tls",
                "type": "clickhouse",
                "servers": [{"host": "database.example", "port": port}],
                "db": "default",
                "username": "reader",
            }
        )
    )
    connector._decided_settings[("database.example", port)] = {"readonly": 1}
    monkeypatch.setattr(connector, "_resolve_binary", lambda name: name)
    commands = []

    @asynccontextmanager
    async def tunnel(server=None):
        yield 45678 if tunneled else None

    async def spawn(*command, **kwargs):
        commands.append(command)
        return FakeCLIProcess(["x\n", "1\n"])

    monkeypatch.setattr(connector, "_get_ssh_tunnel", tunnel)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)

    assert await connector.execute_query("SELECT 1 AS x") == "x\n1"
    command = commands[0]
    assert ("--secure" in command) == (port in (8443, 9440))
    assert ("--tls-sni-override" in command) == (tunneled and port in (8443, 9440))
    if "--tls-sni-override" in command:
        assert command[command.index("--tls-sni-override") + 1] == "database.example"
    expected_port = 45678 if tunneled else {8123: 9000, 8443: 9440}.get(port, port)
    assert command[command.index("--port") + 1] == str(expected_port)
