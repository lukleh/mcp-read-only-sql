import pytest


@pytest.mark.anyio
async def test_clickhouse_python_falls_back_to_cli(monkeypatch):
    from tests.conftest import make_connection
    from src.connectors.clickhouse.python import ClickHousePythonConnector
    from types import SimpleNamespace

    config = make_connection(
        {
            "connection_name": "ch_http",
            "type": "clickhouse",
            "servers": ["example.com:8123"],
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

    connector = ClickHousePythonConnector(config)

    class FakeSSHTunnel:
        def __init__(self, *args, **kwargs):
            pass

        async def start(self):
            raise RuntimeError("SSH: Authentication failed - bad key")

        async def stop(self):
            pass

    cli_start_called = False
    cli_stop_called = False

    class FakeCLITunnel:
        def __init__(self, ssh_config, remote_host, remote_port):
            self.remote_host = remote_host
            self.remote_port = remote_port

        async def start(self):
            nonlocal cli_start_called
            cli_start_called = True
            return 60000

        async def stop(self):
            nonlocal cli_stop_called
            cli_stop_called = True

    class FakeClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def query(self, query, column_oriented=False):
            return SimpleNamespace(column_names=["version()"], result_rows=[["24.1"]])

        def close(self):
            pass

    monkeypatch.setattr("src.utils.ssh_tunnel.SSHTunnel", FakeSSHTunnel)
    monkeypatch.setattr("src.connectors.clickhouse.python.CLISSHTunnel", FakeCLITunnel)
    monkeypatch.setattr("src.connectors.clickhouse.python.clickhouse_connect.get_client", lambda **kw: FakeClient(**kw))

    result = await connector.execute_query("SELECT version()")

    assert "version()" in result
    assert "24.1" in result
    assert cli_start_called
    assert cli_stop_called
