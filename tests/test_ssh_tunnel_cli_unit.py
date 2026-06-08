import asyncio

import pytest

from mcp_read_only_sql.config.connection import SSHTunnelConfig
from mcp_read_only_sql.utils.ssh_tunnel_cli import CLISSHTunnel


@pytest.mark.anyio
async def test_cli_tunnel_waits_until_forwarded_port_accepts_connections(monkeypatch):
    """Agent-only CLI tunnel should return only after the local listener is ready."""
    ssh_config = SSHTunnelConfig.from_dict(
        {
            "host": "bastion.example.com",
            "port": 22,
            "user": "tunnel",
        }
    )
    assert ssh_config is not None

    class FakeProcess:
        returncode = None

        async def communicate(self):
            return b"", b""

    class FakeWriter:
        def close(self):
            pass

        async def wait_closed(self):
            pass

    ssh_args = []

    async def fake_create_subprocess_exec(*args, **kwargs):
        ssh_args.extend(args)
        return FakeProcess()

    attempts = 0

    async def fake_open_connection(host, port):
        nonlocal attempts
        attempts += 1
        assert host == "127.0.0.1"
        assert port == 45454
        if attempts < 3:
            raise ConnectionRefusedError
        return object(), FakeWriter()

    async def fake_sleep(delay):
        assert delay == 0.25

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    monkeypatch.setattr(asyncio, "open_connection", fake_open_connection)
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    tunnel = CLISSHTunnel(ssh_config, "db.internal", 5432)
    monkeypatch.setattr(tunnel, "_find_free_port", lambda: 45454)

    local_port = await tunnel.start()

    assert local_port == 45454
    assert attempts == 3
    assert ssh_args[0] == "ssh"
    assert "-i" not in ssh_args
    assert "tunnel@bastion.example.com" in ssh_args
