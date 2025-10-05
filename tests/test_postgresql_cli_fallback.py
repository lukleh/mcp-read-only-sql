import asyncio
import types

import pytest

from src.connectors.postgresql.cli import PostgreSQLCLIConnector


class DummyStdout:
    def __init__(self, lines):
        self._lines = [line.encode() for line in lines]

    async def readline(self):
        if self._lines:
            return self._lines.pop(0)
        return b""


class DummyStderr:
    def __init__(self, data=b""):
        self._data = data
        self._read = False

    async def read(self):
        if self._read:
            return b""
        self._read = True
        return self._data


class DummyProcess:
    def __init__(self, lines, returncode=0, stderr=b""):
        self.stdout = DummyStdout(lines)
        self.stderr = DummyStderr(stderr)
        self.returncode = returncode

    def kill(self):
        pass

    async def wait(self):
        return self.returncode


@pytest.mark.anyio
async def test_postgres_cli_retries_without_pgoptions(monkeypatch):
    from tests.conftest import make_connection

    config = make_connection(
        {
            "connection_name": "pg_cli_retry",
            "type": "postgresql",
            "servers": ["localhost:5432"],
            "db": "postgres",
            "username": "user",
            "password": "pass",
            "implementation": "cli",
        }
    )

    connector = PostgreSQLCLIConnector(config)

    call_log = []

    async def fake_create_subprocess_exec(*cmd, stdout=None, stderr=None, env=None):
        call_log.append(env.copy())
        if len(call_log) == 1:
            raise RuntimeError("psql: unsupported startup parameter in options: default_transaction_read_only")
        return DummyProcess(["column", "value"], returncode=0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    result = await connector.execute_query("SELECT version()")

    assert "value" in result
    assert len(call_log) == 2
    assert call_log[0]["PGOPTIONS"].startswith("-c default_transaction_read_only")
    assert "PGOPTIONS" not in call_log[1]

