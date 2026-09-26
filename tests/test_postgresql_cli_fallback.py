import asyncio

import pytest

from mcp_read_only_sql.connectors.postgresql.cli import PostgreSQLCLIConnector
from tests.conftest import FakeCLIProcess


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
            raise RuntimeError(
                "psql: unsupported startup parameter in options: default_transaction_read_only"
            )
        return FakeCLIProcess(["column", "value"])

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    result = await connector.execute_query("SELECT version()")

    assert "value" in result
    assert len(call_log) == 2
    assert call_log[0]["PGOPTIONS"].startswith("-c default_transaction_read_only")
    assert "PGOPTIONS" not in call_log[1]
