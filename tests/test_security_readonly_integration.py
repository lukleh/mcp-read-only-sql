"""Read-only enforcement tests executed against real Docker databases."""

import shutil

import pytest

from mcp_read_only_sql.connectors.postgresql.cli import PostgreSQLCLIConnector
from mcp_read_only_sql.connectors.postgresql.python import PostgreSQLPythonConnector
from mcp_read_only_sql.connectors.clickhouse.cli import ClickHouseCLIConnector
from mcp_read_only_sql.connectors.clickhouse.python import ClickHousePythonConnector
from mcp_read_only_sql.utils.sql_guard import ReadOnlyQueryError
from tests.docker_test_config import docker_test_server

from tests.sql_statement_lists import (
    CLICKHOUSE_ALLOWED_LITERAL_QUERIES,
    CLICKHOUSE_INTEGRATION_BLOCKED_STATEMENTS,
    POSTGRESQL_ALLOWED_LITERAL_QUERIES,
    POSTGRESQL_INTEGRATION_BLOCKED_STATEMENTS,
)

POSTGRES_BASE_CONFIG = {
    "connection_name": "integration_postgres",
    "type": "postgresql",
    "servers": [docker_test_server("postgresql")],
    "db": "testdb",
    "username": "testuser",
    "password": "testpass",
}

CLICKHOUSE_BASE_CONFIG = {
    "connection_name": "integration_clickhouse",
    "type": "clickhouse",
    "servers": [docker_test_server("clickhouse")],
    "db": "testdb",
    "username": "testuser",
    "password": "testpass",
}


def _is_readonly_error(message: str) -> bool:
    lowered = message.lower()
    keywords = [
        "read only",
        "read-only",
        "readonly",
        "cannot execute",
        "permission denied",
        "readonly mode",
    ]
    return any(keyword in lowered for keyword in keywords)


async def _verify_connection(connector, vendor: str):
    """Ensure the target database is reachable; otherwise skip."""
    try:
        await connector.execute_query("SELECT 1")
    except Exception as exc:  # pragma: no cover - only exercised when docker is absent
        pytest.skip(f"{vendor} not reachable: {exc}")


def _build_postgres_connector(implementation: str):
    from conftest import make_connection

    config = make_connection(POSTGRES_BASE_CONFIG.copy())
    if implementation == "cli":
        if shutil.which("psql") is None:
            pytest.skip("psql command not available in PATH")
        return PostgreSQLCLIConnector(config)
    return PostgreSQLPythonConnector(config)


def _build_clickhouse_connector(implementation: str):
    from conftest import make_connection

    config = make_connection(CLICKHOUSE_BASE_CONFIG.copy())
    if implementation == "cli":
        if shutil.which("clickhouse-client") is None:
            pytest.skip("clickhouse-client command not available in PATH")
        return ClickHouseCLIConnector(config)
    return ClickHousePythonConnector(config)


@pytest.mark.anyio
@pytest.mark.security
@pytest.mark.docker
@pytest.mark.parametrize("implementation", ["python", "cli"])
@pytest.mark.parametrize("statement", POSTGRESQL_INTEGRATION_BLOCKED_STATEMENTS)
async def test_postgres_real_blocks_mutations(implementation, statement):
    connector = _build_postgres_connector(implementation)
    await _verify_connection(connector, "PostgreSQL")

    with pytest.raises(RuntimeError) as exc_info:
        await connector.execute_query(statement)

    assert _is_readonly_error(
        str(exc_info.value)
    ), f"Expected read-only error for {statement}"


@pytest.mark.anyio
@pytest.mark.security
@pytest.mark.docker
@pytest.mark.parametrize("implementation", ["python", "cli"])
@pytest.mark.parametrize("query", POSTGRESQL_ALLOWED_LITERAL_QUERIES)
async def test_postgres_real_allows_selects_with_keywords(implementation, query):
    connector = _build_postgres_connector(implementation)
    await _verify_connection(connector, "PostgreSQL")

    result = await connector.execute_query(query)
    assert "INSERT" in result, "Keyword inside literal should be preserved"


@pytest.mark.anyio
@pytest.mark.security
@pytest.mark.docker
@pytest.mark.parametrize("implementation", ["python", "cli"])
async def test_postgres_real_blocks_transaction_escape(implementation):
    connector = _build_postgres_connector(implementation)
    await _verify_connection(connector, "PostgreSQL")

    payload = "SET transaction_read_only TO false ; DROP TABLE newtable"
    with pytest.raises(ReadOnlyQueryError):
        await connector.execute_query(payload)


@pytest.mark.anyio
@pytest.mark.security
@pytest.mark.docker
@pytest.mark.parametrize("implementation", ["python", "cli"])
@pytest.mark.parametrize("statement", CLICKHOUSE_INTEGRATION_BLOCKED_STATEMENTS)
async def test_clickhouse_real_blocks_mutations(implementation, statement):
    connector = _build_clickhouse_connector(implementation)
    await _verify_connection(connector, "ClickHouse")

    with pytest.raises(RuntimeError) as exc_info:
        await connector.execute_query(statement)

    assert _is_readonly_error(
        str(exc_info.value)
    ), f"Expected read-only error for {statement}"


@pytest.mark.anyio
@pytest.mark.security
@pytest.mark.docker
@pytest.mark.parametrize("implementation", ["python", "cli"])
@pytest.mark.parametrize("query", CLICKHOUSE_ALLOWED_LITERAL_QUERIES)
async def test_clickhouse_real_allows_selects_with_keywords(implementation, query):
    connector = _build_clickhouse_connector(implementation)
    await _verify_connection(connector, "ClickHouse")

    result = await connector.execute_query(query)
    assert "INSERT" in result, "Keyword inside literal should be preserved"
