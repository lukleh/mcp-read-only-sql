"""
Test to verify that all database implementations enforce read-only mode at the database session level.
This is a critical security test for Layer 2 of our three-layer security model.
"""

import pytest
from src.connectors.postgresql.python import PostgreSQLPythonConnector
from src.connectors.postgresql.cli import PostgreSQLCLIConnector
from src.connectors.clickhouse.python import ClickHousePythonConnector
from src.connectors.clickhouse.cli import ClickHouseCLIConnector


def _assert_readonly_error(exc_info, connector_name: str):
    error = str(exc_info.value).lower()
    assert any(
        keyword in error for keyword in ["read", "permission", "readonly", "cannot"]
    ), f"{connector_name}: expected read-only style error, got: {error[:120]}"


@pytest.fixture
def postgres_config():
    """PostgreSQL test configuration"""
    return {
        "connection_name": "test_postgres",
        "type": "postgresql",
        "servers": [{"host": "localhost", "port": 5432}],
        "username": "testuser",
        "password": "testpass",
        "db": "testdb"
    }


@pytest.fixture
def clickhouse_config():
    """ClickHouse test configuration"""
    return {
        "connection_name": "test_clickhouse",
        "type": "clickhouse",
        "servers": [{"host": "localhost", "port": 9000}],
        "username": "testuser",
        "password": "testpass",
        "db": "testdb"
    }


@pytest.mark.anyio
async def test_postgresql_python_readonly(postgres_config):
    """Test PostgreSQL Python connector enforces read-only mode"""
    connector = PostgreSQLPythonConnector(postgres_config)

    # Test write operations are blocked
    write_queries = [
        "INSERT INTO users (username, email) VALUES ('testuser', 'test@example.com')",
        "UPDATE users SET email = 'changed@example.com' WHERE id = 1",
        "DELETE FROM users WHERE id = 1",
        "DROP TABLE users",
        "CREATE TABLE new_table (id INT)"
    ]

    for query in write_queries:
        with pytest.raises(RuntimeError) as exc_info:
            await connector.execute_query(query)
        _assert_readonly_error(exc_info, "PostgreSQL Python")

    # Test SELECT still works
    result = await connector.execute_query("SELECT 1")
    assert isinstance(result, str), "SELECT query should return TSV"


@pytest.mark.anyio
async def test_postgresql_cli_readonly(postgres_config):
    """Test PostgreSQL CLI connector enforces read-only mode"""
    connector = PostgreSQLCLIConnector(postgres_config)

    # Test SELECT works with session wrapping
    result = await connector.execute_query("SELECT 1 as test")
    assert isinstance(result, str), "SELECT query should return TSV"

    # Test write operations fail due to wrapped read-only session
    write_queries = [
        "INSERT INTO users (username, email) VALUES ('testuser2', 'test2@example.com')",
        "UPDATE users SET email = 'changed@example.com' WHERE id = 1",
        "DELETE FROM users WHERE id = 1"
    ]

    for query in write_queries:
        with pytest.raises(RuntimeError) as exc_info:
            await connector.execute_query(query)
        _assert_readonly_error(exc_info, "PostgreSQL CLI")


@pytest.mark.anyio
async def test_clickhouse_python_readonly(clickhouse_config):
    """Test ClickHouse Python connector enforces read-only mode"""
    connector = ClickHousePythonConnector(clickhouse_config)

    # Test write operations are blocked
    write_queries = [
        "INSERT INTO events VALUES (now(), 'test', 'test_type', '{}')",
        "ALTER TABLE events ADD COLUMN test String",
        "DROP TABLE events",
        "CREATE TABLE test_table (id Int32) ENGINE = Memory"
    ]

    for query in write_queries:
        with pytest.raises(RuntimeError) as exc_info:
            await connector.execute_query(query)
        _assert_readonly_error(exc_info, "ClickHouse Python")

    # Test SELECT still works
    result = await connector.execute_query("SELECT 1")
    assert isinstance(result, str), "SELECT query should return TSV"


@pytest.mark.anyio
async def test_clickhouse_cli_readonly(clickhouse_config):
    """Test ClickHouse CLI connector enforces read-only mode"""
    connector = ClickHouseCLIConnector(clickhouse_config)

    # Test SELECT works with readonly flag
    result = await connector.execute_query("SELECT 1 as test")
    assert isinstance(result, str), "SELECT query should return TSV"

    # Test write operations are blocked by --readonly=1
    write_queries = [
        "INSERT INTO events VALUES (now(), 'test', 'test_type', '{}')",
        "CREATE TABLE test_table (id Int32) ENGINE = Memory",
        "DROP TABLE events"
    ]

    for query in write_queries:
        with pytest.raises(RuntimeError) as exc_info:
            await connector.execute_query(query)
        _assert_readonly_error(exc_info, "ClickHouse CLI")


@pytest.mark.anyio
class TestReadOnlyEnforcement:
    """Test suite for read-only enforcement across all implementations"""

    async def test_all_connectors_block_writes(self, postgres_config, clickhouse_config):
        """Verify all connectors block write operations"""
        connectors = [
            ("PostgreSQL Python", PostgreSQLPythonConnector(postgres_config)),
            ("PostgreSQL CLI", PostgreSQLCLIConnector(postgres_config)),
            ("ClickHouse Python", ClickHousePythonConnector(clickhouse_config)),
            ("ClickHouse CLI", ClickHouseCLIConnector(clickhouse_config))
        ]

        for name, connector in connectors:
            # Test a simple SELECT works
            result = await connector.execute_query("SELECT 1")
            assert isinstance(result, str), f"{name}: SELECT should return TSV"

            # Test INSERT is blocked (most basic write operation)
            if "PostgreSQL" in name:
                insert_query = "INSERT INTO users (username, email) VALUES ('testuser3', 'test3@example.com')"
            else:
                insert_query = "INSERT INTO events VALUES (now(), 'test', 'type', '{}')"

            with pytest.raises(RuntimeError) as exc_info:
                await connector.execute_query(insert_query)
            _assert_readonly_error(exc_info, name)


@pytest.mark.anyio
async def test_postgres_malicious_queries_blocked(postgres_config):
    """Ensure advanced PostgreSQL write attempts are rejected."""

    attack_queries = [
        "WITH up AS (UPDATE users SET email = 'blocked@example.com' WHERE id = 1 RETURNING *) SELECT count(*) FROM up",
        "DO $$ BEGIN INSERT INTO users (username, email) VALUES ('blocked_do', 'blocked@example.com'); END $$;",
        "CREATE TEMP TABLE temp_blocked (id INT)",
    ]

    connectors = [
        ("PostgreSQL Python", PostgreSQLPythonConnector(postgres_config)),
        ("PostgreSQL CLI", PostgreSQLCLIConnector(postgres_config)),
    ]

    for name, connector in connectors:
        for query in attack_queries:
            with pytest.raises(RuntimeError) as exc_info:
                await connector.execute_query(query)
            _assert_readonly_error(exc_info, name)


@pytest.mark.anyio
async def test_clickhouse_malicious_queries_blocked(clickhouse_config):
    """Ensure ClickHouse rejects trickier write or DDL statements."""

    attack_queries = [
        "TRUNCATE TABLE events",
        "ALTER TABLE events UPDATE event_type = 'blocked' WHERE 1",
        "ALTER TABLE events DELETE WHERE 1",
        "CREATE TEMPORARY TABLE temp_blocked (id Int32) ENGINE = Memory",
    ]

    connectors = [
        ("ClickHouse Python", ClickHousePythonConnector(clickhouse_config)),
        ("ClickHouse CLI", ClickHouseCLIConnector(clickhouse_config)),
    ]

    for name, connector in connectors:
        for query in attack_queries:
            with pytest.raises(RuntimeError) as exc_info:
                await connector.execute_query(query)
            _assert_readonly_error(exc_info, name)
