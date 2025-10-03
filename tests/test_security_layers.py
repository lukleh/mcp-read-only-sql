#!/usr/bin/env python3
"""
Security layers integration tests for MCP SQL Server
Tests all three security layers working together:
- Layer 1 & 2: Read-only enforcement
- Layer 3: Timeout and size limits
"""

import pytest
from src.config.parser import ConfigParser
from src.connectors.postgresql.python import PostgreSQLPythonConnector
from src.connectors.clickhouse.python import ClickHousePythonConnector
from src.utils.sql_guard import ReadOnlyQueryError


@pytest.fixture(scope="module")
def test_config():
    """Load test configuration"""
    parser = ConfigParser("tests/connections-test.yaml")
    configs = parser.load_config()
    return configs


@pytest.fixture
def postgres_connector(test_config):
    """Create PostgreSQL connector for testing"""
    from conftest import make_connection
    config = next((c for c in test_config if c["connection_name"] == "test_postgres"), None)
    if not config:
        pytest.skip("test_postgres connection not found")
    # Ensure password is set
    if not config.get("password"):
        config["password"] = "testpass"
    return PostgreSQLPythonConnector(make_connection(config))


@pytest.fixture
def clickhouse_connector(test_config):
    """Create ClickHouse connector for testing"""
    from conftest import make_connection
    config = next((c for c in test_config if c["connection_name"] == "test_clickhouse"), None)
    if not config:
        pytest.skip("test_clickhouse connection not found")
    # Ensure password is set
    if not config.get("password"):
        config["password"] = "testpass"
    return ClickHousePythonConnector(make_connection(config))


@pytest.mark.security
@pytest.mark.docker
@pytest.mark.anyio
class TestWriteOperationBlocking:
    """Test that all write operations are blocked"""

    @pytest.mark.parametrize("query,operation", [
        ("INSERT INTO users (username, email) VALUES ('test', 'test@test.com')", "INSERT"),
        ("UPDATE users SET email = 'new@test.com' WHERE id = 1", "UPDATE"),
        ("DELETE FROM users WHERE id = 1", "DELETE"),
        ("DROP TABLE users", "DROP"),
        ("CREATE TABLE test (id INT)", "CREATE"),
        ("ALTER TABLE users ADD COLUMN test VARCHAR(50)", "ALTER"),
        ("TRUNCATE TABLE users", "TRUNCATE"),
    ])
    async def test_block_write_operations(self, postgres_connector, query, operation):
        """Test blocking of write operations"""
        with pytest.raises((RuntimeError, ReadOnlyQueryError)) as exc_info:
            await postgres_connector.execute_query(query)

        error_msg = str(exc_info.value).lower()
        assert any(word in error_msg for word in ["read-only", "cannot execute", "permission", "denied"]), (
            f"{operation} should be blocked"
        )

    async def test_block_multi_statement(self, postgres_connector):
        """Test blocking of multi-statement SQL injection"""
        query = "SELECT * FROM users; DELETE FROM users"
        with pytest.raises(ReadOnlyQueryError):
            await postgres_connector.execute_query(query)

    async def test_comment_in_query_allowed(self, postgres_connector):
        """Test that comments in queries are allowed (no pattern matching)"""
        query = "SELECT * FROM users -- This is a comment"
        result = await postgres_connector.execute_query(query)

        # Comments are fine, query should succeed (returns TSV string)
        assert isinstance(result, str), "Query with comment should return TSV"
        lines = result.strip().split('\n')
        assert len(lines) > 0  # Should have at least headers


@pytest.mark.security
@pytest.mark.docker
@pytest.mark.anyio
class TestReadOnlySession:
    """Test that database sessions are read-only"""

    async def test_postgres_readonly_session(self, postgres_connector):
        """Test PostgreSQL session is read-only"""
        # Check session status
        result = await postgres_connector.execute_query(
            "SHOW default_transaction_read_only"
        )

        assert isinstance(result, str)
        lines = result.strip().split('\n')
        if len(lines) > 1:
            # Parse TSV - first line is header, second is value
            value_line = lines[1].split('\t')
            readonly_status = value_line[0] if value_line else "unknown"
            assert readonly_status == "on", "Session should be read-only"

    async def test_valid_select_queries_work(self, postgres_connector, clickhouse_connector):
        """Test that valid SELECT queries still work"""
        # PostgreSQL
        result = await postgres_connector.execute_query(
            "SELECT COUNT(*) as count FROM users"
        )
        assert isinstance(result, str)
        lines = result.strip().split('\n')
        assert len(lines) >= 2  # Header + at least one row
        row_values = lines[1].split('\t')
        assert int(row_values[0]) > 0  # count > 0

        # ClickHouse
        result = await clickhouse_connector.execute_query(
            "SELECT COUNT(*) as count FROM testdb.events"
        )
        assert isinstance(result, str)
        lines = result.strip().split('\n')
        assert len(lines) >= 2  # Header + at least one row
        row_values = lines[1].split('\t')
        assert int(row_values[0]) > 0  # count > 0

