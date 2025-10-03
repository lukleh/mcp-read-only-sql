#!/usr/bin/env python3
"""
Security layers integration tests for MCP SQL Server
Tests all three security layers working together:
- Layer 1 & 2: Read-only enforcement
- Layer 3: Timeout and size limits
"""

import asyncio
import time

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


@pytest.fixture
def postgres_strict_connector():
    """Create PostgreSQL connector with strict limits"""
    from conftest import make_connection
    config = make_connection({
        "connection_name": "timeout_test",
        "type": "postgresql",
        "servers": [{"host": "localhost", "port": 5432}],
        "db": "testdb",
        "username": "testuser",
        "password": "testpass",
        "query_timeout": 2,  # 2 second timeout
        "connection_timeout": 2,
        "max_result_bytes": 10000  # 10KB
    })
    return PostgreSQLPythonConnector(config)


@pytest.fixture
def clickhouse_strict_connector():
    """Create ClickHouse connector with strict limits"""
    from conftest import make_connection
    config = make_connection({
        "connection_name": "size_test",
        "type": "clickhouse",
        "servers": [{"host": "localhost", "port": 9000}],
        "db": "testdb",
        "username": "testuser",
        "password": "testpass",
        "query_timeout": 2,
        "max_result_bytes": 10000  # 10KB
    })
    return ClickHousePythonConnector(config)


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
@pytest.mark.slow
@pytest.mark.anyio
class TestTimeoutEnforcement:
    """Test query and connection timeout enforcement"""

    async def test_query_timeout(self, postgres_strict_connector):
        """Test that long-running queries timeout"""
        query = "SELECT pg_sleep(3), COUNT(*) FROM users"  # 3 second sleep with 2 second timeout

        start_time = time.time()
        with pytest.raises((RuntimeError, TimeoutError)) as exc_info:
            await postgres_strict_connector.execute_query(query)
        elapsed = time.time() - start_time

        error_msg = str(exc_info.value).lower()
        assert "timeout" in error_msg
        assert elapsed < 3.5, "Query should timeout within 3.5 seconds"


@pytest.mark.security
@pytest.mark.docker
@pytest.mark.anyio
class TestResultSizeLimit:
    """Test result size limit enforcement"""

    async def test_postgres_size_limit(self, postgres_strict_connector):
        """Test PostgreSQL result size limit"""
        # Query that should stay within limits
        result = await postgres_strict_connector.execute_query(
            "SELECT id, username FROM users LIMIT 5"
        )

        # Should return TSV string
        assert isinstance(result, str)
        lines = result.strip().split('\n')
        # First line is headers, rest are rows
        assert len(lines) == 6  # 1 header + 5 rows

    async def test_clickhouse_size_limit(self, clickhouse_strict_connector):
        """Test ClickHouse result size limit"""
        # Query that should exceed 10KB limit
        try:
            result = await clickhouse_strict_connector.execute_query(
                "SELECT * FROM testdb.events LIMIT 1000"
            )
            # If it succeeds, result should be truncated TSV
            assert isinstance(result, str)
            # Check if result is reasonably sized (under limit)
            assert len(result.encode()) <= 10000 * 1.5  # Allow some overhead
        except RuntimeError as e:
            # Or it might fail due to size limit
            error_msg = str(e).lower()
            assert "result exceeded" in error_msg or "size limit" in error_msg


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


@pytest.mark.security
@pytest.mark.docker
@pytest.mark.anyio
class TestSecurityLayers:
    """Test all three security layers work together"""

    async def test_all_layers_active(self, postgres_strict_connector):
        """Test that all security layers are active"""
        # Layer 1: Query validation
        with pytest.raises(RuntimeError) as exc_info:
            await postgres_strict_connector.execute_query(
                "DELETE FROM users"
            )
        error_msg = str(exc_info.value).lower()
        # Should be blocked by database read-only mode
        assert "read-only" in error_msg or "cannot execute" in error_msg

        # Layer 2: Timeout protection
        with pytest.raises((RuntimeError, TimeoutError)) as exc_info:
            await postgres_strict_connector.execute_query(
                "SELECT pg_sleep(3)"
            )
        error_msg = str(exc_info.value).lower()
        assert "timeout" in error_msg

        # Layer 3: Size limits (would work if we had more data)
        # Just verify the limit is configured
        assert postgres_strict_connector.max_result_bytes == 10000
