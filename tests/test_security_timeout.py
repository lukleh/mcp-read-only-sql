#!/usr/bin/env python3
"""
Security timeout tests
Tests query timeout enforcement (Layer 3 of security model)
"""

import time
import pytest
from src.config.parser import ConfigParser
from src.connectors.postgresql.python import PostgreSQLPythonConnector
from src.connectors.clickhouse.python import ClickHousePythonConnector


@pytest.fixture
def postgres_timeout_connector():
    """Create PostgreSQL connector with 2 second timeout"""
    config = {
        "connection_name": "timeout_test",
        "type": "postgresql",
        "servers": [{"host": "localhost", "port": 5432}],
        "db": "testdb",
        "username": "testuser",
        "password": "testpass",
        "query_timeout": 2,  # 2 second timeout
        "connection_timeout": 2,
    }
    return PostgreSQLPythonConnector(config)


@pytest.fixture
def clickhouse_timeout_connector():
    """Create ClickHouse connector with 2 second timeout"""
    config = {
        "connection_name": "timeout_test",
        "type": "clickhouse",
        "servers": [{"host": "localhost", "port": 9000}],
        "db": "testdb",
        "username": "testuser",
        "password": "testpass",
        "query_timeout": 2,  # 2 second timeout
    }
    return ClickHousePythonConnector(config)


@pytest.mark.security
@pytest.mark.docker
@pytest.mark.slow
@pytest.mark.anyio
class TestTimeoutEnforcement:
    """Test query timeout enforcement (Security Layer 3)"""

    async def test_postgres_query_timeout(self, postgres_timeout_connector):
        """Test that long-running PostgreSQL queries timeout"""
        query = "SELECT pg_sleep(3), COUNT(*) FROM users"  # 3 second sleep with 2 second timeout

        start_time = time.time()
        with pytest.raises((RuntimeError, TimeoutError)) as exc_info:
            await postgres_timeout_connector.execute_query(query)
        elapsed = time.time() - start_time

        error_msg = str(exc_info.value).lower()
        assert "timeout" in error_msg
        assert elapsed < 3.5, "Query should timeout within 3.5 seconds"

    async def test_clickhouse_query_timeout(self, clickhouse_timeout_connector):
        """Test that long-running ClickHouse queries timeout"""
        # ClickHouse sleep function
        query = "SELECT sleep(3)"  # 3 second sleep with 2 second timeout

        start_time = time.time()
        with pytest.raises((RuntimeError, TimeoutError)) as exc_info:
            await clickhouse_timeout_connector.execute_query(query)
        elapsed = time.time() - start_time

        error_msg = str(exc_info.value).lower()
        # Should timeout
        assert "timeout" in error_msg or elapsed < 3.5

    async def test_normal_queries_complete(self, postgres_timeout_connector):
        """Test that normal queries complete without timeout"""
        result = await postgres_timeout_connector.execute_query(
            "SELECT COUNT(*) as count FROM users"
        )

        # Should return TSV string
        assert isinstance(result, str)
        lines = result.strip().split('\n')
        assert len(lines) >= 2  # Header + at least one row
        row_values = lines[1].split('\t')
        assert int(row_values[0]) > 0  # count > 0

    async def test_connection_timeout(self):
        """Test that connection attempts timeout when server is unreachable"""
        # Create a connector pointing to a non-existent server
        config = {
            "connection_name": "unreachable_test",
            "type": "postgresql",
            "servers": [{"host": "192.168.255.255", "port": 5432}],  # Unreachable IP
            "db": "testdb",
            "username": "testuser",
            "password": "testpass",
            "connection_timeout": 1,  # 1 second timeout
        }
        connector = PostgreSQLPythonConnector(config)

        start_time = time.time()
        with pytest.raises(RuntimeError) as exc_info:
            await connector.execute_query("SELECT 1")
        elapsed = time.time() - start_time

        error_msg = str(exc_info.value).lower()
        # Check that it failed due to connection issue (not query issue)
        assert "connect" in error_msg or "reach" in error_msg or "timeout" in error_msg
        # Should timeout within 2 seconds (1 second timeout + overhead)
        assert elapsed < 3, f"Connection should timeout within 3 seconds, took {elapsed:.1f}s"