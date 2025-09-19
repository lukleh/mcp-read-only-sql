#!/usr/bin/env python3
"""
Test hard timeout wrapper
Tests that the absolute hard timeout prevents the server from hanging
"""

import pytest
import time
from src.connectors.postgresql.python import PostgreSQLPythonConnector
from src.connectors.postgresql.cli import PostgreSQLCLIConnector
from src.utils.timeout_wrapper import HardTimeoutError


@pytest.fixture
def postgres_fast_hard_timeout():
    """Create PostgreSQL connector with very short timeouts for hard timeout test"""
    config = {
        "connection_name": "hard_timeout_test",
        "type": "postgresql",
        "servers": [{"host": "localhost", "port": 5432}],
        "db": "testdb",
        "username": "testuser",
        "password": "testpass",
        "query_timeout": 1,  # 1 second query timeout
        "connection_timeout": 1,  # 1 second connection timeout
        # hard_timeout will be calculated as sum: 1 + 1 + 5 (default SSH) = 7 seconds
    }
    return PostgreSQLPythonConnector(config)

@pytest.fixture
def postgres_hard_timeout_test():
    """Create PostgreSQL connector to test hard timeout separately"""
    config = {
        "connection_name": "hard_timeout_only",
        "type": "postgresql",
        "servers": [{"host": "localhost", "port": 5432}],
        "db": "testdb",
        "username": "testuser",
        "password": "testpass",
        "query_timeout": 30,  # 30 second query timeout (won't trigger)
        "connection_timeout": 1,  # 1 second connection timeout
        # hard_timeout will be calculated as sum: 30 + 1 + 5 = 36 seconds
        # But we'll mock a situation where hard timeout needs to trigger
    }
    return PostgreSQLPythonConnector(config)


@pytest.fixture
def postgres_cli_hard_timeout():
    """Create PostgreSQL CLI connector with short timeouts"""
    config = {
        "connection_name": "hard_timeout_test",
        "type": "postgresql",
        "servers": [{"host": "localhost", "port": 5432}],
        "db": "testdb",
        "username": "testuser",
        "password": "testpass",
        "query_timeout": 1,  # 1 second query timeout
        "connection_timeout": 1,  # 1 second connection timeout
        # hard_timeout will be calculated as sum: 1 + 1 + 5 (default SSH) = 7 seconds
    }
    return PostgreSQLCLIConnector(config)


@pytest.mark.docker
@pytest.mark.anyio
class TestHardTimeout:
    """Test hard timeout enforcement"""

    async def test_hard_timeout_prevents_hanging(self, postgres_fast_hard_timeout):
        """Test that hard timeout prevents queries from running too long"""
        # Query that would take 2 seconds - longer than query timeout (1s) but less than hard timeout (7s)
        query = "SELECT pg_sleep(2), COUNT(*) FROM users"

        start_time = time.time()
        with pytest.raises(TimeoutError) as exc_info:
            await postgres_fast_hard_timeout.execute_query_with_timeout(query)
        elapsed = time.time() - start_time

        # Should fail due to query timeout (1s), not hard timeout (7s)
        assert "PostgreSQL" in str(exc_info.value)
        # Should timeout at query limit (1s) with some overhead
        assert elapsed < 2.5, f"Query timeout should trigger within 2.5 seconds, took {elapsed:.1f}s"

    async def test_actual_hard_timeout(self):
        """Test that hard timeout works as absolute limit"""
        # Create connector with query timeout disabled to test hard timeout alone
        config = {
            "connection_name": "hard_timeout_only",
            "type": "postgresql",
            "servers": [{"host": "localhost", "port": 5432}],
            "db": "testdb",
            "username": "testuser",
            "password": "testpass",
            "query_timeout": 100,  # Very long query timeout
            "connection_timeout": 1,  # 1 second connection timeout
            # hard_timeout = 100 + 1 + 5 = 106 seconds
        }
        connector = PostgreSQLPythonConnector(config)
        # Override hard timeout for testing
        connector.hard_timeout = 2  # Force a 2-second hard timeout

        query = "SELECT pg_sleep(5)"  # 5-second query (enough to test hard timeout)

        start_time = time.time()
        with pytest.raises(HardTimeoutError) as exc_info:
            await connector.execute_query_with_timeout(query)
        elapsed = time.time() - start_time

        # Should fail due to hard timeout (2s)
        assert "hard timeout" in str(exc_info.value).lower()
        assert elapsed < 3, "Should timeout at hard limit (2s)"

    async def test_normal_query_completes(self, postgres_fast_hard_timeout):
        """Test that normal queries complete successfully within hard timeout"""
        result = await postgres_fast_hard_timeout.execute_query_with_timeout(
            "SELECT COUNT(*) as count FROM users"
        )

        # Should return TSV string
        assert isinstance(result, str)
        assert "count" in result  # Header should be present
        lines = result.strip().split('\n')
        assert len(lines) == 2  # Header + 1 data row

    async def test_cli_hard_timeout(self, postgres_cli_hard_timeout):
        """Test that CLI connector also respects timeouts"""
        # Long-running query with CLI connector
        query = "SELECT pg_sleep(2)"

        start_time = time.time()
        with pytest.raises((TimeoutError, RuntimeError)) as exc_info:
            await postgres_cli_hard_timeout.execute_query_with_timeout(query)
        elapsed = time.time() - start_time

        # Should fail due to timeout (statement timeout of 1s from wrapped query)
        assert "timeout" in str(exc_info.value).lower() or "statement timeout" in str(exc_info.value).lower()
        assert elapsed < 3, "CLI should respect timeouts"

    async def test_connection_timeout(self):
        """Test that connection timeout works properly"""
        # Try to connect to a non-existent server
        config = {
            "connection_name": "unreachable",
            "type": "postgresql",
            "servers": [{"host": "192.168.255.255", "port": 5432}],  # Unreachable
            "db": "testdb",
            "username": "testuser",
            "password": "testpass",
            "connection_timeout": 1,  # 1 second connection timeout
            "query_timeout": 1,  # 1 second query timeout
            # hard_timeout will be 1 + 1 + 5 = 7 seconds
        }
        connector = PostgreSQLPythonConnector(config)

        start_time = time.time()
        with pytest.raises((TimeoutError, RuntimeError)) as exc_info:
            await connector.execute_query_with_timeout("SELECT 1")
        elapsed = time.time() - start_time

        # Should fail within connection timeout period (1s) with some overhead
        assert elapsed < 3, f"Should timeout within 3 seconds, took {elapsed:.1f}s"