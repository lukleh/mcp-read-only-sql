#!/usr/bin/env python3
"""
Security size limit tests
Tests result size limit enforcement (Layer 3 of security model)
"""

import pytest
from src.connectors.postgresql.python import PostgreSQLPythonConnector
from src.connectors.clickhouse.python import ClickHousePythonConnector


@pytest.fixture
def postgres_size_limit_connector():
    """Create PostgreSQL connector with 10KB size limit"""
    config = {
        "connection_name": "size_test",
        "type": "postgresql",
        "servers": [{"host": "localhost", "port": 5432}],
        "db": "testdb",
        "username": "testuser",
        "password": "testpass",
        "max_result_bytes": 10000  # 10KB limit
    }
    return PostgreSQLPythonConnector(config)


@pytest.fixture
def clickhouse_size_limit_connector():
    """Create ClickHouse connector with 10KB size limit"""
    config = {
        "connection_name": "size_test",
        "type": "clickhouse",
        "servers": [{"host": "localhost", "port": 9000}],
        "db": "testdb",
        "username": "testuser",
        "password": "testpass",
        "max_result_bytes": 10000  # 10KB limit
    }
    return ClickHousePythonConnector(config)


@pytest.mark.security
@pytest.mark.docker
@pytest.mark.anyio
class TestResultSizeLimit:
    """Test result size limit enforcement (Security Layer 3)"""

    async def test_postgres_size_limit_small_query(self, postgres_size_limit_connector):
        """Test PostgreSQL query within size limit"""
        result = await postgres_size_limit_connector.execute_query(
            "SELECT id, username FROM users LIMIT 5"
        )

        assert isinstance(result, str)  # Should return TSV on success
        lines = result.strip().split('\n')
        assert len(lines) == 6  # Header + 5 rows

    async def test_postgres_size_limit_large_query(self, postgres_size_limit_connector):
        """Test PostgreSQL query that might exceed size limit"""
        # Query all data which might exceed 10KB
        try:
            result = await postgres_size_limit_connector.execute_query(
                "SELECT * FROM users, products"  # Cartesian product - lots of data
            )
            # If it succeeds, should be truncated TSV
            assert isinstance(result, str)
            # Check size is within limit
            assert len(result.encode()) <= 15000  # Allow some overhead
        except RuntimeError as e:
            # Or it might fail due to size limit
            error_msg = str(e).lower()
            assert "size" in error_msg or "exceeded" in error_msg

    async def test_clickhouse_size_limit(self, clickhouse_size_limit_connector):
        """Test ClickHouse result size limit"""
        # Query that should exceed 10KB limit
        try:
            result = await clickhouse_size_limit_connector.execute_query(
                "SELECT * FROM testdb.events LIMIT 1000"
            )
            # If it succeeds, should be truncated TSV
            assert isinstance(result, str)
            # Check size is within limit
            assert len(result.encode()) <= 15000  # Allow some overhead
        except RuntimeError as e:
            # Or it might fail due to size limit
            error_msg = str(e).lower()
            assert "result exceeded" in error_msg or "size" in error_msg

    async def test_size_limit_configuration(self, postgres_size_limit_connector):
        """Test that size limit is properly configured"""
        assert postgres_size_limit_connector.max_result_bytes == 10000