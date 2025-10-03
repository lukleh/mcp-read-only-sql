#!/usr/bin/env python3
"""
CLI connector size limit tests
Tests result size limit enforcement for CLI connectors
"""

import pytest
from src.connectors.postgresql.cli import PostgreSQLCLIConnector
from src.connectors.clickhouse.cli import ClickHouseCLIConnector


def count_tsv_rows(tsv_str):
    """Count rows in TSV string (excluding header)"""
    lines = tsv_str.strip().split('\n')
    # First line is headers in our TSV format
    return len(lines) - 1 if len(lines) > 1 else 0


@pytest.fixture
def postgres_cli_size_limit():
    """Create PostgreSQL CLI connector with 1KB size limit"""
    from conftest import make_connection
    config = make_connection({
        "connection_name": "cli_size_test",
        "type": "postgresql",
        "servers": [{"host": "localhost", "port": 5432}],
        "db": "testdb",
        "username": "testuser",
        "password": "testpass",
        "max_result_bytes": 1000  # 1KB limit (very small for testing)
    })
    return PostgreSQLCLIConnector(config)


@pytest.fixture
def clickhouse_cli_size_limit():
    """Create ClickHouse CLI connector with 1KB size limit"""
    from conftest import make_connection
    config = make_connection({
        "connection_name": "cli_size_test",
        "type": "clickhouse",
        "servers": [{"host": "localhost", "port": 9000}],
        "db": "testdb",
        "username": "testuser",
        "password": "testpass",
        "max_result_bytes": 1000  # 1KB limit (very small for testing)
    })
    return ClickHouseCLIConnector(config)


@pytest.mark.security
@pytest.mark.docker
@pytest.mark.anyio
class TestCLISizeLimit:
    """Test CLI connector result size limit enforcement"""

    async def test_postgres_cli_small_query(self, postgres_cli_size_limit):
        """Test PostgreSQL CLI with query within size limit"""
        result = await postgres_cli_size_limit.execute_query(
            "SELECT id FROM users LIMIT 1"
        )

        assert isinstance(result, str)  # Should return TSV on success
        assert count_tsv_rows(result) == 1

    async def test_postgres_cli_large_query(self, postgres_cli_size_limit):
        """Test PostgreSQL CLI with query exceeding size limit"""
        # Query that will produce more than 1KB of TSV output
        try:
            result = await postgres_cli_size_limit.execute_query(
                "SELECT * FROM users, products"  # Cartesian product
            )
            # If it returns, should be truncated TSV
            assert isinstance(result, str)
            assert len(result.encode()) <= 1500  # Should be close to limit
        except RuntimeError as e:
            # Or it might fail due to size limit
            assert "exceeded" in str(e).lower()

    async def test_clickhouse_cli_small_query(self, clickhouse_cli_size_limit):
        """Test ClickHouse CLI with query within size limit"""
        result = await clickhouse_cli_size_limit.execute_query(
            "SELECT 1 as num"
        )

        assert isinstance(result, str)  # Should return TSV on success
        lines = result.strip().split('\n')
        assert len(lines) == 2  # Header + 1 row

    async def test_clickhouse_cli_large_query(self, clickhouse_cli_size_limit):
        """Test ClickHouse CLI with query exceeding size limit"""
        # Query that will produce more than 1KB of TSV output
        try:
            result = await clickhouse_cli_size_limit.execute_query(
                "SELECT * FROM testdb.events LIMIT 100"
            )
            # If it returns, should be truncated TSV
            assert isinstance(result, str)
            assert len(result.encode()) <= 1500  # Should be close to limit
        except RuntimeError as e:
            # Or it might fail due to size limit
            assert "exceeded" in str(e).lower()

    async def test_postgres_cli_truncation_preserves_success(self, postgres_cli_size_limit):
        """Test that PostgreSQL CLI can handle truncation gracefully"""
        # Query larger than 1KB
        try:
            result = await postgres_cli_size_limit.execute_query(
                "SELECT username, email, created_at FROM users"
            )
            # If successful, should return TSV (possibly truncated)
            assert isinstance(result, str)
            # Check it's not too large
            assert len(result.encode()) <= 1500
        except RuntimeError as e:
            # May fail if result too large
            assert "exceeded" in str(e).lower()

    async def test_size_limit_configuration(self, postgres_cli_size_limit, clickhouse_cli_size_limit):
        """Test that size limits are properly configured"""
        assert postgres_cli_size_limit.max_result_bytes == 1000
        assert clickhouse_cli_size_limit.max_result_bytes == 1000