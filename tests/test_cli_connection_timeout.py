#!/usr/bin/env python3
"""
Connection timeout tests for CLI connectors
Tests that CLI connectors properly handle connection timeouts
"""

import pytest
import asyncio
import time
from src.connectors.postgresql.cli import PostgreSQLCLIConnector
from src.connectors.clickhouse.cli import ClickHouseCLIConnector


@pytest.mark.docker
@pytest.mark.anyio
class TestCLIConnectionTimeout:
    """Test connection timeout for CLI connectors"""

    async def test_postgresql_cli_with_timeout(self):
        """Test PostgreSQL CLI connector with connection string timeout"""
        config = {
            "connection_name": "pg_cli_timeout",
            "type": "postgresql",
            "servers": [{"host": "localhost", "port": 5432}],
            "db": "testdb",
            "username": "testuser",
            "password": "testpass",
            "connection_timeout": 2,
        }
        connector = PostgreSQLCLIConnector(config)

        # Should connect successfully to real server
        result = await connector.execute_query("SELECT 1 as test")
        assert isinstance(result, str)  # Should return TSV on success
        lines = result.strip().split('\n')
        assert len(lines) == 2  # Header + 1 row
        assert lines[1].split('\t')[0] == '1'

    async def test_clickhouse_cli_respects_timeout(self):
        """Test ClickHouse CLI connector respects connection timeout"""
        config = {
            "connection_name": "ch_cli_timeout",
            "type": "clickhouse",
            "servers": [{"host": "localhost", "port": 9000}],
            "db": "testdb",
            "username": "testuser",
            "password": "testpass",
            "connection_timeout": 2,
        }
        connector = ClickHouseCLIConnector(config)

        # Should connect successfully
        result = await connector.execute_query("SELECT 1 as test")
        assert isinstance(result, str)  # Should return TSV on success
        lines = result.strip().split('\n')
        assert len(lines) == 2  # Header + 1 row
        assert lines[1].split('\t')[0] == '1'

    async def test_cli_unreachable_with_hard_timeout(self):
        """Test that hard timeout prevents hanging on unreachable hosts"""
        config = {
            "connection_name": "unreachable",
            "type": "postgresql",
            "servers": [{"host": "10.255.255.1", "port": 5432}],  # Non-routable IP
            "db": "testdb",
            "username": "testuser",
            "password": "testpass",
            "connection_timeout": 1,
            "hard_timeout": 3,  # Hard timeout will kill it
        }
        connector = PostgreSQLCLIConnector(config)

        start_time = time.time()
        # Use the hard timeout wrapper
        with pytest.raises(RuntimeError) as exc_info:
            await connector.execute_query_with_timeout("SELECT 1")
        elapsed = time.time() - start_time

        # Should be killed by hard timeout
        assert elapsed < 4, f"Should be killed by hard timeout within 4 seconds, took {elapsed:.1f}s"
        assert "timeout" in str(exc_info.value).lower()