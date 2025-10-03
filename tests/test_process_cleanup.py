"""Test that CLI processes are properly cleaned up on timeout"""

import asyncio
import pytest
import signal
import os
from unittest.mock import patch, MagicMock, AsyncMock
import anyio
from src.connectors.postgresql.cli import PostgreSQLCLIConnector
from src.connectors.clickhouse.cli import ClickHouseCLIConnector


@pytest.mark.anyio
async def test_postgresql_cli_process_cleanup_on_timeout():
    """Test that psql process is killed when timeout occurs"""
    from conftest import make_connection

    config = make_connection({
        "connection_name": "test_postgres",
        "type": "postgresql",
        "servers": [{"host": "localhost", "port": 5432}],
        "db": "testdb",
        "username": "testuser",
        "password": "testpass",
        "query_timeout": 0.1  # Very short timeout to trigger cancellation
    })

    connector = PostgreSQLCLIConnector(config)

    # Mock the subprocess to simulate a long-running query
    mock_process = MagicMock()
    mock_process.returncode = None
    mock_process.kill = MagicMock()
    mock_process.wait = AsyncMock(return_value=None)

    async def slow_readline():
        await asyncio.sleep(10)
        return b""

    mock_stdout = MagicMock()
    mock_stdout.readline = AsyncMock(side_effect=slow_readline)
    mock_process.stdout = mock_stdout

    mock_stderr = MagicMock()
    mock_stderr.read = AsyncMock(return_value=b"")
    mock_process.stderr = mock_stderr

    # Create an async function that will be cancelled
    async def long_running_communicate():
        try:
            await asyncio.sleep(10)  # Simulate long query
            return b"result", b""
        except asyncio.CancelledError:
            # Re-raise to simulate actual behavior
            raise

    mock_process.communicate = long_running_communicate

    with patch('asyncio.create_subprocess_exec', return_value=mock_process):
        try:
            # The connector has a 0.1 second timeout
            await connector.execute_query("SELECT pg_sleep(10)")
            assert False, "Should have timed out"
        except (asyncio.TimeoutError, RuntimeError) as e:
            # Expected - the query timed out
            # The connector wraps TimeoutError in RuntimeError
            assert "timed out" in str(e).lower() or isinstance(e, asyncio.TimeoutError)

    # Verify that kill was called on the process
    mock_process.kill.assert_called_once()


@pytest.mark.anyio
async def test_clickhouse_cli_process_cleanup_on_timeout():
    """Test that clickhouse-client process is killed when timeout occurs"""
    from conftest import make_connection

    config = make_connection({
        "connection_name": "test_clickhouse",
        "type": "clickhouse",
        "servers": [{"host": "localhost", "port": 9000}],
        "db": "testdb",
        "username": "testuser",
        "password": "testpass",
        "query_timeout": 0.1  # Very short timeout
    })

    connector = ClickHouseCLIConnector(config)

    # Mock the subprocess
    mock_process = MagicMock()
    mock_process.returncode = None
    mock_process.kill = MagicMock()
    mock_process.wait = AsyncMock(return_value=None)

    async def slow_readline():
        await asyncio.sleep(10)
        return b""

    mock_stdout = MagicMock()
    mock_stdout.readline = AsyncMock(side_effect=slow_readline)
    mock_process.stdout = mock_stdout

    mock_stderr = MagicMock()
    mock_stderr.read = AsyncMock(return_value=b"")
    mock_process.stderr = mock_stderr

    # Create an async function that will be cancelled
    async def long_running_communicate():
        try:
            await asyncio.sleep(10)  # Simulate long query
            return b"result", b""
        except asyncio.CancelledError:
            # Re-raise to simulate actual behavior
            raise

    mock_process.communicate = long_running_communicate

    with patch('asyncio.create_subprocess_exec', return_value=mock_process):
        try:
            # The connector has a 0.1 second timeout
            await connector.execute_query("SELECT sleep(10)")
            assert False, "Should have timed out"
        except (asyncio.TimeoutError, RuntimeError) as e:
            # Expected - the query timed out
            # The connector wraps TimeoutError in RuntimeError
            assert "timed out" in str(e).lower() or isinstance(e, asyncio.TimeoutError)

    # Verify cleanup
    mock_process.kill.assert_called_once()

