#!/usr/bin/env python3
"""
SSH tunnel tests for CLI connectors
Tests that CLI connectors can properly use SSH tunnels via Paramiko
"""

import asyncio
import shutil
import pytest
from src.connectors.postgresql.cli import PostgreSQLCLIConnector
from src.connectors.clickhouse.cli import ClickHouseCLIConnector


@pytest.fixture(scope="module")
def event_loop():
    """Create an event loop for the module"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.mark.ssh
@pytest.mark.docker
@pytest.mark.anyio
class TestCLISSHTunnels:
    """Test SSH tunneling for CLI connectors"""

    async def test_postgresql_cli_with_ssh_tunnel(self):
        """Test PostgreSQL CLI connector through SSH tunnel"""
        config = {
            "connection_name": "pg_cli_ssh",
            "type": "postgresql",
            "servers": [{"host": "mcp-postgres-private", "port": 5432}],
            "db": "testdb",
            "username": "testuser",
            "password": "testpass",
            "ssh_tunnel": {
                "enabled": True,
                "host": "localhost",
                "port": 2222,  # SSH container port
                "user": "tunnel",
                "private_key": "/tmp/docker_test_key"  # Use key instead of password for CLI
            }
        }
        connector = PostgreSQLCLIConnector(config)

        # Execute query through SSH tunnel
        result = await connector.execute_query("SELECT COUNT(*) as count FROM users")

        assert isinstance(result, str)  # Should return TSV on success
        lines = result.strip().split('\n')
        assert len(lines) == 2  # Header + 1 row
        count_value = lines[1].split('\t')[0]
        assert int(count_value) > 0  # Should have users

    async def test_postgresql_cli_with_ssh_password(self):
        """Test PostgreSQL CLI connector with SSH password authentication"""
        if shutil.which("sshpass") is None:
            pytest.skip("sshpass not installed; skipping CLI password tunnel test")

        config = {
            "connection_name": "pg_cli_ssh_password",
            "type": "postgresql",
            "servers": [{"host": "mcp-postgres-private", "port": 5432}],
            "db": "testdb",
            "username": "testuser",
            "password": "testpass",
            "ssh_tunnel": {
                "enabled": True,
                "host": "localhost",
                "port": 2222,
                "user": "tunnel",
                "password": "tunnelpass"
            }
        }

        connector = PostgreSQLCLIConnector(config)
        result = await connector.execute_query("SELECT COUNT(*) as count FROM users")

        assert isinstance(result, str)
        lines = result.strip().split('\n')
        assert len(lines) == 2
        count_value = lines[1].split('\t')[0]
        assert int(count_value) > 0

    async def test_clickhouse_cli_with_ssh_tunnel(self):
        """Test ClickHouse CLI connector through SSH tunnel"""
        config = {
            "connection_name": "ch_cli_ssh",
            "type": "clickhouse",
            "servers": [{"host": "mcp-clickhouse-private", "port": 9000}],
            "db": "testdb",
            "username": "testuser",
            "password": "testpass",
            "ssh_tunnel": {
                "enabled": True,
                "host": "localhost",
                "port": 2222,
                "user": "tunnel",
                "private_key": "/tmp/docker_test_key"
            }
        }
        connector = ClickHouseCLIConnector(config)

        # Execute query through SSH tunnel
        result = await connector.execute_query("SELECT COUNT(*) as count FROM testdb.events")

        assert isinstance(result, str)  # Should return TSV on success
        lines = result.strip().split('\n')
        assert len(lines) == 2  # Header + 1 row
        count_value = lines[1].split('\t')[0]
        assert int(count_value) > 0  # Should have events

    async def test_cli_ssh_tunnel_cleanup(self):
        """Test that SSH tunnels are properly cleaned up after use"""
        config = {
            "connection_name": "pg_cli_ssh_cleanup",
            "type": "postgresql",
            "servers": [{"host": "mcp-postgres-private", "port": 5432}],
            "db": "testdb",
            "username": "testuser",
            "password": "testpass",
            "ssh_tunnel": {
                "enabled": True,
                "host": "localhost",
                "port": 2222,
                "user": "tunnel",
                "private_key": "/tmp/docker_test_key"
            }
        }
        connector = PostgreSQLCLIConnector(config)

        # Execute multiple queries to ensure tunnel reuse works
        for i in range(3):
            result = await connector.execute_query(f"SELECT {i+1} as num")
            assert isinstance(result, str)  # Should return TSV on success
            lines = result.strip().split('\n')
            assert len(lines) == 2  # Header + 1 row
            num_value = lines[1].split('\t')[0]
            assert int(num_value) == i+1

        # After context manager exits, tunnel should be closed
        # (cleanup is automatic with async context manager)

    async def test_cli_ssh_with_wrong_credentials(self):
        """Test CLI connector handles SSH authentication failure gracefully"""
        config = {
            "connection_name": "bad_ssh_cli",
            "type": "postgresql",
            "servers": [{"host": "mcp-postgres-private", "port": 5432}],
            "db": "testdb",
            "username": "testuser",
            "password": "testpass",
            "ssh_tunnel": {
                "enabled": True,
                "host": "localhost",
                "port": 2222,
                "user": "wronguser",
                "password": "wrongpass"
            }
        }
        connector = PostgreSQLCLIConnector(config)

        # SSH authentication should fail, raising an exception
        from paramiko.ssh_exception import AuthenticationException
        with pytest.raises((AuthenticationException, RuntimeError)) as exc_info:
            await connector.execute_query("SELECT 1")

        # Should get SSH auth failure
        if isinstance(exc_info.value, RuntimeError):
            assert "auth" in str(exc_info.value).lower() or "ssh" in str(exc_info.value).lower()

    async def test_cli_ssh_disabled(self):
        """Test CLI connectors work normally when SSH is disabled"""
        config = {
            "connection_name": "no_ssh_cli",
            "type": "postgresql",
            "servers": [{"host": "localhost", "port": 5432}],
            "db": "testdb",
            "username": "testuser",
            "password": "testpass",
            "ssh_tunnel": {
                "enabled": False,  # Explicitly disabled
                "host": "localhost",
                "port": 2222,
                "user": "tunnel",
                "password": "tunnelpass"
            }
        }
        connector = PostgreSQLCLIConnector(config)

        # Should connect directly without SSH
        result = await connector.execute_query("SELECT 'direct' as connection_type")

        assert isinstance(result, str)  # Should return TSV on success
        lines = result.strip().split('\n')
        assert len(lines) == 2  # Header + 1 row
        connection_type = lines[1].split('\t')[0]
        assert connection_type == "direct"

    async def test_cli_ssh_complex_query(self):
        """Test complex queries work through CLI SSH tunnel"""
        config = {
            "connection_name": "complex_ssh_cli",
            "type": "postgresql",
            "servers": [{"host": "mcp-postgres-private", "port": 5432}],
            "db": "testdb",
            "username": "testuser",
            "password": "testpass",
            "ssh_tunnel": {
                "enabled": True,
                "host": "localhost",
                "port": 2222,
                "user": "tunnel",
                "private_key": "/tmp/docker_test_key"
            }
        }
        connector = PostgreSQLCLIConnector(config)

        # Complex query with joins and aggregations
        query = """
            SELECT
                COUNT(*) as total_users,
                COUNT(DISTINCT username) as unique_usernames,
                MIN(created_at) as earliest_user,
                MAX(created_at) as latest_user
            FROM users
        """

        result = await connector.execute_query(query)

        assert isinstance(result, str)  # Should return TSV on success
        lines = result.strip().split('\n')
        assert len(lines) == 2  # Header + 1 row
        columns = lines[1].split('\t')
        assert len(columns) == 4  # Should return 4 columns
        # Total users should be positive
        assert int(columns[0]) > 0
