#!/usr/bin/env python3
"""
Test CLI connectors with system SSH (not Paramiko)
"""

import pytest
import asyncio
import os
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
class TestCLISystemSSH:
    """Test system SSH tunneling for CLI connectors"""

    async def test_postgresql_cli_with_system_ssh(self):
        """Test PostgreSQL CLI connector through system SSH tunnel"""
        from conftest import make_connection

        config = make_connection({
            "connection_name": "pg_cli_system_ssh",
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
                "private_key": "/tmp/docker_test_key"  # Use key-based auth
            }
        })
        connector = PostgreSQLCLIConnector(config)

        # Execute query through SSH tunnel
        result = await connector.execute_query("SELECT COUNT(*) as count FROM users")

        assert isinstance(result, str)  # Should return TSV on success
        lines = result.strip().split('\n')
        assert len(lines) == 2  # Header + 1 row
        count_value = lines[1].split('\t')[0]
        assert int(count_value) > 0  # Should have users

    async def test_clickhouse_cli_with_system_ssh(self):
        """Test ClickHouse CLI connector through system SSH tunnel"""
        from conftest import make_connection

        config = make_connection({
            "connection_name": "ch_cli_system_ssh",
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
        })
        connector = ClickHouseCLIConnector(config)

        # Execute query through SSH tunnel
        result = await connector.execute_query("SELECT COUNT(*) as count FROM testdb.events")

        assert isinstance(result, str)  # Should return TSV on success
        lines = result.strip().split('\n')
        assert len(lines) == 2  # Header + 1 row
        count_value = lines[1].split('\t')[0]
        assert int(count_value) > 0  # Should have events

    async def test_system_ssh_without_private_key(self):
        """Configurator must supply credentials when enabling SSH"""
        from conftest import make_connection

        with pytest.raises(ValueError, match="requires either 'private_key' or 'password'"):
            make_connection({
                "connection_name": "pg_cli_default_ssh",
                "type": "postgresql",
                "servers": [{"host": "mcp-postgres-private", "port": 5432}],
                "db": "testdb",
                "username": "testuser",
                "password": "testpass",
                "ssh_tunnel": {
                    "enabled": True,
                    "host": "localhost",
                    "port": 2222,
                    "user": "tunnel"
                    # Missing credentials should be rejected up front
                }
            })

    async def test_system_ssh_disabled(self):
        """Test CLI connectors work normally when SSH is disabled"""
        from conftest import make_connection

        config = make_connection({
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
                "private_key": "/tmp/docker_test_key"
            }
        })
        connector = PostgreSQLCLIConnector(config)

        # Should connect directly without SSH
        result = await connector.execute_query("SELECT 'direct' as connection_type")

        assert isinstance(result, str)  # Should return TSV on success
        lines = result.strip().split('\n')
        assert len(lines) == 2  # Header + 1 row
        connection_type = lines[1].split('\t')[0]
        assert connection_type == "direct"

    async def test_system_ssh_multiple_queries(self):
        """Test multiple queries through system SSH tunnel"""
        from conftest import make_connection

        config = make_connection({
            "connection_name": "pg_multi_ssh",
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
        })
        connector = PostgreSQLCLIConnector(config)

        # Execute multiple queries to test tunnel reuse
        for i in range(3):
            result = await connector.execute_query(f"SELECT {i+1} as num")
            assert isinstance(result, str)  # Should return TSV on success
            lines = result.strip().split('\n')
            assert len(lines) == 2  # Header + 1 row
            num_value = lines[1].split('\t')[0]
            assert int(num_value) == i+1
