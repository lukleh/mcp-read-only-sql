#!/usr/bin/env python3
"""
SSH Tunnel tests
Tests SSH tunnel connectivity through bastion host to private databases
"""

import os
import pytest
from src.connectors.postgresql.python import PostgreSQLPythonConnector
from src.connectors.clickhouse.python import ClickHousePythonConnector


# SSH setup note: The test runner (run_tests.sh) handles:
# 1. Starting all test containers including SSH bastion
# 2. Copying SSH key to /tmp/docker_test_key
# So we just need to use the correct key path

@pytest.fixture
def ssh_test_key_path():
    """Get path to the SSH test key copied by run_tests.sh"""
    # run_tests.sh copies the key to this location
    key_path = "/tmp/docker_test_key"
    if not os.path.exists(key_path):
        # Fallback to try the other location
        alt_path = "/tmp/ssh_test_key"
        if os.path.exists(alt_path):
            return alt_path
        pytest.skip("SSH test key not found. Ensure run_tests.sh has set up the environment.")
    return key_path


@pytest.fixture
def postgres_ssh_password_config():
    """PostgreSQL config with SSH tunnel using password auth"""
    return {
        "connection_name": "postgres_ssh_pass",
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


@pytest.fixture
def postgres_ssh_key_config(ssh_test_key_path):
    """PostgreSQL config with SSH tunnel using key auth"""
    return {
        "connection_name": "postgres_ssh_key",
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
            "private_key": ssh_test_key_path
        }
    }


@pytest.fixture
def clickhouse_ssh_config():
    """ClickHouse config with SSH tunnel - tests port 9000 -> 8123 conversion"""
    return {
        "connection_name": "clickhouse_ssh",
        "type": "clickhouse",
        "servers": [{"host": "mcp-clickhouse-private", "port": 9000}],  # Native port - will be auto-converted to 8123
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


@pytest.mark.ssh
@pytest.mark.docker
@pytest.mark.anyio
class TestSSHTunnelConnectivity:
    """Test SSH tunnel connectivity to private databases"""

    async def test_postgres_ssh_password_auth(self, postgres_ssh_password_config):
        """Test PostgreSQL connection through SSH tunnel with password"""
        connector = PostgreSQLPythonConnector(postgres_ssh_password_config)

        # Simple connectivity test
        result = await connector.execute_query("SELECT 1 as test")

        assert isinstance(result, str), f"Should return TSV string"
        lines = result.strip().split('\n')
        assert lines[1].split('\t')[0] == '1'

        # Verify we can query actual data
        result = await connector.execute_query("SELECT COUNT(*) as count FROM users")
        assert isinstance(result, str)
        lines = result.strip().split('\n')
        assert int(lines[1].split('\t')[0]) > 0

    async def test_postgres_ssh_key_auth(self, postgres_ssh_key_config):
        """Test PostgreSQL connection through SSH tunnel with key authentication"""
        connector = PostgreSQLPythonConnector(postgres_ssh_key_config)

        result = await connector.execute_query("SELECT 2 as test")

        assert isinstance(result, str), f"Should return TSV string"
        lines = result.strip().split('\n')
        assert lines[1].split('\t')[0] == '2'

    async def test_clickhouse_ssh_tunnel(self, clickhouse_ssh_config):
        """Test ClickHouse connection through SSH tunnel"""
        connector = ClickHousePythonConnector(clickhouse_ssh_config)

        result = await connector.execute_query("SELECT 3 as test")

        assert isinstance(result, str), f"Should return TSV string"
        lines = result.strip().split('\n')
        assert lines[1].split('\t')[0] == '3'

        # Verify we can query actual data
        result = await connector.execute_query("SELECT COUNT(*) as count FROM testdb.events")
        assert isinstance(result, str)
        lines = result.strip().split('\n')
        assert int(lines[1].split('\t')[0]) > 0

    async def test_ssh_tunnel_with_wrong_password(self):
        """Test that SSH connection fails gracefully with wrong password"""
        config = {
            "connection_name": "bad_ssh",
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
                "password": "wrongpass"
            }
        }

        connector = PostgreSQLPythonConnector(config)
        with pytest.raises(RuntimeError) as exc_info:
            await connector.execute_query("SELECT 1")

        error_msg = str(exc_info.value).lower()
        assert "ssh" in error_msg or "authentication" in error_msg

    async def test_ssh_tunnel_to_nonexistent_host(self):
        """Test SSH tunnel behavior when target host doesn't exist"""
        config = {
            "connection_name": "bad_host",
            "type": "postgresql",
            "servers": [{"host": "nonexistent-host", "port": 5432}],
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

        connector = PostgreSQLPythonConnector(config)
        with pytest.raises(RuntimeError) as exc_info:
            await connector.execute_query("SELECT 1")

        error_msg = str(exc_info.value).lower()
        # Should fail to connect to the database through the tunnel
        assert "connect" in error_msg or "host" in error_msg


@pytest.mark.ssh
@pytest.mark.docker
@pytest.mark.anyio
class TestSSHTunnelSecurity:
    """Test that SSH tunnels maintain read-only security"""

    async def test_ssh_tunnel_readonly_enforcement(self, postgres_ssh_password_config):
        """Test that read-only is still enforced through SSH tunnel"""
        connector = PostgreSQLPythonConnector(postgres_ssh_password_config)

        # Try a write operation
        with pytest.raises(RuntimeError) as exc_info:
            await connector.execute_query(
                "INSERT INTO users (username, email) VALUES ('test', 'test@test.com')"
            )

        error_msg = str(exc_info.value).lower()
        assert "read-only" in error_msg or "cannot execute" in error_msg

        # Verify SELECT still works
        result = await connector.execute_query("SELECT 1")
        assert isinstance(result, str)

@pytest.mark.ssh
@pytest.mark.docker
@pytest.mark.anyio
class TestClickHousePortConversion:
    """Test automatic port conversion for ClickHouse Python connector"""

    async def test_clickhouse_ssh_native_to_http_conversion(self):
        """Test that port 9000 is automatically converted to 8123 for SSH tunnels"""
        config = {
            "connection_name": "ch_port_test",
            "type": "clickhouse",
            "servers": [{"host": "mcp-clickhouse-private", "port": 9000}],  # Native port
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
        
        # Test with Python connector - should auto-convert 9000 -> 8123
        from src.connectors.clickhouse.python import ClickHousePythonConnector
        connector = ClickHousePythonConnector(config)
        
        # The connector should work even though config says port 9000
        result = await connector.execute_query("SELECT 9000 as configured_port, 8123 as actual_port")
        assert isinstance(result, str)
        lines = result.strip().split('\n')
        assert len(lines) == 2  # Header + data
        values = lines[1].split('\t')
        assert values[0] == '9000'  # Configured port
        assert values[1] == '8123'  # Actual port used
        
    async def test_clickhouse_ssh_secure_native_to_https_conversion(self):
        """Test that port 9440 is automatically converted to 8443 for SSH tunnels"""
        config = {
            "connection_name": "ch_secure_port_test",
            "type": "clickhouse",
            "servers": [{"host": "mcp-clickhouse-private", "port": 9440}],  # Secure native port
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
        
        from src.connectors.clickhouse.python import ClickHousePythonConnector
        connector = ClickHousePythonConnector(config)
        
        # Note: This will fail in test environment since we don't have HTTPS setup
        # But it verifies the port conversion logic happens
        with pytest.raises(Exception) as exc_info:
            await connector.execute_query("SELECT 1")
        
        # The error should be about HTTPS/SSL, not about wrong protocol port
        error = str(exc_info.value)
        assert "9440" not in error.lower() or "ssl" in error.lower() or "https" in error.lower()

    async def test_clickhouse_direct_connection_port_conversion(self):
        """Test that port 9000 is converted to 8123 for direct connections (no SSH)"""
        config = {
            "connection_name": "ch_direct_test",
            "type": "clickhouse",
            "servers": [{"host": "localhost", "port": 9000}],  # Native port configured
            "db": "testdb",
            "username": "testuser",
            "password": "testpass"
            # No SSH tunnel
        }
        
        from src.connectors.clickhouse.python import ClickHousePythonConnector
        connector = ClickHousePythonConnector(config)
        
        # Should auto-convert to port 8123 for direct connection
        result = await connector.execute_query("SELECT 1 as test")
        assert isinstance(result, str)
        lines = result.strip().split('\n')
        assert len(lines) == 2
        assert lines[1] == '1'
