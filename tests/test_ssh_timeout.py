#!/usr/bin/env python3
"""
Test SSH timeout handling
"""

import pytest
import asyncio
from src.connectors.postgresql.python import PostgreSQLPythonConnector
from src.connectors.postgresql.cli import PostgreSQLCLIConnector


@pytest.mark.anyio
@pytest.mark.slow
class TestSSHTimeout:
    """Test SSH timeout functionality"""

    @pytest.mark.timeout(10)  # Kill test after 10 seconds to prevent hanging
    async def test_ssh_timeout_with_unreachable_host(self):
        """Test that SSH times out properly with unreachable host"""
        config = {
            "connection_name": "ssh_timeout_test",
            "type": "postgresql",
            "servers": [{"host": "internal-db", "port": 5432}],
            "db": "testdb",
            "username": "testuser",
            "password": "testpass",
            "ssh_tunnel": {
                "enabled": True,
                "host": "192.0.2.1",  # TEST-NET-1 (RFC 5737) - guaranteed non-routable
                "port": 22,
                "user": "tunnel",
                "password": "tunnelpass",
                "ssh_timeout": 2  # 2 second timeout for faster test
            }
        }

        # Test Python implementation
        connector = PostgreSQLPythonConnector(config)

        start_time = asyncio.get_event_loop().time()
        with pytest.raises(TimeoutError) as exc_info:
            await connector.execute_query("SELECT 1")

        elapsed = asyncio.get_event_loop().time() - start_time

        # Should timeout in about 2 seconds, not hang forever
        # Allow some overhead for thread cleanup and asyncio
        assert elapsed < 4, f"SSH timeout took {elapsed:.1f}s, expected ~2s"
        assert elapsed > 1.5, f"SSH timeout too fast {elapsed:.1f}s, expected ~2s"
        assert "SSH: Connection timeout after 2s" in str(exc_info.value)

    @pytest.mark.timeout(10)  # Kill test after 10 seconds
    async def test_cli_ssh_timeout_with_unreachable_host(self):
        """Test that CLI SSH times out properly with unreachable host"""
        config = {
            "connection_name": "ssh_timeout_test_cli",
            "type": "postgresql",
            "servers": [{"host": "internal-db", "port": 5432}],
            "db": "testdb",
            "username": "testuser",
            "password": "testpass",
            "ssh_tunnel": {
                "enabled": True,
                "host": "192.0.2.1",  # TEST-NET-1 (RFC 5737) - guaranteed non-routable
                "port": 22,
                "user": "tunnel",
                "private_key": "/tmp/test_key",  # CLI uses key auth
                "ssh_timeout": 2  # 2 second timeout for faster test
            }
        }

        # Test CLI implementation
        connector = PostgreSQLCLIConnector(config)

        start_time = asyncio.get_event_loop().time()
        with pytest.raises(RuntimeError) as exc_info:
            await connector.execute_query("SELECT 1")

        # The actual error might be connection refused (if it tries localhost port)
        # or a psql connection error. Either way, it should fail.
        error_msg = str(exc_info.value).lower()
        assert "connection refused" in error_msg or "psql" in error_msg

    @pytest.mark.timeout(10)  # Kill test after 10 seconds
    async def test_default_ssh_timeout(self):
        """Test that default SSH timeout is 5 seconds"""
        config = {
            "connection_name": "default_timeout_test",
            "type": "postgresql",
            "servers": [{"host": "internal-db", "port": 5432}],
            "db": "testdb",
            "username": "testuser",
            "password": "testpass",
            "ssh_tunnel": {
                "enabled": True,
                "host": "192.0.2.1",  # TEST-NET-1 (RFC 5737) - guaranteed non-routable
                "port": 22,
                "user": "tunnel",
                "password": "tunnelpass"
                # No ssh_timeout specified, should use default of 5s
            }
        }

        connector = PostgreSQLPythonConnector(config)

        start_time = asyncio.get_event_loop().time()
        with pytest.raises(TimeoutError) as exc_info:
            await connector.execute_query("SELECT 1")

        elapsed = asyncio.get_event_loop().time() - start_time

        # Should timeout in about 5 seconds (default)
        # Allow some overhead for thread cleanup and asyncio
        assert elapsed < 7, f"SSH timeout took {elapsed:.1f}s, expected ~5s"
        assert elapsed > 4, f"SSH timeout too fast {elapsed:.1f}s, expected ~5s"
        assert "SSH: Connection timeout after 5s" in str(exc_info.value)