"""Tests for server selection parameter feature"""
import pytest
from src.connectors.base import BaseConnector


class MockConnector(BaseConnector):
    """Mock connector for testing base functionality"""

    async def execute_query(self, query: str, database=None, server=None):
        return "mock result"

    def _get_default_port(self) -> int:
        return 5432


class TestServerSelection:
    """Test the server selection parameter functionality"""

    def test_select_server_default(self):
        """Test that _select_server returns first server by default"""
        config = {
            "connection_name": "test",
            "type": "postgresql",
            "servers": [
                {"host": "server1.example.com", "port": 5432},
                {"host": "server2.example.com", "port": 5432},
                {"host": "server3.example.com", "port": 5433},
            ],
            "db": "test_db",
            "username": "test"
        }
        connector = MockConnector(config)
        server = connector._select_server()

        assert server["host"] == "server1.example.com"
        assert server["port"] == 5432

    def test_select_server_by_host_only(self):
        """Test selecting server by host only (matches first with that host)"""
        config = {
            "connection_name": "test",
            "type": "postgresql",
            "servers": [
                {"host": "server1.example.com", "port": 5432},
                {"host": "server2.example.com", "port": 5432},
                {"host": "server2.example.com", "port": 5433},
            ],
            "db": "test_db",
            "username": "test"
        }
        connector = MockConnector(config)
        server = connector._select_server("server2.example.com")

        assert server["host"] == "server2.example.com"
        assert server["port"] == 5432  # First matching host

    def test_select_server_by_host_and_port(self):
        """Test selecting server by exact host:port combination"""
        config = {
            "connection_name": "test",
            "type": "postgresql",
            "servers": [
                {"host": "server1.example.com", "port": 5432},
                {"host": "server2.example.com", "port": 5432},
                {"host": "server2.example.com", "port": 5433},
            ],
            "db": "test_db",
            "username": "test"
        }
        connector = MockConnector(config)
        server = connector._select_server("server2.example.com:5433")

        assert server["host"] == "server2.example.com"
        assert server["port"] == 5433

    def test_select_server_not_found_by_host(self):
        """Test that ValueError is raised when server host not found"""
        config = {
            "connection_name": "test_conn",
            "type": "postgresql",
            "servers": [
                {"host": "server1.example.com", "port": 5432},
                {"host": "server2.example.com", "port": 5432},
            ],
            "db": "test_db",
            "username": "test"
        }
        connector = MockConnector(config)

        with pytest.raises(ValueError) as exc_info:
            connector._select_server("nonexistent.example.com")

        error_msg = str(exc_info.value)
        assert "Server 'nonexistent.example.com' not found" in error_msg
        assert "test_conn" in error_msg
        assert "server1.example.com:5432" in error_msg
        assert "server2.example.com:5432" in error_msg

    def test_select_server_not_found_by_port(self):
        """Test that ValueError is raised when host matches but port doesn't"""
        config = {
            "connection_name": "test_conn",
            "type": "postgresql",
            "servers": [
                {"host": "server1.example.com", "port": 5432},
                {"host": "server2.example.com", "port": 5432},
            ],
            "db": "test_db",
            "username": "test"
        }
        connector = MockConnector(config)

        with pytest.raises(ValueError) as exc_info:
            connector._select_server("server1.example.com:9999")

        error_msg = str(exc_info.value)
        assert "Server 'server1.example.com:9999' not found" in error_msg

    def test_select_server_invalid_port_format(self):
        """Test that ValueError is raised for invalid port in specification"""
        config = {
            "connection_name": "test",
            "type": "postgresql",
            "servers": [
                {"host": "server1.example.com", "port": 5432},
            ],
            "db": "test_db",
            "username": "test"
        }
        connector = MockConnector(config)

        with pytest.raises(ValueError) as exc_info:
            connector._select_server("server1.example.com:invalid")

        assert "Invalid port in server specification" in str(exc_info.value)

    def test_select_server_with_colon_in_host(self):
        """Test handling of multiple colons (rsplit should use rightmost)"""
        config = {
            "connection_name": "test",
            "type": "postgresql",
            "servers": [
                {"host": "2001:db8::1", "port": 5432},
            ],
            "db": "test_db",
            "username": "test"
        }
        connector = MockConnector(config)
        server = connector._select_server("2001:db8::1:5432")

        assert server["host"] == "2001:db8::1"
        assert server["port"] == 5432

    def test_select_server_no_servers_configured(self):
        """Test default localhost behavior when no servers configured"""
        config = {
            "connection_name": "test",
            "type": "postgresql",
            "servers": [],
            "db": "test_db",
            "username": "test"
        }
        connector = MockConnector(config)
        server = connector._select_server()

        assert server["host"] == "localhost"
        assert server["port"] == 5432  # MockConnector's default port

    def test_select_server_none_parameter(self):
        """Test that None parameter explicitly uses default (first server)"""
        config = {
            "connection_name": "test",
            "type": "postgresql",
            "servers": [
                {"host": "server1.example.com", "port": 5432},
                {"host": "server2.example.com", "port": 5432},
            ],
            "db": "test_db",
            "username": "test"
        }
        connector = MockConnector(config)
        server = connector._select_server(None)

        assert server["host"] == "server1.example.com"
        assert server["port"] == 5432

    def test_select_server_multiple_matches_returns_first(self):
        """Test that when multiple servers match host, first is returned"""
        config = {
            "connection_name": "test",
            "type": "postgresql",
            "servers": [
                {"host": "db.example.com", "port": 5432},
                {"host": "db.example.com", "port": 5433},
                {"host": "db.example.com", "port": 5434},
            ],
            "db": "test_db",
            "username": "test"
        }
        connector = MockConnector(config)
        server = connector._select_server("db.example.com")

        assert server["host"] == "db.example.com"
        assert server["port"] == 5432  # First matching


class TestServerSelectionClickHouse:
    """Test server selection with ClickHouse-specific ports"""

    def test_select_clickhouse_http_port(self):
        """Test selecting ClickHouse server with HTTP port"""
        config = {
            "connection_name": "test",
            "type": "clickhouse",
            "servers": [
                {"host": "ch1.example.com", "port": 8123},
                {"host": "ch2.example.com", "port": 9000},
            ],
            "db": "default",
            "username": "test"
        }
        connector = MockConnector(config)
        server = connector._select_server("ch1.example.com:8123")

        assert server["host"] == "ch1.example.com"
        assert server["port"] == 8123

    def test_select_clickhouse_native_port(self):
        """Test selecting ClickHouse server with native port"""
        config = {
            "connection_name": "test",
            "type": "clickhouse",
            "servers": [
                {"host": "ch1.example.com", "port": 8123},
                {"host": "ch2.example.com", "port": 9000},
            ],
            "db": "default",
            "username": "test"
        }
        connector = MockConnector(config)
        server = connector._select_server("ch2.example.com:9000")

        assert server["host"] == "ch2.example.com"
        assert server["port"] == 9000
