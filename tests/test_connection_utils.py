import pytest
from src.utils.connection_utils import get_connection_target


class TestGetConnectionTarget:
    """Test the get_connection_target function"""

    def test_direct_connection_postgresql(self):
        """Test direct PostgreSQL connection without SSH"""
        config = {
            "type": "postgresql",
            "db": "mydb",
            "servers": [{"host": "db.example.com", "port": 5432}]
        }

        result = get_connection_target(config)

        assert result["host"] == "db.example.com"
        assert result["port"] == 5432
        assert result["database"] == "mydb"
        assert result["connection_type"] == "direct"

    def test_direct_connection_clickhouse(self):
        """Test direct ClickHouse connection without SSH"""
        config = {
            "type": "clickhouse",
            "db": "default",
            "servers": [{"host": "ch.example.com", "port": 8123}]
        }

        result = get_connection_target(config)

        assert result["host"] == "ch.example.com"
        assert result["port"] == 8123
        assert result["database"] == "default"
        assert result["connection_type"] == "direct"

    def test_ssh_tunnel_to_localhost(self):
        """Test SSH tunnel where DB is on the SSH host itself"""
        config = {
            "type": "postgresql",
            "db": "mydb",
            "servers": [{"host": "localhost", "port": 5432}],
            "ssh_tunnel": {"host": "ssh.example.com"}
        }

        result = get_connection_target(config)

        # Should use SSH host as the actual DB location
        assert result["host"] == "ssh.example.com"
        assert result["port"] == 5432
        assert result["database"] == "mydb"
        assert result["connection_type"] == "ssh_local"

    def test_ssh_tunnel_to_127_0_0_1(self):
        """Test SSH tunnel where DB is on 127.0.0.1 (same as localhost)"""
        config = {
            "type": "clickhouse",
            "db": "default",
            "servers": [{"host": "127.0.0.1", "port": 8123}],
            "ssh_tunnel": {"host": "ssh.example.com"}
        }

        result = get_connection_target(config)

        # Should use SSH host as the actual DB location
        assert result["host"] == "ssh.example.com"
        assert result["port"] == 8123
        assert result["database"] == "default"
        assert result["connection_type"] == "ssh_local"

    def test_ssh_tunnel_as_jump(self):
        """Test SSH tunnel used as jump server to reach remote DB"""
        config = {
            "type": "postgresql",
            "db": "mydb",
            "servers": [{"host": "internal-db.local", "port": 5432}],
            "ssh_tunnel": {"host": "jump.example.com"}
        }

        result = get_connection_target(config)

        # Should use the actual DB host (SSH is just a jump)
        assert result["host"] == "internal-db.local"
        assert result["port"] == 5432
        assert result["database"] == "mydb"
        assert result["connection_type"] == "ssh_jump"
        assert result["ssh_host"] == "jump.example.com"

    def test_server_as_string_with_port(self):
        """Test server specified as 'host:port' string"""
        config = {
            "type": "postgresql",
            "db": "mydb",
            "servers": ["db.example.com:5433"]
        }

        result = get_connection_target(config)

        assert result["host"] == "db.example.com"
        assert result["port"] == 5433
        assert result["database"] == "mydb"
        assert result["connection_type"] == "direct"

    def test_server_as_string_without_port(self):
        """Test server specified as string without port (uses default)"""
        config = {
            "type": "postgresql",
            "db": "mydb",
            "servers": ["db.example.com"]
        }

        result = get_connection_target(config)

        assert result["host"] == "db.example.com"
        assert result["port"] == 5432  # Default PostgreSQL port
        assert result["database"] == "mydb"
        assert result["connection_type"] == "direct"

    def test_clickhouse_default_port(self):
        """Test ClickHouse uses correct default port"""
        config = {
            "type": "clickhouse",
            "db": "default",
            "servers": ["ch.example.com"]
        }

        result = get_connection_target(config)

        assert result["host"] == "ch.example.com"
        assert result["port"] == 8123  # Default ClickHouse port
        assert result["database"] == "default"
        assert result["connection_type"] == "direct"

    def test_no_servers_postgresql(self):
        """Test PostgreSQL with no servers specified (uses localhost)"""
        config = {
            "type": "postgresql",
            "db": "mydb"
        }

        result = get_connection_target(config)

        assert result["host"] == "localhost"
        assert result["port"] == 5432
        assert result["database"] == "mydb"
        assert result["connection_type"] == "direct"

    def test_no_servers_clickhouse(self):
        """Test ClickHouse with no servers specified (uses localhost)"""
        config = {
            "type": "clickhouse",
            "db": "default"
        }

        result = get_connection_target(config)

        assert result["host"] == "localhost"
        assert result["port"] == 8123
        assert result["database"] == "default"
        assert result["connection_type"] == "direct"

    def test_empty_database(self):
        """Test with empty database name"""
        config = {
            "type": "postgresql",
            "servers": [{"host": "db.example.com", "port": 5432}]
        }

        result = get_connection_target(config)

        assert result["host"] == "db.example.com"
        assert result["port"] == 5432
        assert result["database"] == ""
        assert result["connection_type"] == "direct"

    def test_ssh_tunnel_string_server_localhost(self):
        """Test SSH tunnel with server as string 'localhost:port'"""
        config = {
            "type": "postgresql",
            "db": "mydb",
            "servers": ["localhost:5432"],
            "ssh_tunnel": {"host": "ssh.example.com"}
        }

        result = get_connection_target(config)

        # Should use SSH host as the actual DB location
        assert result["host"] == "ssh.example.com"
        assert result["port"] == 5432
        assert result["database"] == "mydb"
        assert result["connection_type"] == "ssh_local"

    def test_ssh_tunnel_string_server_remote(self):
        """Test SSH tunnel with server as string 'remote:port'"""
        config = {
            "type": "clickhouse",
            "db": "default",
            "servers": ["internal.local:8124"],
            "ssh_tunnel": {"host": "jump.example.com"}
        }

        result = get_connection_target(config)

        # Should use the actual DB host (SSH is just a jump)
        assert result["host"] == "internal.local"
        assert result["port"] == 8124
        assert result["database"] == "default"
        assert result["connection_type"] == "ssh_jump"
        assert result["ssh_host"] == "jump.example.com"

    def test_real_world_staging(self):
        """Test real-world example: staging connection"""
        config = {
            "connection_name": "staging",
            "description": "postgresql on staging-worker-7.example.com/postgres (imported from DBeaver: STAGING)",
            "type": "postgresql",
            "servers": ["localhost:5432"],
            "db": "postgres",
            "ssh_tunnel": {
                "host": "staging-worker-7.example.com",
                "user": "lukas",
                "port": 22,
                "private_key": "/Users/lukas/.ssh/vault/id_rsa_ab-data-team"
            },
            "implementation": "cli"
        }

        result = get_connection_target(config)

        # DB is on localhost via SSH, so use SSH host
        assert result["host"] == "staging-worker-7.example.com"
        assert result["port"] == 5432
        assert result["database"] == "postgres"
        assert result["connection_type"] == "ssh_local"

    def test_real_world_tracker_db(self):
        """Test real-world example: tracker DB with jump server"""
        config = {
            "connection_name": "prod_esh-tracker-db-1",
            "type": "postgresql",
            "servers": ["tracker-db-1.example.com:8433"],
            "db": "adjust",
            "ssh_tunnel": {
                "host": "jump-1.example.com",
                "user": "lukas",
                "private_key": "/Users/lukas/.ssh/vault/id_rsa_ab-data-team"
            },
            "implementation": "cli"
        }

        result = get_connection_target(config)

        # DB is remote, SSH is just a jump
        assert result["host"] == "tracker-db-1.example.com"
        assert result["port"] == 8433
        assert result["database"] == "adjust"
        assert result["connection_type"] == "ssh_jump"
        assert result["ssh_host"] == "jump-1.example.com"
