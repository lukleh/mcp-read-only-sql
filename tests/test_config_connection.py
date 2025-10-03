#!/usr/bin/env python3
"""
Tests for Connection configuration classes
"""

import pytest
import tempfile
import os
from src.config import Connection, Server, SSHTunnelConfig, load_connections


class TestServer:
    """Test Server dataclass"""

    def test_server_from_dict(self):
        """Test creating Server from dict"""
        server = Server.from_dict({"host": "localhost", "port": 5432})
        assert server.host == "localhost"
        assert server.port == 5432

    def test_server_from_string_with_port(self):
        """Test creating Server from 'host:port' string"""
        server = Server.from_dict("localhost:5432", "postgresql", "cli")
        assert server.host == "localhost"
        assert server.port == 5432

    def test_server_from_string_without_port_postgresql(self):
        """Test creating Server from 'host' string for PostgreSQL"""
        server = Server.from_dict("localhost", "postgresql", "cli")
        assert server.host == "localhost"
        assert server.port == 5432

    def test_server_from_string_without_port_clickhouse_cli(self):
        """Test creating Server from 'host' string for ClickHouse CLI"""
        server = Server.from_dict("localhost", "clickhouse", "cli")
        assert server.host == "localhost"
        assert server.port == 9000

    def test_server_from_string_without_port_clickhouse_python(self):
        """Test creating Server from 'host' string for ClickHouse Python"""
        server = Server.from_dict("localhost", "clickhouse", "python")
        assert server.host == "localhost"
        assert server.port == 8123

    def test_server_from_dict_missing_host(self):
        """Test Server validation catches missing host"""
        with pytest.raises(ValueError, match="missing required field 'host'"):
            Server.from_dict({"port": 5432})

    def test_server_from_dict_missing_port(self):
        """Test Server validation catches missing port"""
        with pytest.raises(ValueError, match="missing required field 'port'"):
            Server.from_dict({"host": "localhost"})


class TestSSHTunnelConfig:
    """Test SSHTunnelConfig dataclass"""

    def test_ssh_tunnel_with_private_key(self):
        """Test SSH tunnel config with private key"""
        config = SSHTunnelConfig.from_dict({
            "host": "bastion.example.com",
            "port": 22,
            "user": "tunneluser",
            "private_key": "~/.ssh/id_rsa"
        })
        assert config.host == "bastion.example.com"
        assert config.port == 22
        assert config.user == "tunneluser"
        assert config.private_key is not None
        assert config.password is None

    def test_ssh_tunnel_with_password(self):
        """Test SSH tunnel config with password"""
        config = SSHTunnelConfig.from_dict({
            "host": "bastion.example.com",
            "user": "tunneluser",
            "password": "secret"
        })
        assert config.password == "secret"
        assert config.private_key is None

    def test_ssh_tunnel_missing_host(self):
        """Test SSH tunnel validation catches missing host"""
        with pytest.raises(ValueError, match="missing required field 'host'"):
            SSHTunnelConfig.from_dict({
                "user": "tunneluser",
                "private_key": "~/.ssh/id_rsa"
            })

    def test_ssh_tunnel_missing_user(self):
        """Test SSH tunnel validation catches missing user"""
        with pytest.raises(ValueError, match="missing required field 'user'"):
            SSHTunnelConfig.from_dict({
                "host": "bastion.example.com",
                "private_key": "~/.ssh/id_rsa"
            })

    def test_ssh_tunnel_no_auth_method(self):
        """Test SSH tunnel validation catches missing auth"""
        with pytest.raises(ValueError, match="requires either 'private_key' or 'password'"):
            SSHTunnelConfig.from_dict({
                "host": "bastion.example.com",
                "user": "tunneluser"
            })

    def test_ssh_tunnel_disabled(self):
        """Test SSH tunnel returns None when disabled"""
        config = SSHTunnelConfig.from_dict({
            "enabled": False,
            "host": "bastion.example.com",
            "user": "tunneluser",
            "private_key": "~/.ssh/id_rsa"
        })
        assert config is None


class TestConnection:
    """Test Connection class"""

    def test_connection_minimal_valid(self):
        """Test creating minimal valid connection"""
        env = {"DB_PASSWORD_TEST": "testpass"}
        conn = Connection({
            "connection_name": "test",
            "type": "postgresql",
            "servers": [{"host": "localhost", "port": 5432}],
            "db": "testdb",
            "username": "testuser"
        }, env=env)

        assert conn.name == "test"
        assert conn.db_type == "postgresql"
        assert len(conn.servers) == 1
        assert conn.servers[0].host == "localhost"
        assert conn.servers[0].port == 5432
        assert conn.database == "testdb"
        assert conn.username == "testuser"
        assert conn.password == "testpass"
        assert conn.implementation == "cli"  # default
        assert conn.ssh_tunnel is None

    def test_connection_with_ssh_tunnel(self):
        """Test connection with SSH tunnel"""
        env = {"DB_PASSWORD_TEST": "testpass"}
        conn = Connection({
            "connection_name": "test",
            "type": "postgresql",
            "servers": [{"host": "db.internal", "port": 5432}],
            "db": "testdb",
            "username": "testuser",
            "ssh_tunnel": {
                "host": "bastion.example.com",
                "user": "tunneluser",
                "private_key": "~/.ssh/id_rsa"
            }
        }, env=env)

        assert conn.ssh_tunnel is not None
        assert conn.ssh_tunnel.host == "bastion.example.com"
        assert conn.ssh_tunnel.user == "tunneluser"

    def test_connection_missing_name(self):
        """Test connection validation catches missing name"""
        with pytest.raises(ValueError, match="missing required field 'connection_name'"):
            Connection({
                "type": "postgresql",
                "servers": [{"host": "localhost", "port": 5432}],
                "db": "testdb",
                "username": "testuser",
                "password": "testpass"
            })

    def test_connection_missing_type(self):
        """Test connection validation catches missing type"""
        with pytest.raises(ValueError, match="missing required field 'type'"):
            Connection({
                "connection_name": "test",
                "servers": [{"host": "localhost", "port": 5432}],
                "db": "testdb",
                "username": "testuser",
                "password": "testpass"
            })

    def test_connection_invalid_type(self):
        """Test connection validation catches invalid type"""
        with pytest.raises(ValueError, match="Invalid database type"):
            Connection({
                "connection_name": "test",
                "type": "mysql",  # Not supported
                "servers": [{"host": "localhost", "port": 3306}],
                "db": "testdb",
                "username": "testuser",
                "password": "testpass"
            })

    def test_connection_missing_servers(self):
        """Test connection validation catches missing servers"""
        with pytest.raises(ValueError, match="missing required field 'servers'"):
            Connection({
                "connection_name": "test",
                "type": "postgresql",
                "db": "testdb",
                "username": "testuser",
                "password": "testpass"
            })

    def test_connection_empty_servers(self):
        """Test connection validation catches empty servers list"""
        with pytest.raises(ValueError, match="must be non-empty list"):
            Connection({
                "connection_name": "test",
                "type": "postgresql",
                "servers": [],
                "db": "testdb",
                "username": "testuser",
                "password": "testpass"
            })

    def test_connection_missing_db(self):
        """Test connection validation catches missing db"""
        with pytest.raises(ValueError, match="missing required field 'db'"):
            Connection({
                "connection_name": "test",
                "type": "postgresql",
                "servers": [{"host": "localhost", "port": 5432}],
                "username": "testuser",
                "password": "testpass"
            })

    def test_connection_missing_username(self):
        """Test connection validation catches missing username"""
        with pytest.raises(ValueError, match="missing required field 'username'"):
            Connection({
                "connection_name": "test",
                "type": "postgresql",
                "servers": [{"host": "localhost", "port": 5432}],
                "db": "testdb",
                "password": "testpass"
            })

    def test_connection_password_from_env_auto(self):
        """Test connection auto-loads password from environment"""
        env = {"DB_PASSWORD_MY_TEST_CONNECTION": "secret123"}
        conn = Connection({
            "connection_name": "my-test-connection",
            "type": "postgresql",
            "servers": [{"host": "localhost", "port": 5432}],
            "db": "testdb",
            "username": "testuser"
        }, env=env)

        assert conn.password == "secret123"

    def test_connection_password_explicit(self):
        """Test connection with explicit password"""
        conn = Connection({
            "connection_name": "test",
            "type": "postgresql",
            "servers": [{"host": "localhost", "port": 5432}],
            "db": "testdb",
            "username": "testuser",
            "password": "explicit_pass"
        })

        assert conn.password == "explicit_pass"

    def test_connection_password_env_var(self):
        """Test connection with password_env"""
        env = {"MY_CUSTOM_PASSWORD": "custom_pass"}
        conn = Connection({
            "connection_name": "test",
            "type": "postgresql",
            "servers": [{"host": "localhost", "port": 5432}],
            "db": "testdb",
            "username": "testuser",
            "password_env": "MY_CUSTOM_PASSWORD"
        }, env=env)

        assert conn.password == "custom_pass"

    def test_connection_password_missing_env_var(self):
        """Test connection fails when password_env not found"""
        with pytest.raises(ValueError, match="Password environment variable 'MISSING_VAR' not found"):
            Connection({
                "connection_name": "test",
                "type": "postgresql",
                "servers": [{"host": "localhost", "port": 5432}],
                "db": "testdb",
                "username": "testuser",
                "password_env": "MISSING_VAR"
            }, env={})

    def test_connection_empty_password_allowed(self):
        """Test connection allows empty password for compatibility"""
        conn = Connection({
            "connection_name": "test",
            "type": "postgresql",
            "servers": [{"host": "localhost", "port": 5432}],
            "db": "testdb",
            "username": "testuser"
        }, env={})

        assert conn.password == ""

    def test_connection_ssh_password_env_fallback(self):
        """SSH password falls back to SSH_PASSWORD_<NAME> when no key is provided"""
        env = {
            "DB_PASSWORD_TEST": "dbpass",
            "SSH_PASSWORD_TEST": "sshpass",
        }

        conn = Connection({
            "connection_name": "test",
            "type": "postgresql",
            "servers": [{"host": "localhost", "port": 5432}],
            "db": "testdb",
            "username": "testuser",
            "ssh_tunnel": {
                "host": "bastion.example.com",
                "user": "tunneluser",
                # No password/key fields provided so fallback should trigger
            },
        }, env=env)

        assert conn.ssh_tunnel is not None
        assert conn.ssh_tunnel.password == "sshpass"
        assert conn.ssh_tunnel.private_key is None

    def test_connection_string_servers(self):
        """Test connection parses string servers"""
        env = {"DB_PASSWORD_TEST": "pass"}
        conn = Connection({
            "connection_name": "test",
            "type": "postgresql",
            "servers": ["localhost:5432", "backup:5433"],
            "db": "testdb",
            "username": "testuser"
        }, env=env)

        assert len(conn.servers) == 2
        assert conn.servers[0].host == "localhost"
        assert conn.servers[0].port == 5432
        assert conn.servers[1].host == "backup"
        assert conn.servers[1].port == 5433


class TestLoadConnections:
    """Test load_connections function"""

    def test_load_connections_valid_yaml(self):
        """Test loading valid connections from YAML"""
        yaml_content = """
- connection_name: test1
  type: postgresql
  servers:
    - localhost:5432
  db: testdb
  username: testuser
  password: testpass

- connection_name: test2
  type: clickhouse
  servers:
    - localhost:8123
  db: default
  username: clickuser
  password: clickpass
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            connections = load_connections(temp_path)
            assert len(connections) == 2
            assert "test1" in connections
            assert "test2" in connections
            assert connections["test1"].db_type == "postgresql"
            assert connections["test2"].db_type == "clickhouse"
        finally:
            os.unlink(temp_path)

    def test_load_connections_duplicate_names(self):
        """Test loader catches duplicate connection names"""
        yaml_content = """
- connection_name: duplicate
  type: postgresql
  servers:
    - localhost:5432
  db: testdb
  username: testuser
  password: testpass

- connection_name: duplicate
  type: clickhouse
  servers:
    - localhost:8123
  db: default
  username: clickuser
  password: clickpass
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            with pytest.raises(ValueError, match="Duplicate connection name: 'duplicate'"):
                load_connections(temp_path)
        finally:
            os.unlink(temp_path)

    def test_load_connections_file_not_found(self):
        """Test loader handles missing file"""
        with pytest.raises(FileNotFoundError):
            load_connections("/nonexistent/path.yaml")

    def test_load_connections_empty_file(self):
        """Test loader handles empty file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("")
            f.flush()
            temp_path = f.name

        try:
            with pytest.raises(ValueError, match="Configuration file is empty"):
                load_connections(temp_path)
        finally:
            os.unlink(temp_path)

    def test_load_connections_invalid_yaml(self):
        """Test loader handles invalid YAML structure"""
        yaml_content = "just_a_string"  # Valid YAML but not a list
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            with pytest.raises(ValueError, match="must contain a list"):
                load_connections(temp_path)
        finally:
            os.unlink(temp_path)

    def test_load_connections_collects_all_errors(self):
        """Test loader collects all validation errors"""
        yaml_content = """
- connection_name: bad1
  type: invalid_type
  servers:
    - localhost:5432
  db: testdb
  username: testuser
  password: testpass

- connection_name: bad2
  type: postgresql
  db: testdb
  username: testuser
  password: testpass
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            with pytest.raises(ValueError) as exc_info:
                load_connections(temp_path)

            error_msg = str(exc_info.value)
            assert "bad1" in error_msg
            assert "bad2" in error_msg
            assert "Invalid database type" in error_msg
            assert "missing required field 'servers'" in error_msg
        finally:
            os.unlink(temp_path)
