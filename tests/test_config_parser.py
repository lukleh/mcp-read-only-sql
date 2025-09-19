import os
import tempfile
import yaml
import pytest
from pathlib import Path

from src.config.parser import ConfigParser


def test_load_empty_config():
    """Test loading empty or non-existent config"""
    parser = ConfigParser("non_existent.yaml")
    config = parser.load_config()
    assert config == []


def test_process_connection():
    """Test processing a single connection"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        config_data = [
            {
                "connection_name": "test_db",
                "type": "postgresql",
                "servers": ["localhost:5432"],
                "db": "testdb",
                "username": "testuser"
            }
        ]
        yaml.dump(config_data, f)
        temp_path = f.name

    try:
        parser = ConfigParser(temp_path)
        config = parser.load_config()

        assert len(config) == 1
        conn = config[0]
        assert conn["connection_name"] == "test_db"
        assert conn["type"] == "postgresql"
        assert conn["implementation"] == "cli"  # Default
        assert len(conn["servers"]) == 1
        assert conn["servers"][0]["host"] == "localhost"
        assert conn["servers"][0]["port"] == 5432
    finally:
        os.unlink(temp_path)


def test_password_from_environment():
    """Test password loading from environment"""
    os.environ["DB_PASSWORD_TEST_CONN"] = "secret123"

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        config_data = [
            {
                "connection_name": "test_conn",
                "type": "postgresql",
                "servers": ["localhost"],
                "username": "user"
            }
        ]
        yaml.dump(config_data, f)
        temp_path = f.name

    try:
        parser = ConfigParser(temp_path)
        config = parser.load_config()

        assert config[0]["password"] == "secret123"
    finally:
        os.unlink(temp_path)
        del os.environ["DB_PASSWORD_TEST_CONN"]


def test_ssh_tunnel_config():
    """Test SSH tunnel configuration processing"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        config_data = [
            {
                "connection_name": "remote_db",
                "type": "postgresql",
                "servers": ["localhost:5432"],
                "username": "user",
                "ssh_tunnel": {
                    "host": "bastion.example.com",
                    "user": "tunnel_user",
                    "private_key": "~/ssh/key"
                }
            }
        ]
        yaml.dump(config_data, f)
        temp_path = f.name

    try:
        parser = ConfigParser(temp_path)
        config = parser.load_config()

        ssh_config = config[0]["ssh_tunnel"]
        assert ssh_config["host"] == "bastion.example.com"
        assert ssh_config["user"] == "tunnel_user"
        assert ssh_config["private_key"] == os.path.expanduser("~/ssh/key")
    finally:
        os.unlink(temp_path)


def test_multiple_servers():
    """Test multiple server configuration"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        config_data = [
            {
                "connection_name": "cluster",
                "type": "clickhouse",
                "servers": [
                    "ch1.example.com:8123",
                    "ch2.example.com:8124",
                    "ch3.example.com"  # Should use default port
                ],
                "username": "reader"
            }
        ]
        yaml.dump(config_data, f)
        temp_path = f.name

    try:
        parser = ConfigParser(temp_path)
        config = parser.load_config()

        servers = config[0]["servers"]
        assert len(servers) == 3
        assert servers[0] == {"host": "ch1.example.com", "port": 8123}
        assert servers[1] == {"host": "ch2.example.com", "port": 8124}
        assert servers[2] == {"host": "ch3.example.com", "port": 9000}  # Default ClickHouse CLI port
    finally:
        os.unlink(temp_path)
