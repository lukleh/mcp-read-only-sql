"""
Test that the default implementation is CLI when not specified.
"""

import pytest
import tempfile
import yaml
import os
from src.config.parser import ConfigParser
from src.server import ReadOnlySQLServer


def test_parser_default_implementation():
    """Test that ConfigParser defaults to CLI implementation"""
    test_config = [{
        "connection_name": "test_conn",
        "type": "postgresql",
        "servers": ["localhost:5432"],
        "username": "testuser"
    }]

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(test_config, f)
        temp_file = f.name

    try:
        parser = ConfigParser(temp_file)
        loaded = parser.load_config()

        assert loaded[0].get('implementation') == 'cli', "Default implementation should be 'cli'"
    finally:
        os.unlink(temp_file)


def test_server_default_implementation():
    """Test that server defaults to CLI implementation when not specified in config"""
    test_config = [{
        "connection_name": "test_postgres",
        "type": "postgresql",
        "servers": ["localhost:5432"],
        "username": "testuser",
        "password": "testpass",
        "db": "testdb"
        # Note: implementation not specified - should default to CLI
    }]

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(test_config, f)
        temp_file = f.name

    try:
        # Create server with the config
        server = ReadOnlySQLServer(temp_file)

        # Check the loaded connection uses CLI implementation
        conn = server.connections.get("test_postgres")
        assert conn is not None, "Connection should be loaded"

        # Check it's a CLI connector
        from src.connectors.postgresql.cli import PostgreSQLCLIConnector
        assert isinstance(conn, PostgreSQLCLIConnector), "Should use CLI connector by default"
    finally:
        os.unlink(temp_file)


def test_explicit_python_implementation():
    """Test that explicitly specifying Python implementation still works"""
    test_config = [{
        "connection_name": "test_postgres_python",
        "type": "postgresql",
        "servers": ["localhost:5432"],
        "username": "testuser",
        "password": "testpass",
        "db": "testdb",
        "implementation": "python"  # Explicitly specify Python
    }]

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(test_config, f)
        temp_file = f.name

    try:
        server = ReadOnlySQLServer(temp_file)
        conn = server.connections.get("test_postgres_python")
        assert conn is not None, "Connection should be loaded"

        # Check it's a Python connector
        from src.connectors.postgresql.python import PostgreSQLPythonConnector
        assert isinstance(conn, PostgreSQLPythonConnector), "Should use Python connector when explicitly specified"
    finally:
        os.unlink(temp_file)


def test_explicit_cli_implementation():
    """Test that explicitly specifying CLI implementation works"""
    test_config = [{
        "connection_name": "test_clickhouse_cli",
        "type": "clickhouse",
        "servers": ["localhost:9000"],
        "username": "testuser",
        "password": "testpass",
        "db": "testdb",
        "implementation": "cli"  # Explicitly specify CLI
    }]

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(test_config, f)
        temp_file = f.name

    try:
        server = ReadOnlySQLServer(temp_file)
        conn = server.connections.get("test_clickhouse_cli")
        assert conn is not None, "Connection should be loaded"

        # Check it's a CLI connector
        from src.connectors.clickhouse.cli import ClickHouseCLIConnector
        assert isinstance(conn, ClickHouseCLIConnector), "Should use CLI connector when explicitly specified"
    finally:
        os.unlink(temp_file)