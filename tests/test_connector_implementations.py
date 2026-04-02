"""
Test that the default implementation is CLI when not specified.
"""

import tempfile
import yaml
import os
from mcp_read_only_sql.config.parser import ConfigParser
from mcp_read_only_sql.server import ReadOnlySQLServer
from mcp_read_only_sql.runtime_paths import resolve_runtime_paths


def test_parser_default_implementation():
    """Test that ConfigParser defaults to CLI implementation"""
    test_config = [
        {
            "connection_name": "test_conn",
            "type": "postgresql",
            "servers": ["localhost:5432"],
            "username": "testuser",
        }
    ]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(test_config, f)
        temp_file = f.name

    try:
        parser = ConfigParser(temp_file)
        loaded = parser.load_config()

        assert (
            loaded[0].get("implementation") == "cli"
        ), "Default implementation should be 'cli'"
    finally:
        os.unlink(temp_file)


def test_server_default_implementation(tmp_path):
    """Test that server defaults to CLI implementation when not specified in config"""
    test_config = [
        {
            "connection_name": "test_postgres",
            "type": "postgresql",
            "servers": ["localhost:5432"],
            "username": "testuser",
            "password": "testpass",
            "db": "testdb",
            # Note: implementation not specified - should default to CLI
        }
    ]

    config_file = tmp_path / "connections.yaml"
    config_file.write_text(yaml.dump(test_config))
    runtime_paths = resolve_runtime_paths(
        config_dir=tmp_path,
        state_dir=tmp_path / "state",
        cache_dir=tmp_path / "cache",
    )
    runtime_paths.ensure_directories()

    server = ReadOnlySQLServer(runtime_paths)

    # Check the loaded connection uses CLI implementation
    conn = server.connections.get("test_postgres")
    assert conn is not None, "Connection should be loaded"

    # Check it's a CLI connector
    from mcp_read_only_sql.connectors.postgresql.cli import PostgreSQLCLIConnector

    assert isinstance(
        conn, PostgreSQLCLIConnector
    ), "Should use CLI connector by default"


def test_explicit_python_implementation(tmp_path):
    """Test that explicitly specifying Python implementation still works"""
    test_config = [
        {
            "connection_name": "test_postgres_python",
            "type": "postgresql",
            "servers": ["localhost:5432"],
            "username": "testuser",
            "password": "testpass",
            "db": "testdb",
            "implementation": "python",  # Explicitly specify Python
        }
    ]

    config_file = tmp_path / "connections.yaml"
    config_file.write_text(yaml.dump(test_config))
    runtime_paths = resolve_runtime_paths(
        config_dir=tmp_path,
        state_dir=tmp_path / "state",
        cache_dir=tmp_path / "cache",
    )
    runtime_paths.ensure_directories()

    server = ReadOnlySQLServer(runtime_paths)
    conn = server.connections.get("test_postgres_python")
    assert conn is not None, "Connection should be loaded"

    # Check it's a Python connector
    from mcp_read_only_sql.connectors.postgresql.python import PostgreSQLPythonConnector

    assert isinstance(
        conn, PostgreSQLPythonConnector
    ), "Should use Python connector when explicitly specified"


def test_explicit_cli_implementation(tmp_path):
    """Test that explicitly specifying CLI implementation works"""
    test_config = [
        {
            "connection_name": "test_clickhouse_cli",
            "type": "clickhouse",
            "servers": ["localhost:9000"],
            "username": "testuser",
            "password": "testpass",
            "db": "testdb",
            "implementation": "cli",  # Explicitly specify CLI
        }
    ]

    config_file = tmp_path / "connections.yaml"
    config_file.write_text(yaml.dump(test_config))
    runtime_paths = resolve_runtime_paths(
        config_dir=tmp_path,
        state_dir=tmp_path / "state",
        cache_dir=tmp_path / "cache",
    )
    runtime_paths.ensure_directories()

    server = ReadOnlySQLServer(runtime_paths)
    conn = server.connections.get("test_clickhouse_cli")
    assert conn is not None, "Connection should be loaded"

    # Check it's a CLI connector
    from mcp_read_only_sql.connectors.clickhouse.cli import ClickHouseCLIConnector

    assert isinstance(
        conn, ClickHouseCLIConnector
    ), "Should use CLI connector when explicitly specified"
