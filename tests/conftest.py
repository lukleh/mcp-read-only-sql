#!/usr/bin/env python3
"""
Shared test fixtures for MCP SQL Server tests
Uses the minimal_client.py pattern for real MCP protocol testing
"""

import json
import os
import subprocess
from typing import Any, Dict

import pytest
import anyio
from anyio import create_task_group
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from src.config import Connection
from src.connectors.base import BaseConnector


# Helper function to create Connection objects from dict configs
def make_connection(config_dict: Dict[str, Any]) -> Connection:
    """
    Helper to create Connection objects from dict configs for tests.
    This allows existing test fixtures that return dicts to work with the new Connection class.

    Args:
        config_dict: Dictionary configuration (old format)

    Returns:
        Connection object
    """
    return Connection(config_dict)


class RecordingConnector(BaseConnector):
    """In-memory connector that records selected servers for assertions."""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self.last_selected = None

    async def execute_query(self, query: str, database=None, server=None) -> str:
        selected = self._select_server(server)
        self.last_selected = selected
        return "ok"


def make_recording_connector(config_dict: Dict[str, Any]) -> RecordingConnector:
    """Convenience helper returning a RecordingConnector from a raw dict config."""
    return RecordingConnector(make_connection(config_dict))


# Common test config fixtures - return Connection objects
@pytest.fixture
def postgres_config():
    """PostgreSQL test configuration as Connection object"""
    return make_connection({
        "connection_name": "test_postgres",
        "type": "postgresql",
        "servers": [{"host": "localhost", "port": 5432}],
        "username": "testuser",
        "password": "testpass",
        "db": "testdb"
    })


@pytest.fixture
def clickhouse_config():
    """ClickHouse test configuration as Connection object"""
    return make_connection({
        "connection_name": "test_clickhouse",
        "type": "clickhouse",
        "servers": [{"host": "localhost", "port": 9000}],
        "username": "testuser",
        "password": "testpass",
        "db": "testdb"
    })


# Configure anyio to use asyncio backend only
@pytest.fixture(scope="session")
def anyio_backend():
    """Force anyio to use asyncio backend only."""
    return "asyncio"


@pytest.fixture
def test_config_file(tmp_path):
    """Create a test configuration file"""
    config_content = """
- connection_name: test_connection
  type: postgresql
  implementation: cli
  servers:
    - "localhost:5432"
    - "127.0.0.1:5432"
  db: testdb
  username: testuser
  password: testpass
"""
    config_file = tmp_path / "test_config.yaml"
    config_file.write_text(config_content)
    return str(config_file)


@pytest.fixture
async def mcp_client(test_config_file):
    """
    Connect to MCP server using real MCP protocol over stdio.
    This follows the minimal_client.py pattern.
    """
    server_params = StdioServerParameters(
        command="uv",
        args=["run", "python", "-m", "src.server", test_config_file],
        env=dict(os.environ)
    )

    # Use the pattern from minimal_client.py exactly
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            yield session


# Docker-based fixtures for integration tests
@pytest.fixture(scope="session")
def docker_check():
    """Check if Docker containers are running"""
    result = subprocess.run(
        ["docker", "ps", "--format", "table {{.Names}}"],
        capture_output=True,
        text=True
    )
    running_containers = result.stdout

    if "mcp-postgres" not in running_containers or "mcp-clickhouse" not in running_containers:
        pytest.skip("Docker containers not running. Run: just docker-test-setup")


@pytest.fixture(scope="session")
def ssh_container_check():
    """Check if SSH bastion container is running"""
    result = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}"],
        capture_output=True,
        text=True
    )
    running_containers = result.stdout

    if "mcp-ssh-bastion" not in running_containers:
        pytest.skip("SSH bastion container not running. Run: docker-compose --profile test up -d")


@pytest.fixture
def integration_config_file(tmp_path):
    """Create integration test configuration with real databases"""
    config_content = """
# Integration test configuration
- connection_name: test_postgres_python
  type: postgresql
  implementation: python
  servers:
    - "localhost:5432"
  db: testdb
  username: testuser
  password: testpass
  query_timeout: 30

- connection_name: test_postgres_cli
  type: postgresql
  implementation: cli
  servers:
    - "localhost:5432"
  db: testdb
  username: testuser
  password: testpass

- connection_name: test_clickhouse_python
  type: clickhouse
  implementation: python
  servers:
    - "localhost:9000"
  db: testdb
  username: testuser
  password: testpass

- connection_name: test_clickhouse_cli
  type: clickhouse
  implementation: cli
  servers:
    - "localhost:9000"
  db: testdb
  username: testuser
  password: testpass

# Connection with strict limits
- connection_name: test_postgres_strict
  type: postgresql
  implementation: python
  servers:
    - "localhost:5432"
  db: testdb
  username: testuser
  password: testpass
  query_timeout: 2
  max_result_bytes: 10000
"""

    config_file = tmp_path / "integration_config.yaml"
    config_file.write_text(config_content)

    # Set password environment variables
    env_vars = {
        "DB_PASSWORD_TEST_POSTGRES_PYTHON": "testpass",
        "DB_PASSWORD_TEST_POSTGRES_CLI": "testpass",
        "DB_PASSWORD_TEST_CLICKHOUSE_PYTHON": "testpass",
        "DB_PASSWORD_TEST_CLICKHOUSE_CLI": "testpass",
        "DB_PASSWORD_TEST_POSTGRES_STRICT": "testpass",
    }
    for key, value in env_vars.items():
        os.environ[key] = value

    return str(config_file)


@pytest.fixture
async def integration_client(integration_config_file, docker_check):
    """Client connected to integration test server"""
    server_params = StdioServerParameters(
        command="uv",
        args=["run", "python", "-m", "src.server", integration_config_file],
        env=dict(os.environ)
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            yield session


# Helper functions for tests
async def call_tool(session: ClientSession, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """
    Helper to call a tool and parse the response.
    All responses are now in TSV format or plain text (no JSON).

    Args:
        session: MCP client session
        tool_name: Name of the tool to call
        arguments: Arguments for the tool

    Returns:
        Response as dict with success status and parsed data
    """
    result = await session.call_tool(tool_name, arguments=arguments)

    # Check if this is an error response
    if hasattr(result, 'isError') and result.isError:
        if result.content and len(result.content) > 0:
            error_text = result.content[0].text
            return {"success": False, "error": error_text}
        return {"success": False, "error": "Unknown error"}

    if result.content and len(result.content) > 0:
        text_content = result.content[0].text

        # Check if this is an error message
        if text_content.startswith("Error"):
            return {"success": False, "error": text_content}

        # For list_connections, parse TSV format
        if tool_name == "list_connections":
            lines = text_content.strip().split('\n')
            if lines:
                # First line is headers
                headers = lines[0].split('\t')
                connections = []
                for line in lines[1:]:
                    if line:  # Skip empty lines
                        values = line.split('\t')
                        conn = {}
                        for i, header in enumerate(headers):
                            if i < len(values):
                                conn[header] = values[i]
                        connections.append(conn)
                return connections
            return []

        # For queries, parse TSV response
        lines = text_content.strip().split('\n')
        if lines:
            # First line is headers
            columns = lines[0].split('\t')
            rows = []
            for line in lines[1:]:
                if line:  # Skip empty lines
                    rows.append(line.split('\t'))
            return {
                "success": True,
                "columns": columns,
                "rows": rows,
                "rowCount": len(rows)
            }
        return {"success": True, "data": text_content}

    return {"success": False, "error": "No result returned"}


async def execute_query(
    session: ClientSession,
    connection_name: str,
    query: str,
    server: str | None = None,
) -> Dict[str, Any]:
    """
    Helper to execute a SQL query.

    Args:
        session: MCP client session
        connection_name: Name of the database connection
        query: SQL query to execute

    Returns:
        Query result as dict
    """
    payload = {
        "connection_name": connection_name,
        "query": query
    }
    if server is not None:
        payload["server"] = server

    return await call_tool(session, "run_query_read_only", payload)


async def list_connections(session: ClientSession) -> list:
    """
    Helper to list all connections.

    Args:
        session: MCP client session

    Returns:
        List of connection info
    """
    return await call_tool(session, "list_connections", {})
