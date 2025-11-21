#!/usr/bin/env python3
"""Tests for saving query results to disk via file_path parameter."""

from pathlib import Path

import pytest
from mcp.server.fastmcp import FastMCP

from src.config import Connection
from src.connectors.base import BaseConnector, DataSizeLimitError
from src.server import ReadOnlySQLServer


class StubConnector(BaseConnector):
    """Connector that returns static TSV content for testing."""

    async def execute_query(self, query: str, database=None, server=None) -> str:  # type: ignore[override]
        self.last_query = query
        return "id\tvalue\n1\ttest"


def build_stub_server(connector: BaseConnector) -> ReadOnlySQLServer:
    """Create a ReadOnlySQLServer instance wired with a stub connector."""

    server = ReadOnlySQLServer.__new__(ReadOnlySQLServer)
    server.config_path = "stub"
    server.connections = {connector.name: connector}
    server.mcp = FastMCP("mcp-read-only-sql-test")
    server._setup_tools()
    return server


@pytest.mark.anyio
async def test_run_query_writes_to_file(tmp_path):
    connection = Connection(
        {
            "connection_name": "stub_conn",
            "type": "postgresql",
            "servers": [{"host": "localhost", "port": 5432}],
            "db": "testdb",
            "username": "tester",
            "password": "secret",
        }
    )

    connector = StubConnector(connection)
    server = build_stub_server(connector)

    output_file = tmp_path / "results" / "out.tsv"

    result = await server.mcp._tool_manager.call_tool(
        "run_query_read_only",
        {
            "connection_name": "stub_conn",
            "query": "SELECT 1 AS id, 'test' AS value",
            "file_path": str(output_file),
        },
        convert_result=False,
    )

    assert Path(result) == output_file.resolve()
    assert output_file.exists()
    assert output_file.read_text() == "id\tvalue\n1\ttest"


@pytest.mark.anyio
async def test_run_query_file_path_already_exists(tmp_path):
    connection = Connection(
        {
            "connection_name": "stub_conn",
            "type": "postgresql",
            "servers": [{"host": "localhost", "port": 5432}],
            "db": "testdb",
            "username": "tester",
            "password": "secret",
        }
    )

    connector = StubConnector(connection)
    server = build_stub_server(connector)

    output_file = tmp_path / "results" / "out.tsv"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text("existing")

    with pytest.raises(Exception) as excinfo:
        await server.mcp._tool_manager.call_tool(
            "run_query_read_only",
            {
                "connection_name": "stub_conn",
                "query": "SELECT 1 AS id, 'test' AS value",
                "file_path": str(output_file),
            },
            convert_result=False,
        )

    # The tool wraps ValueError in a ToolError, so just assert message text
    assert "already exists" in str(excinfo.value)


class LimitedConnector(BaseConnector):
    """Connector that enforces max_result_bytes using effective guard."""

    def __init__(self, connection, payload: str):
        super().__init__(connection)
        self.payload = payload

    async def execute_query(self, query: str, database=None, server=None) -> str:  # type: ignore[override]
        max_bytes = self._effective_max_result_bytes()
        if max_bytes and len(self.payload.encode()) > max_bytes:
            raise DataSizeLimitError(f"Result size exceeds max_result_bytes={max_bytes}")
        return self.payload


@pytest.mark.anyio
async def test_file_path_bypasses_max_result_limit(tmp_path):
    payload = "col1\n" + ("x" * 50)
    connection = Connection(
        {
            "connection_name": "limited_conn",
            "type": "postgresql",
            "servers": [{"host": "localhost", "port": 5432}],
            "db": "testdb",
            "username": "tester",
            "password": "secret",
            "max_result_bytes": 10,
        }
    )
    connector = LimitedConnector(connection, payload)
    server = build_stub_server(connector)

    output_file = tmp_path / "results" / "big.tsv"
    result = await server.mcp._tool_manager.call_tool(
        "run_query_read_only",
        {
            "connection_name": "limited_conn",
            "query": "SELECT 1",
            "file_path": str(output_file),
        },
        convert_result=False,
    )

    assert Path(result) == output_file.resolve()
    assert output_file.read_text() == payload


@pytest.mark.anyio
async def test_max_result_limit_enforced_without_file_path():
    payload = "col1\n" + ("x" * 50)
    connection = Connection(
        {
            "connection_name": "limited_conn",
            "type": "postgresql",
            "servers": [{"host": "localhost", "port": 5432}],
            "db": "testdb",
            "username": "tester",
            "password": "secret",
            "max_result_bytes": 10,
        }
    )
    connector = LimitedConnector(connection, payload)
    server = build_stub_server(connector)

    with pytest.raises(Exception) as excinfo:
        await server.mcp._tool_manager.call_tool(
            "run_query_read_only",
            {
                "connection_name": "limited_conn",
                "query": "SELECT 1",
            },
            convert_result=False,
        )

    assert "max_result_bytes" in str(excinfo.value)
