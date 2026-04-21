#!/usr/bin/env python3
"""Tests for managed query-result files."""

import os
from pathlib import Path
from stat import S_IMODE

import pytest
from mcp.server.fastmcp import FastMCP

from mcp_read_only_sql.config import Connection
from mcp_read_only_sql.connectors.base import BaseConnector
from mcp_read_only_sql.runtime_paths import RuntimePaths
from mcp_read_only_sql.server import ReadOnlySQLServer


class StubConnector(BaseConnector):
    """Connector that returns static TSV content for testing."""

    async def execute_query(self, query: str, database=None, server=None) -> str:  # type: ignore[override]
        self.last_query = query
        return "id\tvalue\n1\ttest"


def build_stub_server(
    connector: BaseConnector, runtime_paths: RuntimePaths
) -> ReadOnlySQLServer:
    """Create a ReadOnlySQLServer instance wired with a stub connector."""

    server = ReadOnlySQLServer.__new__(ReadOnlySQLServer)
    server.runtime_paths = runtime_paths
    server.connections = {connector.name: connector}
    server._connections_config_marker = None
    server.mcp = FastMCP("mcp-read-only-sql-test")
    server._setup_tools()
    return server


def make_runtime_paths(tmp_path: Path) -> RuntimePaths:
    """Create managed runtime paths for query-result tests."""

    runtime_paths = RuntimePaths(
        config_dir=tmp_path / "config",
        state_dir=tmp_path / "state",
        cache_dir=tmp_path / "cache",
    )
    runtime_paths.ensure_directories()
    return runtime_paths


def make_stub_connector() -> StubConnector:
    """Create a basic PostgreSQL connector for isolated server tests."""

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
    return StubConnector(connection)


@pytest.mark.anyio
async def test_run_query_writes_to_managed_results_dir(tmp_path):
    runtime_paths = make_runtime_paths(tmp_path)
    connector = make_stub_connector()
    server = build_stub_server(connector, runtime_paths)

    result = await server.mcp._tool_manager.call_tool(
        "run_query_read_only",
        {
            "connection_name": "stub_conn",
            "query": "SELECT 1 AS id, 'test' AS value",
        },
        convert_result=False,
    )

    output_file = Path(result)
    assert output_file.exists()
    assert output_file.parent.parent == runtime_paths.results_dir
    assert output_file.parent.name.startswith("stub_conn-")
    assert output_file.read_text(encoding="utf-8") == "id\tvalue\n1\ttest"
    assert S_IMODE(output_file.stat().st_mode) == 0o600
    assert S_IMODE(runtime_paths.results_dir.stat().st_mode) == 0o700
    assert S_IMODE(output_file.parent.stat().st_mode) == 0o700


@pytest.mark.anyio
async def test_run_query_creates_unique_result_files(tmp_path):
    runtime_paths = make_runtime_paths(tmp_path)
    connector = make_stub_connector()
    server = build_stub_server(connector, runtime_paths)

    first = await server.mcp._tool_manager.call_tool(
        "run_query_read_only",
        {
            "connection_name": "stub_conn",
            "query": "SELECT 1",
        },
        convert_result=False,
    )
    second = await server.mcp._tool_manager.call_tool(
        "run_query_read_only",
        {
            "connection_name": "stub_conn",
            "query": "SELECT 2",
        },
        convert_result=False,
    )

    first_path = Path(first)
    second_path = Path(second)

    assert first_path != second_path
    assert first_path.exists()
    assert second_path.exists()


def test_build_result_path_sanitizes_special_connection_names(tmp_path):
    runtime_paths = make_runtime_paths(tmp_path)
    connector = make_stub_connector()
    server = build_stub_server(connector, runtime_paths)

    sanitized_path = server._build_result_path("prod/db@1")
    fallback_path = server._build_result_path("!!!")
    collision_candidate = server._build_result_path("prod:db@1")

    assert sanitized_path.parent.parent == runtime_paths.results_dir
    assert sanitized_path.parent.name.startswith("prod-db-1-")
    assert fallback_path.parent.parent == runtime_paths.results_dir
    assert fallback_path.parent.name.startswith("query-")
    assert sanitized_path.parent != collision_candidate.parent
    assert sanitized_path.suffix == ".tsv"
    assert fallback_path.suffix == ".tsv"


def test_create_result_file_fails_when_output_path_is_unwritable(tmp_path, monkeypatch):
    runtime_paths = make_runtime_paths(tmp_path)
    connector = make_stub_connector()
    server = build_stub_server(connector, runtime_paths)
    output_path = runtime_paths.results_dir / "stub_conn-denied" / "denied.tsv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    original_os_open = os.open

    def raising_open(path, flags, mode=0o777):
        if os.fspath(path) == os.fspath(output_path):
            raise PermissionError("permission denied")
        return original_os_open(path, flags, mode)

    monkeypatch.setattr(
        server, "_build_result_path", lambda connection_name: output_path
    )
    monkeypatch.setattr(os, "open", raising_open)

    with pytest.raises(PermissionError, match="permission denied"):
        server._create_result_file("stub_conn")
