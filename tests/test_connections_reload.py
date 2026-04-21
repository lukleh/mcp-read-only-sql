#!/usr/bin/env python3
"""Tests for runtime reloading of connections.yaml."""

import logging
from pathlib import Path

import pytest
import yaml
from mcp.server.fastmcp.exceptions import ToolError

import mcp_read_only_sql.server as server_module
from mcp_read_only_sql.config import Connection
from mcp_read_only_sql.connectors.base import BaseConnector
from mcp_read_only_sql.runtime_paths import RuntimePaths
from mcp_read_only_sql.server import ReadOnlySQLServer


class ReloadTestConnector(BaseConnector):
    """Connector stub that surfaces the currently loaded config in TSV output."""

    async def execute_query(self, query: str, database=None, server=None) -> str:  # type: ignore[override]
        selected_server = self._select_server(server)
        selected_database = self._resolve_database(database)
        return (
            "connection\tserver\tdatabase\tuser\n"
            f"{self.name}\t{selected_server.host}\t{selected_database}\t{self.username}"
        )


def make_runtime_paths(tmp_path: Path) -> RuntimePaths:
    """Create isolated runtime paths for reload tests."""
    runtime_paths = RuntimePaths(
        config_dir=tmp_path / "config",
        state_dir=tmp_path / "state",
        cache_dir=tmp_path / "cache",
    )
    runtime_paths.ensure_directories()
    return runtime_paths


def write_connections_file(path: Path, connections: list[dict[str, object]]) -> None:
    """Write a YAML connections file for a reload scenario."""
    path.write_text(
        yaml.safe_dump(connections, sort_keys=False),
        encoding="utf-8",
    )


def parse_tsv_rows(tsv_text: str) -> list[dict[str, str]]:
    """Parse the server's tab-separated list_connections response."""
    lines = tsv_text.splitlines()
    headers = lines[0].split("\t")
    rows: list[dict[str, str]] = []
    for line in lines[1:]:
        if not line:
            continue
        rows.append(dict(zip(headers, line.split("\t"))))
    return rows


def apply_stub_connectors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Route server connector construction to the in-memory reload test stub."""
    monkeypatch.setattr(
        server_module, "PostgreSQLCLIConnector", ReloadTestConnector
    )
    monkeypatch.setattr(
        server_module, "PostgreSQLPythonConnector", ReloadTestConnector
    )
    monkeypatch.setattr(
        server_module, "ClickHouseCLIConnector", ReloadTestConnector
    )
    monkeypatch.setattr(
        server_module, "ClickHousePythonConnector", ReloadTestConnector
    )


def count_load_connections(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    """Wrap load_connections so tests can assert whether reloads happened."""
    real_load_connections = server_module.load_connections_from_text
    call_counter = {"count": 0}

    def wrapped_load_connections(
        yaml_text: str, source: str | Path = "<memory>"
    ) -> dict[str, Connection]:
        call_counter["count"] += 1
        return real_load_connections(yaml_text, source)

    monkeypatch.setattr(
        server_module, "load_connections_from_text", wrapped_load_connections
    )
    return call_counter


async def list_connections(server: ReadOnlySQLServer) -> list[dict[str, str]]:
    """Call list_connections directly on the in-process FastMCP server."""
    result = await server.mcp._tool_manager.call_tool(
        "list_connections",
        {},
        convert_result=False,
    )
    assert isinstance(result, str)
    return parse_tsv_rows(result)


async def run_query(server: ReadOnlySQLServer, connection_name: str) -> Path:
    """Run a query against the in-process FastMCP server and return the TSV path."""
    result = await server.mcp._tool_manager.call_tool(
        "run_query_read_only",
        {"connection_name": connection_name, "query": "SELECT 1"},
        convert_result=False,
    )
    assert isinstance(result, str)
    return Path(result)


@pytest.mark.anyio
async def test_tools_reload_connections_after_config_changes(tmp_path, monkeypatch):
    apply_stub_connectors(monkeypatch)
    runtime_paths = make_runtime_paths(tmp_path)
    write_connections_file(
        runtime_paths.connections_file,
        [
            {
                "connection_name": "alpha",
                "type": "postgresql",
                "implementation": "cli",
                "servers": ["alpha-db:5432"],
                "db": "analytics",
                "username": "alpha_user",
                "password": "secret",
            },
            {
                "connection_name": "beta",
                "type": "postgresql",
                "implementation": "cli",
                "servers": ["beta-db:5432"],
                "db": "warehouse",
                "username": "beta_user",
                "password": "secret",
            },
        ],
    )
    server = ReadOnlySQLServer(runtime_paths)

    initial_connections = await list_connections(server)
    assert [row["name"] for row in initial_connections] == ["alpha", "beta"]

    write_connections_file(
        runtime_paths.connections_file,
        [
            {
                "connection_name": "beta",
                "type": "postgresql",
                "implementation": "cli",
                "servers": ["beta-db-v2:5432"],
                "db": "warehouse",
                "username": "beta_user_v2",
                "password": "secret",
            },
            {
                "connection_name": "gamma",
                "type": "postgresql",
                "implementation": "cli",
                "servers": ["gamma-db:5432"],
                "db": "warehouse",
                "username": "gamma_user",
                "password": "secret",
            },
        ],
    )

    reloaded_connections = await list_connections(server)
    assert [row["name"] for row in reloaded_connections] == ["beta", "gamma"]
    reloaded_map = {row["name"]: row for row in reloaded_connections}
    assert reloaded_map["beta"]["servers"] == "beta-db-v2"
    assert reloaded_map["beta"]["user"] == "beta_user_v2"

    with pytest.raises(ToolError, match="Connection 'alpha' not found"):
        await run_query(server, "alpha")

    gamma_result = await run_query(server, "gamma")
    assert gamma_result.read_text(encoding="utf-8").splitlines() == [
        "connection\tserver\tdatabase\tuser",
        "gamma\tgamma-db\twarehouse\tgamma_user",
    ]


@pytest.mark.anyio
async def test_reload_skips_unchanged_config(tmp_path, monkeypatch):
    apply_stub_connectors(monkeypatch)
    load_calls = count_load_connections(monkeypatch)
    runtime_paths = make_runtime_paths(tmp_path)
    write_connections_file(
        runtime_paths.connections_file,
        [
            {
                "connection_name": "alpha",
                "type": "postgresql",
                "implementation": "cli",
                "servers": ["alpha-db:5432"],
                "db": "analytics",
                "username": "alpha_user",
                "password": "secret",
            }
        ],
    )
    server = ReadOnlySQLServer(runtime_paths)

    assert load_calls["count"] == 1

    await list_connections(server)
    await list_connections(server)
    await run_query(server, "alpha")

    assert load_calls["count"] == 1


@pytest.mark.anyio
async def test_invalid_reload_keeps_last_good_connections(
    tmp_path, monkeypatch, caplog
):
    apply_stub_connectors(monkeypatch)
    load_calls = count_load_connections(monkeypatch)
    runtime_paths = make_runtime_paths(tmp_path)
    write_connections_file(
        runtime_paths.connections_file,
        [
            {
                "connection_name": "alpha",
                "type": "postgresql",
                "implementation": "cli",
                "servers": ["alpha-db:5432"],
                "db": "analytics",
                "username": "alpha_user",
                "password": "secret",
            }
        ],
    )
    server = ReadOnlySQLServer(runtime_paths)
    caplog.set_level(logging.WARNING)

    runtime_paths.connections_file.write_text("invalid: true\n", encoding="utf-8")

    preserved_connections = await list_connections(server)
    assert [row["name"] for row in preserved_connections] == ["alpha"]

    alpha_result = await run_query(server, "alpha")
    assert alpha_result.read_text(encoding="utf-8").splitlines() == [
        "connection\tserver\tdatabase\tuser",
        "alpha\talpha-db\tanalytics\talpha_user",
    ]

    await list_connections(server)

    assert load_calls["count"] == 4
    assert "keeping 1 previously loaded connection(s)" in caplog.text


@pytest.mark.anyio
async def test_reload_retries_after_file_changes_during_reload(tmp_path, monkeypatch):
    apply_stub_connectors(monkeypatch)
    runtime_paths = make_runtime_paths(tmp_path)
    write_connections_file(
        runtime_paths.connections_file,
        [
            {
                "connection_name": "alpha",
                "type": "postgresql",
                "implementation": "cli",
                "servers": ["alpha-db:5432"],
                "db": "analytics",
                "username": "alpha_user",
                "password": "secret",
            }
        ],
    )
    server = ReadOnlySQLServer(runtime_paths)

    beta_connections = [
        {
            "connection_name": "beta",
            "type": "postgresql",
            "implementation": "cli",
            "servers": ["beta-db:5432"],
            "db": "warehouse",
            "username": "beta_user",
            "password": "secret",
        }
    ]
    gamma_connections = [
        {
            "connection_name": "gamma",
            "type": "postgresql",
            "implementation": "cli",
            "servers": ["gamma-db:5432"],
            "db": "warehouse",
            "username": "gamma_user",
            "password": "secret",
        }
    ]
    write_connections_file(runtime_paths.connections_file, beta_connections)

    original_build_connector = server._build_connector
    triggered_reload_edit = False

    def build_connector_and_mutate_file(connection: Connection) -> BaseConnector:
        nonlocal triggered_reload_edit
        connector = original_build_connector(connection)
        if not triggered_reload_edit and connection.name == "beta":
            write_connections_file(runtime_paths.connections_file, gamma_connections)
            triggered_reload_edit = True
        return connector

    monkeypatch.setattr(server, "_build_connector", build_connector_and_mutate_file)

    beta_result = await list_connections(server)
    assert [row["name"] for row in beta_result] == ["beta"]

    gamma_result = await list_connections(server)
    assert [row["name"] for row in gamma_result] == ["gamma"]


@pytest.mark.anyio
async def test_missing_connections_file_keeps_last_good_config(
    tmp_path, monkeypatch, caplog
):
    apply_stub_connectors(monkeypatch)
    runtime_paths = make_runtime_paths(tmp_path)
    write_connections_file(
        runtime_paths.connections_file,
        [
            {
                "connection_name": "alpha",
                "type": "postgresql",
                "implementation": "cli",
                "servers": ["alpha-db:5432"],
                "db": "analytics",
                "username": "alpha_user",
                "password": "secret",
            }
        ],
    )
    server = ReadOnlySQLServer(runtime_paths)
    caplog.set_level(logging.WARNING)

    runtime_paths.connections_file.unlink()

    preserved_connections = await list_connections(server)
    assert [row["name"] for row in preserved_connections] == ["alpha"]
    assert "Failed to reload connections" in caplog.text
