#!/usr/bin/env python3
"""
MCP Read-Only SQL Server - FastMCP Implementation
A secure MCP server providing read-only SQL query capabilities for PostgreSQL and ClickHouse databases.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional

from mcp.server.fastmcp import FastMCP

from .config import load_connections
from .config.connection import (
    DEFAULT_CONNECTION_TIMEOUT,
    DEFAULT_MAX_RESULT_BYTES,
    DEFAULT_QUERY_TIMEOUT,
)
from .config.env_files import build_runtime_env
from .connectors.base import BaseConnector
from .connectors.clickhouse.cli import ClickHouseCLIConnector
from .connectors.clickhouse.python import ClickHousePythonConnector
from .connectors.postgresql.cli import PostgreSQLCLIConnector
from .connectors.postgresql.python import PostgreSQLPythonConnector
from .runtime_paths import resolve_runtime_paths, RuntimePaths

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _display_hosts_for_connector(connector: BaseConnector) -> List[str]:
    """Return unique display hostnames for a connector."""
    servers: List[str] = []
    local_hosts = {"localhost", "127.0.0.1", "::1"}

    for server in connector.connection.servers:
        host = server.host
        display_host = host

        if connector.ssh_config and host in local_hosts:
            ssh_host = connector.ssh_config.host
            if ssh_host:
                display_host = ssh_host

        if display_host not in servers:
            servers.append(display_host)

    return servers


class ReadOnlySQLServer:
    """MCP Read-Only SQL Server using FastMCP."""

    def __init__(self, runtime_paths: RuntimePaths):
        self.runtime_paths = runtime_paths
        self.connections: Dict[str, BaseConnector] = {}

        self.mcp = FastMCP("mcp-read-only-sql")

        self._load_connections()
        self._setup_tools()

    def _load_connections(self) -> None:
        try:
            runtime_env = build_runtime_env(self.runtime_paths.credentials_file)
            connections_config = load_connections(
                self.runtime_paths.connections_file,
                env=runtime_env,
            )

            errors = []

            for conn_name, connection in connections_config.items():
                try:
                    if connection.db_type == "postgresql":
                        if connection.implementation == "cli":
                            self.connections[conn_name] = PostgreSQLCLIConnector(connection)
                        else:
                            self.connections[conn_name] = PostgreSQLPythonConnector(connection)
                    elif connection.db_type == "clickhouse":
                        if connection.implementation == "cli":
                            self.connections[conn_name] = ClickHouseCLIConnector(connection)
                        else:
                            self.connections[conn_name] = ClickHousePythonConnector(connection)

                    logger.info(
                        "Loaded connection: %s (%s, %s)",
                        conn_name,
                        connection.db_type,
                        connection.implementation,
                    )

                except ImportError as exc:
                    errors.append(f"  - {conn_name}: Missing dependency - {exc}")
                except Exception as exc:
                    errors.append(f"  - {conn_name}: {exc}")

            if errors:
                error_msg = "Failed to load some connections:\n" + "\n".join(errors)
                logger.error(error_msg)
                raise RuntimeError(error_msg)

        except Exception as exc:
            logger.error("Failed to load connections: %s", exc)
            raise

    def _setup_tools(self) -> None:
        """Setup MCP tools using FastMCP decorators."""

        @self.mcp.tool()
        async def run_query_read_only(
            connection_name: str,
            query: str,
            database: Optional[str] = None,
            server: Optional[str] = None,
            file_path: Optional[str] = None,
        ) -> str:
            if connection_name not in self.connections:
                raise ValueError(
                    f"Connection '{connection_name}' not found. Available connections: {', '.join(self.connections.keys())}"
                )

            connector = self.connections[connection_name]
            execute = connector.execute_query_with_timeout
            if file_path:
                with connector.disable_result_limit():
                    result = await execute(query, database=database, server=server)

                output_path = Path(file_path).expanduser().resolve()

                if output_path.exists():
                    raise ValueError(f"File path already exists: {output_path}")

                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_text(result, encoding="utf-8")
                return str(output_path)

            return await execute(query, database=database, server=server)

        @self.mcp.tool()
        async def list_connections() -> str:
            conn_list = []

            for conn_name, connector in self.connections.items():
                conn_type = connector.connection.db_type
                servers = _display_hosts_for_connector(connector)

                conn_info = {
                    "name": conn_name,
                    "type": conn_type,
                    "description": connector.connection.description or "",
                    "servers": servers,
                    "database": connector.database,
                    "databases": connector.allowed_databases,
                    "user": connector.username or "",
                }

                if connector.query_timeout != DEFAULT_QUERY_TIMEOUT:
                    conn_info["query_timeout"] = connector.query_timeout
                if connector.connection_timeout != DEFAULT_CONNECTION_TIMEOUT:
                    conn_info["connection_timeout"] = connector.connection_timeout
                if connector.max_result_bytes != DEFAULT_MAX_RESULT_BYTES:
                    conn_info["max_result_bytes"] = connector.max_result_bytes

                conn_list.append(conn_info)

            if not conn_list:
                return "name\ttype\tdescription\tservers\tdatabase\tdatabases\tuser"

            headers = [
                "name",
                "type",
                "description",
                "servers",
                "database",
                "databases",
                "user",
            ]
            rows = ["\t".join(headers)]

            for conn in conn_list:
                row = [
                    conn.get("name", ""),
                    conn.get("type", ""),
                    conn.get("description", ""),
                    ",".join(conn.get("servers", [])),
                    conn.get("database", ""),
                    ",".join(conn.get("databases", [])),
                    conn.get("user", ""),
                ]
                rows.append("\t".join(row))

            return "\n".join(rows)

    def run(self) -> None:
        if not self.connections:
            logger.warning("No connections loaded. Check your configuration file.")
        else:
            logger.info("Loaded %s connection(s)", len(self.connections))

        self.mcp.run()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MCP Read-Only SQL Server")
    parser.add_argument(
        "--config-dir",
        help="Directory containing connections.yaml and credentials.env",
    )
    parser.add_argument(
        "--state-dir",
        help="Directory reserved for local state files",
    )
    parser.add_argument(
        "--cache-dir",
        help="Directory reserved for cache files",
    )
    parser.add_argument(
        "--print-paths",
        action="store_true",
        help="Print resolved config/state/cache paths and exit",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    runtime_paths = resolve_runtime_paths(
        config_dir=args.config_dir,
        state_dir=args.state_dir,
        cache_dir=args.cache_dir,
    )

    if args.print_paths:
        print(runtime_paths.render())
        return

    runtime_paths.ensure_directories()

    if not runtime_paths.connections_file.exists():
        logger.error("Configuration file not found: %s", runtime_paths.connections_file)
        logger.info("Expected SQL config at %s", runtime_paths.config_dir)
        sys.exit(1)

    logger.info("Loading connections from %s", runtime_paths.connections_file)
    server = ReadOnlySQLServer(runtime_paths)
    server.run()


if __name__ == "__main__":
    main()
