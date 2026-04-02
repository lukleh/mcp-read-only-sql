#!/usr/bin/env python3
"""
MCP Read-Only SQL Server - FastMCP Implementation
A secure MCP server providing read-only SQL query capabilities for PostgreSQL and ClickHouse databases.
"""

import argparse
from contextlib import suppress
from hashlib import blake2b
from importlib.resources import files
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional
from uuid import uuid4

from mcp.server.fastmcp import FastMCP

from . import __version__
from .config import dbeaver_import, load_connections
from .config.connection import (
    DEFAULT_CONNECTION_TIMEOUT,
    DEFAULT_QUERY_TIMEOUT,
)
from .connectors.base import BaseConnector
from .connectors.clickhouse.cli import ClickHouseCLIConnector
from .connectors.clickhouse.python import ClickHousePythonConnector
from .connectors.postgresql.cli import PostgreSQLCLIConnector
from .connectors.postgresql.python import PostgreSQLPythonConnector
from .runtime_paths import (
    PRIVATE_DIR_MODE,
    PRIVATE_FILE_MODE,
    resolve_runtime_paths,
    RuntimePaths,
)
from .tools import test_connection, test_ssh_tunnel, validate_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
RESULT_FILE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")
RESULT_DIR_HASH_BYTES = 6
SAMPLE_CONNECTIONS_YAML = (
    files("mcp_read_only_sql")
    .joinpath("connections.yaml.sample")
    .read_text(encoding="utf-8")
)
SUBCOMMAND_HANDLERS: dict[str, Callable[[], None]] = {
    "import-dbeaver": dbeaver_import.main,
    "validate-config": validate_config.main,
    "test-connection": test_connection.main,
    "test-ssh-tunnel": test_ssh_tunnel.main,
}


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

        self.runtime_paths.ensure_directories()
        self.mcp = FastMCP("mcp-read-only-sql")

        self._load_connections()
        self._setup_tools()

    def _build_result_path(self, connection_name: str) -> Path:
        """Create a collision-safe result path under the managed state directory."""
        safe_name = (
            RESULT_FILE_NAME_RE.sub("-", connection_name).strip("._-") or "query"
        )
        digest = blake2b(
            connection_name.encode("utf-8"), digest_size=RESULT_DIR_HASH_BYTES
        ).hexdigest()
        output_dir = self.runtime_paths.results_dir / f"{safe_name}-{digest}"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_dir.chmod(PRIVATE_DIR_MODE)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return output_dir / f"{timestamp}-{uuid4().hex[:8]}.tsv"

    def _create_result_file(self, connection_name: str) -> Path:
        """Create an empty managed result file with private permissions."""
        for _ in range(5):
            output_path = self._build_result_path(connection_name)
            try:
                fd = os.open(
                    output_path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    PRIVATE_FILE_MODE,
                )
            except FileExistsError:
                continue
            os.close(fd)
            return output_path
        raise FileExistsError("Could not allocate a unique managed result file")

    def _load_connections(self) -> None:
        try:
            connections_config = load_connections(self.runtime_paths.connections_file)

            errors = []

            for conn_name, connection in connections_config.items():
                try:
                    if connection.db_type == "postgresql":
                        if connection.implementation == "cli":
                            self.connections[conn_name] = PostgreSQLCLIConnector(
                                connection
                            )
                        else:
                            self.connections[conn_name] = PostgreSQLPythonConnector(
                                connection
                            )
                    elif connection.db_type == "clickhouse":
                        if connection.implementation == "cli":
                            self.connections[conn_name] = ClickHouseCLIConnector(
                                connection
                            )
                        else:
                            self.connections[conn_name] = ClickHousePythonConnector(
                                connection
                            )

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
        ) -> str:
            """Run a read-only SQL query and return a managed TSV result path.

            Args:
                connection_name: Connection name returned by ``list_connections``.
                query: Read-only SQL text to execute.
                database: Optional database override. Must be in the connection's
                    allowed database list.
                server: Optional hostname override targeting a specific configured
                    server. Defaults to the first server for the connection.

            Returns:
                Absolute path to a TSV file written under the managed state
                directory for this server instance. Successful query results
                are retained there until removed by the operator.
            """
            if connection_name not in self.connections:
                raise ValueError(
                    f"Connection '{connection_name}' not found. Available connections: {', '.join(self.connections.keys())}"
                )

            connector = self.connections[connection_name]
            output_path = self._create_result_file(connection_name)
            try:
                await connector.execute_query_to_file_with_timeout(
                    query,
                    output_path,
                    database=database,
                    server=server,
                )
            except Exception:
                with suppress(FileNotFoundError):
                    output_path.unlink()
                raise
            return str(output_path.resolve())

        @self.mcp.tool()
        async def list_connections() -> str:
            """List configured database connections as TSV metadata.

            Returns:
                Tab-separated text with the columns ``name``, ``type``,
                ``description``, ``servers``, ``database``, ``databases``, and
                ``user``. The ``servers`` column contains the resolved display
                hosts for each connection, while ``database`` and ``databases``
                describe the default database and allowed database list.
            """
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


def write_sample_config(
    runtime_paths: RuntimePaths, *, overwrite: bool = False
) -> Path:
    """Write a sample connections.yaml for package-based installs."""
    runtime_paths.ensure_directories()

    config_path = runtime_paths.connections_file
    if config_path.exists() and not overwrite:
        raise FileExistsError(
            f"Config file already exists at {config_path}. Re-run with --overwrite to replace it."
        )

    config_path.write_text(SAMPLE_CONNECTIONS_YAML, encoding="utf-8")
    config_path.chmod(0o600)
    return config_path


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mcp-read-only-sql",
        description="MCP Read-Only SQL Server",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    parser.add_argument(
        "--config-dir",
        help="Directory containing connections.yaml",
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
    parser.add_argument(
        "--write-sample-config",
        action="store_true",
        help="Write a sample connections.yaml to the resolved config path and exit",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace connections.yaml when used with --write-sample-config",
    )
    parser.add_argument(
        "command",
        nargs="?",
        choices=sorted(SUBCOMMAND_HANDLERS),
        help="Optional management command to run instead of starting the MCP server",
    )
    parser.add_argument(
        "command_args",
        nargs=argparse.REMAINDER,
        help=argparse.SUPPRESS,
    )
    return parser


def _forward_shared_runtime_args(args: argparse.Namespace) -> list[str]:
    forwarded: list[str] = []
    if args.config_dir:
        forwarded.extend(["--config-dir", args.config_dir])
    if args.state_dir:
        forwarded.extend(["--state-dir", args.state_dir])
    if args.cache_dir:
        forwarded.extend(["--cache-dir", args.cache_dir])
    if args.print_paths:
        forwarded.append("--print-paths")
    return forwarded


def _dispatch_subcommand(args: argparse.Namespace) -> None:
    """Execute a management subcommand through the public root CLI."""
    forwarded_args = _forward_shared_runtime_args(args)
    command_argv = [
        f"mcp-read-only-sql {args.command}",
        *forwarded_args,
        *args.command_args,
    ]

    original_argv = sys.argv
    try:
        sys.argv = command_argv
        SUBCOMMAND_HANDLERS[args.command]()
    finally:
        sys.argv = original_argv


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    if args.command and (args.write_sample_config or args.overwrite):
        parser.error(
            "--write-sample-config and --overwrite can only be used without a subcommand"
        )

    if args.command:
        _dispatch_subcommand(args)
        return

    if args.overwrite and not args.write_sample_config:
        parser.error("--overwrite can only be used with --write-sample-config")

    runtime_paths = resolve_runtime_paths(
        config_dir=args.config_dir,
        state_dir=args.state_dir,
        cache_dir=args.cache_dir,
    )

    if args.write_sample_config:
        try:
            config_path = write_sample_config(runtime_paths, overwrite=args.overwrite)
        except FileExistsError as exc:
            parser.error(str(exc))
        print(f"Wrote sample config to {config_path}")
        if not args.print_paths:
            return

    if args.print_paths:
        print(runtime_paths.render())
        return

    if not runtime_paths.connections_file.exists():
        logger.error("Configuration file not found: %s", runtime_paths.connections_file)
        logger.info("Expected SQL config at %s", runtime_paths.config_dir)
        sys.exit(1)

    logger.info("Loading connections from %s", runtime_paths.connections_file)
    server = ReadOnlySQLServer(runtime_paths)
    server.run()


if __name__ == "__main__":
    main()
