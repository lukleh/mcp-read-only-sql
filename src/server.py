#!/usr/bin/env python3
"""
MCP Read-Only SQL Server - FastMCP Implementation
A secure MCP server providing read-only SQL query capabilities for PostgreSQL and ClickHouse databases.
"""

import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import FastMCP

from .config import load_connections
from .config.connection import (
    DEFAULT_QUERY_TIMEOUT,
    DEFAULT_CONNECTION_TIMEOUT,
    DEFAULT_MAX_RESULT_BYTES,
)
from .connectors.base import BaseConnector
from .connectors.postgresql.python import PostgreSQLPythonConnector
from .connectors.postgresql.cli import PostgreSQLCLIConnector
from .connectors.clickhouse.python import ClickHousePythonConnector
from .connectors.clickhouse.cli import ClickHouseCLIConnector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ReadOnlySQLServer:
    """MCP Read-Only SQL Server using FastMCP"""

    def __init__(self, config_path: str = "connections.yaml"):
        """Initialize the server with configuration"""
        self.config_path = config_path
        self.connections: Dict[str, BaseConnector] = {}

        # Initialize FastMCP server
        self.mcp = FastMCP("mcp-read-only-sql")

        # Load connections
        self._load_connections()

        # Setup tools
        self._setup_tools()

    def _load_connections(self):
        """Load all connections from config file"""
        try:
            # Load and validate all connections
            connections_config = load_connections(self.config_path)

            errors = []

            # Create connectors from validated Connection objects
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

                    logger.info(f"Loaded connection: {conn_name} ({connection.db_type}, {connection.implementation})")

                except ImportError as e:
                    errors.append(f"  - {conn_name}: Missing dependency - {e}")
                except Exception as e:
                    errors.append(f"  - {conn_name}: {e}")

            if errors:
                error_msg = "Failed to load some connections:\n" + "\n".join(errors)
                logger.error(error_msg)
                raise RuntimeError(error_msg)

        except Exception as e:
            logger.error(f"Failed to load connections: {e}")
            raise

    def _setup_tools(self):
        """Setup MCP tools using FastMCP decorators"""

        @self.mcp.tool()
        async def run_query_read_only(connection_name: str, query: str, server: Optional[str] = None) -> str:
            """
            Execute a read-only SQL statement on a configured connection.

            Args:
                connection_name: Identifier returned by list_connections
                query: SQL text that must remain read-only
                server: Optional server specification in format "host:port" or "host".
                       If not provided, uses the first server in the connection's list.

            Returns:
                TSV string where the first line contains column headers and
                subsequent lines contain tab-delimited rows. Output may be empty
                when no rows match and is capped by max_result_bytes.
            """
            if connection_name not in self.connections:
                raise ValueError(f"Connection '{connection_name}' not found. Available connections: {', '.join(self.connections.keys())}")

            connector = self.connections[connection_name]
            # Use the hard timeout wrapper to prevent hanging
            # This will return TSV string on success or raise exception on error
            result = await connector.execute_query_with_timeout(query, server=server)
            return result

        @self.mcp.tool()
        async def list_connections() -> str:
            """
            List all available database connections with their configuration details.

            Returns:
                TSV string with header columns: name, type, description, servers, database, user.
                Servers are comma-separated host:port pairs showing the resolved database
                endpoints (SSH/VPN adjustments applied) loaded at startup.
            """
            conn_list = []

            for conn_name, connector in self.connections.items():
                # Type is required - if connector exists, it must have a type
                conn_type = connector.connection.db_type
                implementation = connector.connection.implementation

                servers = []
                local_hosts = {"localhost", "127.0.0.1", "::1"}
                for server in connector.connection.servers:
                    # Server is now a Server object, not a dict
                    host = server.host
                    port = server.port

                    display_host = host
                    if connector.ssh_config and host in local_hosts:
                        ssh_host = connector.ssh_config.host
                        if ssh_host:
                            display_host = ssh_host

                    if conn_type == "clickhouse" and port is not None:
                        if implementation == "python":
                            effective_port = 8123 if port == 9000 else 8443 if port == 9440 else port
                        else:
                            effective_port = 9000 if port == 8123 else 9440 if port == 8443 else port
                    else:
                        effective_port = port if port is not None else connector._get_default_port()

                    servers.append(f"{display_host}:{effective_port}")

                conn_info = {
                    "name": conn_name,
                    "type": conn_type,
                    "description": connector.connection.description or "",
                    "servers": servers,
                    "database": connector.database,
                    "user": connector.username or ""
                }

                # Add security limits if configured
                if connector.query_timeout != DEFAULT_QUERY_TIMEOUT:
                    conn_info["query_timeout"] = connector.query_timeout
                if connector.connection_timeout != DEFAULT_CONNECTION_TIMEOUT:
                    conn_info["connection_timeout"] = connector.connection_timeout
                if connector.max_result_bytes != DEFAULT_MAX_RESULT_BYTES:
                    conn_info["max_result_bytes"] = connector.max_result_bytes

                conn_list.append(conn_info)

            # Return as TSV for consistency with query results
            if not conn_list:
                return "name\ttype\tdescription\tservers\tdatabase\tuser"

            # Build TSV with headers
            headers = ["name", "type", "description", "servers", "database", "user"]
            rows = ["\t".join(headers)]

            for conn in conn_list:
                row = [
                    conn.get("name", ""),
                    conn.get("type", ""),
                    conn.get("description", ""),
                    ",".join(conn.get("servers", [])),  # Join multiple servers with comma
                    conn.get("database", ""),
                    conn.get("user", "")
                ]
                rows.append("\t".join(row))

            return "\n".join(rows)

    def run(self):
        """Run the FastMCP server"""
        if not self.connections:
            logger.warning("No connections loaded. Check your configuration file.")
        else:
            logger.info(f"Loaded {len(self.connections)} connection(s)")

        # Run the FastMCP server (defaults to stdio transport)
        self.mcp.run()


def main():
    """Main entry point for the MCP server"""
    # Get config file path from command line or use default
    config_path = sys.argv[1] if len(sys.argv) > 1 else "connections.yaml"

    # Check if config file exists
    if not Path(config_path).exists():
        logger.error(f"Configuration file not found: {config_path}")
        logger.info("Please create a connections.yaml file or specify a different path")
        sys.exit(1)

    # Create and run server
    logger.info(f"Loading connections from {config_path}")
    server = ReadOnlySQLServer(config_path)
    server.run()


if __name__ == "__main__":
    main()
