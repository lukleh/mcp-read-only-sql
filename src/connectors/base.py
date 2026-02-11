from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import asyncio
import sys
import contextvars
from contextlib import asynccontextmanager, contextmanager

from ..config import Connection, Server, SSHTunnelConfig
from ..utils.ssh_tunnel import SSHTunnel
from ..utils.timeout_wrapper import with_hard_timeout, HardTimeoutError


class ConnectionTimeoutError(Exception):
    """Raised when a connection or query times out"""
    pass


class DataSizeLimitError(Exception):
    """Raised when returned data exceeds size limit"""
    pass


class BaseConnector(ABC):
    """Base class for database connectors"""

    # Default SSH timeout (used when not in Connection config)
    DEFAULT_SSH_TIMEOUT = 5  # seconds (SSH connection)

    def __init__(self, connection: Connection):
        """
        Initialize connector with validated Connection object.

        Args:
            connection: Validated Connection object
        """
        self.connection = connection
        self.name = connection.name
        self.servers = connection.servers
        self.database = connection.database
        self.allowed_databases = connection.allowed_databases
        self.username = connection.username
        self.password = connection.password
        self.ssh_config = connection.ssh_tunnel
        self.ssh_tunnel = None

        # Timeouts and limits
        self.query_timeout = connection.query_timeout
        self.connection_timeout = connection.connection_timeout
        if self.ssh_config and self.ssh_config.ssh_timeout:
            self.ssh_timeout = self.ssh_config.ssh_timeout
        else:
            self.ssh_timeout = self.DEFAULT_SSH_TIMEOUT
        # Hard timeout is the sum of all component timeouts
        self.hard_timeout = self.ssh_timeout + self.connection_timeout + self.query_timeout
        self.max_result_bytes = connection.max_result_bytes
        # Context-local flag to optionally disable result-size enforcement
        self._enforce_limit_var = contextvars.ContextVar("enforce_limit", default=True)

    @asynccontextmanager
    async def _get_ssh_tunnel(self, server: Optional[str] = None):
        """
        Context manager for SSH tunnel

        Args:
            server: Optional server specification to tunnel to
        """
        if self.ssh_config:
            # Get the server to connect to
            selected_server = self._select_server(server)

            # Pass SSHTunnelConfig and remote server info to tunnel
            tunnel = SSHTunnel(self.ssh_config, selected_server.host, selected_server.port)
            local_port = await tunnel.start()
            try:
                yield local_port
            finally:
                await tunnel.stop()
        else:
            yield None

    def _select_server(self, server: Optional[str] = None) -> Server:
        """
        Select a server from the configured list.

        Args:
            server: Optional server hostname. If None or empty, uses the first server
                   in the list.

        Returns:
            Server object

        Raises:
            ValueError: If specified server is not found in configured servers
        """
        if not self.servers:
            raise ValueError(f"Connection '{self.name}' has no servers configured")

        # If no server specified, use first one (default behavior)
        if server is None:
            return self.servers[0]

        # Parse the server specification (hostnames only)
        server_str = server.strip()
        if not server_str:
            return self.servers[0]

        requested_host = server_str

        # Direct host match (supports IPv6 literals containing colons)
        for srv in self.servers:
            if srv.host == requested_host:
                return srv

        if ':' in server_str:
            raise ValueError(
                f"Server specification '{server_str}' must be a hostname without port"
            )

        # Allow SSH display host to map back to localhost-style canonical hosts
        if self.ssh_config:
            ssh_host = self.ssh_config.host
            local_hosts = {"localhost", "127.0.0.1", "::1"}
            if requested_host == ssh_host:
                for srv in self.servers:
                    if srv.host in local_hosts:
                        return srv

        # No match found
        available_hosts: List[str] = []
        local_hosts = {"localhost", "127.0.0.1", "::1"}
        for srv in self.servers:
            display_host = srv.host
            if self.ssh_config and srv.host in local_hosts and self.ssh_config.host:
                display_host = self.ssh_config.host
            if display_host not in available_hosts:
                available_hosts.append(display_host)

        raise ValueError(
            f"Server '{server}' not found in connection '{self.name}'. "
            f"Available servers: {', '.join(available_hosts)}"
        )

    def _get_default_port(self) -> int:
        """Get default port for the database type"""
        return 5432  # Override in subclasses

    def _resolve_database(self, database: Optional[str] = None) -> str:
        """Resolve and validate database selection for this connection."""
        return self.connection.resolve_database(database)


    def _check_result_size(self, data: Any) -> int:
        """Check if result data size is within limits"""
        # Estimate size of the result
        size = sys.getsizeof(data)

        if isinstance(data, dict):
            for key, value in data.items():
                size += sys.getsizeof(key)
                if isinstance(value, (list, dict)):
                    size += self._check_result_size(value)
                else:
                    size += sys.getsizeof(value)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, (list, dict)):
                    size += self._check_result_size(item)
                else:
                    size += sys.getsizeof(item)

        if size > self.max_result_bytes:
            raise DataSizeLimitError(
                f"Result size ({size:,} bytes) exceeds maximum allowed "
                f"({self.max_result_bytes:,} bytes)"
            )

        return size

    def _effective_max_result_bytes(self) -> int:
        """Return the per-call effective max_result_bytes (0 when disabled)."""
        return self.max_result_bytes if self._enforce_limit_var.get() else 0

    @contextmanager
    def disable_result_limit(self):
        """Temporarily disable result-size enforcement for this task."""
        token = self._enforce_limit_var.set(False)
        try:
            yield
        finally:
            self._enforce_limit_var.reset(token)

    async def execute_query_with_timeout(self, query: str, database: Optional[str] = None, server: Optional[str] = None) -> str:
        """
        Execute a query with hard timeout protection.

        This method wraps the actual execute_query implementation with a hard timeout
        to prevent the MCP server from hanging indefinitely.

        Args:
            query: SQL query to execute
            database: Optional database to use (overrides configured database)
            server: Optional server hostname

        Returns TSV string on success, raises exception on error.
        """
        # Call the actual implementation with hard timeout
        result = await with_hard_timeout(
            self.execute_query(query, database, server),
            self.hard_timeout,
            f"execute_query({query[:50]}...)"
        )
        return result

    @abstractmethod
    async def execute_query(self, query: str, database: Optional[str] = None, server: Optional[str] = None) -> str:
        """
        Execute a read-only query and return TSV results (implementation-specific)

        Args:
            query: SQL query to execute
            database: Optional database to use (overrides configured database)
            server: Optional server hostname
        """
        pass

    async def test_connection(self) -> bool:
        """Test if connection is working"""
        try:
            await self.execute_query("SELECT 1")
            return True
        except Exception:
            return False
