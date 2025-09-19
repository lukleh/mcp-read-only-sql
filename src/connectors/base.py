from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import asyncio
import sys
from contextlib import asynccontextmanager

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

    # Default limits
    DEFAULT_QUERY_TIMEOUT = 10  # seconds (database-level)
    DEFAULT_CONNECTION_TIMEOUT = 5  # seconds (database-level)
    DEFAULT_SSH_TIMEOUT = 5  # seconds (SSH connection)
    DEFAULT_MAX_RESULT_BYTES = 5 * 1024  # 5 KB

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.name = config.get("connection_name", "unnamed")
        self.servers = config.get("servers", [])

        # Get database with proper defaults based on type
        self.database = config.get("db")
        if not self.database:
            # Use type-specific default database
            conn_type = config.get("type", "").lower()
            if conn_type == "postgresql":
                self.database = "postgres"
            elif conn_type == "clickhouse":
                self.database = "default"
            else:
                # For unknown types, still require explicit database
                raise ValueError(f"Database 'db' is required for connection {self.name}")

        self.username = config.get("username", "")
        self.password = config.get("password", "")
        self.ssh_config = config.get("ssh_tunnel")
        self.ssh_tunnel = None

        # Security settings
        self.query_timeout = config.get("query_timeout", self.DEFAULT_QUERY_TIMEOUT)
        self.connection_timeout = config.get("connection_timeout", self.DEFAULT_CONNECTION_TIMEOUT)
        self.ssh_timeout = self.DEFAULT_SSH_TIMEOUT
        if self.ssh_config:
            self.ssh_timeout = self.ssh_config.get("ssh_timeout", self.DEFAULT_SSH_TIMEOUT)
        # Hard timeout is the sum of all component timeouts
        self.hard_timeout = self.ssh_timeout + self.connection_timeout + self.query_timeout
        self.max_result_bytes = config.get("max_result_bytes", self.DEFAULT_MAX_RESULT_BYTES)

    @asynccontextmanager
    async def _get_ssh_tunnel(self):
        """Context manager for SSH tunnel"""
        if self.ssh_config and self.ssh_config.get("enabled", True):
            # Get the server to connect to
            server = self._select_server()
            # Add remote host/port to SSH config
            ssh_config = self.ssh_config.copy()
            ssh_config["remote_host"] = server["host"]
            ssh_config["remote_port"] = server["port"]

            tunnel = SSHTunnel(ssh_config)
            local_port = await tunnel.start()
            try:
                yield local_port
            finally:
                await tunnel.stop()
        else:
            yield None

    def _select_server(self) -> Dict[str, Any]:
        """Get the first server from the list"""
        if not self.servers:
            return {"host": "localhost", "port": self._get_default_port()}
        return self.servers[0]  # Always use the first server

    def _get_default_port(self) -> int:
        """Get default port for the database type"""
        return 5432  # Override in subclasses


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

    async def execute_query_with_timeout(self, query: str, database: Optional[str] = None) -> str:
        """
        Execute a query with hard timeout protection.

        This method wraps the actual execute_query implementation with a hard timeout
        to prevent the MCP server from hanging indefinitely.

        Returns TSV string on success, raises exception on error.
        """
        # Call the actual implementation with hard timeout
        result = await with_hard_timeout(
            self.execute_query(query, database),
            self.hard_timeout,
            f"execute_query({query[:50]}...)"
        )
        return result

    @abstractmethod
    async def execute_query(self, query: str, database: Optional[str] = None) -> str:
        """Execute a read-only query and return TSV results (implementation-specific)"""
        pass

    async def test_connection(self) -> bool:
        """Test if connection is working"""
        try:
            await self.execute_query("SELECT 1")
            return True
        except Exception:
            return False