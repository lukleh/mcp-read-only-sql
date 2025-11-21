import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional, Tuple

import clickhouse_connect
from clickhouse_connect.driver.exceptions import ClickHouseError

from ..base import BaseConnector
from ...utils.tsv_formatter import format_tsv_line
from ...utils.ssh_tunnel_cli import CLISSHTunnel

logger = logging.getLogger(__name__)


class ClickHousePythonConnector(BaseConnector):
    """ClickHouse connector using clickhouse-connect (supports both HTTP and native protocols)"""

    def _get_default_port(self) -> int:
        return 8123  # HTTP port (clickhouse-connect default)

    @asynccontextmanager
    async def _get_ssh_tunnel(self, server: Optional[str] = None):
        """Override SSH tunnel to ensure we tunnel to correct HTTP/HTTPS port for clickhouse-connect"""
        if not self.ssh_config:
            yield None
            return

        # Get the server to connect to
        selected_server = self._select_server(server)

        # Map native ports to HTTP/HTTPS ports for SSH tunnel
        remote_port = selected_server.port
        remote_host = selected_server.host
        if remote_port == 9000:
            logger.debug("Changing SSH tunnel remote port from 9000 to 8123 for clickhouse-connect")
            remote_port = 8123
        elif remote_port == 9440:
            logger.debug("Changing SSH tunnel remote port from 9440 to 8443 for clickhouse-connect")
            remote_port = 8443
        # Ports 8123 and 8443 stay as-is

        from ...utils.ssh_tunnel import SSHTunnel

        # Attempt Paramiko-based tunnel first
        tunnel = SSHTunnel(self.ssh_config, remote_host, remote_port)
        try:
            local_port = await tunnel.start()
        except RuntimeError as exc:
            message = str(exc)
            if "SSH: Authentication failed" not in message:
                raise
            logger.info(
                "SSH: Paramiko authentication failed for %s; falling back to system ssh tunnel", remote_host
            )
        except TimeoutError:
            raise
        else:
            try:
                yield local_port
            finally:
                await tunnel.stop()
            return

        # Fall back to CLI-based tunnel (system ssh) if Paramiko cannot authenticate
        cli_tunnel = CLISSHTunnel(self.ssh_config, remote_host, remote_port)
        local_port = await cli_tunnel.start()
        try:
            yield local_port
        finally:
            await cli_tunnel.stop()

    async def execute_query(self, query: str, database: Optional[str] = None, server: Optional[str] = None) -> str:
        """Execute a read-only query using clickhouse-connect and return TSV"""
        selected_server = self._select_server(server)
        original_port = selected_server.port  # Track the original port for protocol detection

        total_timeout = self.connection_timeout + self.query_timeout

        try:
            async with self._get_ssh_tunnel(server) as local_port:
                # Use SSH tunnel port if available
                if local_port:
                    host = "127.0.0.1"
                    port = local_port
                    # Pass original port so we know if HTTPS is needed
                    is_ssh_tunnel = True
                else:
                    host = selected_server.host
                    port = selected_server.port
                    is_ssh_tunnel = False

                # Use specified database or configured database
                db_name = database or self.database
                # Run synchronous clickhouse-connect in executor with timeout
                loop = asyncio.get_event_loop()
                max_bytes = self._effective_max_result_bytes()
                tsv_output, truncated = await asyncio.wait_for(
                    loop.run_in_executor(
                        None,
                        self._execute_sync_query,
                        host,
                        port,
                        db_name,
                        query,
                        max_bytes,
                        original_port,  # Pass original port for protocol detection
                        is_ssh_tunnel
                    ),
                    timeout=total_timeout
                )
                if truncated:
                    notice = f"[RESULT TRUNCATED: exceeded max_result_bytes={self.max_result_bytes} bytes]"
                    tsv_output = (tsv_output + "\n" if tsv_output else "") + notice

                return tsv_output

        except TimeoutError as e:
            # Re-raise SSH timeout as-is
            if "SSH:" in str(e):
                raise
            # Otherwise it's a query timeout from asyncio.wait_for
            raise TimeoutError(
                f"ClickHouse: Operation exceeded combined timeout of {total_timeout} seconds"
            )
        except ClickHouseError as e:
            # ClickHouse-specific database errors get prefixed
            logger.error(f"ClickHouse database error: {e}")
            raise RuntimeError(f"ClickHouse: {e}")
        # Let other exceptions (programming errors) propagate unchanged

    def _execute_sync_query(
        self,
        host: str,
        port: int,
        database: str,
        query: str,
        max_result_bytes: int,
        original_port: int = None,
        is_ssh_tunnel: bool = False,
    ) -> Tuple[str, bool]:
        """Execute query synchronously and stream results as TSV within size limits."""
        client = None
        try:
            # Use original_port if provided (for SSH tunnels), otherwise use port
            config_port = original_port if original_port is not None else port

            # Determine interface and actual port based on the configured port
            if config_port == 8123:
                interface = 'http'
                if not is_ssh_tunnel:
                    port = 8123
            elif config_port == 8443:
                interface = 'https'
                if not is_ssh_tunnel:
                    port = 8443
            elif config_port == 9000:
                # Native port - convert to HTTP
                interface = 'http'
                if not is_ssh_tunnel:
                    port = 8123
                    logger.debug(f"Switching from native port 9000 to HTTP port 8123")
            elif config_port == 9440:
                # Secure native port - convert to HTTPS
                interface = 'https'
                if not is_ssh_tunnel:
                    port = 8443
                    logger.debug(f"Switching from secure native port 9440 to HTTPS port 8443")
            else:
                # Unknown/custom port - assume HTTP
                interface = 'http'
                logger.debug(f"Unknown port {config_port}, assuming HTTP protocol")

            # Connect with read-only settings
            client = clickhouse_connect.get_client(
                interface=interface,
                host=host,
                port=port,
                database=database,
                username=self.username,
                password=self.password,
                connect_timeout=self.connection_timeout,
                query_limit=0,  # No limit on query result size (we handle it ourselves)
                settings={
                    'readonly': 1,  # ClickHouse read-only mode
                    'max_execution_time': self.query_timeout,
                }
            )

            # Execute query and get result
            result = client.query(query, column_oriented=False)

            # Get column names and data
            columns = result.column_names if hasattr(result, 'column_names') else []
            data = result.result_rows if hasattr(result, 'result_rows') else []

            lines = []
            truncated = False
            newline_bytes = len("\n".encode())
            total_bytes = 0
            max_bytes = max_result_bytes or 0
            enforce_limit = max_bytes > 0

            def append_line(line: str) -> bool:
                nonlocal total_bytes, truncated
                encoded = line.encode()
                additional = len(encoded) if not lines else newline_bytes + len(encoded)
                if enforce_limit and lines and (total_bytes + additional) > max_bytes:
                    truncated = True
                    return False
                lines.append(line)
                total_bytes += additional
                if enforce_limit and total_bytes > max_bytes:
                    truncated = True
                    return False
                return True

            if columns:
                header_line = format_tsv_line(columns)
                if not append_line(header_line):
                    return "\n".join(lines), True

            for row in data:
                if columns and isinstance(row, dict):
                    values = [row.get(col) for col in columns]
                elif isinstance(row, (list, tuple)):
                    values = list(row)
                else:
                    values = [row]

                if columns and len(values) != len(columns):
                    if len(values) < len(columns):
                        values.extend([None] * (len(columns) - len(values)))
                    else:
                        values = values[:len(columns)]

                line = format_tsv_line(values)
                if not append_line(line):
                    break

            return "\n".join(lines), truncated
        finally:
            if client:
                client.close()
