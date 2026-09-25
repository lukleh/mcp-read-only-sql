import asyncio
import logging
from contextlib import asynccontextmanager, closing
from io import IOBase
from pathlib import Path
from typing import cast

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError

from ...config import Connection
from ...errors import ConnectorError
from ...utils.sql_guard import sanitize_read_only_sql
from ...utils.ssh_tunnel_cli import CLISSHTunnel
from ...utils.tsv_formatter import format_tsv_line
from ..base import BaseConnector
from .settings import client_settings, without_refused

logger = logging.getLogger(__name__)


class ClickHousePythonConnector(BaseConnector):
    """ClickHouse connector using clickhouse-connect (supports both HTTP and native protocols)"""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        # Client-side settings this login accepted when the first client was
        # created; None until then (see _open_client).
        self._accepted_settings: dict[str, object] | None = None

    def _get_default_port(self) -> int:
        return 8123  # HTTP port (clickhouse-connect default)

    @asynccontextmanager
    async def _get_ssh_tunnel(self, server: str | None = None):
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
            logger.debug(
                "Changing SSH tunnel remote port from 9000 to 8123 for clickhouse-connect"
            )
            remote_port = 8123
        elif remote_port == 9440:
            logger.debug(
                "Changing SSH tunnel remote port from 9440 to 8443 for clickhouse-connect"
            )
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
                "SSH: Paramiko authentication failed for %s; falling back to system ssh tunnel",
                remote_host,
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

    async def execute_query(
        self, query: str, database: str | None = None, server: str | None = None
    ) -> str:
        """Execute a read-only query using clickhouse-connect and return TSV"""
        return await self._run_executor_query(
            self._execute_sync_query, query, database=database, server=server
        )

    async def execute_query_to_file(
        self,
        query: str,
        output_path: Path,
        database: str | None = None,
        server: str | None = None,
    ) -> None:
        """Execute a read-only query using clickhouse-connect and stream TSV to a file."""
        await self._run_executor_query(
            self._execute_sync_query_to_file,
            query,
            database=database,
            server=server,
            output_path=str(output_path),
        )

    async def _run_executor_query(
        self,
        worker,
        query: str,
        database: str | None = None,
        server: str | None = None,
        *,
        output_path: str | None = None,
    ):
        """Resolve connection settings and run a synchronous worker in the executor."""
        sanitized_query = sanitize_read_only_sql(query)
        selected_server = self._select_server(server)
        original_port = (
            selected_server.port
        )  # Track the original port for protocol detection

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

                # Use specified database or configured database (validated)
                db_name = self._resolve_database(database)
                # Run synchronous clickhouse-connect in executor with timeout
                loop = asyncio.get_event_loop()
                worker_args = [
                    host,
                    port,
                    db_name,
                    sanitized_query,
                    original_port,  # Pass original port for protocol detection
                    is_ssh_tunnel,
                ]
                if output_path is not None:
                    worker_args.append(output_path)
                return await asyncio.wait_for(
                    loop.run_in_executor(
                        None,
                        worker,
                        *worker_args,
                    ),
                    timeout=total_timeout,
                )

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
            raise ConnectorError(f"ClickHouse: {e}")
        # Let other exceptions (programming errors) propagate unchanged

    def _resolve_client_endpoint(
        self, port: int, original_port: int | None, is_ssh_tunnel: bool
    ) -> tuple[str, int]:
        """Map configured ClickHouse ports onto the HTTP(S) client endpoint."""
        config_port = original_port if original_port is not None else port

        if config_port == 8123:
            interface = "http"
            if not is_ssh_tunnel:
                port = 8123
        elif config_port == 8443:
            interface = "https"
            if not is_ssh_tunnel:
                port = 8443
        elif config_port == 9000:
            interface = "http"
            if not is_ssh_tunnel:
                port = 8123
                logger.debug("Switching from native port 9000 to HTTP port 8123")
        elif config_port == 9440:
            interface = "https"
            if not is_ssh_tunnel:
                port = 8443
                logger.debug(
                    "Switching from secure native port 9440 to HTTPS port 8443"
                )
        else:
            interface = "http"
            logger.debug(f"Unknown port {config_port}, assuming HTTP protocol")

        return interface, port

    def _create_client(
        self,
        host: str,
        port: int,
        database: str,
        original_port: int | None,
        is_ssh_tunnel: bool,
        *,
        settings: dict[str, object] | None = None,
    ):
        """Create a configured clickhouse-connect client for this endpoint."""
        interface, resolved_port = self._resolve_client_endpoint(
            port, original_port, is_ssh_tunnel
        )
        return clickhouse_connect.get_client(
            interface=interface,
            host=host,
            port=resolved_port,
            database=database,
            username=self.username,
            password=self.password,
            connect_timeout=self.connection_timeout,
            query_limit=0,  # No limit on query result size (we handle it ourselves)
            settings=settings,
        )

    def _open_client(
        self,
        host: str,
        port: int,
        database: str,
        original_port: int | None,
        is_ssh_tunnel: bool,
    ) -> tuple[Client, dict[str, object] | None]:
        """Create the client with the settings this login accepts.

        clickhouse-connect validates client-side settings against
        ``system.settings`` for this login when the client is created, so the
        decision never involves a statement. A profile that already sets
        ``readonly`` refuses settings by name; each refused one is dropped and
        the result is remembered for the connector's lifetime. Returns the
        client and the settings to send with each query, ``None`` when the
        login accepts none of them.
        """
        settings = self._accepted_settings
        if settings is not None:
            client = self._create_client(
                host, port, database, original_port, is_ssh_tunnel,
                settings=settings or None,
            )
            return client, settings or None

        settings = client_settings(self.query_timeout)
        while True:
            try:
                client = self._create_client(
                    host, port, database, original_port, is_ssh_tunnel,
                    settings=settings or None,
                )
            except ClickHouseError as exc:
                reduced = without_refused(settings, str(exc))
                if reduced is None:
                    raise
                logger.warning(
                    "ClickHouse: the profile of user %s refuses the client-side "
                    "%s setting; continuing without it (%s)",
                    self.username,
                    set(settings) - set(reduced),
                    exc,
                )
                settings = reduced
                continue
            self._accepted_settings = settings
            return client, settings or None

    def _execute_sync_query(
        self,
        host: str,
        port: int,
        database: str,
        query: str,
        original_port: int | None = None,
        is_ssh_tunnel: bool = False,
        output_path: str | None = None,
    ) -> str:
        """Execute query synchronously and return TSV output."""
        if output_path is not None:
            self._execute_sync_query_to_file(
                host, port, database, query, original_port, is_ssh_tunnel, output_path
            )
            return ""

        client = None
        try:
            client, _settings = self._open_client(
                host, port, database, original_port, is_ssh_tunnel
            )

            # Execute query and get result
            result = client.query(query, column_oriented=False)

            # Get column names and data
            columns = (
                list(result.column_names) if hasattr(result, "column_names") else []
            )
            data = result.result_rows if hasattr(result, "result_rows") else []

            lines = []

            if columns:
                lines.append(format_tsv_line(columns))

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
                        values = values[: len(columns)]

                lines.append(format_tsv_line(values))

            return "\n".join(lines)
        finally:
            if client:
                client.close()

    def _execute_sync_query_to_file(
        self,
        host: str,
        port: int,
        database: str,
        query: str,
        original_port: int | None = None,
        is_ssh_tunnel: bool = False,
        output_path: str = "",
    ) -> None:
        """Execute query synchronously and stream raw TSV output to a file."""
        client = None
        try:
            client, settings = self._open_client(
                host, port, database, original_port, is_ssh_tunnel
            )

            # The HTTP client returns a file-like response; cast for closing().
            raw_stream = cast(
                IOBase,
                client.raw_stream(
                    query, fmt="TabSeparatedWithNames", settings=settings
                ),
            )
            with Path(output_path).open("wb") as handle, closing(raw_stream) as stream:
                while True:
                    chunk = stream.read(64 * 1024)
                    if not chunk:
                        break
                    if isinstance(chunk, str):
                        chunk = chunk.encode("utf-8")
                    handle.write(chunk)
        finally:
            if client:
                client.close()
