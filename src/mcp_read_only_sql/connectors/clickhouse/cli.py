import asyncio
import logging
import os
import re
from collections.abc import Callable
from contextlib import asynccontextmanager, suppress
from pathlib import Path

from ...errors import ConnectorError
from ...utils.sql_guard import ReadOnlyQueryError, sanitize_read_only_sql
from ...utils.ssh_tunnel_cli import CLISSHTunnel
from ...utils.tsv_formatter import write_tsv_text_line
from ..base_cli import BaseCLIConnector
from .settings import client_settings, refuses_client_settings

logger = logging.getLogger(__name__)

# clickhouse-client writes this prompt to stderr under --ask-password; it is
# noise in front of the actual error text.
_PASSWORD_PROMPT = re.compile(r"^Password for user \([^)]*\):\s*")


class ClickHouseCLIConnector(BaseCLIConnector):
    """ClickHouse connector using clickhouse-client CLI tool"""

    def _get_default_port(self) -> int:
        # clickhouse-client uses native protocol port, not HTTP port
        return 9000

    @asynccontextmanager
    async def _get_ssh_tunnel(self, server: str | None = None):
        """Override SSH tunnel to ensure we tunnel to native port for clickhouse-client"""
        if self.ssh_config:
            # Get the server to connect to
            selected_server = self._select_server(server)

            # For ClickHouse CLI, we need native port (9000), not HTTP port (8123)
            # If config specifies port 8123, change it to 9000 for the SSH tunnel
            remote_port = selected_server.port
            remote_host = selected_server.host
            if remote_port == 8123:
                logger.debug(
                    "Changing SSH tunnel remote port from 8123 to 9000 for clickhouse-client"
                )
                remote_port = 9000
            elif remote_port == 8443:
                logger.debug(
                    "Changing SSH tunnel remote port from 8443 to 9440 for clickhouse-client"
                )
                remote_port = 9440

            tunnel = CLISSHTunnel(self.ssh_config, remote_host, remote_port)
            local_port = await tunnel.start()
            try:
                yield local_port
            finally:
                await tunnel.stop()
        else:
            yield None

    async def execute_query(
        self, query: str, database: str | None = None, server: str | None = None
    ) -> str:
        """Execute a read-only query using clickhouse-client and return raw TSV output"""
        result = await self._run_query(
            query, database=database, server=server, output_path=None
        )
        return result if result is not None else ""

    async def execute_query_to_file(
        self,
        query: str,
        output_path: Path,
        database: str | None = None,
        server: str | None = None,
    ) -> None:
        """Execute a read-only query using clickhouse-client and stream TSV to a file."""
        await self._run_query(
            query,
            database=database,
            server=server,
            output_path=output_path,
        )

    async def _run_query(
        self,
        query: str,
        database: str | None = None,
        server: str | None = None,
        output_path: Path | None = None,
    ) -> str | None:
        """Run clickhouse-client and optionally stream output to a managed file."""
        sanitized_query = sanitize_read_only_sql(query)
        selected_server = self._select_server(server)

        async with self._get_ssh_tunnel(server) as local_port:
            # Use SSH tunnel port if available
            if local_port:
                host = "127.0.0.1"
                port = local_port
            else:
                host = selected_server.host
                port = selected_server.port

                # For direct connections, if port is HTTP (8123/8443), convert to native
                if port == 8123:
                    logger.debug(
                        "Changing port from 8123 to 9000 for clickhouse-client direct connection"
                    )
                    port = 9000
                elif port == 8443:
                    logger.debug(
                        "Changing port from 8443 to 9440 for clickhouse-client direct connection"
                    )
                    port = 9440

            # Use specified database or configured database (validated)
            db_name = self._resolve_database(database)

            # Build clickhouse-client command with read-only enforcement.
            # Resolve the client binary explicitly so installs that are not on
            # PATH still work (mirrors the psql connector).
            binary = self._resolve_binary("clickhouse-client")
            env = os.environ.copy()

            def build_command(with_client_settings: bool) -> list[str]:
                cmd = [binary]
                if port == 9440:
                    cmd.append("--secure")  # TLS native port
                    logger.debug("Adding --secure flag for TLS port 9440")
                cmd += [
                    "--host",
                    host,
                    "--port",
                    str(port),
                    "--user",
                    self.username,
                    "--database",
                    db_name,
                    "--connect_timeout",
                    str(self.connection_timeout),  # Connection timeout
                    "--format",
                    "TabSeparatedWithNames",  # Use TSV format with headers
                    "--query",
                    sanitized_query,
                ]
                if with_client_settings:
                    # Server-side read-only mode and query timeout. A login
                    # whose profile already sets readonly refuses these; the
                    # loop below then retries without them.
                    for name, value in client_settings(self.query_timeout).items():
                        cmd += [f"--{name}", str(value)]
                if self.password:
                    cmd.append("--ask-password")
                return cmd

            async def run_client(cmd: list[str]) -> str | None:
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=env,
                )

                stdin = getattr(process, "stdin", None)
                if self.password and stdin is not None:
                    stdin.write(f"{self.password}\n".encode())
                    drain = getattr(stdin, "drain", None)
                    if callable(drain):
                        drain_result = drain()
                        if asyncio.iscoroutine(drain_result):
                            await drain_result
                    stdin.close()
                elif self.password:
                    process.kill()
                    await process.wait()
                    raise ConnectorError(
                        "clickhouse-client: failed to create subprocess stdin pipe"
                    )

                stdout = process.stdout
                stderr_stream = process.stderr
                if stdout is None or stderr_stream is None:
                    process.kill()
                    await process.wait()
                    raise ConnectorError(
                        "clickhouse-client: failed to create subprocess pipes"
                    )

                stderr_task = asyncio.create_task(stderr_stream.read())
                lines: list[str] = []
                loop = asyncio.get_event_loop()
                deadline = loop.time() + self.query_timeout

                async def stream_output(emit_line: Callable[[str], None]) -> None:
                    # Every stdout line is data: the header, then one line per
                    # row. An empty line is a row whose only column is empty.
                    async def read_line_with_timeout() -> bytes:
                        remaining = deadline - loop.time()
                        if remaining <= 0:
                            raise TimeoutError
                        return await asyncio.wait_for(
                            stdout.readline(), timeout=remaining
                        )

                    try:
                        while True:
                            line_bytes = await read_line_with_timeout()
                            if not line_bytes:
                                break
                            emit_line(line_bytes.decode(errors="replace").rstrip("\r\n"))
                    except TimeoutError:
                        logger.warning(
                            "Query timeout - terminating clickhouse-client process"
                        )
                        process.kill()
                        with suppress(asyncio.CancelledError):
                            stderr_task.cancel()
                            await stderr_task
                        # Wait for process to clean up subprocess transport
                        with suppress(asyncio.TimeoutError):
                            await asyncio.wait_for(process.wait(), timeout=1.0)
                        raise TimeoutError(
                            f"clickhouse-client: Query timeout after {self.query_timeout}s"
                        )

                async def finalize_process() -> None:
                    try:
                        await asyncio.wait_for(process.wait(), timeout=1.0)
                    except TimeoutError:
                        logger.error(
                            "clickhouse-client process did not terminate cleanly"
                        )
                        process.kill()
                        await process.wait()

                    try:
                        stderr = (
                            await stderr_task
                            if not stderr_task.done()
                            else stderr_task.result()
                        )
                    except asyncio.CancelledError:
                        stderr = b""

                    returncode = process.returncode
                    if returncode is None:
                        logger.debug(
                            "clickhouse-client process still running after wait(); treating as successful termination"
                        )
                    if returncode not in (0, None):
                        error_msg = (
                            _PASSWORD_PROMPT.sub("", stderr.decode()).strip()
                            if stderr
                            else "Unknown error"
                        )
                        logger.error(f"clickhouse-client error: {error_msg}")
                        raise ConnectorError(f"clickhouse-client: {error_msg}")

                if output_path is None:
                    await stream_output(lines.append)
                    await finalize_process()
                    return "\n".join(lines)

                wrote_content = False

                def emit_file_line(line: str) -> None:
                    nonlocal wrote_content
                    wrote_content = write_tsv_text_line(handle, line, wrote_content)

                with Path(output_path).open(  # noqa: ASYNC230 -- local file writes are fast; async file IO would add a dependency for no benefit
                    "w", encoding="utf-8", newline=""
                ) as handle:
                    await stream_output(emit_file_line)
                    await finalize_process()
                return None

            for with_client_settings in (True, False):
                try:
                    return await run_client(build_command(with_client_settings))
                except FileNotFoundError:
                    raise FileNotFoundError(
                        "clickhouse-client: command not found. Please install ClickHouse client tools."
                    )
                except ReadOnlyQueryError:
                    raise
                except TimeoutError as exc:
                    logger.error(f"Query execution error: {exc}")
                    raise
                except ConnectorError as exc:
                    if with_client_settings and refuses_client_settings(str(exc)):
                        logger.warning(
                            "ClickHouse: the profile of user %s already enforces "
                            "read-only and refuses client-side settings; retrying "
                            "without readonly=1 and max_execution_time (%s)",
                            self.username,
                            exc,
                        )
                        continue
                    raise
                except OSError as e:
                    # Spawning or talking to the clickhouse-client process failed
                    logger.error(f"Query execution error: {e}")
                    raise ConnectorError(f"clickhouse-client: {e}") from e
            return None
