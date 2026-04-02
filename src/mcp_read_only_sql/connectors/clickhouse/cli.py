import asyncio
import logging
import os
from contextlib import asynccontextmanager, nullcontext, suppress
from pathlib import Path
from typing import Optional

from ..base_cli import BaseCLIConnector
from ...utils.ssh_tunnel_cli import CLISSHTunnel
from ...utils.sql_guard import ReadOnlyQueryError, sanitize_read_only_sql
from ...utils.tsv_formatter import write_tsv_text_line

logger = logging.getLogger(__name__)


class ClickHouseCLIConnector(BaseCLIConnector):
    """ClickHouse connector using clickhouse-client CLI tool"""

    def _get_default_port(self) -> int:
        # clickhouse-client uses native protocol port, not HTTP port
        return 9000

    @asynccontextmanager
    async def _get_ssh_tunnel(self, server: Optional[str] = None):
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
        self, query: str, database: Optional[str] = None, server: Optional[str] = None
    ) -> str:
        """Execute a read-only query using clickhouse-client and return raw TSV output"""
        return await self._run_query(
            query, database=database, server=server, output_path=None
        )

    async def execute_query_to_file(
        self,
        query: str,
        output_path: Path,
        database: Optional[str] = None,
        server: Optional[str] = None,
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
        database: Optional[str] = None,
        server: Optional[str] = None,
        output_path: Optional[Path] = None,
    ) -> Optional[str]:
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

            # Build clickhouse-client command with read-only enforcement
            cmd = [
                "clickhouse-client",
                "--host",
                host,
                "--port",
                str(port),
                "--user",
                self.username,
                "--database",
                db_name,
                "--readonly",
                "1",  # Enforce read-only mode at database level
                "--max_execution_time",
                str(self.query_timeout),  # Query timeout in seconds
                "--connect_timeout",
                str(self.connection_timeout),  # Connection timeout
                "--format",
                "TabSeparatedWithNames",  # Use TSV format with headers
                "--query",
                sanitized_query,
            ]

            # Add --secure flag for TLS ports (9440)
            if port == 9440:
                cmd.insert(1, "--secure")  # Insert after "clickhouse-client"
                logger.debug("Adding --secure flag for TLS port 9440")

            # Add password if provided
            if self.password:
                cmd.append("--ask-password")

            # Preserve PATH in environment
            env = os.environ.copy()

            try:
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

                stdout = process.stdout
                stderr_task = asyncio.create_task(process.stderr.read())

                lines = [] if output_path is None else None
                pending_line = None
                wrote_content = False

                def emit_line(line: str) -> None:
                    nonlocal wrote_content
                    if output_path is None:
                        lines.append(line)
                    else:
                        wrote_content = write_tsv_text_line(handle, line, wrote_content)

                loop = asyncio.get_event_loop()
                deadline = loop.time() + self.query_timeout

                async def read_line_with_timeout():
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        raise asyncio.TimeoutError
                    return await asyncio.wait_for(stdout.readline(), timeout=remaining)

                with (
                    Path(output_path).open("w", encoding="utf-8", newline="")
                    if output_path is not None
                    else nullcontext()
                ) as handle:
                    try:
                        while True:
                            line_bytes = await read_line_with_timeout()
                            if not line_bytes:
                                break

                            line = line_bytes.decode(errors="replace").rstrip("\r\n")

                            if pending_line is not None:
                                emit_line(pending_line)
                            pending_line = line

                    except asyncio.TimeoutError:
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

                try:
                    await asyncio.wait_for(process.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    logger.error("clickhouse-client process did not terminate cleanly")
                    process.kill()
                    await process.wait()

                if not stderr_task.done():
                    stderr = await stderr_task
                else:
                    stderr = stderr_task.result()

                returncode = process.returncode
                if returncode is None:
                    logger.debug(
                        "clickhouse-client process still running after wait(); treating as successful termination"
                    )
                if returncode not in (0, None):
                    error_msg = stderr.decode() if stderr else "Unknown error"
                    logger.error(f"clickhouse-client error: {error_msg}")
                    raise RuntimeError(f"clickhouse-client: {error_msg}")

                if pending_line not in (None, ""):
                    emit_line(pending_line)

                if output_path is None:
                    return "\n".join(lines)
                return None

            except FileNotFoundError:
                raise FileNotFoundError(
                    "clickhouse-client: command not found. Please install ClickHouse client tools."
                )
            except ReadOnlyQueryError:
                raise
            except asyncio.TimeoutError as exc:
                logger.error(f"Query execution error: {exc}")
                raise
            except Exception as e:
                logger.error(f"Query execution error: {e}")
                # Re-raise with clickhouse-client prefix if not already prefixed
                if not str(e).startswith("clickhouse-client:"):
                    raise RuntimeError(f"clickhouse-client: {e}")
                raise
