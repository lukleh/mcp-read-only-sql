import asyncio
import logging
import os
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path

from ...errors import ConnectorError
from ...utils.sql_guard import (
    SHADOW_GUARD_PREFIX,
    ReadOnlyQueryError,
    postgresql_shadow_guard_block,
    postgresql_shadow_query,
    sanitize_postgresql_read_only_sql,
)
from ...utils.tsv_formatter import write_tsv_text_line
from ..base_cli import BaseCLIConnector

logger = logging.getLogger(__name__)


class PostgreSQLCLIConnector(BaseCLIConnector):
    """PostgreSQL connector using psql CLI tool"""

    def _get_default_port(self) -> int:
        return 5432

    async def execute_query(
        self, query: str, database: str | None = None, server: str | None = None
    ) -> str:
        """Execute a read-only query using psql and return raw TSV output"""
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
        """Execute a read-only query using psql and stream TSV to a file."""
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
        """Run the psql command and optionally stream output to a managed file."""
        sanitized_query = sanitize_postgresql_read_only_sql(
            query, self.connection.allowed_functions
        )
        shadow_query = postgresql_shadow_query(
            query, self.connection.allowed_functions
        )
        shadow_guard = (
            postgresql_shadow_guard_block(shadow_query) + ";" if shadow_query else ""
        )
        selected_server = self._select_server(server)

        async with self._get_ssh_tunnel(server) as local_port:
            # Use SSH tunnel port if available
            if local_port:
                host = "127.0.0.1"
                port = local_port
            else:
                host = selected_server.host
                port = selected_server.port

            # Use specified database or configured database (validated)
            db_name = self._resolve_database(database)

            # Build psql command with read-only enforcement
            # Wrap the query in a read-only transaction
            wrapped_query = f"""
                BEGIN;
                SET TRANSACTION READ ONLY;
                SET LOCAL statement_timeout = {self.query_timeout * 1000};
                {shadow_guard}
                {sanitized_query};
                COMMIT;
            """

            # Build psql command with individual parameters.
            # Resolve the client binary explicitly so installs that are not on
            # PATH (e.g. Homebrew keg-only libpq on macOS) still work.
            cmd = [
                self._resolve_binary("psql"),
                "--single-transaction",
                "-v",
                "ON_ERROR_STOP=1",
                "-h",
                host,  # Host
                "-p",
                str(port),  # Port
                "-d",
                db_name,  # Database
                "-U",
                self.username,  # Username
                "-A",  # Unaligned output mode
                "-F",
                "\t",  # Use tab as field separator
                "-c",
                wrapped_query,  # Query to execute
            ]

            async def run_psql(env_vars: dict[str, str]) -> str | None:
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=env_vars,
                )

                stdout = process.stdout
                stderr_stream = process.stderr
                if stdout is None or stderr_stream is None:
                    process.kill()
                    await process.wait()
                    raise ConnectorError("psql: failed to create subprocess pipes")

                stderr_task = asyncio.create_task(stderr_stream.read())
                lines: list[str] = []
                loop = asyncio.get_event_loop()
                deadline = loop.time() + self.query_timeout

                async def stream_output(emit_line: Callable[[str], None]) -> str | None:
                    pending_line: str | None = None

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

                            line = line_bytes.decode(errors="replace").rstrip("\r\n")

                            if line in ("BEGIN", "SET", "DO", "COMMIT", "ROLLBACK"):
                                continue
                            if (
                                line.startswith("(")
                                and line.endswith(")")
                                and " row" in line
                            ):
                                continue

                            if pending_line is not None:
                                emit_line(pending_line)
                            pending_line = line
                    except TimeoutError:
                        logger.warning("Query timeout - terminating psql process")
                        process.kill()
                        with suppress(asyncio.CancelledError):
                            stderr_task.cancel()
                            await stderr_task
                        with suppress(asyncio.TimeoutError):
                            await asyncio.wait_for(process.wait(), timeout=1.0)
                        raise TimeoutError(
                            f"psql: Query timeout after {self.query_timeout}s"
                        )
                    return pending_line

                async def finalize_process(
                    emit_line: Callable[[str], None], pending_line: str | None
                ) -> None:
                    try:
                        await asyncio.wait_for(process.wait(), timeout=1.0)
                    except TimeoutError:
                        logger.error("psql process did not terminate cleanly")
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
                            "psql process still running after wait(); treating as successful termination"
                        )
                    if returncode not in (0, None):
                        error_msg = stderr.decode() if stderr else "Unknown error"
                        if SHADOW_GUARD_PREFIX in error_msg:
                            start = error_msg.index(SHADOW_GUARD_PREFIX)
                            raise ReadOnlyQueryError(
                                error_msg[start:].splitlines()[0].strip()
                            )
                        logger.error(f"psql error: {error_msg}")
                        raise ConnectorError(f"psql: {error_msg}")

                    if pending_line not in (None, ""):
                        emit_line(pending_line)

                if output_path is None:
                    pending_line = await stream_output(lines.append)
                    await finalize_process(lines.append, pending_line)
                    return "\n".join(lines)

                wrote_content = False

                def emit_file_line(line: str) -> None:
                    nonlocal wrote_content
                    wrote_content = write_tsv_text_line(handle, line, wrote_content)

                assert output_path is not None
                with Path(output_path).open(  # noqa: ASYNC230 -- local file writes are fast; async file IO would add a dependency for no benefit
                    "w", encoding="utf-8", newline=""
                ) as handle:
                    pending_line = await stream_output(emit_file_line)
                    await finalize_process(emit_file_line, pending_line)
                return None

            use_pgoptions = getattr(self.connection, "cli_requires_pgoptions", True)
            attempts = [True] if not use_pgoptions else [True, False]

            for first_attempt in attempts:
                env = os.environ.copy()
                env["PGPASSWORD"] = self.password
                env["PGCONNECT_TIMEOUT"] = str(self.connection_timeout)
                if first_attempt and use_pgoptions:
                    env["PGOPTIONS"] = "-c default_transaction_read_only=on"
                else:
                    env.pop("PGOPTIONS", None)

                try:
                    return await run_psql(env)
                except RuntimeError as exc:
                    message = str(exc).lower()
                    if (
                        first_attempt
                        and use_pgoptions
                        and "unsupported startup parameter" in message
                    ):
                        logger.warning(
                            "psql: remote server rejected default_transaction_read_only; retrying without PGOPTIONS"
                        )
                        continue
                    raise
                except FileNotFoundError:
                    raise FileNotFoundError(
                        "psql: command not found. Please install PostgreSQL client tools."
                    )
                except ReadOnlyQueryError:
                    raise
                except TimeoutError as exc:
                    logger.error(f"Query execution error: {exc}")
                    raise
                except OSError as e:
                    # Spawning or talking to the psql process failed
                    logger.error(f"Query execution error: {e}")
                    raise ConnectorError(f"psql: {e}") from e
