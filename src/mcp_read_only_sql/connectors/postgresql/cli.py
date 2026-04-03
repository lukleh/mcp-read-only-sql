import asyncio
import logging
import os
from contextlib import suppress
from pathlib import Path
from typing import Callable, Optional

from ..base_cli import BaseCLIConnector
from ...utils.sql_guard import sanitize_read_only_sql, ReadOnlyQueryError
from ...utils.tsv_formatter import write_tsv_text_line

logger = logging.getLogger(__name__)


class PostgreSQLCLIConnector(BaseCLIConnector):
    """PostgreSQL connector using psql CLI tool"""

    def _get_default_port(self) -> int:
        return 5432

    async def execute_query(
        self, query: str, database: Optional[str] = None, server: Optional[str] = None
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
        database: Optional[str] = None,
        server: Optional[str] = None,
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
        database: Optional[str] = None,
        server: Optional[str] = None,
        output_path: Optional[Path] = None,
    ) -> Optional[str]:
        """Run the psql command and optionally stream output to a managed file."""
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

            # Use specified database or configured database (validated)
            db_name = self._resolve_database(database)

            # Build psql command with read-only enforcement
            # Wrap the query in a read-only transaction
            wrapped_query = f"""
                BEGIN;
                SET TRANSACTION READ ONLY;
                SET LOCAL statement_timeout = {self.query_timeout * 1000};
                {sanitized_query};
                COMMIT;
            """

            # Build psql command with individual parameters
            cmd = [
                "psql",
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

            async def run_psql(env_vars: dict[str, str]) -> Optional[str]:
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
                    raise RuntimeError("psql: failed to create subprocess pipes")

                stderr_task = asyncio.create_task(stderr_stream.read())
                lines: list[str] = []
                loop = asyncio.get_event_loop()
                deadline = loop.time() + self.query_timeout

                async def stream_output(emit_line: Callable[[str], None]) -> str | None:
                    pending_line: str | None = None

                    async def read_line_with_timeout() -> bytes:
                        remaining = deadline - loop.time()
                        if remaining <= 0:
                            raise asyncio.TimeoutError
                        return await asyncio.wait_for(
                            stdout.readline(), timeout=remaining
                        )

                    try:
                        while True:
                            line_bytes = await read_line_with_timeout()
                            if not line_bytes:
                                break

                            line = line_bytes.decode(errors="replace").rstrip("\r\n")

                            if line in ("BEGIN", "SET", "COMMIT", "ROLLBACK"):
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
                    except asyncio.TimeoutError:
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
                    except asyncio.TimeoutError:
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
                        logger.error(f"psql error: {error_msg}")
                        raise RuntimeError(f"psql: {error_msg}")

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
                with Path(output_path).open("w", encoding="utf-8", newline="") as handle:
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
                except asyncio.TimeoutError as exc:
                    logger.error(f"Query execution error: {exc}")
                    raise
                except Exception as e:
                    logger.error(f"Query execution error: {e}")
                    if not str(e).startswith("psql:"):
                        raise RuntimeError(f"psql: {e}")
                    raise
