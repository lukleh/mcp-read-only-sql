import asyncio
import logging
import os
from contextlib import suppress
from typing import Optional

from ..base_cli import BaseCLIConnector
from ...utils.sql_guard import sanitize_read_only_sql, ReadOnlyQueryError

logger = logging.getLogger(__name__)


class PostgreSQLCLIConnector(BaseCLIConnector):
    """PostgreSQL connector using psql CLI tool"""

    def _get_default_port(self) -> int:
        return 5432

    async def execute_query(self, query: str, database: Optional[str] = None, server: Optional[str] = None) -> str:
        """Execute a read-only query using psql and return raw TSV output"""
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

            # Use specified database or configured database
            db_name = database or self.database

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
                "-v", "ON_ERROR_STOP=1",
                "-h", host,           # Host
                "-p", str(port),      # Port
                "-d", db_name,        # Database
                "-U", self.username,  # Username
                "-A",                 # Unaligned output mode
                "-F", "\t",          # Use tab as field separator
                "-c", wrapped_query   # Query to execute
            ]

            # Set environment variables while preserving PATH
            env = os.environ.copy()
            env["PGPASSWORD"] = self.password
            env["PGCONNECT_TIMEOUT"] = str(self.connection_timeout)
            env["PGOPTIONS"] = "-c default_transaction_read_only=on"

            try:
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=env
                )

                stdout = process.stdout
                stderr_task = asyncio.create_task(process.stderr.read())

                lines = []
                total_bytes = 0
                newline_bytes = len('\n'.encode())
                max_bytes = self.max_result_bytes or 0
                enforce_limit = max_bytes > 0
                truncated = False

                loop = asyncio.get_event_loop()
                # Rely on PostgreSQL connection and statement timeouts; enforce query timeout here
                deadline = loop.time() + self.query_timeout

                async def read_line_with_timeout():
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        raise asyncio.TimeoutError
                    return await asyncio.wait_for(stdout.readline(), timeout=remaining)

                try:
                    while True:
                        line_bytes = await read_line_with_timeout()
                        if not line_bytes:
                            break

                        line = line_bytes.decode(errors="replace").rstrip('\r\n')

                        if line in ('BEGIN', 'SET', 'COMMIT', 'ROLLBACK'):
                            continue
                        if line.startswith('(') and line.endswith(')') and ' row' in line:
                            continue

                        encoded_len = len(line.encode())
                        additional = encoded_len if not lines else newline_bytes + encoded_len

                        if enforce_limit and lines and (total_bytes + additional) > max_bytes:
                            truncated = True
                            break

                        lines.append(line)
                        total_bytes += additional

                        if enforce_limit and total_bytes > max_bytes:
                            truncated = True
                            break

                except asyncio.TimeoutError:
                    logger.warning("Query timeout - terminating psql process")
                    process.kill()
                    with suppress(asyncio.CancelledError):
                        stderr_task.cancel()
                        await stderr_task
                    # Wait for process to clean up subprocess transport
                    with suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(process.wait(), timeout=1.0)
                    raise TimeoutError(f"psql: Query timeout after {self.query_timeout}s")

                if truncated and process.returncode is None:
                    process.kill()

                try:
                    await asyncio.wait_for(process.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    logger.error("psql process did not terminate cleanly")
                    process.kill()
                    await process.wait()

                if not stderr_task.done():
                    stderr = await stderr_task
                else:
                    stderr = stderr_task.result()

                returncode = process.returncode
                if returncode is None:
                    logger.debug("psql process still running after wait(); treating as successful termination")
                if returncode not in (0, None) and not truncated:
                    error_msg = stderr.decode() if stderr else "Unknown error"
                    logger.error(f"psql error: {error_msg}")
                    raise RuntimeError(f"psql: {error_msg}")

                if truncated and process.returncode not in (0, None):
                    logger.info("psql process terminated after reaching result size limit")

                if lines and lines[-1] == '':
                    lines = lines[:-1]

                output = '\n'.join(lines)
                if truncated:
                    notice = f"[RESULT TRUNCATED: exceeded max_result_bytes={self.max_result_bytes} bytes]"
                    output = (output + '\n' if output else '') + notice

                return output

            except FileNotFoundError:
                raise FileNotFoundError("psql: command not found. Please install PostgreSQL client tools.")
            except ReadOnlyQueryError as exc:
                raise ReadOnlyQueryError(str(exc))
            except asyncio.TimeoutError as exc:
                logger.error(f"Query execution error: {exc}")
                raise
            except Exception as e:
                logger.error(f"Query execution error: {e}")
                if not str(e).startswith("psql:"):
                    raise RuntimeError(f"psql: {e}")
                raise
