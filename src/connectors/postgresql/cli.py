import asyncio
import logging
import os
import re
from contextlib import suppress
from typing import Optional

from ..base_cli import BaseCLIConnector

logger = logging.getLogger(__name__)


class PostgreSQLCLIConnector(BaseCLIConnector):
    """PostgreSQL connector using psql CLI tool"""

    def _get_default_port(self) -> int:
        return 5432

    async def execute_query(self, query: str, database: Optional[str] = None) -> str:
        """Execute a read-only query using psql and return raw TSV output"""
        sanitized_query = self._prepare_user_query(query)
        server = self._select_server()

        async with self._get_ssh_tunnel() as local_port:
            # Use SSH tunnel port if available
            if local_port:
                host = "127.0.0.1"
                port = local_port
            else:
                host = server["host"]
                port = server["port"]

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
            existing_pgoptions = env.get("PGOPTIONS", "").strip()
            readonly_option = "-c default_transaction_read_only=on"
            env["PGOPTIONS"] = f"{existing_pgoptions} {readonly_option}".strip()

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
                deadline = loop.time() + self.connection_timeout + self.query_timeout

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
                    raise asyncio.TimeoutError(f"Query execution timed out after {self.query_timeout}s")

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

                if process.returncode != 0 and not truncated:
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
            except Exception as e:
                logger.error(f"Query execution error: {e}")
                # Re-raise with psql prefix if not already prefixed
                if not str(e).startswith("psql:"):
                    raise RuntimeError(f"psql: {e}")
                raise

    @staticmethod
    def _prepare_user_query(query: str) -> str:
        """Ensure the user query cannot break out of the enforced read-only transaction."""
        if query is None:
            raise ValueError("Query must not be None")

        stripped = query.strip()
        if not stripped:
            raise ValueError("Query must not be empty")

        PostgreSQLCLIConnector._ensure_single_statement(stripped)
        PostgreSQLCLIConnector._reject_transaction_control(stripped)

        return stripped

    @staticmethod
    def _ensure_single_statement(query: str) -> None:
        """Disallow multiple SQL statements to prevent transaction escape."""
        semicolons = PostgreSQLCLIConnector._find_semicolons_outside_literals(query)
        if not semicolons:
            return

        # Allow a single trailing semicolon (after optional whitespace/comments)
        if len(semicolons) > 1:
            raise ValueError("Multiple SQL statements are not allowed in read-only mode")

        last_semicolon = semicolons[0]
        if not PostgreSQLCLIConnector._only_trailing_semicolon(query, last_semicolon):
            raise ValueError("Multiple SQL statements are not allowed in read-only mode")

    @staticmethod
    def _reject_transaction_control(query: str) -> None:
        """Reject standalone transaction control commands that could end the guard."""
        upper = query.lstrip().upper()

        transaction_patterns = [
            r"^(COMMIT)(\s|;|$)",
            r"^(ROLLBACK)(\s|;|$)",
            r"^(ABORT)(\s|;|$)",
            r"^(END)(\s|;|$)",
            r"^(BEGIN)(\s|;|$)",
            r"^(START\s+TRANSACTION)(\s|;|$)",
            r"^(SET\s+TRANSACTION)(\s|;|$)",
            r"^(SET\s+SESSION\s+CHARACTERISTICS)(\s|;|$)",
            r"^(SAVEPOINT)(\s|;|$)",
            r"^(RELEASE\s+SAVEPOINT)(\s|;|$)",
            r"^(ROLLBACK\s+TO\s+SAVEPOINT)(\s|;|$)",
            r"^(PREPARE\s+TRANSACTION)(\s|;|$)",
            r"^(COMMIT\s+PREPARED)(\s|;|$)",
            r"^(ROLLBACK\s+PREPARED)(\s|;|$)",
        ]

        for pattern in transaction_patterns:
            if re.match(pattern, upper):
                raise ValueError("Transaction control statements are not allowed in read-only mode")

    @staticmethod
    def _only_trailing_semicolon(query: str, index: int) -> bool:
        """Return True if all code after the semicolon is whitespace or comments."""
        tail = query[index + 1:]
        stripped_tail = PostgreSQLCLIConnector._remove_comments(tail).strip()
        return stripped_tail == ""

    @staticmethod
    def _remove_comments(sql: str) -> str:
        """Remove SQL comments to help with tail checks."""
        result = []
        length = len(sql)
        i = 0
        in_block = 0
        while i < length:
            ch = sql[i]
            nxt = sql[i + 1] if i + 1 < length else ""
            if in_block:
                if ch == "*" and nxt == "/":
                    in_block -= 1
                    i += 2
                elif ch == "/" and nxt == "*":
                    in_block += 1
                    i += 2
                else:
                    i += 1
                continue
            if ch == "-" and nxt == "-":
                # Skip rest of line
                i += 2
                while i < length and sql[i] not in "\r\n":
                    i += 1
                continue
            if ch == "/" and nxt == "*":
                in_block = 1
                i += 2
                continue
            result.append(ch)
            i += 1
        return "".join(result)

    @staticmethod
    def _find_semicolons_outside_literals(query: str) -> list[int]:
        """Locate semicolons that are not inside strings or comments."""
        semicolons = []
        length = len(query)
        i = 0
        in_single = False
        in_double = False
        in_line_comment = False
        in_block_comment = 0
        dollar_tag = None

        while i < length:
            ch = query[i]
            nxt = query[i + 1] if i + 1 < length else ""

            if in_line_comment:
                if ch in "\r\n":
                    in_line_comment = False
                i += 1
                continue

            if in_block_comment:
                if ch == "*" and nxt == "/":
                    in_block_comment -= 1
                    i += 2
                    continue
                if ch == "/" and nxt == "*":
                    in_block_comment += 1
                    i += 2
                    continue
                i += 1
                continue

            if dollar_tag:
                if query.startswith(dollar_tag, i):
                    i += len(dollar_tag)
                    dollar_tag = None
                else:
                    i += 1
                continue

            if in_single:
                if ch == "'":
                    if nxt == "'":
                        i += 2
                        continue
                    in_single = False
                i += 1
                continue

            if in_double:
                if ch == '"':
                    if nxt == '"':
                        i += 2
                        continue
                    in_double = False
                i += 1
                continue

            if ch == "-" and nxt == "-":
                in_line_comment = True
                i += 2
                continue

            if ch == "/" and nxt == "*":
                in_block_comment = 1
                i += 2
                continue

            if ch == "'":
                in_single = True
                i += 1
                continue

            if ch == '"':
                in_double = True
                i += 1
                continue

            if ch == "$":
                tag_end = i + 1
                while tag_end < length and (query[tag_end].isalnum() or query[tag_end] == "_"):
                    tag_end += 1
                if tag_end < length and query[tag_end] == "$":
                    dollar_tag = query[i:tag_end + 1]
                    i = tag_end + 1
                    continue

            if ch == ";":
                semicolons.append(i)

            i += 1

        return semicolons
