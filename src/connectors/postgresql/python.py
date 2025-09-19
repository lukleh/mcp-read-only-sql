import asyncio
import logging
from typing import Optional, Tuple

import psycopg2
from psycopg2 import errors as psycopg_errors
from psycopg2.extras import RealDictCursor

from ..base import BaseConnector
from ...utils.tsv_formatter import format_tsv_line

logger = logging.getLogger(__name__)


class PostgreSQLPythonConnector(BaseConnector):
    """PostgreSQL connector using psycopg2"""

    def _get_default_port(self) -> int:
        return 5432

    async def execute_query(self, query: str, database: Optional[str] = None) -> str:
        """Execute a read-only query using psycopg2"""
        server = self._select_server()

        try:
            async with self._get_ssh_tunnel() as local_port:
                total_timeout = self.connection_timeout + self.query_timeout
                # Use SSH tunnel port if available
                if local_port:
                    host = "127.0.0.1"
                    port = local_port
                else:
                    host = server["host"]
                    port = server["port"]

                # Use specified database or configured database
                db_name = database or self.database

                # Run synchronous psycopg2 in executor with timeout
                loop = asyncio.get_event_loop()
                total_timeout = self.connection_timeout + self.query_timeout
                tsv_output, truncated = await asyncio.wait_for(
                    loop.run_in_executor(
                        None,
                        self._execute_sync_query,
                        host,
                        port,
                        db_name,
                        query,
                        self.max_result_bytes
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
                f"PostgreSQL: Operation exceeded combined timeout of {self.connection_timeout + self.query_timeout} seconds"
            )
        except psycopg_errors.QueryCanceled as e:
            logger.error(f"PostgreSQL query canceled: {e}")
            raise TimeoutError(f"PostgreSQL: {e}")
        except psycopg2.Error as e:
            # Database-specific errors get prefixed
            logger.error(f"PostgreSQL database error: {e}")
            raise RuntimeError(f"PostgreSQL: {e}")
        # Let other exceptions (programming errors) propagate unchanged

    def _execute_sync_query(
        self,
        host: str,
        port: int,
        database: str,
        query: str,
        max_result_bytes: int,
    ) -> Tuple[str, bool]:
        """Execute query synchronously and stream results as TSV within size limits."""
        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=self.username,
                password=self.password,
                connect_timeout=self.connection_timeout,
                options='-c default_transaction_read_only=on'  # Force read-only mode
            )

            # Set session to read-only
            conn.set_session(readonly=True, autocommit=True)

            # Set statement timeout
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(f"SET statement_timeout = {self.query_timeout * 1000}")  # Convert to milliseconds

            # Execute the actual query
            cursor.execute(query)

            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            lines = []
            truncated = False
            newline_bytes = len("\n".encode())
            total_bytes = 0
            max_bytes = max_result_bytes or 0
            enforce_limit = max_bytes > 0

            def append_line(line: str) -> bool:
                nonlocal total_bytes, truncated
                if line is None:
                    return True
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
                append_line(header_line)

            fetch_size = 100
            while True:
                batch = cursor.fetchmany(fetch_size)
                if not batch:
                    break
                for row in batch:
                    if isinstance(row, dict):
                        values = [row.get(col) for col in columns] if columns else list(row.values())
                    else:
                        values = list(row)
                    line = format_tsv_line(values)
                    if not append_line(line):
                        break
                if truncated:
                    break

            tsv_output = "\n".join(lines)
            return tsv_output, truncated
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
