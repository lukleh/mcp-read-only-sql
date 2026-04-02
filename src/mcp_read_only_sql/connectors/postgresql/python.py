import asyncio
import logging
from pathlib import Path
from typing import Optional

import psycopg2
from psycopg2 import errors as psycopg_errors
from psycopg2.extras import RealDictCursor

from ..base import BaseConnector
from ...utils.tsv_formatter import format_tsv_line, write_tsv_text_line
from ...utils.sql_guard import sanitize_read_only_sql

logger = logging.getLogger(__name__)


class PostgreSQLPythonConnector(BaseConnector):
    """PostgreSQL connector using psycopg2"""

    def _get_default_port(self) -> int:
        return 5432

    async def execute_query(
        self, query: str, database: Optional[str] = None, server: Optional[str] = None
    ) -> str:
        """Execute a read-only query using psycopg2"""
        return await self._run_executor_query(
            self._execute_sync_query, query, database=database, server=server
        )

    async def execute_query_to_file(
        self,
        query: str,
        output_path: Path,
        database: Optional[str] = None,
        server: Optional[str] = None,
    ) -> None:
        """Execute a read-only query using psycopg2 and stream TSV to a file."""
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
        database: Optional[str] = None,
        server: Optional[str] = None,
        *,
        output_path: Optional[str] = None,
    ):
        """Resolve connection settings and run a synchronous worker in the executor."""
        sanitized_query = sanitize_read_only_sql(query)
        selected_server = self._select_server(server)

        try:
            async with self._get_ssh_tunnel(server) as local_port:
                total_timeout = self.connection_timeout + self.query_timeout
                # Use SSH tunnel port if available
                if local_port:
                    host = "127.0.0.1"
                    port = local_port
                else:
                    host = selected_server.host
                    port = selected_server.port

                # Use specified database or configured database (validated)
                db_name = self._resolve_database(database)

                # Run synchronous psycopg2 in executor with timeout
                loop = asyncio.get_event_loop()
                worker_args = [host, port, db_name, sanitized_query]
                if output_path is not None:
                    worker_args.append(output_path)

                return await asyncio.wait_for(
                    loop.run_in_executor(None, worker, *worker_args),
                    timeout=total_timeout,
                )

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
        output_path: Optional[str] = None,
    ) -> str:
        """Execute query synchronously and return TSV output."""
        if output_path is not None:
            self._execute_sync_query_to_file(host, port, database, query, output_path)
            return ""

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
                options="-c default_transaction_read_only=on",  # Force read-only mode
            )

            # Set session to read-only
            conn.set_session(readonly=True, autocommit=True)

            # Set statement timeout
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(
                f"SET statement_timeout = {self.query_timeout * 1000}"
            )  # Convert to milliseconds

            # Execute the actual query
            cursor.execute(query)

            columns = (
                [desc[0] for desc in cursor.description] if cursor.description else []
            )
            lines = []

            if columns:
                lines.append(format_tsv_line(columns))

            fetch_size = 100
            while True:
                batch = cursor.fetchmany(fetch_size)
                if not batch:
                    break
                for row in batch:
                    if isinstance(row, dict):
                        values = (
                            [row.get(col) for col in columns]
                            if columns
                            else list(row.values())
                        )
                    else:
                        values = list(row)
                    lines.append(format_tsv_line(values))

            return "\n".join(lines)
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def _execute_sync_query_to_file(
        self,
        host: str,
        port: int,
        database: str,
        query: str,
        output_path: str,
    ) -> None:
        """Execute query synchronously and stream TSV output to a file."""
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
                options="-c default_transaction_read_only=on",
            )

            conn.set_session(readonly=True, autocommit=True)

            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(f"SET statement_timeout = {self.query_timeout * 1000}")
            cursor.execute(query)

            columns = (
                [desc[0] for desc in cursor.description] if cursor.description else []
            )
            wrote_content = False

            with Path(output_path).open("w", encoding="utf-8", newline="") as handle:
                if columns:
                    wrote_content = write_tsv_text_line(
                        handle, format_tsv_line(columns), wrote_content
                    )

                fetch_size = 100
                while True:
                    batch = cursor.fetchmany(fetch_size)
                    if not batch:
                        break
                    for row in batch:
                        if isinstance(row, dict):
                            values = (
                                [row.get(col) for col in columns]
                                if columns
                                else list(row.values())
                            )
                        else:
                            values = list(row)
                        wrote_content = write_tsv_text_line(
                            handle, format_tsv_line(values), wrote_content
                        )
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
