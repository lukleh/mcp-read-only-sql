import asyncio
import functools
import logging
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from threading import Event, Lock

import psycopg2
from psycopg2 import errors as psycopg_errors

from ...errors import ConnectorError
from ...utils.sql_guard import (
    SHADOW_GUARD_PREFIX,
    SHADOW_GUARD_SUFFIX,
    ReadOnlyQueryError,
    postgresql_shadow_query,
    postgresql_statement_is_select,
    sanitize_postgresql_read_only_sql,
)
from ...utils.tsv_formatter import format_tsv_line, write_tsv_text_line
from ..base import BaseConnector

logger = logging.getLogger(__name__)

# Startup option, the same one the psql connector passes as PGOPTIONS. A
# pooler in transaction mode may reject it; the transaction below is then
# the only read-only layer, and it is a per-transaction one on purpose.
_STARTUP_OPTIONS = "-c default_transaction_read_only=on"
# Rows fetched per round trip from the server-side cursor.
_FETCH_SIZE = 1000
_CURSOR_NAME = "mcp_read_only_sql"


class _QueryCancellation:
    """Stop an executor worker and its active PostgreSQL statement."""

    def __init__(self) -> None:
        self._cancelled = Event()
        self._lock = Lock()
        self._connection = None

    def attach(self, conn) -> None:
        with self._lock:
            self.check()
            self._connection = conn

    def detach(self, conn) -> None:
        with self._lock:
            if self._connection is conn:
                self._connection = None

    def check(self) -> None:
        if self._cancelled.is_set():
            raise TimeoutError("PostgreSQL: query cancelled")

    def cancel(self) -> None:
        with self._lock:
            self._cancelled.set()
            conn = self._connection
        if conn is not None:
            # psycopg2 supports cancelling a running query from another thread.
            with suppress(psycopg2.Error):
                conn.cancel()


class PostgreSQLPythonConnector(BaseConnector):
    """PostgreSQL connector using psycopg2.

    Every statement runs inside one explicit transaction, the same shape the
    psql connector uses: ``SET TRANSACTION READ ONLY`` and ``SET LOCAL
    statement_timeout`` first, then the shadow-name check, then the query.
    Nothing is session state, so the guarantees hold behind a transaction
    pooler such as PgBouncer, where consecutive statements outside a
    transaction may run on different server connections. SELECT-shaped
    statements are read through a server-side cursor, so a large result is
    streamed to the result file instead of being loaded into memory.
    """

    async def execute_query(
        self, query: str, database: str | None = None, server: str | None = None
    ) -> str:
        """Execute a read-only query using psycopg2"""
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
        database: str | None = None,
        server: str | None = None,
        *,
        output_path: str | None = None,
    ):
        """Resolve connection settings and run a synchronous worker in the executor."""
        sanitized_query = sanitize_postgresql_read_only_sql(
            query, self.connection.allowed_functions
        )
        shadow_query = postgresql_shadow_query(
            query, self.connection.allowed_functions
        )
        streams = postgresql_statement_is_select(sanitized_query)
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
                cancellation = _QueryCancellation()
                job = functools.partial(
                    worker,
                    *worker_args,
                    shadow_query=shadow_query,
                    streams=streams,
                    cancellation=cancellation,
                )

                future = loop.run_in_executor(None, job)
                try:
                    done, _ = await asyncio.wait({future}, timeout=total_timeout)
                    if done:
                        return await future
                    cancellation.cancel()
                    # The result path belongs to the caller. Finish the worker
                    # before the caller can unlink it after a timeout.
                    with suppress(Exception):
                        await future
                    raise TimeoutError
                except asyncio.CancelledError:
                    cancellation.cancel()
                    with suppress(Exception):
                        await future
                    raise

        except TimeoutError as e:
            # Re-raise SSH timeout as-is
            if "SSH:" in str(e):
                raise
            # Otherwise it's a query timeout from asyncio.wait_for
            raise TimeoutError(
                f"PostgreSQL: Operation exceeded combined timeout of {self.connection_timeout + self.query_timeout} seconds"
            )
        except psycopg2.Error as e:
            query_canceled_type = getattr(psycopg_errors, "QueryCanceled", None)
            if isinstance(query_canceled_type, type) and isinstance(
                e, query_canceled_type
            ):
                logger.error(f"PostgreSQL query canceled: {e}")
                raise TimeoutError(f"PostgreSQL: {e}")
            # Database-specific errors get prefixed
            logger.error(f"PostgreSQL database error: {e}")
            raise ConnectorError(f"PostgreSQL: {e}")
        # Let other exceptions (programming errors) propagate unchanged

    def _connect(self, host: str, port: int, database: str):
        """Open the connection, without the startup option if the server rejects it."""
        kwargs: dict[str, object] = {
            "host": host,
            "port": port,
            "database": database,
            "user": self.username,
            "password": self.password,
            "connect_timeout": self.connection_timeout,
            "options": _STARTUP_OPTIONS,
        }
        try:
            return psycopg2.connect(**kwargs)
        except psycopg2.OperationalError as exc:
            if "unsupported startup parameter" not in str(exc).lower():
                raise
            logger.warning(
                "psycopg2: remote server rejected default_transaction_read_only; "
                "retrying without startup options"
            )
            del kwargs["options"]
            return psycopg2.connect(**kwargs)

    @contextmanager
    def _read_only_transaction(
        self,
        host: str,
        port: int,
        database: str,
        shadow_query: str | None,
        cancellation: _QueryCancellation | None = None,
    ):
        """One transaction, read-only and time-limited before anything else runs.

        psycopg2 opens the transaction with the first statement; that
        statement makes it read-only and sets the timeout for it alone, so
        nothing outlives the transaction or depends on session state. The
        transaction is rolled back when the block ends.
        """
        if cancellation is not None:
            cancellation.check()
        conn = self._connect(host, port, database)
        try:
            if cancellation is not None:
                cancellation.attach(conn)
            conn.autocommit = False
            cursor = conn.cursor()
            try:
                if cancellation is not None:
                    cancellation.check()
                cursor.execute(
                    "SET TRANSACTION READ ONLY; "
                    f"SET LOCAL statement_timeout = {int(self.query_timeout * 1000)}"
                )
                self._reject_shadowed_names(cursor, shadow_query)
            finally:
                cursor.close()
            if cancellation is not None:
                cancellation.check()
            yield conn
        finally:
            if cancellation is not None:
                cancellation.detach(conn)
            with suppress(psycopg2.Error):
                conn.rollback()
            conn.close()

    @staticmethod
    def _reject_shadowed_names(cursor, shadow_query: str | None) -> None:
        """Refuse the query if a bare name resolves outside pg_catalog."""
        if shadow_query is None:
            return
        cursor.execute(shadow_query)
        shadows = [row[0] for row in cursor.fetchall()]
        if shadows:
            raise ReadOnlyQueryError(
                f"{SHADOW_GUARD_PREFIX} {', '.join(shadows)} {SHADOW_GUARD_SUFFIX}"
            )

    @staticmethod
    def _rows(
        conn, query: str, streams: bool, cancellation: _QueryCancellation | None = None
    ) -> Iterator[list]:
        """Yield the column names, then every row, inside the open transaction.

        A SELECT-shaped statement is declared as a server-side cursor and
        fetched in batches; EXPLAIN and SHOW cannot be, and run on a plain
        cursor. A plain tuple cursor is used either way: a dict cursor
        collapses duplicate column names (SELECT 1 AS a, 2 AS a) into one.
        """
        cursor = conn.cursor(name=_CURSOR_NAME) if streams else conn.cursor()
        try:
            if streams:
                cursor.itersize = _FETCH_SIZE
            if cancellation is not None:
                cancellation.check()
            cursor.execute(query)
            # A server-side cursor learns its columns from the first FETCH.
            batch = cursor.fetchmany(_FETCH_SIZE)
            if cancellation is not None:
                cancellation.check()
            yield [desc[0] for desc in cursor.description or []]
            while batch:
                for row in batch:
                    if cancellation is not None:
                        cancellation.check()
                    yield list(row)
                if cancellation is not None:
                    cancellation.check()
                batch = cursor.fetchmany(_FETCH_SIZE)
        finally:
            cursor.close()

    def _execute_sync_query(
        self,
        host: str,
        port: int,
        database: str,
        query: str,
        output_path: str | None = None,
        *,
        shadow_query: str | None = None,
        streams: bool = True,
        cancellation: _QueryCancellation | None = None,
    ) -> str:
        """Execute query synchronously and return TSV output."""
        if output_path is not None:
            self._execute_sync_query_to_file(
                host,
                port,
                database,
                query,
                output_path,
                shadow_query=shadow_query,
                streams=streams,
                cancellation=cancellation,
            )
            return ""

        with self._read_only_transaction(
            host, port, database, shadow_query, cancellation
        ) as conn:
            rows = self._rows(conn, query, streams, cancellation)
            columns = next(rows)
            lines = [format_tsv_line(columns)] if columns else []
            lines.extend(format_tsv_line(row) for row in rows)
            return "\n".join(lines)

    def _execute_sync_query_to_file(
        self,
        host: str,
        port: int,
        database: str,
        query: str,
        output_path: str,
        *,
        shadow_query: str | None = None,
        streams: bool = True,
        cancellation: _QueryCancellation | None = None,
    ) -> None:
        """Execute query synchronously and stream TSV output to a file."""
        with self._read_only_transaction(
            host, port, database, shadow_query, cancellation
        ) as conn:
            rows = self._rows(conn, query, streams, cancellation)
            columns = next(rows)
            wrote_content = False
            if cancellation is not None:
                cancellation.check()
            with Path(output_path).open("w", encoding="utf-8", newline="") as handle:
                if columns:
                    wrote_content = write_tsv_text_line(
                        handle, format_tsv_line(columns), wrote_content
                    )
                for row in rows:
                    wrote_content = write_tsv_text_line(
                        handle, format_tsv_line(row), wrote_content
                    )
