"""The psycopg2 connector runs every statement inside one explicit transaction.

The transaction is made read-only and time-limited by its first statement,
so nothing depends on session state and the guarantees hold behind a
transaction pooler. SELECT-shaped statements are read through a server-side
cursor in batches; EXPLAIN and SHOW, which cannot be declared as cursors,
run on a plain cursor. These tests drive the connector with a fake
connection that records what it was asked to do.
"""

from threading import Event

import psycopg2
import pytest

from mcp_read_only_sql.connectors.postgresql.python import PostgreSQLPythonConnector
from mcp_read_only_sql.utils.sql_guard import (
    ReadOnlyQueryError,
    postgresql_shadow_query,
    postgresql_statement_is_select,
)

STARTUP_REJECTION = psycopg2.OperationalError(
    "unsupported startup parameter in options: default_transaction_read_only"
)


class FakeCursor:
    def __init__(self, log, name, rows, shadows):
        self.log = log
        self.name = name
        self.itersize = None
        self.description = None
        self._rows = None
        self._pending = rows
        self._shadows = shadows

    def execute(self, sql):
        self.log.append(("execute", self.name, sql))
        if sql.startswith("SET TRANSACTION"):
            return
        if "pg_catalog.pg_proc" in sql or "shadow" in sql:
            self._rows = [(name,) for name in self._shadows]
            self.description = [("shadow",)]
            return
        self._rows = list(self._pending)
        # A server-side cursor learns its columns only from the first FETCH.
        if self.name is None:
            self.description = [("col",)]

    def fetchall(self):
        rows, self._rows = self._rows, []
        return rows

    def fetchmany(self, size):
        self.log.append(("fetchmany", self.name, size))
        self.description = [("col",)]
        batch, self._rows = self._rows[:size], self._rows[size:]
        return batch

    def close(self):
        self.log.append(("close", self.name))


class FakeConnection:
    def __init__(self, log, rows, shadows):
        self.log = log
        self.autocommit = True
        self._rows = rows
        self._shadows = shadows

    def cursor(self, name=None):
        return FakeCursor(self.log, name, self._rows, self._shadows)

    def rollback(self):
        self.log.append(("rollback",))

    def close(self):
        self.log.append(("close_connection",))


def _fake_connect(monkeypatch, rows=((1,),), shadows=(), reject_options=False):
    """Install a psycopg2.connect fake; returns the shared event log."""
    log = []
    connections = []

    def fake_connect(**kwargs):
        log.append(("connect", kwargs))
        if reject_options and "options" in kwargs:
            raise STARTUP_REJECTION
        conn = FakeConnection(log, [tuple(r) for r in rows], list(shadows))
        connections.append(conn)
        return conn

    monkeypatch.setattr(psycopg2, "connect", fake_connect)
    return log, connections


def _executed(log):
    return [entry[2] for entry in log if entry[0] == "execute"]


def _cursor_names(log):
    return {entry[1] for entry in log if entry[0] == "execute"}


class TestStatementShape:
    def test_select_shapes_stream(self):
        assert postgresql_statement_is_select("SELECT 1")
        assert postgresql_statement_is_select("VALUES (1), (2)")
        assert postgresql_statement_is_select("TABLE users")
        assert postgresql_statement_is_select("WITH t AS (SELECT 1) SELECT * FROM t")

    def test_explain_and_show_do_not(self):
        assert not postgresql_statement_is_select("EXPLAIN SELECT 1")
        assert not postgresql_statement_is_select("SHOW search_path")


class TestTransaction:
    @pytest.mark.anyio
    async def test_transaction_is_read_only_and_time_limited_before_the_query(
        self, postgres_config, monkeypatch
    ):
        log, connections = _fake_connect(monkeypatch)
        connector = PostgreSQLPythonConnector(postgres_config)

        assert await connector.execute_query("SELECT 1") == "col\n1"

        (conn,) = connections
        assert conn.autocommit is False, "psycopg2 must open a transaction"
        first, *rest = _executed(log)
        assert first.startswith("SET TRANSACTION READ ONLY; SET LOCAL statement_timeout = ")
        assert first.endswith(str(int(postgres_config.query_timeout * 1000)))
        assert rest[-1] == "SELECT 1"
        assert log[-2:] == [("rollback",), ("close_connection",)]

    @pytest.mark.anyio
    async def test_startup_option_is_sent_and_dropped_only_when_rejected(
        self, postgres_config, monkeypatch
    ):
        log, _ = _fake_connect(monkeypatch, reject_options=True)

        await PostgreSQLPythonConnector(postgres_config).execute_query("SELECT 1")

        connects = [entry[1] for entry in log if entry[0] == "connect"]
        assert len(connects) == 2
        assert connects[0]["options"] == "-c default_transaction_read_only=on"
        assert "options" not in connects[1]
        assert connects[1]["user"] == postgres_config.username
        assert _executed(log)[0].startswith("SET TRANSACTION READ ONLY")

    @pytest.mark.anyio
    async def test_other_connection_errors_are_not_retried(
        self, postgres_config, monkeypatch
    ):
        calls = []

        def fake_connect(**kwargs):
            calls.append(kwargs)
            raise psycopg2.OperationalError("connection refused")

        monkeypatch.setattr(psycopg2, "connect", fake_connect)

        with pytest.raises(RuntimeError, match="connection refused"):
            await PostgreSQLPythonConnector(postgres_config).execute_query("SELECT 1")

        assert len(calls) == 1

    @pytest.mark.anyio
    async def test_shadowed_name_is_refused_inside_the_transaction(
        self, postgres_config, monkeypatch
    ):
        log, _ = _fake_connect(monkeypatch, shadows=["public.length"])
        connector = PostgreSQLPythonConnector(postgres_config)

        with pytest.raises(ReadOnlyQueryError, match="public.length"):
            await connector.execute_query("SELECT length('x')")

        executed = _executed(log)
        assert executed[0].startswith("SET TRANSACTION READ ONLY")
        assert executed[1] == postgresql_shadow_query("SELECT length('x')", ())
        assert "SELECT length('x')" not in executed, "the query never ran"
        assert log[-2:] == [("rollback",), ("close_connection",)]


class TestCursors:
    @pytest.mark.anyio
    async def test_select_runs_on_a_server_side_cursor_in_batches(
        self, postgres_config, monkeypatch
    ):
        rows = [(i,) for i in range(2500)]
        log, _ = _fake_connect(monkeypatch, rows=rows)

        result = await PostgreSQLPythonConnector(postgres_config).execute_query(
            "SELECT i FROM t"
        )

        assert result.split("\n") == ["col", *(str(i) for i in range(2500))]
        assert ("execute", "mcp_read_only_sql", "SELECT i FROM t") in log
        fetches = [entry for entry in log if entry[0] == "fetchmany"]
        assert fetches == [("fetchmany", "mcp_read_only_sql", 1000)] * 4

    @pytest.mark.anyio
    async def test_empty_result_still_has_a_header(self, postgres_config, monkeypatch):
        _fake_connect(monkeypatch, rows=[])

        result = await PostgreSQLPythonConnector(postgres_config).execute_query(
            "SELECT 1 WHERE false"
        )

        assert result == "col"

    @pytest.mark.anyio
    @pytest.mark.parametrize("statement", ["EXPLAIN SELECT 1", "SHOW search_path"])
    async def test_explain_and_show_run_on_a_plain_cursor(
        self, postgres_config, monkeypatch, statement
    ):
        log, _ = _fake_connect(monkeypatch, rows=[("plan",)])

        result = await PostgreSQLPythonConnector(postgres_config).execute_query(
            statement
        )

        assert result == "col\nplan"
        assert ("execute", None, statement) in log
        assert _cursor_names(log) == {None}, "no server-side cursor was declared"

    @pytest.mark.anyio
    async def test_file_output_is_written_row_by_row(
        self, postgres_config, monkeypatch, tmp_path
    ):
        rows = [(i,) for i in range(1500)]
        log, _ = _fake_connect(monkeypatch, rows=rows)
        output = tmp_path / "out.tsv"

        await PostgreSQLPythonConnector(postgres_config).execute_query_to_file(
            "SELECT i FROM t", output
        )

        assert output.read_text().split("\n") == ["col", *(str(i) for i in range(1500))]
        # 1000 rows, 500 rows, then the empty fetch that ends the loop.
        assert [e for e in log if e[0] == "fetchmany"] == [
            ("fetchmany", "mcp_read_only_sql", 1000)
        ] * 3
        assert log[-2:] == [("rollback",), ("close_connection",)]

    @pytest.mark.anyio
    async def test_timeout_stops_fetching_before_returning(
        self, postgres_config, monkeypatch, tmp_path
    ):
        log = []
        fetching = Event()
        cancelled = Event()
        release = Event()
        rows = [(i,) for i in range(2500)]

        class BlockingCursor(FakeCursor):
            def __init__(self, name):
                super().__init__(log, name, rows, [])
                self.fetches = 0

            def fetchmany(self, size):
                self.fetches += 1
                if self.fetches == 2:
                    fetching.set()
                    release.wait(timeout=2)
                    if cancelled.is_set():
                        raise psycopg2.OperationalError("query cancelled")
                return super().fetchmany(size)

        class BlockingConnection(FakeConnection):
            def __init__(self):
                super().__init__(log, rows, [])

            def cursor(self, name=None):
                return BlockingCursor(name)

            def cancel(self):
                log.append(("cancel",))
                cancelled.set()
                release.set()

        monkeypatch.setattr(psycopg2, "connect", lambda **kwargs: BlockingConnection())
        connector = PostgreSQLPythonConnector(postgres_config)
        connector.connection_timeout = 0.05
        connector.query_timeout = 0.05
        output = tmp_path / "timed-out.tsv"

        try:
            with pytest.raises(TimeoutError, match="combined timeout"):
                await connector.execute_query_to_file("SELECT i FROM t", output)
        finally:
            release.set()

        assert fetching.is_set()
        assert cancelled.is_set()
        assert ("cancel",) in log
        assert log[-2:] == [("rollback",), ("close_connection",)]
        assert len(output.read_text().splitlines()) == 1001
