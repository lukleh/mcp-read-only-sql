"""
Test to verify that all database implementations enforce read-only mode at the database session level.
This is a critical security test for Layer 2 of our three-layer security model.
"""

import asyncio
from typing import ClassVar

import clickhouse_connect
import psycopg2
import pytest
from clickhouse_connect.driver.exceptions import ClickHouseError

from mcp_read_only_sql.connectors.clickhouse.cli import ClickHouseCLIConnector
from mcp_read_only_sql.connectors.clickhouse.python import ClickHousePythonConnector
from mcp_read_only_sql.connectors.postgresql.cli import PostgreSQLCLIConnector
from mcp_read_only_sql.connectors.postgresql.python import PostgreSQLPythonConnector
from mcp_read_only_sql.utils.sql_guard import ReadOnlyQueryError, sanitize_read_only_sql
from tests.sql_statement_lists import (
    CLICKHOUSE_DDL_STATEMENTS,
    CLICKHOUSE_DML_STATEMENTS,
    CLICKHOUSE_KILL_STATEMENTS,
    CLICKHOUSE_SYSTEM_STATEMENTS,
    POSTGRESQL_ALLOWED_LITERAL_QUERIES,
    POSTGRESQL_DDL_ALTER_STATEMENTS,
    POSTGRESQL_DDL_CREATE_STATEMENTS,
    POSTGRESQL_DDL_DROP_STATEMENTS,
    POSTGRESQL_DML_STATEMENTS,
    POSTGRESQL_LOCK_STATEMENTS,
    POSTGRESQL_MAINTENANCE_STATEMENTS,
    POSTGRESQL_PROCEDURAL_STATEMENTS,
    POSTGRESQL_TRANSACTION_STATEMENTS,
)

POSTGRESQL_PYTHON_BLOCKED_STATEMENTS = (
    POSTGRESQL_DML_STATEMENTS
    + POSTGRESQL_DDL_CREATE_STATEMENTS
    + POSTGRESQL_DDL_ALTER_STATEMENTS
    + POSTGRESQL_DDL_DROP_STATEMENTS
    + POSTGRESQL_MAINTENANCE_STATEMENTS
    + POSTGRESQL_PROCEDURAL_STATEMENTS
    + POSTGRESQL_TRANSACTION_STATEMENTS
    + POSTGRESQL_LOCK_STATEMENTS
)

CLICKHOUSE_PYTHON_BLOCKED_STATEMENTS = (
    CLICKHOUSE_DML_STATEMENTS
    + CLICKHOUSE_DDL_STATEMENTS
    + CLICKHOUSE_SYSTEM_STATEMENTS
    + CLICKHOUSE_KILL_STATEMENTS
)


class _FakeStdout:
    """Minimal stdout stream for mocked subprocesses."""

    def __init__(self, lines=None):
        self._lines = [line.encode() for line in (lines or [])]

    async def readline(self):
        if self._lines:
            return self._lines.pop(0)
        return b""


class _FakeStderr:
    """Minimal stderr stream returning a single payload once."""

    def __init__(self, message: str):
        self._message = message.encode()
        self._sent = False

    async def read(self):
        if self._sent:
            return b""
        self._sent = True
        return self._message


class _FakeProcess:
    """Subprocess stub tailored for CLI connector tests."""

    def __init__(self, stderr_message: str, stdout_lines=None, returncode: int = 1):
        self.stdout = _FakeStdout(stdout_lines)
        self.stderr = _FakeStderr(stderr_message)
        self.stdin = None
        self.returncode = returncode

    async def wait(self):
        return self.returncode

    def kill(self):
        self.returncode = -9


def _assert_readonly_error(exc_info, connector_name: str):
    error = str(exc_info.value).lower()
    assert any(
        keyword in error for keyword in ["read", "permission", "readonly", "cannot"]
    ), f"{connector_name}: expected read-only style error, got: {error[:120]}"


@pytest.mark.anyio
@pytest.mark.docker
@pytest.mark.usefixtures("docker_check")
async def test_postgresql_python_readonly(postgres_config):
    """Test PostgreSQL Python connector enforces read-only mode"""
    connector = PostgreSQLPythonConnector(postgres_config)

    # Test write operations are blocked
    write_queries = [
        "INSERT INTO users (username, email) VALUES ('testuser', 'test@example.com')",
        "UPDATE users SET email = 'changed@example.com' WHERE id = 1",
        "DELETE FROM users WHERE id = 1",
        "DROP TABLE users",
        "CREATE TABLE new_table (id INT)",
    ]

    for query in write_queries:
        with pytest.raises(ReadOnlyQueryError) as exc_info:
            await connector.execute_query(query)
        _assert_readonly_error(exc_info, "PostgreSQL Python")

    # Test SELECT still works
    result = await connector.execute_query("SELECT 1")
    assert isinstance(result, str), "SELECT query should return TSV"


@pytest.mark.anyio
@pytest.mark.docker
@pytest.mark.usefixtures("docker_check")
async def test_postgresql_cli_readonly(postgres_config):
    """Test PostgreSQL CLI connector enforces read-only mode"""
    connector = PostgreSQLCLIConnector(postgres_config)

    # Test SELECT works with session wrapping
    result = await connector.execute_query("SELECT 1 as test")
    assert isinstance(result, str), "SELECT query should return TSV"

    # Test write operations fail due to wrapped read-only session
    write_queries = [
        "INSERT INTO users (username, email) VALUES ('testuser2', 'test2@example.com')",
        "UPDATE users SET email = 'changed@example.com' WHERE id = 1",
        "DELETE FROM users WHERE id = 1",
    ]

    for query in write_queries:
        with pytest.raises(ReadOnlyQueryError) as exc_info:
            await connector.execute_query(query)
        _assert_readonly_error(exc_info, "PostgreSQL CLI")


@pytest.mark.anyio
async def test_postgresql_cli_blocks_multi_statement_escape(postgres_config):
    """Ensure multi-statement attempts are rejected before execution."""
    connector = PostgreSQLCLIConnector(postgres_config)

    malicious_query = "COMMIT; INSERT INTO users (username) VALUES ('oops')"
    with pytest.raises(ReadOnlyQueryError) as exc_info:
        await connector.execute_query(malicious_query)

    assert "multiple sql statements" in str(exc_info.value).lower()


@pytest.mark.anyio
async def test_postgresql_cli_blocks_transaction_control(postgres_config):
    """Ensure direct transaction control commands are rejected."""
    connector = PostgreSQLCLIConnector(postgres_config)

    for statement in POSTGRESQL_TRANSACTION_STATEMENTS:
        with pytest.raises(ReadOnlyQueryError) as exc_info:
            await connector.execute_query(statement)
        assert "transaction control" in str(exc_info.value).lower()


def test_postgresql_cli_query_sanitizer_allows_trailing_semicolon():
    """Trailing semicolons and whitespace should remain valid."""
    query = "SELECT 1;   "
    assert sanitize_read_only_sql(query) == "SELECT 1;"


def test_postgresql_cli_query_sanitizer_handles_literals():
    """Semicolons inside string literals must not trigger multi-statement rejections."""
    query = "SELECT 'value;still literal'"
    assert sanitize_read_only_sql(query) == query


@pytest.mark.parametrize("query", POSTGRESQL_ALLOWED_LITERAL_QUERIES)
def test_postgresql_cli_query_sanitizer_allows_keywords_inside_literals(query):
    """Ensure keywords inside string literals are preserved."""
    assert sanitize_read_only_sql(query) == query


@pytest.mark.anyio
async def test_postgresql_cli_includes_readonly_flags(postgres_config, monkeypatch):
    """Verify the CLI connector builds the psql command with read-only protections."""

    captured = {}

    class DummyStdout:
        def __init__(self, lines):
            self._lines = [line.encode() for line in lines]

        async def readline(self):
            if self._lines:
                return self._lines.pop(0)
            return b""

    class DummyStderr:
        async def read(self):
            return b""

    class DummyProcess:
        def __init__(self):
            self.stdout = DummyStdout(["col\n"])
            self.stderr = DummyStderr()
            self.returncode = 0

        async def wait(self):
            return 0

        def kill(self):
            self.returncode = -9

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        captured["cmd"] = list(cmd)
        captured["env"] = kwargs.get("env", {})
        process = DummyProcess()
        captured["process"] = process
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    connector = PostgreSQLCLIConnector(postgres_config)
    result = await connector.execute_query("SELECT 1 as test")

    assert result == "col"
    cmd = captured["cmd"]
    assert "--single-transaction" in cmd
    assert "-v" in cmd and "ON_ERROR_STOP=1" in cmd
    assert "-q" in cmd and "footer=off" in cmd
    command_string = cmd[cmd.index("-c") + 1]
    assert "SELECT 1 as test" in command_string
    assert command_string.lstrip().startswith("SET TRANSACTION READ ONLY;")
    assert "BEGIN" not in command_string and "COMMIT" not in command_string
    env = captured["env"]
    assert "default_transaction_read_only=on" in env.get("PGOPTIONS", "")


@pytest.mark.anyio
@pytest.mark.parametrize("statement", POSTGRESQL_DML_STATEMENTS)
async def test_postgresql_cli_blocks_write_statements(
    statement, postgres_config, monkeypatch
):
    """Write-oriented SQL is refused by the AST guard before psql is invoked."""

    connector = PostgreSQLCLIConnector(postgres_config)

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        raise AssertionError("psql must not be invoked for a rejected statement")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(ReadOnlyQueryError) as exc_info:
        await connector.execute_query(statement)

    assert "read-only" in str(exc_info.value).lower()


@pytest.mark.anyio
async def test_postgresql_cli_surfaces_server_readonly_error(
    postgres_config, monkeypatch
):
    """With the AST guard bypassed, a psql read-only error becomes a RuntimeError."""

    monkeypatch.setattr(
        "mcp_read_only_sql.connectors.postgresql.cli.sanitize_postgresql_read_only_sql",
        lambda query, allowed_functions=(): query.strip(),
    )
    monkeypatch.setattr(
        "mcp_read_only_sql.connectors.postgresql.cli.postgresql_shadow_query",
        lambda query, allowed_functions=(): None,
    )
    connector = PostgreSQLCLIConnector(postgres_config)
    called = {"value": False}

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        called["value"] = True
        return _FakeProcess("ERROR: cannot execute INSERT in a read-only transaction")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(RuntimeError) as exc_info:
        await connector.execute_query("INSERT INTO users (id) VALUES (1)")

    assert called["value"], "psql was not invoked"
    assert str(exc_info.value).startswith("psql:")
    _assert_readonly_error(exc_info, "PostgreSQL CLI")


@pytest.mark.anyio
async def test_postgresql_cli_runs_shadow_guard_and_surfaces_it(
    postgres_config, monkeypatch
):
    """Bare names add a DO guard to the psql script; its RAISE becomes ReadOnlyQueryError."""

    captured = {}

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        captured["cmd"] = list(cmd)
        return _FakeProcess(
            "ERROR:  Read-only guard: public.md5(text) shadow a name this query uses\n"
            "CONTEXT:  PL/pgSQL function inline_code_block line 1 at RAISE\n"
        )

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    connector = PostgreSQLCLIConnector(postgres_config)

    with pytest.raises(ReadOnlyQueryError) as exc_info:
        await connector.execute_query("SELECT md5('x')")

    script = captured["cmd"][-1]
    assert "DO $readonly_guard$" in script
    assert script.index("$readonly_guard$") < script.index("SELECT md5('x')")
    assert str(exc_info.value) == (
        "Read-only guard: public.md5(text) shadow a name this query uses"
    )


@pytest.mark.anyio
@pytest.mark.parametrize("statement", POSTGRESQL_DDL_CREATE_STATEMENTS)
async def test_postgresql_cli_blocks_create_statements(
    statement, postgres_config, monkeypatch
):
    """All CREATE statements must be blocked in read-only mode."""

    connector = PostgreSQLCLIConnector(postgres_config)

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        raise AssertionError("psql must not be invoked for a rejected statement")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(ReadOnlyQueryError):
        await connector.execute_query(statement)


@pytest.mark.anyio
@pytest.mark.parametrize("statement", POSTGRESQL_DDL_ALTER_STATEMENTS)
async def test_postgresql_cli_blocks_alter_statements(
    statement, postgres_config, monkeypatch
):
    """ALTER statements should be rejected by the CLI connector."""

    connector = PostgreSQLCLIConnector(postgres_config)

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        raise AssertionError("psql must not be invoked for a rejected statement")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(ReadOnlyQueryError):
        await connector.execute_query(statement)


@pytest.mark.anyio
@pytest.mark.parametrize("statement", POSTGRESQL_DDL_DROP_STATEMENTS)
async def test_postgresql_cli_blocks_drop_statements(
    statement, postgres_config, monkeypatch
):
    """DROP statements must fail in read-only mode."""

    connector = PostgreSQLCLIConnector(postgres_config)

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        raise AssertionError("psql must not be invoked for a rejected statement")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(ReadOnlyQueryError):
        await connector.execute_query(statement)


@pytest.mark.anyio
@pytest.mark.parametrize("statement", POSTGRESQL_MAINTENANCE_STATEMENTS)
async def test_postgresql_cli_blocks_maintenance_statements(
    statement, postgres_config, monkeypatch
):
    """Maintenance commands that mutate state should be rejected."""

    connector = PostgreSQLCLIConnector(postgres_config)

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        raise AssertionError("psql must not be invoked for a rejected statement")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(ReadOnlyQueryError):
        await connector.execute_query(statement)


@pytest.mark.anyio
@pytest.mark.parametrize("statement", POSTGRESQL_PROCEDURAL_STATEMENTS)
async def test_postgresql_cli_blocks_procedural_statements(
    statement, postgres_config, monkeypatch
):
    """Procedural constructs should not bypass read-only enforcement."""

    connector = PostgreSQLCLIConnector(postgres_config)

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        raise AssertionError("psql must not be invoked for a rejected statement")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(ReadOnlyQueryError):
        await connector.execute_query(statement)


@pytest.mark.anyio
@pytest.mark.parametrize("statement", POSTGRESQL_LOCK_STATEMENTS)
async def test_postgresql_cli_blocks_lock_statements(
    statement, postgres_config, monkeypatch
):
    """Locking operations that require write access should fail."""

    connector = PostgreSQLCLIConnector(postgres_config)

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        raise AssertionError("psql must not be invoked for a rejected statement")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(ReadOnlyQueryError):
        await connector.execute_query(statement)


@pytest.mark.anyio
async def test_clickhouse_cli_includes_readonly_flag(clickhouse_config, monkeypatch):
    """Verify the ClickHouse CLI connector includes --readonly and related guards."""

    captured = {}

    class DummyStdout:
        def __init__(self, lines):
            self._lines = [line.encode() for line in lines]

        async def readline(self):
            if self._lines:
                return self._lines.pop(0)
            return b""

    class DummyStderr:
        async def read(self):
            return b""

    class DummyStdin:
        def __init__(self):
            self.writes = []
            self.closed = False
            self.drained = False

        def write(self, data):
            self.writes.append(data)

        async def drain(self):
            self.drained = True

        def close(self):
            self.closed = True

    class DummyProcess:
        def __init__(self):
            self.stdout = DummyStdout(["col\n"])
            self.stderr = DummyStderr()
            self.stdin = DummyStdin()
            self.returncode = 0

        async def wait(self):
            return 0

        def kill(self):
            self.returncode = -9

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        captured["cmd"] = list(cmd)
        captured["env"] = kwargs.get("env", {})
        process = DummyProcess()
        captured["process"] = process
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    connector = ClickHouseCLIConnector(clickhouse_config)
    result = await connector.execute_query("SELECT 1")

    assert result == "col"
    cmd = captured["cmd"]
    assert "--readonly" in cmd
    assert "--max_execution_time" in cmd
    assert "--ask-password" in cmd
    assert "--password" not in cmd
    assert any(part == "SELECT 1" for part in cmd)
    assert connector.password == "testpass"
    assert captured["process"].stdin.writes == [b"testpass\n"]
    assert captured["process"].stdin.drained is True
    assert captured["process"].stdin.closed is True
    # No environment mutations expected, but keep assertion for completeness
    assert captured["env"] is not None


@pytest.mark.anyio
@pytest.mark.parametrize("statement", CLICKHOUSE_DML_STATEMENTS)
async def test_clickhouse_cli_blocks_mutations(
    statement, clickhouse_config, monkeypatch
):
    """Mutating ClickHouse statements must fail under --readonly=1."""

    connector = ClickHouseCLIConnector(clickhouse_config)

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        return _FakeProcess(f"READONLY: {statement}")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(RuntimeError) as exc_info:
        await connector.execute_query(statement)

    assert "clickhouse-client" in str(exc_info.value).lower()


@pytest.mark.anyio
@pytest.mark.parametrize("statement", CLICKHOUSE_DDL_STATEMENTS)
async def test_clickhouse_cli_blocks_ddl(statement, clickhouse_config, monkeypatch):
    """DDL should be rejected in read-only mode for ClickHouse CLI."""

    connector = ClickHouseCLIConnector(clickhouse_config)

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        return _FakeProcess(f"READONLY: {statement}")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(RuntimeError):
        await connector.execute_query(statement)


@pytest.mark.anyio
@pytest.mark.parametrize("statement", CLICKHOUSE_SYSTEM_STATEMENTS)
async def test_clickhouse_cli_blocks_system_commands(
    statement, clickhouse_config, monkeypatch
):
    """SYSTEM commands that mutate state must be refused."""

    connector = ClickHouseCLIConnector(clickhouse_config)

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        return _FakeProcess(f"READONLY: {statement}")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(RuntimeError):
        await connector.execute_query(statement)


@pytest.mark.anyio
@pytest.mark.parametrize("statement", CLICKHOUSE_KILL_STATEMENTS)
async def test_clickhouse_cli_blocks_kill_statements(
    statement, clickhouse_config, monkeypatch
):
    """KILL statements also require write permissions and must fail."""

    connector = ClickHouseCLIConnector(clickhouse_config)

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        return _FakeProcess(f"READONLY: {statement}")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(RuntimeError):
        await connector.execute_query(statement)


def test_postgresql_python_sets_readonly_options(monkeypatch, postgres_config):
    """psycopg2 connection should be created with session-level read-only guards."""

    captured = {}

    class DummyCursor:
        def __init__(self):
            self.description = None
            self._rows = []

        def execute(self, sql):
            captured.setdefault("executed", []).append(sql)
            if sql.startswith("SET statement_timeout"):
                return
            self.description = [("col",)]
            self._rows = [(1,)]

        def fetchmany(self, _size):
            if self._rows:
                rows = self._rows
                self._rows = []
                return rows
            return []

        def close(self):
            return None

    class DummyConnection:
        def __init__(self, **kwargs):
            captured["connect_kwargs"] = kwargs
            self.session_args = None

        def set_session(self, readonly, autocommit):
            self.session_args = (readonly, autocommit)
            captured["session_args"] = (readonly, autocommit)

        def cursor(self, cursor_factory=None):
            return DummyCursor()

        def close(self):
            captured["closed"] = True

    def fake_connect(**kwargs):
        return DummyConnection(**kwargs)

    monkeypatch.setattr(psycopg2, "connect", fake_connect)

    connector = PostgreSQLPythonConnector(postgres_config)
    output = connector._execute_sync_query(
        host="localhost",
        port=5432,
        database="testdb",
        query="SELECT 1",
    )

    assert output == "col\n1"
    assert (
        captured["connect_kwargs"]["options"] == "-c default_transaction_read_only=on"
    )
    assert captured["session_args"] == (True, True)
    assert any("SET statement_timeout" in sql for sql in captured["executed"])


@pytest.mark.anyio
@pytest.mark.parametrize("statement", POSTGRESQL_PYTHON_BLOCKED_STATEMENTS)
async def test_postgresql_python_blocks_write_statements(
    statement, postgres_config, monkeypatch
):
    """The Python connector refuses every mutation before opening a connection."""

    def fake_sync_query(self, host, port, database, query):
        raise AssertionError("psycopg2 must not be used for a rejected statement")

    monkeypatch.setattr(
        PostgreSQLPythonConnector, "_execute_sync_query", fake_sync_query
    )

    connector = PostgreSQLPythonConnector(postgres_config)

    with pytest.raises(ReadOnlyQueryError) as exc_info:
        await connector.execute_query(statement)

    assert "read-only" in str(exc_info.value).lower()


def test_clickhouse_python_sets_readonly_setting(monkeypatch, clickhouse_config):
    """clickhouse-connect client must be instantiated with readonly=1."""

    captured = {}

    class DummyResult:
        column_names: ClassVar[list[str]] = ["col"]
        result_rows: ClassVar[list[list[int]]] = [[1]]

    class DummyClient:
        def __init__(self, **kwargs):
            captured["client_kwargs"] = kwargs

        def query(self, sql, column_oriented=False):
            captured["query"] = sql
            return DummyResult()

        def close(self):
            captured["closed"] = True

    def fake_get_client(**kwargs):
        captured["kwargs"] = kwargs
        return DummyClient(**kwargs)

    monkeypatch.setattr(clickhouse_connect, "get_client", fake_get_client)

    connector = ClickHousePythonConnector(clickhouse_config)
    output = connector._execute_sync_query(
        host="localhost",
        port=9000,
        database="testdb",
        query="SELECT 1",
        original_port=9000,
        is_ssh_tunnel=False,
    )

    assert output == "col\n1"
    assert captured["kwargs"]["settings"]["readonly"] == 1
    assert captured["query"] == "SELECT 1"


@pytest.mark.anyio
@pytest.mark.parametrize("statement", CLICKHOUSE_PYTHON_BLOCKED_STATEMENTS)
async def test_clickhouse_python_blocks_mutations(
    statement, clickhouse_config, monkeypatch
):
    """Ensure the Python ClickHouse connector returns RuntimeError for writes."""

    def fake_sync_query(
        self,
        host,
        port,
        database,
        query,
        original_port=None,
        is_ssh_tunnel=False,
    ):
        assert query == statement
        raise ClickHouseError("Read-only violation")

    monkeypatch.setattr(
        ClickHousePythonConnector, "_execute_sync_query", fake_sync_query
    )

    connector = ClickHousePythonConnector(clickhouse_config)

    with pytest.raises(RuntimeError) as exc_info:
        await connector.execute_query(statement)

    assert "clickhouse" in str(exc_info.value).lower()


@pytest.mark.anyio
async def test_postgresql_python_write_attempt_raises_runtime(
    monkeypatch, postgres_config
):
    """Server-side read-only violations surface as RuntimeError to the caller."""

    def fake_sync_query(self, *args, **kwargs):
        raise psycopg2.Error("read-only violation")

    monkeypatch.setattr(
        PostgreSQLPythonConnector, "_execute_sync_query", fake_sync_query
    )

    connector = PostgreSQLPythonConnector(postgres_config)

    with pytest.raises(RuntimeError) as exc_info:
        await connector.execute_query("SELECT 1")

    assert "postgresql" in str(exc_info.value).lower()


@pytest.mark.anyio
async def test_clickhouse_python_write_attempt_raises_runtime(
    monkeypatch, clickhouse_config
):
    """Write attempts should surface as RuntimeError when ClickHouse rejects them."""

    def fake_sync_query(self, *args, **kwargs):
        raise ClickHouseError("Read-only violation")

    monkeypatch.setattr(
        ClickHousePythonConnector, "_execute_sync_query", fake_sync_query
    )

    connector = ClickHousePythonConnector(clickhouse_config)

    with pytest.raises(RuntimeError) as exc_info:
        await connector.execute_query("INSERT INTO events VALUES (now(), 'test')")

    assert "clickhouse" in str(exc_info.value).lower()


@pytest.mark.anyio
@pytest.mark.docker
@pytest.mark.usefixtures("docker_check")
async def test_clickhouse_python_readonly(clickhouse_config):
    """Test ClickHouse Python connector enforces read-only mode"""
    connector = ClickHousePythonConnector(clickhouse_config)

    # Test write operations are blocked
    write_queries = [
        "INSERT INTO events VALUES (now(), 'test', 'test_type', '{}')",
        "ALTER TABLE events ADD COLUMN test String",
        "DROP TABLE events",
        "CREATE TABLE test_table (id Int32) ENGINE = Memory",
    ]

    for query in write_queries:
        with pytest.raises(RuntimeError) as exc_info:
            await connector.execute_query(query)
        _assert_readonly_error(exc_info, "ClickHouse Python")

    # Test SELECT still works
    result = await connector.execute_query("SELECT 1")
    assert isinstance(result, str), "SELECT query should return TSV"


@pytest.mark.anyio
@pytest.mark.docker
@pytest.mark.usefixtures("docker_check")
async def test_clickhouse_cli_readonly(clickhouse_config):
    """Test ClickHouse CLI connector enforces read-only mode"""
    connector = ClickHouseCLIConnector(clickhouse_config)

    # Test SELECT works with readonly flag
    result = await connector.execute_query("SELECT 1 as test")
    assert isinstance(result, str), "SELECT query should return TSV"

    # Test write operations are blocked by --readonly=1
    write_queries = [
        "INSERT INTO events VALUES (now(), 'test', 'test_type', '{}')",
        "CREATE TABLE test_table (id Int32) ENGINE = Memory",
        "DROP TABLE events",
    ]

    for query in write_queries:
        with pytest.raises(RuntimeError) as exc_info:
            await connector.execute_query(query)
        _assert_readonly_error(exc_info, "ClickHouse CLI")


@pytest.mark.anyio
@pytest.mark.docker
@pytest.mark.usefixtures("docker_check")
class TestReadOnlyEnforcement:
    """Test suite for read-only enforcement across all implementations"""

    async def test_all_connectors_block_writes(
        self, postgres_config, clickhouse_config
    ):
        """Verify all connectors block write operations"""
        connectors = [
            ("PostgreSQL Python", PostgreSQLPythonConnector(postgres_config)),
            ("PostgreSQL CLI", PostgreSQLCLIConnector(postgres_config)),
            ("ClickHouse Python", ClickHousePythonConnector(clickhouse_config)),
            ("ClickHouse CLI", ClickHouseCLIConnector(clickhouse_config)),
        ]

        for name, connector in connectors:
            # Test a simple SELECT works
            result = await connector.execute_query("SELECT 1")
            assert isinstance(result, str), f"{name}: SELECT should return TSV"

            # Test INSERT is blocked (most basic write operation)
            if "PostgreSQL" in name:
                insert_query = "INSERT INTO users (username, email) VALUES ('testuser3', 'test3@example.com')"
            else:
                insert_query = "INSERT INTO events VALUES (now(), 'test', 'type', '{}')"

            # PostgreSQL is refused by the AST guard, ClickHouse by the server.
            expected = ReadOnlyQueryError if "PostgreSQL" in name else RuntimeError
            with pytest.raises(expected) as exc_info:
                await connector.execute_query(insert_query)
            _assert_readonly_error(exc_info, name)


@pytest.mark.anyio
@pytest.mark.docker
@pytest.mark.usefixtures("docker_check")
async def test_postgres_malicious_queries_blocked(postgres_config):
    """Ensure advanced PostgreSQL write attempts are rejected."""

    attack_queries = [
        "WITH up AS (UPDATE users SET email = 'blocked@example.com' WHERE id = 1 RETURNING *) SELECT count(*) FROM up",
        "DO $$ BEGIN INSERT INTO users (username, email) VALUES ('blocked_do', 'blocked@example.com'); END $$;",
        "CREATE TEMP TABLE temp_blocked (id INT)",
    ]

    connectors = [
        ("PostgreSQL Python", PostgreSQLPythonConnector(postgres_config)),
        ("PostgreSQL CLI", PostgreSQLCLIConnector(postgres_config)),
    ]

    for name, connector in connectors:
        for query in attack_queries:
            with pytest.raises(ReadOnlyQueryError) as exc_info:
                await connector.execute_query(query)
            _assert_readonly_error(exc_info, name)


@pytest.mark.anyio
@pytest.mark.docker
@pytest.mark.usefixtures("docker_check")
async def test_clickhouse_malicious_queries_blocked(clickhouse_config):
    """Ensure ClickHouse rejects trickier write or DDL statements."""

    attack_queries = [
        "TRUNCATE TABLE events",
        "ALTER TABLE events UPDATE event_type = 'blocked' WHERE 1",
        "ALTER TABLE events DELETE WHERE 1",
        "CREATE TEMPORARY TABLE temp_blocked (id Int32) ENGINE = Memory",
    ]

    connectors = [
        ("ClickHouse Python", ClickHousePythonConnector(clickhouse_config)),
        ("ClickHouse CLI", ClickHouseCLIConnector(clickhouse_config)),
    ]

    for name, connector in connectors:
        for query in attack_queries:
            with pytest.raises(RuntimeError) as exc_info:
                await connector.execute_query(query)
            _assert_readonly_error(exc_info, name)
