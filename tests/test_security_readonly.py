"""
Test to verify that all database implementations enforce read-only mode at the database session level.
This is a critical security test for Layer 2 of our three-layer security model.
"""

import asyncio

import psycopg2
import pytest
import clickhouse_connect
from src.connectors.postgresql.python import PostgreSQLPythonConnector
from src.connectors.postgresql.cli import PostgreSQLCLIConnector
from src.connectors.clickhouse.python import ClickHousePythonConnector
from src.connectors.clickhouse.cli import ClickHouseCLIConnector
from clickhouse_connect.driver.exceptions import ClickHouseError
from src.utils.sql_guard import sanitize_read_only_sql, ReadOnlyQueryError

from tests.sql_statement_lists import (
    CLICKHOUSE_DDL_STATEMENTS,
    CLICKHOUSE_DML_STATEMENTS,
    CLICKHOUSE_KILL_STATEMENTS,
    CLICKHOUSE_SYSTEM_STATEMENTS,
    POSTGRESQL_DDL_ALTER_STATEMENTS,
    POSTGRESQL_DDL_CREATE_STATEMENTS,
    POSTGRESQL_DDL_DROP_STATEMENTS,
    POSTGRESQL_DML_STATEMENTS,
    POSTGRESQL_LOCK_STATEMENTS,
    POSTGRESQL_MAINTENANCE_STATEMENTS,
    POSTGRESQL_PROCEDURAL_STATEMENTS,
    POSTGRESQL_ALLOWED_LITERAL_QUERIES,
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


@pytest.fixture
def postgres_config():
    """PostgreSQL test configuration"""
    return {
        "connection_name": "test_postgres",
        "type": "postgresql",
        "servers": [{"host": "localhost", "port": 5432}],
        "username": "testuser",
        "password": "testpass",
        "db": "testdb"
    }


@pytest.fixture
def clickhouse_config():
    """ClickHouse test configuration"""
    return {
        "connection_name": "test_clickhouse",
        "type": "clickhouse",
        "servers": [{"host": "localhost", "port": 9000}],
        "username": "testuser",
        "password": "testpass",
        "db": "testdb"
    }


@pytest.mark.anyio
async def test_postgresql_python_readonly(postgres_config):
    """Test PostgreSQL Python connector enforces read-only mode"""
    connector = PostgreSQLPythonConnector(postgres_config)

    # Test write operations are blocked
    write_queries = [
        "INSERT INTO users (username, email) VALUES ('testuser', 'test@example.com')",
        "UPDATE users SET email = 'changed@example.com' WHERE id = 1",
        "DELETE FROM users WHERE id = 1",
        "DROP TABLE users",
        "CREATE TABLE new_table (id INT)"
    ]

    for query in write_queries:
        with pytest.raises(RuntimeError) as exc_info:
            await connector.execute_query(query)
        _assert_readonly_error(exc_info, "PostgreSQL Python")

    # Test SELECT still works
    result = await connector.execute_query("SELECT 1")
    assert isinstance(result, str), "SELECT query should return TSV"


@pytest.mark.anyio
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
        "DELETE FROM users WHERE id = 1"
    ]

    for query in write_queries:
        with pytest.raises(RuntimeError) as exc_info:
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
            self.stdout = DummyStdout(["BEGIN\n", "SET\n", "col\n", "COMMIT\n"])
            self.stderr = DummyStderr()
            self.returncode = 0

        async def wait(self):
            return 0

        def kill(self):
            self.returncode = -9

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        captured["cmd"] = list(cmd)
        captured["env"] = kwargs.get("env", {})
        return DummyProcess()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    connector = PostgreSQLCLIConnector(postgres_config)
    result = await connector.execute_query("SELECT 1 as test")

    assert result == "col"
    cmd = captured["cmd"]
    assert "--single-transaction" in cmd
    assert "-v" in cmd and "ON_ERROR_STOP=1" in cmd
    assert any("SELECT 1 as test" in part for part in cmd if isinstance(part, str))
    env = captured["env"]
    assert "default_transaction_read_only=on" in env.get("PGOPTIONS", "")


@pytest.mark.anyio
@pytest.mark.parametrize("statement", POSTGRESQL_DML_STATEMENTS)
async def test_postgresql_cli_blocks_write_statements(statement, postgres_config, monkeypatch):
    """Write-oriented SQL should surface as runtime errors in the CLI connector."""

    connector = PostgreSQLCLIConnector(postgres_config)
    called = {"value": False}

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        called["value"] = True
        return _FakeProcess(f"ERROR: cannot execute {statement} in a read-only transaction")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(RuntimeError) as exc_info:
        await connector.execute_query(statement)

    assert called["value"], "psql was not invoked"
    assert "psql" in str(exc_info.value).lower()


@pytest.mark.anyio
@pytest.mark.parametrize("statement", POSTGRESQL_DDL_CREATE_STATEMENTS)
async def test_postgresql_cli_blocks_create_statements(statement, postgres_config, monkeypatch):
    """All CREATE statements must be blocked in read-only mode."""

    connector = PostgreSQLCLIConnector(postgres_config)

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        return _FakeProcess(f"ERROR: READ ONLY: {statement}")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(RuntimeError):
        await connector.execute_query(statement)


@pytest.mark.anyio
@pytest.mark.parametrize("statement", POSTGRESQL_DDL_ALTER_STATEMENTS)
async def test_postgresql_cli_blocks_alter_statements(statement, postgres_config, monkeypatch):
    """ALTER statements should be rejected by the CLI connector."""

    connector = PostgreSQLCLIConnector(postgres_config)

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        return _FakeProcess(f"ERROR: READ ONLY: {statement}")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(RuntimeError):
        await connector.execute_query(statement)


@pytest.mark.anyio
@pytest.mark.parametrize("statement", POSTGRESQL_DDL_DROP_STATEMENTS)
async def test_postgresql_cli_blocks_drop_statements(statement, postgres_config, monkeypatch):
    """DROP statements must fail in read-only mode."""

    connector = PostgreSQLCLIConnector(postgres_config)

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        return _FakeProcess(f"ERROR: READ ONLY: {statement}")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(RuntimeError):
        await connector.execute_query(statement)


@pytest.mark.anyio
@pytest.mark.parametrize("statement", POSTGRESQL_MAINTENANCE_STATEMENTS)
async def test_postgresql_cli_blocks_maintenance_statements(statement, postgres_config, monkeypatch):
    """Maintenance commands that mutate state should be rejected."""

    connector = PostgreSQLCLIConnector(postgres_config)

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        return _FakeProcess(f"ERROR: READ ONLY: {statement}")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(RuntimeError):
        await connector.execute_query(statement)


@pytest.mark.anyio
@pytest.mark.parametrize("statement", POSTGRESQL_PROCEDURAL_STATEMENTS)
async def test_postgresql_cli_blocks_procedural_statements(statement, postgres_config, monkeypatch):
    """Procedural constructs should not bypass read-only enforcement."""

    connector = PostgreSQLCLIConnector(postgres_config)

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        return _FakeProcess(f"ERROR: READ ONLY: {statement}")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(RuntimeError):
        await connector.execute_query(statement)


@pytest.mark.anyio
@pytest.mark.parametrize("statement", POSTGRESQL_LOCK_STATEMENTS)
async def test_postgresql_cli_blocks_lock_statements(statement, postgres_config, monkeypatch):
    """Locking operations that require write access should fail."""

    connector = PostgreSQLCLIConnector(postgres_config)

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        return _FakeProcess(f"ERROR: READ ONLY: {statement}")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(RuntimeError):
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
        return DummyProcess()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    connector = ClickHouseCLIConnector(clickhouse_config)
    result = await connector.execute_query("SELECT 1")

    assert result == "col"
    cmd = captured["cmd"]
    assert "--readonly" in cmd
    assert "--max_execution_time" in cmd
    assert any(part == "SELECT 1" for part in cmd)
    # No environment mutations expected, but keep assertion for completeness
    assert captured["env"] is not None


@pytest.mark.anyio
@pytest.mark.parametrize("statement", CLICKHOUSE_DML_STATEMENTS)
async def test_clickhouse_cli_blocks_mutations(statement, clickhouse_config, monkeypatch):
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
async def test_clickhouse_cli_blocks_system_commands(statement, clickhouse_config, monkeypatch):
    """SYSTEM commands that mutate state must be refused."""

    connector = ClickHouseCLIConnector(clickhouse_config)

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        return _FakeProcess(f"READONLY: {statement}")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(RuntimeError):
        await connector.execute_query(statement)


@pytest.mark.anyio
@pytest.mark.parametrize("statement", CLICKHOUSE_KILL_STATEMENTS)
async def test_clickhouse_cli_blocks_kill_statements(statement, clickhouse_config, monkeypatch):
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
            self._rows = [{"col": 1}]

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
    output, truncated = connector._execute_sync_query(
        host="localhost",
        port=5432,
        database="testdb",
        query="SELECT 1",
        max_result_bytes=0,
    )

    assert output == "col\n1"
    assert truncated is False
    assert captured["connect_kwargs"]["options"] == "-c default_transaction_read_only=on"
    assert captured["session_args"] == (True, True)
    assert any("SET statement_timeout" in sql for sql in captured["executed"])


@pytest.mark.anyio
@pytest.mark.parametrize("statement", POSTGRESQL_PYTHON_BLOCKED_STATEMENTS)
async def test_postgresql_python_blocks_write_statements(statement, postgres_config, monkeypatch):
    """The Python connector should surface read-only errors for every mutation."""

    def fake_sync_query(self, host, port, database, query, max_result_bytes):
        assert query == statement
        raise psycopg2.Error("read-only violation")

    monkeypatch.setattr(PostgreSQLPythonConnector, "_execute_sync_query", fake_sync_query)

    connector = PostgreSQLPythonConnector(postgres_config)

    with pytest.raises((ReadOnlyQueryError, RuntimeError)) as exc_info:
        await connector.execute_query(statement)

    if isinstance(exc_info.value, ReadOnlyQueryError):
        assert "transaction" in str(exc_info.value).lower()
    else:
        assert "postgresql" in str(exc_info.value).lower()


def test_clickhouse_python_sets_readonly_setting(monkeypatch, clickhouse_config):
    """clickhouse-connect client must be instantiated with readonly=1."""

    captured = {}

    class DummyResult:
        column_names = ["col"]
        result_rows = [[1]]

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
    output, truncated = connector._execute_sync_query(
        host="localhost",
        port=9000,
        database="testdb",
        query="SELECT 1",
        max_result_bytes=0,
        original_port=9000,
        is_ssh_tunnel=False,
    )

    assert output == "col\n1"
    assert truncated is False
    assert captured["kwargs"]["settings"]["readonly"] == 1
    assert captured["query"] == "SELECT 1"


@pytest.mark.anyio
@pytest.mark.parametrize("statement", CLICKHOUSE_PYTHON_BLOCKED_STATEMENTS)
async def test_clickhouse_python_blocks_mutations(statement, clickhouse_config, monkeypatch):
    """Ensure the Python ClickHouse connector returns RuntimeError for writes."""

    def fake_sync_query(
        self,
        host,
        port,
        database,
        query,
        max_result_bytes,
        original_port=None,
        is_ssh_tunnel=False,
    ):
        assert query == statement
        raise ClickHouseError("Read-only violation")

    monkeypatch.setattr(ClickHousePythonConnector, "_execute_sync_query", fake_sync_query)

    connector = ClickHousePythonConnector(clickhouse_config)

    with pytest.raises(RuntimeError) as exc_info:
        await connector.execute_query(statement)

    assert "clickhouse" in str(exc_info.value).lower()


@pytest.mark.anyio
async def test_postgresql_python_write_attempt_raises_runtime(monkeypatch, postgres_config):
    """Write attempts should surface as RuntimeError to the caller."""

    def fake_sync_query(self, *args, **kwargs):
        raise psycopg2.Error("read-only violation")

    monkeypatch.setattr(PostgreSQLPythonConnector, "_execute_sync_query", fake_sync_query)

    connector = PostgreSQLPythonConnector(postgres_config)

    with pytest.raises(RuntimeError) as exc_info:
        await connector.execute_query("INSERT INTO users VALUES (1)")

    assert "postgresql" in str(exc_info.value).lower()


@pytest.mark.anyio
async def test_clickhouse_python_write_attempt_raises_runtime(monkeypatch, clickhouse_config):
    """Write attempts should surface as RuntimeError when ClickHouse rejects them."""

    def fake_sync_query(self, *args, **kwargs):
        raise ClickHouseError("Read-only violation")

    monkeypatch.setattr(ClickHousePythonConnector, "_execute_sync_query", fake_sync_query)

    connector = ClickHousePythonConnector(clickhouse_config)

    with pytest.raises(RuntimeError) as exc_info:
        await connector.execute_query("INSERT INTO events VALUES (now(), 'test')")

    assert "clickhouse" in str(exc_info.value).lower()


@pytest.mark.anyio
async def test_clickhouse_python_readonly(clickhouse_config):
    """Test ClickHouse Python connector enforces read-only mode"""
    connector = ClickHousePythonConnector(clickhouse_config)

    # Test write operations are blocked
    write_queries = [
        "INSERT INTO events VALUES (now(), 'test', 'test_type', '{}')",
        "ALTER TABLE events ADD COLUMN test String",
        "DROP TABLE events",
        "CREATE TABLE test_table (id Int32) ENGINE = Memory"
    ]

    for query in write_queries:
        with pytest.raises(RuntimeError) as exc_info:
            await connector.execute_query(query)
        _assert_readonly_error(exc_info, "ClickHouse Python")

    # Test SELECT still works
    result = await connector.execute_query("SELECT 1")
    assert isinstance(result, str), "SELECT query should return TSV"


@pytest.mark.anyio
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
        "DROP TABLE events"
    ]

    for query in write_queries:
        with pytest.raises(RuntimeError) as exc_info:
            await connector.execute_query(query)
        _assert_readonly_error(exc_info, "ClickHouse CLI")


@pytest.mark.anyio
class TestReadOnlyEnforcement:
    """Test suite for read-only enforcement across all implementations"""

    async def test_all_connectors_block_writes(self, postgres_config, clickhouse_config):
        """Verify all connectors block write operations"""
        connectors = [
            ("PostgreSQL Python", PostgreSQLPythonConnector(postgres_config)),
            ("PostgreSQL CLI", PostgreSQLCLIConnector(postgres_config)),
            ("ClickHouse Python", ClickHousePythonConnector(clickhouse_config)),
            ("ClickHouse CLI", ClickHouseCLIConnector(clickhouse_config))
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

            with pytest.raises(RuntimeError) as exc_info:
                await connector.execute_query(insert_query)
            _assert_readonly_error(exc_info, name)


@pytest.mark.anyio
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
            with pytest.raises(RuntimeError) as exc_info:
                await connector.execute_query(query)
            _assert_readonly_error(exc_info, name)


@pytest.mark.anyio
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
