"""ClickHouse logins whose profile already enforces read-only.

Such a profile refuses the ``readonly=1`` and ``max_execution_time`` settings
the connectors send with every query. The connectors must retry without them,
because the profile is stricter than the client, and must not retry for any
other read-only refusal.
"""

import asyncio

import pytest
from clickhouse_connect.driver.exceptions import ProgrammingError

from mcp_read_only_sql.connectors.clickhouse.cli import ClickHouseCLIConnector
from mcp_read_only_sql.connectors.clickhouse.python import ClickHousePythonConnector
from mcp_read_only_sql.connectors.clickhouse.settings import refuses_client_settings
from mcp_read_only_sql.errors import ConnectorError
from tests.conftest import make_connection
from tests.docker_test_config import docker_test_server

PROFILE_REFUSAL = (
    "Password for user (readonly_user): Received exception from server "
    "(version 24.8.14):\nCode: 164. DB::Exception: Received from localhost:9000. "
    "DB::Exception: Cannot modify 'max_execution_time' setting in readonly mode. "
    "(READONLY)\n"
)
WRITE_REFUSAL = (
    "Code: 164. DB::Exception: testuser: Cannot execute query in readonly mode. "
    "(READONLY)\n"
)


class _Stdout:
    def __init__(self, lines):
        self._lines = [line.encode() for line in lines]

    async def readline(self):
        return self._lines.pop(0) if self._lines else b""


class _Stderr:
    def __init__(self, text: str):
        self._text = text.encode()

    async def read(self):
        text, self._text = self._text, b""
        return text


class _Stdin:
    def write(self, data):
        pass

    async def drain(self):
        pass

    def close(self):
        pass


class _Process:
    def __init__(self, stdout_lines, stderr_text="", returncode=0):
        self.stdout = _Stdout(stdout_lines)
        self.stderr = _Stderr(stderr_text)
        self.stdin = _Stdin()
        self.returncode = returncode

    async def wait(self):
        return self.returncode

    def kill(self):
        self.returncode = -9


def _fake_clickhouse_client(monkeypatch, processes):
    """Serve ``processes`` in order and record every command line."""
    commands = []

    async def fake_exec(*cmd, **kwargs):
        commands.append(list(cmd))
        return processes.pop(0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    return commands


@pytest.mark.parametrize(
    "message",
    [
        "Setting max_execution_time is readonly",
        "Setting readonly is unknown or readonly",
        "Cannot modify 'readonly' setting in readonly mode",
        PROFILE_REFUSAL,
    ],
)
def test_profile_refusal_is_recognised(message):
    assert refuses_client_settings(message)


@pytest.mark.parametrize(
    "message",
    [WRITE_REFUSAL, "Not enough privileges", "Connection refused"],
)
def test_other_errors_are_not_profile_refusals(message):
    assert not refuses_client_settings(message)


@pytest.mark.anyio
async def test_cli_retries_without_client_settings(clickhouse_config, monkeypatch):
    commands = _fake_clickhouse_client(
        monkeypatch,
        [_Process([], PROFILE_REFUSAL, returncode=1), _Process(["col\n", "1\n"])],
    )

    result = await ClickHouseCLIConnector(clickhouse_config).execute_query("SELECT 1")

    assert result == "col\n1"
    assert len(commands) == 2
    assert "--readonly" in commands[0] and "--max_execution_time" in commands[0]
    assert "--readonly" not in commands[1]
    assert "--max_execution_time" not in commands[1]
    assert "--ask-password" in commands[1]


@pytest.mark.anyio
async def test_cli_does_not_retry_other_readonly_errors(clickhouse_config, monkeypatch):
    commands = _fake_clickhouse_client(
        monkeypatch, [_Process([], WRITE_REFUSAL, returncode=1)]
    )

    with pytest.raises(ConnectorError, match="readonly mode"):
        await ClickHouseCLIConnector(clickhouse_config).execute_query(
            "SELECT 1"
        )

    assert len(commands) == 1


@pytest.mark.anyio
async def test_cli_error_drops_the_password_prompt(clickhouse_config, monkeypatch):
    _fake_clickhouse_client(
        monkeypatch,
        [_Process([], "Password for user (testuser): Code: 60. Unknown table\n", 1)],
    )

    with pytest.raises(ConnectorError) as exc_info:
        await ClickHouseCLIConnector(clickhouse_config).execute_query("SELECT 1")

    assert str(exc_info.value) == "clickhouse-client: Code: 60. Unknown table"


def _fake_get_client(monkeypatch, captured):
    """get_client that refuses settings the way a read-only profile does."""

    class FakeClient:
        def query(self, query, column_oriented=False):
            from types import SimpleNamespace

            return SimpleNamespace(column_names=["col"], result_rows=[[1]])

        def raw_stream(self, query, fmt, settings):
            captured["stream_settings"] = settings
            from io import BytesIO

            return BytesIO(b"col\n1\n")

        def close(self):
            pass

    def fake_get_client(**kwargs):
        captured.setdefault("settings", []).append(kwargs.get("settings"))
        if kwargs.get("settings"):
            raise ProgrammingError("Setting max_execution_time is readonly")
        return FakeClient()

    monkeypatch.setattr(
        "mcp_read_only_sql.connectors.clickhouse.python.clickhouse_connect.get_client",
        fake_get_client,
    )


@pytest.mark.anyio
async def test_python_retries_without_client_settings(clickhouse_config, monkeypatch):
    captured = {}
    _fake_get_client(monkeypatch, captured)

    result = await ClickHousePythonConnector(clickhouse_config).execute_query(
        "SELECT 1"
    )

    assert result == "col\n1"
    assert captured["settings"][0]["readonly"] == 1
    assert captured["settings"][1] is None


@pytest.mark.anyio
async def test_python_file_output_retries_without_client_settings(
    clickhouse_config, monkeypatch, tmp_path
):
    captured = {}
    _fake_get_client(monkeypatch, captured)
    output = tmp_path / "out.tsv"

    await ClickHousePythonConnector(clickhouse_config).execute_query_to_file(
        "SELECT 1", output
    )

    assert output.read_bytes() == b"col\n1\n"
    assert captured["stream_settings"] is None


@pytest.mark.anyio
async def test_python_does_not_retry_other_errors(clickhouse_config, monkeypatch):
    calls = []

    def fake_get_client(**kwargs):
        calls.append(kwargs)
        raise ProgrammingError("Setting max_threads is unknown")

    monkeypatch.setattr(
        "mcp_read_only_sql.connectors.clickhouse.python.clickhouse_connect.get_client",
        fake_get_client,
    )

    with pytest.raises(ConnectorError, match="max_threads"):
        await ClickHousePythonConnector(clickhouse_config).execute_query("SELECT 1")

    assert len(calls) == 1


def _profile_connector(implementation: str, username: str, password: str):
    connection = make_connection(
        {
            "connection_name": f"{username}_{implementation}",
            "type": "clickhouse",
            "servers": [docker_test_server("clickhouse")],
            "db": "testdb",
            "username": username,
            "password": password,
            "implementation": implementation,
        }
    )
    if implementation == "cli":
        return ClickHouseCLIConnector(connection)
    return ClickHousePythonConnector(connection)


PROFILE_USERS = [("readonly_user", "readonlypass"), ("readonly2_user", "readonly2pass")]


@pytest.mark.docker
@pytest.mark.usefixtures("docker_check")
@pytest.mark.anyio
@pytest.mark.parametrize("implementation", ["cli", "python"])
@pytest.mark.parametrize(("username", "password"), PROFILE_USERS)
async def test_real_profile_user_can_select(implementation, username, password):
    connector = _profile_connector(implementation, username, password)

    result = await connector.execute_query("SELECT count() FROM events")

    assert result.split("\n")[0] == "count()"
    assert int(result.split("\n")[1]) > 0


@pytest.mark.docker
@pytest.mark.usefixtures("docker_check")
@pytest.mark.anyio
@pytest.mark.parametrize("implementation", ["cli", "python"])
@pytest.mark.parametrize(("username", "password"), PROFILE_USERS)
async def test_real_profile_user_still_cannot_write(implementation, username, password):
    connector = _profile_connector(implementation, username, password)

    with pytest.raises(ConnectorError) as exc_info:
        await connector.execute_query("INSERT INTO events (user_id) VALUES (1)")

    message = str(exc_info.value).lower()
    assert "readonly" in message or "privileges" in message


@pytest.mark.docker
@pytest.mark.usefixtures("docker_check")
@pytest.mark.anyio
@pytest.mark.parametrize("implementation", ["cli", "python"])
async def test_real_profile_alone_refuses_table_functions(implementation):
    """readonly_user holds the URL grant, so only the profile stops url()."""
    connector = _profile_connector(implementation, "readonly_user", "readonlypass")

    with pytest.raises(ConnectorError, match="readonly mode"):
        await connector.execute_query(
            "SELECT * FROM url('http://127.0.0.1:8123/', 'RawBLOB') LIMIT 1"
        )
