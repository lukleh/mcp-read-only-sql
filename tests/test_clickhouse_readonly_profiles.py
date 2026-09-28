"""ClickHouse logins whose profile already enforces read-only.

Such a profile refuses the ``readonly=1`` and ``max_execution_time`` settings
the connectors send with every query. Each connector must find out once,
without the caller's statement, which settings the login accepts, drop only
the refused ones, and remember the answer. A statement is never re-run with
weaker settings: a statement's own SETTINGS clause produces the same refusal
text, and re-running it without readonly=1 would run it unguarded.
"""

import asyncio
from io import BytesIO
from types import SimpleNamespace

import pytest
from clickhouse_connect.driver.exceptions import ProgrammingError

from mcp_read_only_sql.connectors.clickhouse.cli import ClickHouseCLIConnector
from mcp_read_only_sql.connectors.clickhouse.python import ClickHousePythonConnector
from mcp_read_only_sql.connectors.clickhouse.settings import (
    refused_setting,
    without_refused,
)
from mcp_read_only_sql.errors import ConnectorError
from tests.conftest import make_connection
from tests.docker_test_config import docker_test_server

# What a readonly=1 profile answers to the client-side settings.
PROFILE_REFUSAL = (
    "Password for user (readonly_user): Received exception from server "
    "(version 24.8.14):\nCode: 164. DB::Exception: Received from localhost:9000. "
    "DB::Exception: Cannot modify 'max_execution_time' setting in readonly mode. "
    "(READONLY)\n"
)
# What a readonly=2 profile answers: only readonly itself is refused.
READONLY2_REFUSAL = (
    "Code: 164. DB::Exception: Cannot modify 'readonly' setting in readonly mode. "
    "(READONLY)\n"
)
# What a statement's own SETTINGS clause produces under readonly=1.
STATEMENT_REFUSAL = (
    "Code: 164. DB::Exception: Cannot modify 'max_threads' setting in readonly "
    "mode. (READONLY)\n"
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


def _ok(*lines):
    return _Process(list(lines))


def _fail(stderr_text):
    return _Process([], stderr_text, returncode=1)


def _fake_clickhouse_client(monkeypatch, processes):
    """Serve ``processes`` in order and record every command line."""
    commands = []

    async def fake_exec(*cmd, **kwargs):
        commands.append(list(cmd))
        return processes.pop(0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    return commands


def _query_of(cmd):
    return cmd[cmd.index("--query") + 1]


class TestSettingsHelpers:
    @pytest.mark.parametrize(
        ("message", "name"),
        [
            ("Setting max_execution_time is readonly", "max_execution_time"),
            ("Setting readonly is unknown or readonly", "readonly"),
            (PROFILE_REFUSAL, "max_execution_time"),
            (READONLY2_REFUSAL, "readonly"),
            (STATEMENT_REFUSAL, "max_threads"),
        ],
    )
    def test_refused_setting_names_the_setting(self, message, name):
        assert refused_setting(message) == name

    @pytest.mark.parametrize(
        "message", [WRITE_REFUSAL, "Not enough privileges", "Connection refused"]
    )
    def test_other_errors_name_nothing(self, message):
        assert refused_setting(message) is None
        assert without_refused({"readonly": 1}, message) is None

    def test_without_refused_drops_only_the_named_setting(self):
        settings = {"readonly": 1, "max_execution_time": 120}
        assert without_refused(settings, READONLY2_REFUSAL) == {
            "max_execution_time": 120
        }
        assert without_refused(settings, PROFILE_REFUSAL) == {"readonly": 1}

    def test_without_refused_ignores_settings_the_client_did_not_send(self):
        """A refusal naming max_threads came from the statement, not from us."""
        assert without_refused({"readonly": 1}, STATEMENT_REFUSAL) is None


class TestCLIProbe:
    @pytest.mark.anyio
    async def test_probe_drops_refused_setting_and_is_remembered(
        self, clickhouse_config, monkeypatch
    ):
        commands = _fake_clickhouse_client(
            monkeypatch,
            [_fail(PROFILE_REFUSAL), _ok(), _ok("col\n", "1\n"), _ok("col\n", "2\n")],
        )
        connector = ClickHouseCLIConnector(clickhouse_config)

        assert await connector.execute_query("SELECT 1 + 1") == "col\n1"

        probe_full, probe_reduced, query = commands
        assert _query_of(probe_full) == "SELECT 1"
        assert "--readonly" in probe_full and "--max_execution_time" in probe_full
        assert _query_of(probe_reduced) == "SELECT 1"
        assert "--readonly" in probe_reduced
        assert "--max_execution_time" not in probe_reduced
        assert _query_of(query) == "SELECT 1 + 1"
        assert "--readonly" in query and "--max_execution_time" not in query
        assert "--ask-password" in query

        assert await connector.execute_query("SELECT 2 + 2") == "col\n2"
        assert len(commands) == 4, "the second query must not probe again"
        assert _query_of(commands[3]) == "SELECT 2 + 2"

    @pytest.mark.anyio
    async def test_probe_keeps_the_timeout_for_a_readonly2_profile(
        self, clickhouse_config, monkeypatch
    ):
        commands = _fake_clickhouse_client(
            monkeypatch, [_fail(READONLY2_REFUSAL), _ok(), _ok("col\n", "1\n")]
        )

        await ClickHouseCLIConnector(clickhouse_config).execute_query("SELECT 1")

        assert "--readonly" not in commands[2]
        assert "--max_execution_time" in commands[2]

    @pytest.mark.anyio
    async def test_running_without_readonly_is_probed_before_every_statement(
        self, clickhouse_config, monkeypatch
    ):
        """Only a read-only profile makes dropping readonly=1 safe, and a
        profile can change, so that answer is never trusted across statements."""
        commands = _fake_clickhouse_client(
            monkeypatch,
            [
                _fail(READONLY2_REFUSAL), _ok(), _ok("col\n", "1\n"),  # first
                _ok(), _ok("col\n", "2\n"),  # second: profile now accepts readonly
            ],
        )
        connector = ClickHouseCLIConnector(clickhouse_config)

        await connector.execute_query("SELECT 1 + 1")
        await connector.execute_query("SELECT 2 + 2")

        assert [_query_of(cmd) for cmd in commands] == [
            "SELECT 1", "SELECT 1", "SELECT 1 + 1", "SELECT 1", "SELECT 2 + 2"
        ]
        assert "--readonly" in commands[4], "the statement runs with readonly again"

    @pytest.mark.anyio
    async def test_settings_are_remembered_per_server(self, monkeypatch):
        """A read-only profile on one server says nothing about another."""
        connection = make_connection(
            {
                "connection_name": "two_servers",
                "type": "clickhouse",
                "servers": ["a.example.com:9000", "b.example.com:9000"],
                "db": "testdb",
                "username": "u",
                "password": "p",
                "implementation": "cli",
            }
        )
        commands = _fake_clickhouse_client(
            monkeypatch,
            [
                _fail(PROFILE_REFUSAL), _ok(), _ok("col\n", "1\n"),  # a: drops timeout
                _ok(), _ok("col\n", "1\n"),  # b: probed on its own, keeps everything
                _ok("col\n", "1\n"),  # a again: remembered
            ],
        )
        connector = ClickHouseCLIConnector(connection)

        await connector.execute_query("SELECT 1 + 1", server="a.example.com")
        await connector.execute_query("SELECT 1 + 1", server="b.example.com")
        await connector.execute_query("SELECT 1 + 1", server="a.example.com")

        hosts = [cmd[cmd.index("--host") + 1] for cmd in commands]
        assert hosts == ["a.example.com"] * 3 + ["b.example.com"] * 2 + ["a.example.com"]
        assert "--max_execution_time" in commands[4], "b keeps the timeout"
        assert "--max_execution_time" not in commands[5], "a stays without it"

    @pytest.mark.anyio
    async def test_refusal_of_an_accepted_setting_forgets_the_answer(
        self, clickhouse_config, monkeypatch
    ):
        """After the probe a profile may still change; the next statement probes again."""
        commands = _fake_clickhouse_client(
            monkeypatch,
            [_ok(), _fail(READONLY2_REFUSAL), _fail(READONLY2_REFUSAL), _ok(), _ok("col\n", "1\n")],
        )
        connector = ClickHouseCLIConnector(clickhouse_config)

        with pytest.raises(ConnectorError, match="readonly"):
            await connector.execute_query("SELECT 1 + 1")
        await connector.execute_query("SELECT 2 + 2")

        assert [_query_of(cmd) for cmd in commands] == [
            "SELECT 1", "SELECT 1 + 1", "SELECT 1", "SELECT 1", "SELECT 2 + 2"
        ]

    @pytest.mark.anyio
    async def test_statement_refusal_never_reruns_without_readonly(
        self, clickhouse_config, monkeypatch
    ):
        """A refusal caused by the statement's own SETTINGS clause is final."""
        commands = _fake_clickhouse_client(
            monkeypatch, [_ok(), _fail(STATEMENT_REFUSAL)]
        )

        with pytest.raises(ConnectorError, match="max_threads"):
            await ClickHouseCLIConnector(clickhouse_config).execute_query(
                "INSERT INTO t SETTINGS max_threads = 1 VALUES (1)"
            )

        assert len(commands) == 2
        assert "--readonly" in commands[1]

    @pytest.mark.anyio
    async def test_write_refusal_is_not_retried(self, clickhouse_config, monkeypatch):
        commands = _fake_clickhouse_client(monkeypatch, [_ok(), _fail(WRITE_REFUSAL)])

        with pytest.raises(ConnectorError, match="readonly mode"):
            await ClickHouseCLIConnector(clickhouse_config).execute_query(
                "INSERT INTO t VALUES (1)"
            )

        assert len(commands) == 2

    @pytest.mark.anyio
    async def test_probe_failure_is_not_remembered(
        self, clickhouse_config, monkeypatch
    ):
        commands = _fake_clickhouse_client(
            monkeypatch, [_fail("Connection refused"), _fail("Connection refused")]
        )
        connector = ClickHouseCLIConnector(clickhouse_config)

        for _ in range(2):
            with pytest.raises(ConnectorError, match="Connection refused"):
                await connector.execute_query("SELECT 1")

        assert len(commands) == 2
        assert all("--readonly" in cmd for cmd in commands)

    @pytest.mark.anyio
    async def test_error_drops_the_password_prompt(self, clickhouse_config, monkeypatch):
        _fake_clickhouse_client(
            monkeypatch,
            [_ok(), _fail("Password for user (testuser): Code: 60. Unknown table\n")],
        )

        with pytest.raises(ConnectorError) as exc_info:
            await ClickHouseCLIConnector(clickhouse_config).execute_query("SELECT 1")

        assert str(exc_info.value) == "clickhouse-client: Code: 60. Unknown table"

    @pytest.mark.anyio
    async def test_error_drops_the_prompt_after_a_warning(
        self, clickhouse_config, monkeypatch
    ):
        _fake_clickhouse_client(
            monkeypatch,
            [_ok(), _fail("Warning: x\nPassword for user (testuser): Code: 60. Y\n")],
        )

        with pytest.raises(ConnectorError) as exc_info:
            await ClickHouseCLIConnector(clickhouse_config).execute_query("SELECT 1")

        assert str(exc_info.value) == "clickhouse-client: Warning: x\nCode: 60. Y"


def _fake_get_client(monkeypatch, captured, refused: str):
    """get_client that refuses ``refused`` the way a read-only profile does."""

    class FakeClient:
        def query(self, query, column_oriented=False):
            return SimpleNamespace(column_names=["col"], result_rows=[[1]])

        def raw_stream(self, query, fmt, settings):
            captured["stream_settings"] = settings
            return BytesIO(b"col\n1\n")

        def close(self):
            pass

    def fake_get_client(**kwargs):
        settings = kwargs.get("settings")
        captured.setdefault("settings", []).append(settings)
        if settings and refused in settings:
            raise ProgrammingError(f"Setting {refused} is readonly")
        return FakeClient()

    monkeypatch.setattr(
        "mcp_read_only_sql.connectors.clickhouse.python.clickhouse_connect.get_client",
        fake_get_client,
    )


class TestPythonProbe:
    @pytest.mark.anyio
    async def test_drops_refused_setting_and_is_remembered(
        self, clickhouse_config, monkeypatch
    ):
        captured = {}
        _fake_get_client(monkeypatch, captured, refused="max_execution_time")
        connector = ClickHousePythonConnector(clickhouse_config)

        assert await connector.execute_query("SELECT 1") == "col\n1"
        assert captured["settings"] == [
            {"readonly": 1, "max_execution_time": connector.query_timeout},
            {"readonly": 1},
        ]

        await connector.execute_query("SELECT 2")
        assert captured["settings"][2:] == [{"readonly": 1}]

    @pytest.mark.anyio
    async def test_keeps_the_timeout_for_a_readonly2_profile(
        self, clickhouse_config, monkeypatch
    ):
        captured = {}
        _fake_get_client(monkeypatch, captured, refused="readonly")
        connector = ClickHousePythonConnector(clickhouse_config)

        await connector.execute_query("SELECT 1")
        await connector.execute_query("SELECT 2")

        timeout = clickhouse_config.query_timeout
        assert captured["settings"] == [
            {"readonly": 1, "max_execution_time": timeout},
            {"max_execution_time": timeout},
            # Without readonly the answer is not trusted across statements.
            {"readonly": 1, "max_execution_time": timeout},
            {"max_execution_time": timeout},
        ]

    @pytest.mark.anyio
    async def test_settings_are_remembered_per_server(self, monkeypatch):
        connection = make_connection(
            {
                "connection_name": "two_servers",
                "type": "clickhouse",
                "servers": ["a.example.com:8123", "b.example.com:8123"],
                "db": "testdb",
                "username": "u",
                "password": "p",
                "implementation": "python",
            }
        )
        captured = {}
        seen_hosts = []

        def fake_get_client(**kwargs):
            seen_hosts.append((kwargs["host"], kwargs.get("settings")))
            if kwargs["host"] == "a.example.com" and kwargs.get("settings") and "max_execution_time" in kwargs["settings"]:
                raise ProgrammingError("Setting max_execution_time is readonly")
            return SimpleNamespace(
                query=lambda q, column_oriented=False: SimpleNamespace(column_names=["c"], result_rows=[[1]]),
                close=lambda: None,
            )

        monkeypatch.setattr(
            "mcp_read_only_sql.connectors.clickhouse.python.clickhouse_connect.get_client",
            fake_get_client,
        )
        connector = ClickHousePythonConnector(connection)
        full = {"readonly": 1, "max_execution_time": connection.query_timeout}

        await connector.execute_query("SELECT 1", server="a.example.com")
        await connector.execute_query("SELECT 1", server="b.example.com")
        await connector.execute_query("SELECT 1", server="a.example.com")

        assert seen_hosts == [
            ("a.example.com", full),
            ("a.example.com", {"readonly": 1}),
            ("b.example.com", full),
            ("a.example.com", {"readonly": 1}),
        ]
        del captured

    @pytest.mark.anyio
    async def test_file_output_uses_the_accepted_settings(
        self, clickhouse_config, monkeypatch, tmp_path
    ):
        captured = {}
        _fake_get_client(monkeypatch, captured, refused="max_execution_time")
        output = tmp_path / "out.tsv"

        await ClickHousePythonConnector(clickhouse_config).execute_query_to_file(
            "SELECT 1", output
        )

        assert output.read_bytes() == b"col\n1\n"
        assert captured["stream_settings"] == {"readonly": 1}

    @pytest.mark.anyio
    async def test_other_errors_are_not_retried(self, clickhouse_config, monkeypatch):
        calls = []

        def fake_get_client(**kwargs):
            calls.append(kwargs)
            raise ProgrammingError("Setting max_threads is unknown")

        monkeypatch.setattr(
            "mcp_read_only_sql.connectors.clickhouse.python.clickhouse_connect.get_client",
            fake_get_client,
        )

        with pytest.raises(ConnectorError, match="max_threads"):
            await ClickHousePythonConnector(clickhouse_config).execute_query(
                "SELECT 1"
            )

        assert len(calls) == 1


def _connector(implementation: str, username: str, password: str):
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


async def _event_count(connector) -> int:
    return int((await connector.execute_query("SELECT count() FROM events")).split("\n")[1])


@pytest.mark.docker
@pytest.mark.usefixtures("docker_check")
@pytest.mark.anyio
@pytest.mark.parametrize("implementation", ["cli", "python"])
class TestAgainstFixture:
    @pytest.mark.parametrize(("username", "password"), PROFILE_USERS)
    async def test_profile_user_can_select(self, implementation, username, password):
        connector = _connector(implementation, username, password)

        result = await connector.execute_query("SELECT count() FROM events")

        assert result.split("\n")[0] == "count()"
        assert int(result.split("\n")[1]) > 0

    @pytest.mark.parametrize(("username", "password"), PROFILE_USERS)
    async def test_profile_user_still_cannot_write(
        self, implementation, username, password
    ):
        """The fixture grants INSERT, so only the profile's readonly refuses it."""
        connector = _connector(implementation, username, password)

        with pytest.raises(ConnectorError, match="readonly mode"):
            await connector.execute_query("INSERT INTO events (user_id) VALUES (1)")

    async def test_profile_alone_refuses_table_functions(self, implementation):
        """readonly_user holds the URL grant, so only the profile stops url()."""
        connector = _connector(implementation, "readonly_user", "readonlypass")

        with pytest.raises(ConnectorError, match="readonly mode"):
            await connector.execute_query(
                "SELECT * FROM url('http://127.0.0.1:8123/', 'RawBLOB') LIMIT 1"
            )

    async def test_write_with_settings_clause_is_refused(self, implementation):
        """The regression behind the probe: a full-privilege login, a write
        whose SETTINGS clause triggers the settings refusal first."""
        connector = _connector(implementation, "testuser", "testpass")
        before = await _event_count(connector)

        with pytest.raises(ConnectorError, match="readonly mode"):
            await connector.execute_query(
                "INSERT INTO events (user_id) SETTINGS max_threads = 1 VALUES (1)"
            )

        assert await _event_count(connector) == before
