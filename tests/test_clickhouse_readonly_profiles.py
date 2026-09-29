"""ClickHouse logins whose profile already enforces read-only.

Such a profile locks the ``readonly`` and ``max_execution_time`` settings the
connectors send with every query. Each connector reads ``system.settings``
first, without any client-side settings, and sends only the settings the
profile leaves changeable. A login whose profile locks ``readonly`` at 0 is
refused. The answer is remembered per server, and a statement is never
re-run with weaker settings: a statement's own SETTINGS clause produces the
same refusal text, and re-running it without readonly=1 would run it
unguarded.
"""

import asyncio
from io import BytesIO
from types import SimpleNamespace

import pytest
from clickhouse_connect.driver.exceptions import ProgrammingError

from mcp_read_only_sql.connectors.clickhouse.cli import ClickHouseCLIConnector
from mcp_read_only_sql.connectors.clickhouse.python import ClickHousePythonConnector
from mcp_read_only_sql.connectors.clickhouse.settings import (
    PROBE_QUERY,
    decide_client_settings,
    refused_setting,
    refuses_sent_setting,
)
from mcp_read_only_sql.errors import ConnectorError
from tests.conftest import make_connection
from tests.docker_test_config import docker_test_server

# system.settings rows (name, value, readonly) as the profiles report them.
NORMAL_PROFILE = [["max_execution_time", "0", "0"], ["readonly", "0", "0"]]
READONLY1_PROFILE = [["max_execution_time", "0", "1"], ["readonly", "1", "1"]]
READONLY2_PROFILE = [["max_execution_time", "0", "0"], ["readonly", "2", "1"]]
LOCKED_WRITABLE_PROFILE = [["max_execution_time", "0", "0"], ["readonly", "0", "1"]]
# A constraint on the timeout alone; readonly stays the client's to set.
TIMEOUT_LOCKED_PROFILE = [["max_execution_time", "0", "1"], ["readonly", "0", "0"]]

# What a readonly=1 profile answers to a client-side setting.
PROFILE_REFUSAL = (
    "Password for user (readonly_user): Received exception from server "
    "(version 26.3.35):\nCode: 164. DB::Exception: Received from localhost:9000. "
    "DB::Exception: Cannot modify 'max_execution_time' setting in readonly mode. "
    "(READONLY)\n"
)
# What a readonly=2 profile answers: only readonly itself is refused.
READONLY2_REFUSAL = (
    "Code: 164. DB::Exception: Cannot modify 'readonly' setting in readonly mode. "
    "(READONLY)\n"
)
# What a profile constraint answers when it locks a previously changeable setting.
CONSTRAINT_REFUSAL = (
    "DB::Exception: Setting readonly should not be changed. "
    "(SETTING_CONSTRAINT_VIOLATION)\n"
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


def _probe(profile):
    """What clickhouse-client prints for PROBE_QUERY in TabSeparated format."""
    return _ok(*("\t".join(row) + "\n" for row in profile))


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


def _format_of(cmd):
    return cmd[cmd.index("--format") + 1]


class TestSettingsHelpers:
    def test_normal_profile_gets_both_settings(self):
        assert decide_client_settings(NORMAL_PROFILE, 120) == {
            "readonly": 1,
            "max_execution_time": 120,
        }

    def test_readonly1_profile_gets_nothing(self):
        assert decide_client_settings(READONLY1_PROFILE, 120) == {}

    def test_readonly2_profile_keeps_the_timeout(self):
        assert decide_client_settings(READONLY2_PROFILE, 120) == {
            "max_execution_time": 120
        }

    def test_locked_timeout_keeps_readonly(self):
        assert decide_client_settings(TIMEOUT_LOCKED_PROFILE, 120) == {"readonly": 1}

    def test_readonly_locked_at_zero_is_refused(self):
        """Neither the client nor the profile makes this login read-only."""
        with pytest.raises(ConnectorError, match="cannot be made read-only"):
            decide_client_settings(LOCKED_WRITABLE_PROFILE, 120)

    def test_missing_rows_leave_the_server_the_last_word(self):
        """Without a row the setting is sent; the server may still refuse it."""
        assert decide_client_settings([], 120) == {
            "readonly": 1,
            "max_execution_time": 120,
        }
        assert decide_client_settings([["x"], ["y", "1"]], 120) == {
            "readonly": 1,
            "max_execution_time": 120,
        }

    def test_typed_rows_are_read_like_text(self):
        """clickhouse-connect returns typed values, clickhouse-client text."""
        assert decide_client_settings([("readonly", "1", 1)], 120) == {
            "max_execution_time": 120
        }

    @pytest.mark.parametrize(
        ("message", "name"),
        [
            ("Setting max_execution_time is readonly", "max_execution_time"),
            ("Setting readonly is unknown or readonly", "readonly"),
            (PROFILE_REFUSAL, "max_execution_time"),
            (READONLY2_REFUSAL, "readonly"),
            (CONSTRAINT_REFUSAL, "readonly"),
            (
                "Setting max_execution_time should not be changed",
                "max_execution_time",
            ),
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
        assert not refuses_sent_setting({"readonly": 1}, message)

    def test_refuses_sent_setting_only_for_settings_the_client_sent(self):
        """A refusal naming max_threads came from the statement, not from us."""
        settings = {"readonly": 1, "max_execution_time": 120}
        assert refuses_sent_setting(settings, READONLY2_REFUSAL)
        assert refuses_sent_setting(settings, PROFILE_REFUSAL)
        assert not refuses_sent_setting({"readonly": 1}, PROFILE_REFUSAL)
        assert not refuses_sent_setting(settings, STATEMENT_REFUSAL)


class TestCLIProbe:
    @pytest.mark.anyio
    async def test_probe_reads_system_settings_without_client_settings(
        self, clickhouse_config, monkeypatch
    ):
        commands = _fake_clickhouse_client(
            monkeypatch, [_probe(NORMAL_PROFILE), _ok("col\n", "1\n")]
        )

        assert (
            await ClickHouseCLIConnector(clickhouse_config).execute_query("SELECT 1")
            == "col\n1"
        )

        probe, query = commands
        assert _query_of(probe) == PROBE_QUERY
        assert _format_of(probe) == "TabSeparated"
        assert "--readonly" not in probe and "--max_execution_time" not in probe
        assert "--ask-password" in probe
        assert _query_of(query) == "SELECT 1"
        assert _format_of(query) == "TabSeparatedWithNames"
        assert "--readonly" in query and "--max_execution_time" in query
        assert "--ask-password" in query

    @pytest.mark.anyio
    async def test_locked_setting_is_not_sent_and_the_answer_is_remembered(
        self, clickhouse_config, monkeypatch
    ):
        commands = _fake_clickhouse_client(
            monkeypatch,
            [_probe(TIMEOUT_LOCKED_PROFILE), _ok("col\n", "1\n"), _ok("col\n", "2\n")],
        )
        connector = ClickHouseCLIConnector(clickhouse_config)

        assert await connector.execute_query("SELECT 1 + 1") == "col\n1"
        assert await connector.execute_query("SELECT 2 + 2") == "col\n2"

        assert [_query_of(cmd) for cmd in commands] == [
            PROBE_QUERY, "SELECT 1 + 1", "SELECT 2 + 2"
        ], "the second statement must not read system.settings again"
        for cmd in commands[1:]:
            assert "--readonly" in cmd and "--max_execution_time" not in cmd

    @pytest.mark.anyio
    async def test_readonly1_profile_sends_nothing(
        self, clickhouse_config, monkeypatch
    ):
        commands = _fake_clickhouse_client(
            monkeypatch, [_probe(READONLY1_PROFILE), _ok("col\n", "1\n")]
        )

        await ClickHouseCLIConnector(clickhouse_config).execute_query("SELECT 1")

        assert "--readonly" not in commands[1]
        assert "--max_execution_time" not in commands[1]

    @pytest.mark.anyio
    async def test_readonly2_profile_keeps_the_timeout(
        self, clickhouse_config, monkeypatch
    ):
        commands = _fake_clickhouse_client(
            monkeypatch, [_probe(READONLY2_PROFILE), _ok("col\n", "1\n")]
        )

        await ClickHouseCLIConnector(clickhouse_config).execute_query("SELECT 1")

        assert "--readonly" not in commands[1]
        assert "--max_execution_time" in commands[1]

    @pytest.mark.anyio
    async def test_readonly_locked_at_zero_refuses_the_statement(
        self, clickhouse_config, monkeypatch
    ):
        commands = _fake_clickhouse_client(
            monkeypatch, [_probe(LOCKED_WRITABLE_PROFILE)]
        )

        with pytest.raises(ConnectorError, match="cannot be made read-only"):
            await ClickHouseCLIConnector(clickhouse_config).execute_query("SELECT 1")

        assert [_query_of(cmd) for cmd in commands] == [PROBE_QUERY]

    @pytest.mark.anyio
    async def test_running_without_readonly_is_reread_before_every_statement(
        self, clickhouse_config, monkeypatch
    ):
        """Only a read-only profile makes dropping readonly=1 safe, and a
        profile can change, so that answer is never trusted across statements."""
        commands = _fake_clickhouse_client(
            monkeypatch,
            [
                _probe(READONLY2_PROFILE), _ok("col\n", "1\n"),  # first
                _probe(NORMAL_PROFILE), _ok("col\n", "2\n"),  # profile changed
            ],
        )
        connector = ClickHouseCLIConnector(clickhouse_config)

        await connector.execute_query("SELECT 1 + 1")
        await connector.execute_query("SELECT 2 + 2")

        assert [_query_of(cmd) for cmd in commands] == [
            PROBE_QUERY, "SELECT 1 + 1", PROBE_QUERY, "SELECT 2 + 2"
        ]
        assert "--readonly" not in commands[1]
        assert "--readonly" in commands[3], "the statement runs with readonly again"

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
                _probe(READONLY2_PROFILE), _ok("col\n", "1\n"),  # a: no readonly
                _probe(NORMAL_PROFILE), _ok("col\n", "1\n"),  # b: read on its own
                _ok("col\n", "1\n"),  # b again: remembered
            ],
        )
        connector = ClickHouseCLIConnector(connection)

        await connector.execute_query("SELECT 1 + 1", server="a.example.com")
        await connector.execute_query("SELECT 1 + 1", server="b.example.com")
        await connector.execute_query("SELECT 1 + 1", server="b.example.com")

        hosts = [cmd[cmd.index("--host") + 1] for cmd in commands]
        assert hosts == ["a.example.com"] * 2 + ["b.example.com"] * 3
        assert "--readonly" not in commands[1], "a runs under its profile"
        assert "--readonly" in commands[3] and "--readonly" in commands[4]

    @pytest.mark.anyio
    async def test_refusal_of_a_sent_setting_forgets_the_answer(
        self, clickhouse_config, monkeypatch
    ):
        """After the read a profile may still change; the next statement reads again."""
        commands = _fake_clickhouse_client(
            monkeypatch,
            [
                _probe(NORMAL_PROFILE), _fail(READONLY2_REFUSAL),
                _probe(READONLY2_PROFILE), _ok("col\n", "1\n"),
            ],
        )
        connector = ClickHouseCLIConnector(clickhouse_config)

        with pytest.raises(ConnectorError, match="readonly"):
            await connector.execute_query("SELECT 1 + 1")
        await connector.execute_query("SELECT 2 + 2")

        assert [_query_of(cmd) for cmd in commands] == [
            PROBE_QUERY, "SELECT 1 + 1", PROBE_QUERY, "SELECT 2 + 2"
        ]

    @pytest.mark.anyio
    async def test_new_readonly_constraint_invalidates_cached_settings(
        self, clickhouse_config, monkeypatch
    ):
        commands = _fake_clickhouse_client(
            monkeypatch,
            [
                _probe(NORMAL_PROFILE),
                _ok("col\n", "1\n"),
                _fail(CONSTRAINT_REFUSAL),
                _probe(LOCKED_WRITABLE_PROFILE),
            ],
        )
        connector = ClickHouseCLIConnector(clickhouse_config)

        await connector.execute_query("SELECT 1")
        with pytest.raises(ConnectorError, match="SETTING_CONSTRAINT_VIOLATION"):
            await connector.execute_query("SELECT 2")
        with pytest.raises(ConnectorError, match="cannot be made read-only"):
            await connector.execute_query("SELECT 3")

        assert [_query_of(cmd) for cmd in commands] == [
            PROBE_QUERY,
            "SELECT 1",
            "SELECT 2",
            PROBE_QUERY,
        ]

    @pytest.mark.anyio
    async def test_statement_refusal_never_reruns_without_readonly(
        self, clickhouse_config, monkeypatch
    ):
        """A refusal caused by the statement's own SETTINGS clause is final."""
        commands = _fake_clickhouse_client(
            monkeypatch, [_probe(NORMAL_PROFILE), _fail(STATEMENT_REFUSAL)]
        )
        connector = ClickHouseCLIConnector(clickhouse_config)

        with pytest.raises(ConnectorError, match="max_threads"):
            await connector.execute_query(
                "INSERT INTO t SETTINGS max_threads = 1 VALUES (1)"
            )

        assert len(commands) == 2
        assert "--readonly" in commands[1]
        assert connector._decided_settings, "not our setting: the answer stays"

    @pytest.mark.anyio
    async def test_write_refusal_is_not_retried(self, clickhouse_config, monkeypatch):
        commands = _fake_clickhouse_client(
            monkeypatch, [_probe(NORMAL_PROFILE), _fail(WRITE_REFUSAL)]
        )

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

        assert [_query_of(cmd) for cmd in commands] == [PROBE_QUERY, PROBE_QUERY]
        assert not connector._decided_settings

    @pytest.mark.anyio
    async def test_error_drops_the_password_prompt(self, clickhouse_config, monkeypatch):
        _fake_clickhouse_client(
            monkeypatch,
            [
                _probe(NORMAL_PROFILE),
                _fail("Password for user (testuser): Code: 60. Unknown table\n"),
            ],
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
            [
                _probe(NORMAL_PROFILE),
                _fail("Warning: x\nPassword for user (testuser): Code: 60. Y\n"),
            ],
        )

        with pytest.raises(ConnectorError) as exc_info:
            await ClickHouseCLIConnector(clickhouse_config).execute_query("SELECT 1")

        assert str(exc_info.value) == "clickhouse-client: Warning: x\nCode: 60. Y"


def _fake_get_client(monkeypatch, captured, profiles, refuse=None):
    """A clickhouse-connect client whose system.settings reports ``profiles``.

    ``profiles`` is served one per client; the last one repeats. ``refuse``
    is an error text raised by every statement that is not the probe.
    Records the settings passed to get_client and to each query.
    """
    served = list(profiles)

    class FakeClient:
        def __init__(self):
            self.profile = served.pop(0) if len(served) > 1 else served[0]

        def query(self, query, column_oriented=False, settings=None):
            if query == PROBE_QUERY:
                assert settings is None, "the probe carries no client settings"
                return SimpleNamespace(
                    column_names=["name", "value", "readonly"],
                    result_rows=[tuple(row) for row in self.profile],
                )
            captured.setdefault("queries", []).append((query, settings))
            if refuse is not None:
                raise ProgrammingError(refuse)
            return SimpleNamespace(column_names=["col"], result_rows=[[1]])

        def raw_stream(self, query, fmt, settings):
            captured["stream_settings"] = settings
            return BytesIO(b"col\n1\n")

        def close(self):
            captured["closed"] = captured.get("closed", 0) + 1

    def fake_get_client(**kwargs):
        captured.setdefault("client_settings", []).append(kwargs.get("settings"))
        return FakeClient()

    monkeypatch.setattr(
        "mcp_read_only_sql.connectors.clickhouse.python.clickhouse_connect.get_client",
        fake_get_client,
    )


class TestPythonProbe:
    @pytest.mark.anyio
    async def test_client_carries_no_settings_and_statements_get_the_decided_ones(
        self, clickhouse_config, monkeypatch
    ):
        captured = {}
        _fake_get_client(monkeypatch, captured, [NORMAL_PROFILE])
        connector = ClickHousePythonConnector(clickhouse_config)

        assert await connector.execute_query("SELECT 1") == "col\n1"

        assert captured["client_settings"] == [None]
        assert captured["queries"] == [
            ("SELECT 1", {"readonly": 1, "max_execution_time": connector.query_timeout})
        ]
        assert captured["closed"] == 1

    @pytest.mark.anyio
    async def test_locked_setting_is_not_sent_and_the_answer_is_remembered(
        self, clickhouse_config, monkeypatch
    ):
        captured = {}
        _fake_get_client(monkeypatch, captured, [TIMEOUT_LOCKED_PROFILE, NORMAL_PROFILE])
        connector = ClickHousePythonConnector(clickhouse_config)

        await connector.execute_query("SELECT 1")
        await connector.execute_query("SELECT 2")

        # The second client would report a normal profile, but with readonly
        # sent under the first answer the settings are not read again.
        assert captured["queries"] == [
            ("SELECT 1", {"readonly": 1}),
            ("SELECT 2", {"readonly": 1}),
        ]

    @pytest.mark.anyio
    async def test_readonly1_profile_sends_nothing(
        self, clickhouse_config, monkeypatch
    ):
        captured = {}
        _fake_get_client(monkeypatch, captured, [READONLY1_PROFILE])

        await ClickHousePythonConnector(clickhouse_config).execute_query("SELECT 1")

        assert captured["queries"] == [("SELECT 1", None)]

    @pytest.mark.anyio
    async def test_readonly2_profile_is_reread_before_every_statement(
        self, clickhouse_config, monkeypatch
    ):
        captured = {}
        _fake_get_client(monkeypatch, captured, [READONLY2_PROFILE, NORMAL_PROFILE])
        connector = ClickHousePythonConnector(clickhouse_config)

        await connector.execute_query("SELECT 1")
        await connector.execute_query("SELECT 2")

        timeout = clickhouse_config.query_timeout
        assert captured["queries"] == [
            ("SELECT 1", {"max_execution_time": timeout}),
            # Without readonly the answer is not trusted across statements.
            ("SELECT 2", {"readonly": 1, "max_execution_time": timeout}),
        ]

    @pytest.mark.anyio
    async def test_readonly_locked_at_zero_refuses_the_statement(
        self, clickhouse_config, monkeypatch
    ):
        captured = {}
        _fake_get_client(monkeypatch, captured, [LOCKED_WRITABLE_PROFILE])

        with pytest.raises(ConnectorError, match="cannot be made read-only"):
            await ClickHousePythonConnector(clickhouse_config).execute_query(
                "SELECT 1"
            )

        assert "queries" not in captured
        assert captured["closed"] == 1

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
        profiles = {"a.example.com": READONLY1_PROFILE, "b.example.com": NORMAL_PROFILE}
        seen = []

        def fake_get_client(**kwargs):
            host = kwargs["host"]

            def query(q, column_oriented=False, settings=None):
                if q == PROBE_QUERY:
                    return SimpleNamespace(result_rows=profiles[host])
                seen.append((host, settings))
                return SimpleNamespace(column_names=["c"], result_rows=[[1]])

            return SimpleNamespace(query=query, close=lambda: None)

        monkeypatch.setattr(
            "mcp_read_only_sql.connectors.clickhouse.python.clickhouse_connect.get_client",
            fake_get_client,
        )
        connector = ClickHousePythonConnector(connection)
        full = {"readonly": 1, "max_execution_time": connection.query_timeout}

        await connector.execute_query("SELECT 1", server="a.example.com")
        await connector.execute_query("SELECT 1", server="b.example.com")
        await connector.execute_query("SELECT 1", server="a.example.com")

        assert seen == [
            ("a.example.com", None),
            ("b.example.com", full),
            ("a.example.com", None),
        ]

    @pytest.mark.anyio
    async def test_refusal_of_a_sent_setting_forgets_the_answer(
        self, clickhouse_config, monkeypatch
    ):
        captured = {}
        _fake_get_client(
            monkeypatch, captured, [NORMAL_PROFILE], refuse=READONLY2_REFUSAL
        )
        connector = ClickHousePythonConnector(clickhouse_config)

        with pytest.raises(ConnectorError, match="readonly"):
            await connector.execute_query("SELECT 1")

        assert not connector._decided_settings

    @pytest.mark.anyio
    async def test_statement_refusal_keeps_the_answer(
        self, clickhouse_config, monkeypatch
    ):
        captured = {}
        _fake_get_client(
            monkeypatch, captured, [NORMAL_PROFILE], refuse=STATEMENT_REFUSAL
        )
        connector = ClickHousePythonConnector(clickhouse_config)

        with pytest.raises(ConnectorError, match="max_threads"):
            await connector.execute_query("SELECT 1 SETTINGS max_threads = 1")

        assert len(captured["queries"]) == 1, "never re-run"
        assert connector._decided_settings

    @pytest.mark.anyio
    async def test_file_output_uses_the_decided_settings(
        self, clickhouse_config, monkeypatch, tmp_path
    ):
        captured = {}
        _fake_get_client(monkeypatch, captured, [READONLY2_PROFILE])
        output = tmp_path / "out.tsv"

        await ClickHousePythonConnector(clickhouse_config).execute_query_to_file(
            "SELECT 1", output
        )

        assert output.read_bytes() == b"col\n1\n"
        assert captured["stream_settings"] == {
            "max_execution_time": clickhouse_config.query_timeout
        }

    @pytest.mark.anyio
    async def test_probe_failure_is_not_remembered(self, clickhouse_config, monkeypatch):
        calls = []

        def fake_get_client(**kwargs):
            calls.append(kwargs)
            raise ProgrammingError("Connection refused")

        monkeypatch.setattr(
            "mcp_read_only_sql.connectors.clickhouse.python.clickhouse_connect.get_client",
            fake_get_client,
        )
        connector = ClickHousePythonConnector(clickhouse_config)

        with pytest.raises(ConnectorError, match="Connection refused"):
            await connector.execute_query("SELECT 1")

        assert len(calls) == 1
        assert not connector._decided_settings


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

    async def test_readonly_locked_at_zero_is_refused_before_the_statement(
        self, implementation
    ):
        """locked_user's profile pins readonly at 0: nothing makes it read-only."""
        connector = _connector(implementation, "locked_user", "lockedpass")

        with pytest.raises(ConnectorError, match="cannot be made read-only"):
            await connector.execute_query("SELECT count() FROM events")

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
