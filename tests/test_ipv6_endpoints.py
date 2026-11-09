"""IPv6 endpoints retain canonical hosts across loading, display and SSH."""

import asyncio
import json
import subprocess

import pytest

from mcp_read_only_sql.config import Server, SSHTunnelConfig, load_connections
from mcp_read_only_sql.config.dbeaver_import import DBeaverImporter
from mcp_read_only_sql.config.parser import ConfigParser
from mcp_read_only_sql.utils.connection_utils import get_connection_target
from mcp_read_only_sql.utils.ssh_tunnel_cli import CLISSHTunnel


@pytest.mark.parametrize("endpoint", [
    "::1", "[::1]", "[::1]:5432", {"host": "::1", "port": 5432},
    {"host": "[::1]", "port": 5432},
    "[0:0:0:0:0:0:0:1]:5432", {"host": "0:0:0:0:0:0:0:1", "port": 5432},
])
def test_ipv6_endpoints_share_runtime_parser(tmp_path, endpoint):
    config = {
        "connection_name": "ipv6", "type": "postgresql", "implementation": "cli",
        "servers": [endpoint], "db": "analytics", "username": "reader",
    }
    path = tmp_path / "connections.yaml"
    path.write_text(json.dumps([config]) + "\n")
    assert path.read_bytes()[-2:-1] != b"\n"
    runtime = load_connections(path)["ipv6"].servers[0]
    legacy = ConfigParser(path).load_config()[0]["servers"][0]
    target = get_connection_target(config)
    assert (runtime.host, runtime.port) == ("::1", 5432)
    assert legacy == {"host": "::1", "port": 5432}
    assert (target["host"], target["port"]) == ("::1", 5432)


@pytest.mark.parametrize(("implementation", "port"), [("cli", 9000), ("python", 8123)])
def test_bare_ipv6_uses_implementation_default(implementation, port):
    endpoint = Server.from_dict("2001:db8::1", "clickhouse", implementation)
    assert (endpoint.host, endpoint.port) == ("2001:db8::1", port)


@pytest.mark.parametrize("endpoint", ["[::1", "[::1]extra", "[]", "[hostname]:5432"])
def test_invalid_bracketed_addresses_fail(endpoint):
    with pytest.raises(ValueError):
        Server.from_dict(endpoint, "postgresql")


@pytest.mark.parametrize("endpoint", ["::1:5432", "2001:db8::1:5432", "fe80::1:22", "2001:db8::1:2"])
def test_ambiguous_ipv6_strings_require_explicit_brackets(endpoint):
    with pytest.raises(ValueError, match="Ambiguous IPv6 endpoint.*\\[address\\]"):
        Server.from_dict(endpoint, "postgresql")
    # Both meanings remain expressible; do not infer a user's port intent.
    assert Server.from_dict(f"[{endpoint}]", "postgresql").host == endpoint
    host, _, port = endpoint.rpartition(":")
    parsed = Server.from_dict(f"[{host}]:{port}", "postgresql")
    assert (parsed.host, parsed.port) == (host, int(port))


@pytest.mark.parametrize("endpoint", ["::", "::1", "2001:db8::1", "2001:db8::a:b"])
def test_unambiguous_bare_ipv6_literals_keep_default_port(endpoint):
    parsed = Server.from_dict(endpoint, "postgresql")
    assert (parsed.host, parsed.port) == (endpoint, 5432)


def test_ipv6_loopback_display_resolves_to_bastion():
    target = get_connection_target({
        "type": "postgresql", "servers": ["[0:0:0:0:0:0:0:1]:5432"],
        "ssh_tunnel": {"host": "bastion"},
    })
    assert target["host"] == "bastion"
    assert target["connection_type"] == "ssh_local"


def test_ipv6_equivalent_endpoints_compare_equal_and_keep_scope_ids():
    assert Server("2001:0DB8:0:0:0:0:0:1", 5432) == Server("2001:db8::1", 5432)
    assert Server.from_dict("::ffff:127.0.0.1", "postgresql") == Server("::ffff:7f00:1", 5432)
    assert Server("FE80:0:0:0:0:0:0:1%en0", 5432).host == "fe80::1%en0"


@pytest.mark.anyio
async def test_connection_diagnostic_brackets_ipv6(tmp_path, monkeypatch, capsys):
    from mcp_read_only_sql.config import Connection
    from mcp_read_only_sql.runtime_paths import RuntimePaths
    from mcp_read_only_sql.tools import test_connection as tool

    connection = Connection({
        "connection_name": "ipv6", "type": "postgresql", "implementation": "python",
        "servers": [{"host": "0:0:0:0:0:0:0:1", "port": 5432}], "db": "analytics", "username": "reader",
    })

    class Connector:
        def __init__(self, connection):
            pass

        async def execute_query(self, *args, **kwargs):
            return "version\nPostgreSQL"

    monkeypatch.setattr(tool, "load_connections", lambda path: {"ipv6": connection})
    monkeypatch.setattr(tool, "PostgreSQLPythonConnector", Connector)
    paths = RuntimePaths(tmp_path / "config", tmp_path / "state", tmp_path / "cache")
    assert await tool.test_connection(paths)
    assert "Servers: [::1]:5432" in capsys.readouterr().out


def test_dbeaver_ipv6_endpoint_round_trips():
    importer = DBeaverImporter("/unused")
    converted = importer._convert_connection("ipv6", {
        "name": "IPv6", "provider": "postgresql", "configuration": {"host": "2001:db8::1", "port": "5433"},
    })
    assert converted["servers"] == ["[2001:db8::1]:5433"]
    parsed = Server.from_dict(converted["servers"][0], "postgresql")
    assert (parsed.host, parsed.port) == ("2001:db8::1", 5433)


@pytest.mark.anyio
async def test_ssh_ipv6_forward_is_accepted_by_openssh(monkeypatch):
    args = []

    class Process:
        returncode = None

    class Writer:
        def close(self):
            pass

        async def wait_closed(self):
            pass

    async def fake_exec(*cmd, **kwargs):
        args.extend(cmd)
        return Process()

    async def fake_open(host, port):
        return object(), Writer()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr(asyncio, "open_connection", fake_open)
    config = SSHTunnelConfig.from_dict({"host": "bastion.example", "user": "tunnel"})
    tunnel = CLISSHTunnel(config, "2001:db8::1", 5432)
    monkeypatch.setattr(tunnel, "_find_free_port", lambda: 45454)
    await tunnel.start()
    forward = args[args.index("-L") + 1]
    assert forward == "45454:[2001:db8::1]:5432"
    result = await asyncio.to_thread(
        subprocess.run,
        ["ssh", "-G", "-F", "/dev/null", "-L", forward, "tunnel@bastion.example"],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
