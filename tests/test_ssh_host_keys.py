"""Bastion host keys are verified the way OpenSSH does it.

``host_key_checking`` defaults to ``accept-new``: a bastion is recorded on
first use and refused if its key changes. ``yes`` refuses unknown hosts and
``no`` restores the old trust-everything behaviour. Both the system-ssh and
the Paramiko tunnel honour it and share the OpenSSH known_hosts format.
"""

import asyncio
import os
import stat
from itertools import pairwise

import paramiko
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from mcp_read_only_sql.config.connection import SSHTunnelConfig
from mcp_read_only_sql.connectors.postgresql.cli import PostgreSQLCLIConnector
from mcp_read_only_sql.connectors.postgresql.python import PostgreSQLPythonConnector
from mcp_read_only_sql.errors import ConnectorError
from mcp_read_only_sql.utils.ssh_tunnel import AcceptNewHostKeyPolicy, SSHTunnel
from mcp_read_only_sql.utils.ssh_tunnel_cli import CLISSHTunnel
from tests.conftest import make_connection
from tests.docker_test_config import (
    docker_test_ssh_host,
    docker_test_ssh_port,
    docker_test_ssh_tunnel,
)

BASTION = {"host": "bastion.example.com", "port": 22, "user": "tunnel"}


def _config(**extra) -> SSHTunnelConfig:
    config = SSHTunnelConfig.from_dict({**BASTION, **extra})
    assert config is not None
    return config


class TestConfig:
    def test_defaults_to_accept_new_and_ssh_default_file(self):
        config = _config()
        assert config.host_key_checking == "accept-new"
        assert config.known_hosts_file is None

    @pytest.mark.parametrize("value", ["accept-new", "yes", "no"])
    def test_accepts_openssh_values(self, value):
        assert _config(host_key_checking=value).host_key_checking == value

    @pytest.mark.parametrize("value", ["ask", "true", True, 1])
    def test_rejects_other_values(self, value):
        with pytest.raises(ValueError, match="host_key_checking"):
            _config(host_key_checking=value)

    def test_known_hosts_file_expands_home(self):
        config = _config(known_hosts_file="~/.ssh/bastions")
        assert config.known_hosts_file == os.path.expanduser("~/.ssh/bastions")

    @pytest.mark.parametrize("value", ["", "   ", 3])
    def test_rejects_bad_known_hosts_file(self, value):
        with pytest.raises(ValueError, match="known_hosts_file"):
            _config(known_hosts_file=value)


async def _cli_ssh_args(monkeypatch, config: SSHTunnelConfig) -> list[str]:
    """Start a CLI tunnel against a fake ssh and return its command line."""

    class FakeProcess:
        returncode = None

        async def communicate(self):
            return b"", b""

    class FakeWriter:
        def close(self):
            pass

        async def wait_closed(self):
            pass

    args: list[str] = []

    async def fake_exec(*cmd, **kwargs):
        args.extend(cmd)
        return FakeProcess()

    async def fake_open_connection(host, port):
        return object(), FakeWriter()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr(asyncio, "open_connection", fake_open_connection)
    tunnel = CLISSHTunnel(config, "db.internal", 5432)
    monkeypatch.setattr(tunnel, "_find_free_port", lambda: 45454)
    await tunnel.start()
    return args


def _option(args: list[str], name: str) -> str | None:
    for flag, value in pairwise(args):
        if flag == "-o" and value.startswith(f"{name}="):
            return value.split("=", 1)[1]
    return None


class TestCLIOptions:
    @pytest.mark.anyio
    async def test_default_is_accept_new_with_ssh_default_file(self, monkeypatch):
        args = await _cli_ssh_args(monkeypatch, _config())
        assert _option(args, "StrictHostKeyChecking") == "accept-new"
        assert _option(args, "UserKnownHostsFile") is None

    @pytest.mark.anyio
    async def test_explicit_known_hosts_file_is_passed(self, monkeypatch):
        args = await _cli_ssh_args(monkeypatch, _config(known_hosts_file="/tmp/kh"))
        assert _option(args, "UserKnownHostsFile") == "/tmp/kh"

    @pytest.mark.anyio
    async def test_yes_refuses_unknown_hosts(self, monkeypatch):
        args = await _cli_ssh_args(monkeypatch, _config(host_key_checking="yes"))
        assert _option(args, "StrictHostKeyChecking") == "yes"

    @pytest.mark.anyio
    async def test_no_keeps_the_legacy_record_nothing_behaviour(self, monkeypatch):
        args = await _cli_ssh_args(monkeypatch, _config(host_key_checking="no"))
        assert _option(args, "StrictHostKeyChecking") == "no"
        assert _option(args, "UserKnownHostsFile") == "/dev/null"


class TestParamikoPolicy:
    def test_accept_new_records_the_key_in_openssh_format(self, tmp_path):
        known_hosts = tmp_path / "ssh" / "known_hosts"
        key = paramiko.RSAKey.generate(1024)

        class FakeClient:
            host_keys = paramiko.HostKeys()

            def get_host_keys(self):
                return self.host_keys

        client = FakeClient()
        AcceptNewHostKeyPolicy(str(known_hosts)).missing_host_key(
            client, "[bastion.example.com]:2222", key
        )

        assert known_hosts.read_text() == (
            f"[bastion.example.com]:2222 ssh-rsa {key.get_base64()}\n"
        )
        assert stat.S_IMODE(known_hosts.stat().st_mode) == 0o600
        assert client.host_keys.lookup("[bastion.example.com]:2222")["ssh-rsa"] == key

    @pytest.mark.parametrize(
        ("checking", "policy_type", "loads_known_hosts"),
        [
            ("accept-new", AcceptNewHostKeyPolicy, True),
            ("yes", paramiko.RejectPolicy, True),
            ("no", paramiko.AutoAddPolicy, False),
        ],
    )
    def test_tunnel_selects_policy(
        self, monkeypatch, tmp_path, checking, policy_type, loads_known_hosts
    ):
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text("")
        seen: dict[str, object] = {}

        class FakeSSHClient:
            def load_system_host_keys(self, filename=None):
                seen["loaded"] = filename

            def set_missing_host_key_policy(self, policy):
                seen["policy"] = policy

            def connect(self, **kwargs):
                raise paramiko.SSHException("stop here")

            def close(self):
                pass

        monkeypatch.setattr(paramiko, "SSHClient", FakeSSHClient)
        config = _config(host_key_checking=checking, known_hosts_file=str(known_hosts))

        with pytest.raises(ConnectorError, match="stop here"):
            SSHTunnel(config, "db.internal", 5432)._start_sync()

        assert isinstance(seen["policy"], policy_type)
        assert ("loaded" in seen) is loads_known_hosts
        if loads_known_hosts:
            assert seen["loaded"] == str(known_hosts)


def _bastion_name() -> str:
    host, port = docker_test_ssh_host(), docker_test_ssh_port()
    return host if port == 22 else f"[{host}]:{port}"


def _wrong_ed25519_line() -> str:
    public = Ed25519PrivateKey.generate().public_key()
    return public.public_bytes(Encoding.OpenSSH, PublicFormat.OpenSSH).decode()


def _tunnelled_connector(implementation: str, **ssh_extra):
    ssh_tunnel = docker_test_ssh_tunnel(private_key="/tmp/docker_test_key")
    ssh_tunnel.update(ssh_extra)
    connection = make_connection(
        {
            "connection_name": f"host_keys_{implementation}",
            "type": "postgresql",
            "servers": [{"host": "mcp-postgres-private", "port": 5432}],
            "db": "testdb",
            "username": "testuser",
            "password": "testpass",
            "implementation": implementation,
            "ssh_tunnel": ssh_tunnel,
        }
    )
    if implementation == "cli":
        return PostgreSQLCLIConnector(connection)
    return PostgreSQLPythonConnector(connection)


@pytest.mark.ssh
@pytest.mark.docker
@pytest.mark.usefixtures("ssh_container_check")
@pytest.mark.anyio
@pytest.mark.parametrize("implementation", ["cli", "python"])
class TestAgainstBastion:
    async def test_first_use_records_the_bastion(self, implementation, tmp_path):
        known_hosts = tmp_path / "known_hosts"
        connector = _tunnelled_connector(
            implementation, known_hosts_file=str(known_hosts)
        )

        result = await connector.execute_query("SELECT 1 AS one")

        assert result.split("\n")[1] == "1"
        recorded = [line.split()[0] for line in known_hosts.read_text().splitlines()]
        assert _bastion_name() in recorded

    async def test_changed_key_is_refused(self, implementation, tmp_path):
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text(f"{_bastion_name()} {_wrong_ed25519_line()}\n")
        connector = _tunnelled_connector(
            implementation, known_hosts_file=str(known_hosts)
        )

        with pytest.raises(ConnectorError, match="(?i)host key"):
            await connector.execute_query("SELECT 1")

    async def test_yes_refuses_an_unrecorded_bastion(self, implementation, tmp_path):
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text("")
        connector = _tunnelled_connector(
            implementation, known_hosts_file=str(known_hosts), host_key_checking="yes"
        )

        with pytest.raises(ConnectorError, match="(?i)host key|known_hosts"):
            await connector.execute_query("SELECT 1")
