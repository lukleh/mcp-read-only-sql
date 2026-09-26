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
from mcp_read_only_sql.utils.ssh_tunnel import (
    GLOBAL_KNOWN_HOSTS_FILE,
    AcceptNewHostKeyPolicy,
    KnownHosts,
    SSHTunnel,
    StrictHostKeyPolicy,
)
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
    @pytest.mark.parametrize("known_hosts_file", [None, "/tmp/kh"])
    async def test_no_records_nothing_whatever_file_is_configured(
        self, monkeypatch, known_hosts_file
    ):
        config = _config(host_key_checking="no", known_hosts_file=known_hosts_file)
        args = await _cli_ssh_args(monkeypatch, config)
        assert _option(args, "StrictHostKeyChecking") == "no"
        assert _option(args, "UserKnownHostsFile") == "/dev/null"


    @pytest.mark.anyio
    async def test_creates_the_directory_ssh_records_into(self, monkeypatch, tmp_path):
        """ssh only warns when it cannot record a key; the directory must exist."""
        known_hosts = tmp_path / "missing" / "known_hosts"
        await _cli_ssh_args(monkeypatch, _config(known_hosts_file=str(known_hosts)))
        assert known_hosts.parent.is_dir()
        assert stat.S_IMODE(known_hosts.parent.stat().st_mode) == 0o700


class _FakeClient:
    def __init__(self):
        self.host_keys = paramiko.HostKeys()

    def get_host_keys(self):
        return self.host_keys


def _known(tmp_path, text: str) -> KnownHosts:
    path = tmp_path / "pins"
    path.write_text(text)
    known = KnownHosts()
    known.load(str(path))
    return known


class TestKnownHostsEntries:
    """The entries ssh matches that Paramiko's literal lookup does not."""

    def test_wildcard_pin_accepts_the_matching_key(self, tmp_path):
        key = paramiko.RSAKey.generate(1024)
        known = _known(tmp_path, f"*.example.com ssh-rsa {key.get_base64()}\n")
        record = tmp_path / "known_hosts"

        AcceptNewHostKeyPolicy(str(record), known).missing_host_key(
            _FakeClient(), "bastion.example.com", key
        )
        StrictHostKeyPolicy(known).missing_host_key(_FakeClient(), "bastion.example.com", key)

        assert not record.exists(), "a pinned host is not recorded again"

    def test_wildcard_pin_refuses_a_different_key(self, tmp_path):
        pinned, other = paramiko.RSAKey.generate(1024), paramiko.RSAKey.generate(1024)
        known = _known(tmp_path, f"*.example.com ssh-rsa {pinned.get_base64()}\n")

        for policy in (AcceptNewHostKeyPolicy(str(tmp_path / "kh"), known), StrictHostKeyPolicy(known)):
            with pytest.raises(paramiko.BadHostKeyException):
                policy.missing_host_key(_FakeClient(), "bastion.example.com", other)

    def test_negated_pattern_excludes_the_host(self, tmp_path):
        key = paramiko.RSAKey.generate(1024)
        known = _known(
            tmp_path, f"!bastion.example.com,*.example.com ssh-rsa {key.get_base64()}\n"
        )

        assert known.pinned_key("bastion.example.com", "ssh-rsa") is None
        assert known.pinned_key("other.example.com", "ssh-rsa") == key

    def test_hashed_name_is_matched(self, tmp_path):
        key = paramiko.RSAKey.generate(1024)
        hashed = paramiko.HostKeys.hash_host("[bastion.example.com]:2222")
        known = _known(tmp_path, f"{hashed} ssh-rsa {key.get_base64()}\n")
        host_keys = paramiko.HostKeys()

        known.apply_to(host_keys)

        assert known.pinned_key("[bastion.example.com]:2222", "ssh-rsa") == key
        assert host_keys.lookup("[bastion.example.com]:2222")["ssh-rsa"] == key

    def test_revoked_key_is_refused(self, tmp_path):
        key = paramiko.RSAKey.generate(1024)
        known = _known(tmp_path, f"@revoked bastion.example.com ssh-rsa {key.get_base64()}\n")

        for policy in (AcceptNewHostKeyPolicy(str(tmp_path / "kh"), known), StrictHostKeyPolicy(known)):
            with pytest.raises(paramiko.SSHException, match="revoked"):
                policy.missing_host_key(_FakeClient(), "bastion.example.com", key)
        assert not (tmp_path / "kh").exists()

    def test_cert_authority_and_bad_lines_are_skipped(self, tmp_path):
        key = paramiko.RSAKey.generate(1024)
        known = _known(
            tmp_path,
            "@cert-authority *.example.com ssh-rsa "
            f"{paramiko.RSAKey.generate(1024).get_base64()}\n"
            "this is not an entry\n"
            f"bastion.example.com ssh-rsa {key.get_base64()}\n",
        )

        assert [entry.key for entry in known.entries] == [key]
        assert known.revoked == []

    def test_missing_file_is_tolerated(self, tmp_path):
        known = KnownHosts()
        known.load(str(tmp_path / "absent"))
        assert known.entries == [] and known.files == [str(tmp_path / "absent")]


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

    def test_accept_new_does_not_record_a_key_twice(self, tmp_path):
        """Two tunnels racing on a fresh bastion append one line, not two."""
        known_hosts = tmp_path / "known_hosts"
        key = paramiko.RSAKey.generate(1024)
        policy = AcceptNewHostKeyPolicy(str(known_hosts))

        class FakeClient:
            def __init__(self):
                self.host_keys = paramiko.HostKeys()

            def get_host_keys(self):
                return self.host_keys

        policy.missing_host_key(FakeClient(), "bastion.example.com", key)
        policy.missing_host_key(FakeClient(), "bastion.example.com", key)

        assert known_hosts.read_text().count("\n") == 1

    @pytest.mark.parametrize(
        ("checking", "policy_type", "loads_known_hosts"),
        [
            ("accept-new", AcceptNewHostKeyPolicy, True),
            ("yes", StrictHostKeyPolicy, True),
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
            host_keys = paramiko.HostKeys()

            def get_host_keys(self):
                return self.host_keys

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

        policy = seen["policy"]
        assert isinstance(policy, policy_type)
        if loads_known_hosts:
            # The same two files ssh reads, global first.
            assert policy.known.files == [GLOBAL_KNOWN_HOSTS_FILE, str(known_hosts)]
        else:
            assert not hasattr(policy, "known")


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
        # ssh may hash the name it records (HashKnownHosts), so look it up
        # rather than reading the first field.
        assert paramiko.HostKeys(str(known_hosts)).lookup(_bastion_name()) is not None

    async def test_first_use_creates_the_directory(self, implementation, tmp_path):
        known_hosts = tmp_path / "missing" / "known_hosts"
        connector = _tunnelled_connector(
            implementation, known_hosts_file=str(known_hosts)
        )

        await connector.execute_query("SELECT 1")

        assert paramiko.HostKeys(str(known_hosts)).lookup(_bastion_name()) is not None

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
