"""Bastion host keys are verified the way OpenSSH does it.

``host_key_checking`` defaults to ``accept-new``: a bastion is recorded on
first use and refused if its key changes. ``yes`` refuses unknown hosts and
``no`` restores the old trust-everything behaviour. Both the system-ssh and
the Paramiko tunnel honour it and share the OpenSSH known_hosts format.
"""

import asyncio
import os
import stat
import subprocess
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


async def _cli_ssh_args(
    monkeypatch, config: SSHTunnelConfig, *, pinned: bool = True
) -> list[str]:
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
    if pinned:
        # The fake ssh records nothing; these tests are about the command line.
        monkeypatch.setattr(CLISSHTunnel, "_pinned", lambda self, record_file: True)
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

    @pytest.mark.anyio
    async def test_first_use_fails_closed_when_ssh_recorded_nothing(
        self, monkeypatch, tmp_path
    ):
        """ssh only warns when it cannot write the file; the tunnel must not."""
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text("")
        config = _config(known_hosts_file=str(known_hosts))

        with pytest.raises(ConnectorError, match="was not recorded"):
            await _cli_ssh_args(monkeypatch, config, pinned=False)

    @pytest.mark.anyio
    async def test_ca_only_entry_runs_ssh_strictly(self, monkeypatch, tmp_path):
        """A CA entry alone cannot tell a verified certificate from a raw
        first-use key, so ssh must require the certificate."""
        known_hosts = tmp_path / "known_hosts"
        ca = paramiko.RSAKey.generate(1024)
        known_hosts.write_text(f"@cert-authority bastion.example.com ssh-rsa {ca.get_base64()}\n")
        config = _config(known_hosts_file=str(known_hosts))

        args = await _cli_ssh_args(monkeypatch, config, pinned=False)

        assert _option(args, "StrictHostKeyChecking") == "yes"
        assert known_hosts.read_text().startswith("@cert-authority")

    @pytest.mark.anyio
    async def test_a_wildcard_pin_counts_as_recorded(self, monkeypatch, tmp_path):
        known_hosts = tmp_path / "known_hosts"
        key = paramiko.RSAKey.generate(1024)
        known_hosts.write_text(f"*.example.com ssh-rsa {key.get_base64()}\n")
        config = _config(known_hosts_file=str(known_hosts))

        args = await _cli_ssh_args(monkeypatch, config, pinned=False)

        assert _option(args, "StrictHostKeyChecking") == "accept-new"


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

        assert known.pinned_keys("bastion.example.com", "ssh-rsa") == []
        assert known.pinned_keys("other.example.com", "ssh-rsa") == [key]

    def test_hashed_name_is_matched(self, tmp_path):
        key = paramiko.RSAKey.generate(1024)
        hashed = paramiko.HostKeys.hash_host("[bastion.example.com]:2222")
        known = _known(tmp_path, f"{hashed} ssh-rsa {key.get_base64()}\n")
        host_keys = paramiko.HostKeys()

        known.apply_to(host_keys, "[bastion.example.com]:2222")

        assert known.pinned_keys("[bastion.example.com]:2222", "ssh-rsa") == [key]
        assert host_keys.lookup("[bastion.example.com]:2222")["ssh-rsa"] == key

    def test_apply_to_feeds_wildcard_pins_and_skips_revoked_keys(self, tmp_path):
        pinned, revoked = paramiko.RSAKey.generate(1024), paramiko.RSAKey.generate(1024)
        known = _known(
            tmp_path,
            f"*.example.com ssh-rsa {pinned.get_base64()}\n"
            f"bastion.example.com ssh-rsa {revoked.get_base64()}\n"
            f"@revoked bastion.example.com ssh-rsa {revoked.get_base64()}\n",
        )
        host_keys = paramiko.HostKeys()

        known.apply_to(host_keys, "bastion.example.com")

        assert host_keys.lookup("bastion.example.com")["ssh-rsa"] == pinned

    def test_several_keys_of_one_type_are_all_valid(self, tmp_path):
        """During a rotation two entries of the same type are valid; ssh
        accepts either, and so must the policy, without recording anything."""
        old_key, new_key, other = (paramiko.RSAKey.generate(1024) for _ in range(3))
        known = _known(
            tmp_path,
            f"bastion.example.com ssh-rsa {old_key.get_base64()}\n"
            f"bastion.example.com ssh-rsa {new_key.get_base64()}\n",
        )
        record = tmp_path / "known_hosts"
        policy = AcceptNewHostKeyPolicy(str(record), known)

        policy.missing_host_key(_FakeClient(), "bastion.example.com", old_key)
        policy.missing_host_key(_FakeClient(), "bastion.example.com", new_key)
        StrictHostKeyPolicy(known).missing_host_key(_FakeClient(), "bastion.example.com", old_key)
        assert not record.exists()

        with pytest.raises(paramiko.BadHostKeyException):
            policy.missing_host_key(_FakeClient(), "bastion.example.com", other)

    def test_apply_to_leaves_out_a_type_with_several_pins(self, tmp_path):
        """Paramiko's store holds one key per type; a second would replace the
        first and its pre-auth check would refuse a valid rotated key."""
        rsa_old, rsa_new = paramiko.RSAKey.generate(1024), paramiko.RSAKey.generate(1024)
        ecdsa = paramiko.ECDSAKey.generate()
        known = _known(
            tmp_path,
            f"bastion.example.com ssh-rsa {rsa_old.get_base64()}\n"
            f"bastion.example.com ssh-rsa {rsa_new.get_base64()}\n"
            f"bastion.example.com {ecdsa.get_name()} {ecdsa.get_base64()}\n",
        )
        host_keys = paramiko.HostKeys()

        known.apply_to(host_keys, "bastion.example.com")

        stored = host_keys.lookup("bastion.example.com")
        assert "ssh-rsa" not in stored
        assert stored[ecdsa.get_name()] == ecdsa

    def test_cert_authority_is_kept_apart_from_host_key_pins(self, tmp_path):
        """A CA entry lets ssh verify a certificate; it is not a pinned key."""
        ca = paramiko.RSAKey.generate(1024)
        # Off port 22 the pattern carries the port, as ssh writes it.
        known = _known(
            tmp_path, f"@cert-authority [*.example.com]:2222 ssh-rsa {ca.get_base64()}\n"
        )

        assert known.certified("[bastion.example.com]:2222")
        assert not known.certified("[bastion.example.com]:22")
        assert not known.pinned_any("[bastion.example.com]:2222")
        assert known.pinned_keys("[bastion.example.com]:2222", "ssh-rsa") == []

    def test_revoked_key_is_refused(self, tmp_path):
        key = paramiko.RSAKey.generate(1024)
        known = _known(tmp_path, f"@revoked bastion.example.com ssh-rsa {key.get_base64()}\n")

        for policy in (AcceptNewHostKeyPolicy(str(tmp_path / "kh"), known), StrictHostKeyPolicy(known)):
            with pytest.raises(paramiko.SSHException, match="revoked"):
                policy.missing_host_key(_FakeClient(), "bastion.example.com", key)
        assert not (tmp_path / "kh").exists()

    def test_revoked_key_is_refused_before_authentication(self, monkeypatch, tmp_path):
        """Paramiko checks its store, then the policy, before it authenticates.
        A key that is pinned and revoked must not reach the store, so the
        policy refuses it and no credential is ever sent."""
        key = paramiko.RSAKey.generate(1024)
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text(
            f"bastion.example.com ssh-rsa {key.get_base64()}\n"
            f"@revoked bastion.example.com ssh-rsa {key.get_base64()}\n"
        )
        seen = {"authenticated": False}

        class FakeSSHClient:
            """connect() in the order SSHClient.connect uses: key exchange,
            host-key check (store, then policy), only then authentication."""

            def __init__(self):
                self.host_keys = paramiko.HostKeys()
                self.policy = None

            def get_host_keys(self):
                return self.host_keys

            def set_missing_host_key_policy(self, policy):
                self.policy = policy

            def connect(self, hostname, port, **kwargs):
                name = hostname if port == 22 else f"[{hostname}]:{port}"
                stored = self.host_keys.lookup(name)
                if stored is None or key.get_name() not in stored:
                    self.policy.missing_host_key(self, name, key)
                elif stored[key.get_name()] != key:
                    raise paramiko.BadHostKeyException(name, key, stored[key.get_name()])
                seen["authenticated"] = True

            def close(self):
                pass

        monkeypatch.setattr(paramiko, "SSHClient", FakeSSHClient)
        config = _config(known_hosts_file=str(known_hosts))

        with pytest.raises(ConnectorError, match="revoked"):
            SSHTunnel(config, "db.internal", 5432)._start_sync()

        assert seen["authenticated"] is False

    def test_unwritable_file_fails_closed(self, tmp_path):
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text("")
        known_hosts.chmod(0o400)
        key = paramiko.RSAKey.generate(1024)

        with pytest.raises(paramiko.SSHException, match="cannot record"):
            AcceptNewHostKeyPolicy(str(known_hosts)).missing_host_key(
                _FakeClient(), "bastion.example.com", key
            )

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


def _bastion_key() -> paramiko.PKey:
    """The host key the fixture bastion presents, fetched over a bare transport."""
    transport = paramiko.Transport((docker_test_ssh_host(), docker_test_ssh_port()))
    try:
        transport.start_client(timeout=10)
        return transport.get_remote_server_key()
    finally:
        transport.close()


def _bastion_accepted_logins() -> int:
    """Successful authentications the fixture sshd has logged so far."""
    log = subprocess.run(
        ["docker", "logs", "mcp-ssh-bastion"], capture_output=True, text=True, check=False
    )
    return (log.stdout + log.stderr).count("Accepted ")


def _fresh_ca_public_line(tmp_path) -> str:
    """The public key line of a CA that has signed nothing."""
    ca = tmp_path / "unrelated_ca"
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(ca)], check=True)
    return (tmp_path / "unrelated_ca.pub").read_text().strip()


def _ca_signed_bastion(tmp_path) -> str:
    """Sign the fixture bastion's host key with a fresh CA and install the
    certificate; returns the CA's public key line. Stays in place for the
    rest of the container's life, which does not affect plain-key tests."""
    ca = tmp_path / "ca"
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(ca)], check=True)
    host_pub = tmp_path / "ssh_host_ed25519_key.pub"
    subprocess.run(
        ["docker", "cp", "mcp-ssh-bastion:/etc/ssh/ssh_host_ed25519_key.pub", str(host_pub)],
        check=True,
    )
    subprocess.run(
        ["ssh-keygen", "-q", "-s", str(ca), "-I", "fixture-bastion", "-h",
         "-n", docker_test_ssh_host(), str(host_pub)],
        check=True,
    )
    cert = tmp_path / "ssh_host_ed25519_key-cert.pub"
    subprocess.run(
        ["docker", "cp", str(cert), "mcp-ssh-bastion:/etc/ssh/ssh_host_ed25519_key-cert.pub"],
        check=True,
    )
    subprocess.run(
        [
            "docker", "exec", "mcp-ssh-bastion", "sh", "-c",
            (
                "grep -q '^HostCertificate' /etc/ssh/sshd_config || "
                "echo 'HostCertificate /etc/ssh/ssh_host_ed25519_key-cert.pub' "
                ">> /etc/ssh/sshd_config; kill -HUP 1"
            ),
        ],
        check=True,
    )
    return (tmp_path / "ca.pub").read_text().strip()


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

    async def test_unwritable_file_fails_closed(self, implementation, tmp_path):
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text("")
        known_hosts.chmod(0o400)
        connector = _tunnelled_connector(
            implementation, known_hosts_file=str(known_hosts)
        )

        with pytest.raises(ConnectorError, match="(?i)record"):
            await connector.execute_query("SELECT 1")

        assert known_hosts.read_text() == ""

    async def test_revoked_key_is_refused_next_to_an_ordinary_pin(
        self, implementation, tmp_path
    ):
        key = _bastion_key()
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text(
            f"{_bastion_name()} {key.get_name()} {key.get_base64()}\n"
            f"@revoked {_bastion_name()} {key.get_name()} {key.get_base64()}\n"
        )
        connector = _tunnelled_connector(
            implementation, known_hosts_file=str(known_hosts)
        )

        accepted_before = _bastion_accepted_logins()
        with pytest.raises(ConnectorError, match="(?i)revoked"):
            await connector.execute_query("SELECT 1")
        assert _bastion_accepted_logins() == accepted_before, "no credential was sent"

    async def test_ca_only_bastion_is_accepted_and_not_recorded(
        self, implementation, tmp_path
    ):
        """With only an @cert-authority entry, ssh verifies the host
        certificate and records no raw key; the tunnel must not be torn down.
        Paramiko cannot verify certificates and treats the host as new."""
        if implementation != "cli":
            pytest.skip("Paramiko cannot verify host certificates")
        ca_line = _ca_signed_bastion(tmp_path)
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text(f"@cert-authority {_bastion_name()} {ca_line}\n")
        connector = _tunnelled_connector(
            implementation, known_hosts_file=str(known_hosts)
        )

        result = await connector.execute_query("SELECT 1 AS one")

        assert result.split("\n")[1] == "1"
        assert known_hosts.read_text() == f"@cert-authority {_bastion_name()} {ca_line}\n"

    async def test_ca_only_file_refuses_a_bastion_without_a_matching_certificate(
        self, implementation, tmp_path
    ):
        """With a CA entry from a CA that never signed the bastion, ssh gets
        a key it cannot verify; it must be refused, not recorded."""
        if implementation != "cli":
            pytest.skip("Paramiko cannot verify host certificates")
        line = f"@cert-authority {_bastion_name()} {_fresh_ca_public_line(tmp_path)}\n"
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text(line)
        connector = _tunnelled_connector(
            implementation, known_hosts_file=str(known_hosts)
        )

        with pytest.raises(ConnectorError, match="(?i)host key"):
            await connector.execute_query("SELECT 1")

        assert known_hosts.read_text() == line

    async def test_rotation_overlap_with_two_keys_of_one_type(self, implementation, tmp_path):
        """Two valid entries of the same type, the bastion presenting the
        first: ssh accepts it, and so must the Paramiko tunnel."""
        key = _bastion_key()
        assert key.get_name() == "ssh-ed25519"
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text(
            f"{_bastion_name()} {key.get_name()} {key.get_base64()}\n"
            f"{_bastion_name()} {_wrong_ed25519_line()}\n"
        )
        connector = _tunnelled_connector(
            implementation, known_hosts_file=str(known_hosts)
        )

        result = await connector.execute_query("SELECT 1 AS one")

        assert result.split("\n")[1] == "1"
        assert len(known_hosts.read_text().splitlines()) == 2, "nothing recorded"

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
