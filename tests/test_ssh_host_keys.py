"""Bastion host keys are verified by ssh, for every connector.

``host_key_checking`` defaults to ``accept-new``: a bastion is recorded on
first use and refused if its key changes. ``yes`` refuses unknown hosts and
``no`` restores the old trust-everything behaviour. Beyond what ssh does on
its own, the tunnel fails closed when ssh could not record a first-use key,
and runs strictly when only a certificate authority entry covers the host.
"""

import asyncio
import os
import stat
import subprocess
import time
from itertools import pairwise

import pytest

from mcp_read_only_sql.config.connection import SSHTunnelConfig
from mcp_read_only_sql.connectors.postgresql.cli import PostgreSQLCLIConnector
from mcp_read_only_sql.connectors.postgresql.python import PostgreSQLPythonConnector
from mcp_read_only_sql.errors import ConnectorError
from mcp_read_only_sql.utils.ssh_tunnel_cli import (
    GLOBAL_KNOWN_HOSTS_FILE,
    CLISSHTunnel,
    KnownHosts,
    known_hosts_name,
)
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


def _public_key_line(tmp_path, name: str = "key") -> str:
    """A fresh ed25519 public key as ssh-keygen prints it: type and base64."""
    path = tmp_path / name
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(path)], check=True)
    key_type, key_data = (tmp_path / f"{name}.pub").read_text().split()[:2]
    return f"{key_type} {key_data}"


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


class TestKnownHostsEntries:
    """What the known_hosts files say about a host, read the way ssh reads them."""

    def _known(self, tmp_path, text: str) -> KnownHosts:
        path = tmp_path / "pins"
        path.write_text(text)
        known = KnownHosts()
        known.load(str(path))
        return known

    def test_name_forms(self):
        assert known_hosts_name("bastion.example.com", 22) == "bastion.example.com"
        assert known_hosts_name("bastion.example.com", 2222) == "[bastion.example.com]:2222"

    def test_plain_and_wildcard_pins(self, tmp_path):
        known = self._known(
            tmp_path,
            f"bastion.example.com {_public_key_line(tmp_path, 'a')}\n"
            f"[*.internal]:2222 {_public_key_line(tmp_path, 'b')}\n",
        )
        assert known.pinned_any("bastion.example.com")
        assert known.pinned_any("BASTION.example.com")
        assert known.pinned_any("[jump.internal]:2222")
        assert not known.pinned_any("[jump.internal]:22")
        assert not known.pinned_any("other.example.com")

    def test_negated_pattern_excludes_the_host(self, tmp_path):
        known = self._known(
            tmp_path, f"!bastion.example.com,*.example.com {_public_key_line(tmp_path)}\n"
        )
        assert not known.pinned_any("bastion.example.com")
        assert known.pinned_any("other.example.com")

    def test_hashed_names_as_ssh_keygen_writes_them(self, tmp_path):
        path = tmp_path / "hashed"
        path.write_text(f"[bastion.example.com]:2222 {_public_key_line(tmp_path)}\n")
        subprocess.run(["ssh-keygen", "-q", "-H", "-f", str(path)], check=True)
        assert path.read_text().startswith("|1|")
        known = KnownHosts()
        known.load(str(path))

        assert known.pinned_any("[bastion.example.com]:2222")
        assert not known.pinned_any("[other.example.com]:2222")

    def test_markers(self, tmp_path):
        known = self._known(
            tmp_path,
            f"@revoked bastion.example.com {_public_key_line(tmp_path, 'r')}\n"
            f"@cert-authority *.example.com {_public_key_line(tmp_path, 'ca')}\n"
            "@unknown-marker other.example.com ssh-ed25519 AAAA\n",
        )
        assert not known.pinned_any("bastion.example.com"), "a revoked entry pins nothing"
        assert known.certified("bastion.example.com")
        assert not known.certified("elsewhere.net")
        assert not known.pinned_any("other.example.com")

    def test_comments_blank_and_short_lines_are_skipped(self, tmp_path):
        known = self._known(tmp_path, "# comment\n\ntwo fields\n")
        assert known.pins == [] and known.cert_authorities == []

    def test_missing_file_is_tolerated(self, tmp_path):
        known = KnownHosts()
        known.load(str(tmp_path / "absent"))
        assert known.pins == [] and known.files == [str(tmp_path / "absent")]


async def _cli_ssh_args(
    monkeypatch, config: SSHTunnelConfig, *, pinned: bool = True
) -> list[str]:
    """Start a tunnel against a fake ssh and return its command line."""

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
    async def test_explicit_known_hosts_file_is_passed(self, monkeypatch, tmp_path):
        args = await _cli_ssh_args(monkeypatch, _config(known_hosts_file=str(tmp_path / "kh")))
        assert _option(args, "UserKnownHostsFile") == str(tmp_path / "kh")

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
    async def test_a_wildcard_pin_counts_as_recorded(self, monkeypatch, tmp_path):
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text(f"*.example.com {_public_key_line(tmp_path)}\n")
        config = _config(known_hosts_file=str(known_hosts))

        args = await _cli_ssh_args(monkeypatch, config, pinned=False)

        assert _option(args, "StrictHostKeyChecking") == "accept-new"

    @pytest.mark.anyio
    async def test_ca_only_entry_runs_ssh_strictly(self, monkeypatch, tmp_path):
        """A CA entry alone cannot tell a verified certificate from a raw
        first-use key, so ssh must require the certificate."""
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text(f"@cert-authority bastion.example.com {_public_key_line(tmp_path)}\n")
        config = _config(known_hosts_file=str(known_hosts))

        args = await _cli_ssh_args(monkeypatch, config, pinned=False)

        assert _option(args, "StrictHostKeyChecking") == "yes"
        assert known_hosts.read_text().startswith("@cert-authority")

    def test_files_ssh_reads(self, tmp_path):
        tunnel = CLISSHTunnel(_config(), "db.internal", 5432)
        assert tunnel._known_hosts_files("/home/u/.ssh/known_hosts") == [
            GLOBAL_KNOWN_HOSTS_FILE,
            "/home/u/.ssh/known_hosts",
            os.path.expanduser("~/.ssh/known_hosts2"),
        ]
        explicit = CLISSHTunnel(_config(known_hosts_file=str(tmp_path / "kh")), "db", 1)
        assert explicit._known_hosts_files(str(tmp_path / "kh")) == [
            GLOBAL_KNOWN_HOSTS_FILE,
            str(tmp_path / "kh"),
        ]


def _bastion_name() -> str:
    return known_hosts_name(docker_test_ssh_host(), docker_test_ssh_port())


def _bastion_key_line() -> str:
    """The fixture bastion's ed25519 host key, type and base64, via ssh-keyscan.

    sshd throttles bursts of new connections (MaxStartups), and the suite
    opens many in a row, so the scan is retried a few times.
    """
    for _ in range(10):
        scan = subprocess.run(
            ["ssh-keyscan", "-t", "ed25519", "-p", str(docker_test_ssh_port()),
             docker_test_ssh_host()],
            capture_output=True,
            text=True,
            check=False,
        )
        for line in scan.stdout.splitlines():
            fields = line.split()
            if len(fields) >= 3 and fields[1] == "ssh-ed25519":
                return f"{fields[1]} {fields[2]}"
        time.sleep(0.5)
    raise AssertionError("no ed25519 host key from ssh-keyscan after retries")


def _recorded(known_hosts, name: str) -> bool:
    """Whether ssh finds ``name`` in the file, hashed entries included."""
    found = subprocess.run(
        ["ssh-keygen", "-F", name, "-f", str(known_hosts)],
        capture_output=True,
        text=True,
        check=False,
    )
    return found.returncode == 0 and found.stdout.strip() != ""


def _bastion_accepted_logins() -> int:
    """Successful authentications the fixture sshd has logged so far."""
    log = subprocess.run(
        ["docker", "logs", "mcp-ssh-bastion"], capture_output=True, text=True, check=False
    )
    return (log.stdout + log.stderr).count("Accepted ")


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
    # sshd re-executes on HUP; wait until it offers the certificate.
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        scan = subprocess.run(
            ["ssh-keyscan", "-c", "-t", "ed25519", "-p", str(docker_test_ssh_port()),
             docker_test_ssh_host()],
            capture_output=True,
            text=True,
            check=False,
        )
        if "-cert-v01@openssh.com" in scan.stdout:
            break
        time.sleep(0.5)
    else:
        raise AssertionError("the fixture sshd did not start offering the host certificate")
    return (tmp_path / "ca.pub").read_text().strip()


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
    """Both implementations go through ssh, so both see the same behaviour."""

    async def test_first_use_records_the_bastion(self, implementation, tmp_path):
        known_hosts = tmp_path / "known_hosts"
        connector = _tunnelled_connector(implementation, known_hosts_file=str(known_hosts))

        result = await connector.execute_query("SELECT 1 AS one")

        assert result.split("\n")[1] == "1"
        assert _recorded(known_hosts, _bastion_name())

    async def test_first_use_creates_the_directory(self, implementation, tmp_path):
        known_hosts = tmp_path / "missing" / "known_hosts"
        connector = _tunnelled_connector(implementation, known_hosts_file=str(known_hosts))

        await connector.execute_query("SELECT 1")

        assert _recorded(known_hosts, _bastion_name())

    async def test_unwritable_file_fails_closed(self, implementation, tmp_path):
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text("")
        known_hosts.chmod(0o400)
        connector = _tunnelled_connector(implementation, known_hosts_file=str(known_hosts))

        with pytest.raises(ConnectorError, match="(?i)record"):
            await connector.execute_query("SELECT 1")

        assert known_hosts.read_text() == ""

    async def test_revoked_key_is_refused_before_authentication(self, implementation, tmp_path):
        """ssh refuses a revoked key at key exchange, even next to an
        ordinary pin for the same key; no credential reaches the bastion."""
        key = _bastion_key_line()
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text(
            f"{_bastion_name()} {key}\n@revoked {_bastion_name()} {key}\n"
        )
        connector = _tunnelled_connector(implementation, known_hosts_file=str(known_hosts))

        accepted_before = _bastion_accepted_logins()
        with pytest.raises(ConnectorError, match="(?i)revoked"):
            await connector.execute_query("SELECT 1")
        assert _bastion_accepted_logins() == accepted_before, "no credential was sent"

    async def test_rotation_overlap_with_two_keys_of_one_type(self, implementation, tmp_path):
        """Two valid entries of the same type, the bastion presenting the
        first: ssh accepts it and records nothing."""
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text(
            f"{_bastion_name()} {_bastion_key_line()}\n"
            f"{_bastion_name()} {_public_key_line(tmp_path)}\n"
        )
        connector = _tunnelled_connector(implementation, known_hosts_file=str(known_hosts))

        result = await connector.execute_query("SELECT 1 AS one")

        assert result.split("\n")[1] == "1"
        assert len(known_hosts.read_text().splitlines()) == 2

    async def test_ca_only_bastion_is_accepted_and_not_recorded(self, implementation, tmp_path):
        """With only an @cert-authority entry, ssh verifies the host
        certificate and records no raw key."""
        ca_line = _ca_signed_bastion(tmp_path)
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text(f"@cert-authority {_bastion_name()} {ca_line}\n")
        connector = _tunnelled_connector(implementation, known_hosts_file=str(known_hosts))

        result = await connector.execute_query("SELECT 1 AS one")

        assert result.split("\n")[1] == "1"
        assert known_hosts.read_text() == f"@cert-authority {_bastion_name()} {ca_line}\n"

    async def test_ca_only_file_refuses_a_bastion_without_a_matching_certificate(
        self, implementation, tmp_path
    ):
        """With a CA entry from a CA that never signed the bastion, ssh gets
        a key it cannot verify; it must be refused, not recorded."""
        line = f"@cert-authority {_bastion_name()} {_public_key_line(tmp_path, 'unrelated_ca')}\n"
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text(line)
        connector = _tunnelled_connector(implementation, known_hosts_file=str(known_hosts))

        with pytest.raises(ConnectorError, match="(?i)host key"):
            await connector.execute_query("SELECT 1")

        assert known_hosts.read_text() == line

    async def test_changed_key_is_refused(self, implementation, tmp_path):
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text(f"{_bastion_name()} {_public_key_line(tmp_path)}\n")
        connector = _tunnelled_connector(implementation, known_hosts_file=str(known_hosts))

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
