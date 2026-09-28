"""Bastion host keys are verified by ssh, for every connector.

``host_key_checking`` defaults to ``yes``: the bastion's key must already be
in a known_hosts file. ``accept-new`` is the explicit trust-on-first-use
mode: a bastion is recorded on first use and refused if its key changes, and
the tunnel fails closed when ssh could not record it. ``no`` restores the
old trust-everything behaviour. What the tunnel needs to know about the
files it asks ssh's own reader, ``ssh-keygen -F``.
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
    CLISSHTunnel,
    HostKeySurvey,
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
    def test_defaults_to_strict_and_ssh_default_file(self):
        config = _config()
        assert config.host_key_checking == "yes"
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


class TestSurvey:
    """The tunnel asks ssh -G for the host-key name, then checks its own files."""

    def _survey(self, tmp_path, text: str, host: str = "bastion.example.com", port: int = 22):
        path = tmp_path / "pins"
        path.write_text(text)
        config = _config(host=host, port=port, known_hosts_file=str(path))
        tunnel = CLISSHTunnel(config, "db.internal", 5432)
        return tunnel._survey(["-p", str(port), "-o", f"UserKnownHostsFile={path}"], f"tunnel@{host}")

    def test_name_forms(self):
        assert known_hosts_name("bastion.example.com", 22) == "bastion.example.com"
        assert known_hosts_name("bastion.example.com", 2222) == "[bastion.example.com]:2222"

    def test_ssh_resolves_name_and_files(self, tmp_path):
        survey = self._survey(tmp_path, "", "bastion.example.com", 2222)
        assert survey.name == "[bastion.example.com]:2222"
        assert survey.record_file == str(tmp_path / "pins")
        assert not survey.pinned and not survey.certified

    def test_plain_pin(self, tmp_path):
        survey = self._survey(tmp_path, f"bastion.example.com {_public_key_line(tmp_path)}\n")
        assert survey.pinned and not survey.certified

    def test_wildcard_pin_with_port(self, tmp_path):
        line = f"[*.internal]:2222 {_public_key_line(tmp_path)}\n"
        assert self._survey(tmp_path, line, "jump.internal", 2222).pinned
        assert not self._survey(tmp_path, line, "jump.internal", 22).pinned

    def test_negated_pattern_excludes_the_host(self, tmp_path):
        line = f"!bastion.example.com,*.example.com {_public_key_line(tmp_path)}\n"
        assert not self._survey(tmp_path, line).pinned

    def test_hashed_names_as_ssh_keygen_writes_them(self, tmp_path):
        path = tmp_path / "pins"
        path.write_text(f"[bastion.example.com]:2222 {_public_key_line(tmp_path)}\n")
        subprocess.run(["ssh-keygen", "-q", "-H", "-f", str(path)], check=True)
        assert path.read_text().startswith("|1|")
        tunnel = CLISSHTunnel(_config(port=2222, known_hosts_file=str(path)), "db", 1)
        survey = tunnel._survey(["-p", "2222", "-o", f"UserKnownHostsFile={path}"], "tunnel@bastion.example.com")
        assert survey.pinned

    def test_markers(self, tmp_path):
        survey = self._survey(
            tmp_path,
            f"@revoked bastion.example.com {_public_key_line(tmp_path, 'r')}\n"
            f"@cert-authority *.example.com {_public_key_line(tmp_path, 'ca')}\n",
        )
        assert not survey.pinned, "a revoked entry pins nothing"
        assert survey.certified

    def test_malformed_key_is_not_a_pin(self, tmp_path):
        """ssh skips a line whose key does not parse; so must the tunnel."""
        line = "[bastion.example.com]:2222 ssh-rsa not-valid-base64\n"
        assert not self._survey(tmp_path, line, "bastion.example.com", 2222).pinned

    def test_missing_file(self, tmp_path):
        tunnel = CLISSHTunnel(_config(known_hosts_file=str(tmp_path / "absent")), "db", 1)
        survey = tunnel._survey(["-o", f"UserKnownHostsFile={tmp_path / 'absent'}"], "bastion.example.com")
        assert not survey.pinned

    def test_ssh_config_decides_the_name_but_not_the_files(self, tmp_path, monkeypatch):
        """The host alias applies, while accept-new uses explicit file paths."""
        work_file = tmp_path / "known_hosts_work"
        work_file.write_text(f"jump.alias {_public_key_line(tmp_path)}\n")
        ssh_config_file = tmp_path / "ignored_by_accept_new"
        monkeypatch.setattr(
            "mcp_read_only_sql.utils.ssh_tunnel_cli._ssh_resolved_config",
            lambda options, destination: {
                "hostname": "10.0.0.5",
                "port": "22",
                "hostkeyalias": "jump.alias",
                "userknownhostsfile": str(ssh_config_file),
                "globalknownhostsfile": "/etc/ssh/ssh_known_hosts /etc/ssh/ssh_known_hosts2",
            },
        )
        tunnel = CLISSHTunnel(_config(known_hosts_file=str(work_file)), "db", 1)

        survey = tunnel._survey([], "tunnel@bastion.example.com")

        assert survey.name == "jump.alias"
        assert survey.record_file == str(work_file)
        assert survey.pinned

    def test_known_hosts_path_with_spaces(self, tmp_path):
        folder = tmp_path / "dir with space"
        folder.mkdir()
        path = folder / "known hosts"
        path.write_text(f"bastion.example.com {_public_key_line(tmp_path)}\n")
        tunnel = CLISSHTunnel(_config(known_hosts_file=str(path)), "db", 1)

        survey = tunnel._survey(["-o", f"UserKnownHostsFile={path}"], "tunnel@bastion.example.com")

        assert survey.record_file == str(path)
        assert survey.pinned

    def test_ambiguous_ssh_g_path_cannot_invent_a_pin(self, tmp_path):
        """A different file with a pin must not disable the recording check."""
        decoy = tmp_path / "known"
        decoy.write_text(f"bastion.example.com {_public_key_line(tmp_path)}\n")
        actual = tmp_path / "known /hosts"
        tunnel = CLISSHTunnel(_config(known_hosts_file=str(actual)), "db", 1)

        survey = tunnel._survey(["-o", f'UserKnownHostsFile="{actual}"'], "tunnel@bastion.example.com")

        assert survey.record_file == str(actual)
        assert not survey.pinned

    def test_fails_closed_when_ssh_g_fails(self, monkeypatch):
        monkeypatch.setattr(
            "mcp_read_only_sql.utils.ssh_tunnel_cli._ssh_resolved_config",
            lambda options, destination: {},
        )
        with pytest.raises(ConnectorError, match="cannot resolve host-key settings"):
            CLISSHTunnel(_config(port=2222), "db", 1)._survey([], "bastion.example.com")


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
        monkeypatch.setattr(
            CLISSHTunnel,
            "_survey",
            lambda self, options, destination: HostKeySurvey(
                "bastion.example.com", "/dev/null", pinned=True, certified=False
            ),
        )
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
    async def test_default_is_strict_with_ssh_default_file(self, monkeypatch):
        args = await _cli_ssh_args(monkeypatch, _config())
        assert _option(args, "StrictHostKeyChecking") == "yes"
        assert _option(args, "UserKnownHostsFile") is None

    @pytest.mark.anyio
    async def test_accept_new_is_an_explicit_mode(self, monkeypatch):
        args = await _cli_ssh_args(monkeypatch, _config(host_key_checking="accept-new"))
        assert _option(args, "StrictHostKeyChecking") == "accept-new"
        assert _option(args, "UserKnownHostsFile") == (
            f'"{os.path.expanduser("~/.ssh/known_hosts")}" '
            f'"{os.path.expanduser("~/.ssh/known_hosts2")}"'
        )
        assert _option(args, "GlobalKnownHostsFile") == (
            '"/etc/ssh/ssh_known_hosts" "/etc/ssh/ssh_known_hosts2"'
        )

    @pytest.mark.anyio
    async def test_accept_new_passes_one_quoted_file_even_with_ambiguous_spaces(
        self, monkeypatch, tmp_path
    ):
        path = tmp_path / "known /hosts"
        config = _config(host_key_checking="accept-new", known_hosts_file=str(path))

        args = await _cli_ssh_args(monkeypatch, config)

        assert _option(args, "UserKnownHostsFile") == f'"{path}"'

    @pytest.mark.anyio
    async def test_explicit_known_hosts_file_is_passed(self, monkeypatch, tmp_path):
        args = await _cli_ssh_args(monkeypatch, _config(known_hosts_file=str(tmp_path / "kh")))
        assert _option(args, "UserKnownHostsFile") == f'"{tmp_path / "kh"}"'

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
        """Under accept-new ssh only warns when it cannot record a key, so the
        directory it records into (as ssh resolves it) must exist first."""
        known_hosts = tmp_path / "missing" / "known_hosts"
        monkeypatch.setattr(
            CLISSHTunnel,
            "_survey",
            lambda self, options, destination: HostKeySurvey(
                "bastion.example.com", str(known_hosts), pinned=True, certified=False
            ),
        )
        config = _config(host_key_checking="accept-new", known_hosts_file=str(known_hosts))

        await _cli_ssh_args(monkeypatch, config, pinned=False)

        assert known_hosts.parent.is_dir()
        assert stat.S_IMODE(known_hosts.parent.stat().st_mode) == 0o700

    @pytest.mark.anyio
    async def test_first_use_fails_closed_when_ssh_recorded_nothing(
        self, monkeypatch, tmp_path
    ):
        """ssh only warns when it cannot write the file; the tunnel must not."""
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text("")
        config = _config(host_key_checking="accept-new", known_hosts_file=str(known_hosts))

        with pytest.raises(ConnectorError, match="was not recorded"):
            await _cli_ssh_args(monkeypatch, config, pinned=False)

    @pytest.mark.anyio
    async def test_a_wildcard_pin_counts_as_recorded(self, monkeypatch, tmp_path):
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text(f"*.example.com {_public_key_line(tmp_path)}\n")
        config = _config(host_key_checking="accept-new", known_hosts_file=str(known_hosts))

        args = await _cli_ssh_args(monkeypatch, config, pinned=False)

        assert _option(args, "StrictHostKeyChecking") == "accept-new"

    @pytest.mark.anyio
    async def test_ca_only_entry_runs_ssh_strictly(self, monkeypatch, tmp_path):
        """A CA entry alone cannot tell a verified certificate from a raw
        first-use key, so ssh must require the certificate."""
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text(f"@cert-authority bastion.example.com {_public_key_line(tmp_path)}\n")
        config = _config(host_key_checking="accept-new", known_hosts_file=str(known_hosts))

        args = await _cli_ssh_args(monkeypatch, config, pinned=False)

        assert _option(args, "StrictHostKeyChecking") == "yes"
        assert known_hosts.read_text().startswith("@cert-authority")

    @pytest.mark.anyio
    async def test_a_pin_next_to_a_ca_entry_stays_accept_new(self, monkeypatch, tmp_path):
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text(
            f"bastion.example.com {_public_key_line(tmp_path, 'k')}\n"
            f"@cert-authority bastion.example.com {_public_key_line(tmp_path, 'ca')}\n"
        )
        config = _config(host_key_checking="accept-new", known_hosts_file=str(known_hosts))

        args = await _cli_ssh_args(monkeypatch, config, pinned=False)

        assert _option(args, "StrictHostKeyChecking") == "accept-new"


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

    async def test_first_use_records_in_a_path_with_ambiguous_spaces(
        self, implementation, tmp_path
    ):
        known_hosts = tmp_path / "known /hosts"
        known_hosts.parent.mkdir()
        connector = _tunnelled_connector(implementation, known_hosts_file=str(known_hosts))

        result = await connector.execute_query("SELECT 1 AS one")

        assert result.split("\n")[1] == "1"
        assert _recorded(known_hosts, _bastion_name())
        assert not (tmp_path / "known").exists()

    async def test_decoy_pin_cannot_hide_an_unwritable_record_file(
        self, implementation, tmp_path
    ):
        decoy = tmp_path / "known"
        decoy.write_text(f"{_bastion_name()} {_bastion_key_line()}\n")
        known_hosts = tmp_path / "known /hosts"
        known_hosts.parent.mkdir()
        known_hosts.write_text("")
        known_hosts.chmod(0o400)
        connector = _tunnelled_connector(implementation, known_hosts_file=str(known_hosts))

        with pytest.raises(ConnectorError, match="(?i)record"):
            await connector.execute_query("SELECT 1")

        assert known_hosts.read_text() == ""
        assert decoy.read_text().startswith(_bastion_name())

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

    async def test_no_mode_trusts_a_changed_key_and_records_nothing(
        self, implementation, tmp_path
    ):
        known_hosts = tmp_path / "known_hosts"
        line = f"{_bastion_name()} {_public_key_line(tmp_path)}\n"
        known_hosts.write_text(line)
        connector = _tunnelled_connector(
            implementation, known_hosts_file=str(known_hosts), host_key_checking="no"
        )

        result = await connector.execute_query("SELECT 1 AS one")

        assert result.split("\n")[1] == "1"
        assert known_hosts.read_text() == line

    async def test_default_refuses_an_unprovisioned_bastion(self, implementation, tmp_path):
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text("")
        connector = _tunnelled_connector(
            implementation, known_hosts_file=str(known_hosts), host_key_checking="yes"
        )

        with pytest.raises(ConnectorError, match="(?i)host key|known_hosts"):
            await connector.execute_query("SELECT 1")
        assert known_hosts.read_text() == ""

    @pytest.mark.parametrize("relative_path", ["known_hosts", "known /hosts"])
    async def test_default_accepts_a_provisioned_bastion(
        self, implementation, tmp_path, relative_path
    ):
        """The intended setup: ssh-keyscan (or a prior ssh session) provisioned the key."""
        known_hosts = tmp_path / relative_path
        known_hosts.parent.mkdir(exist_ok=True)
        known_hosts.write_text(f"{_bastion_name()} {_bastion_key_line()}\n")
        connector = _tunnelled_connector(
            implementation, known_hosts_file=str(known_hosts), host_key_checking="yes"
        )

        result = await connector.execute_query("SELECT 1 AS one")

        assert result.split("\n")[1] == "1"
