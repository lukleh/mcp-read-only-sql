"""
SSH tunnel implementation using system SSH command for CLI connectors.
Supports key-based authentication and password authentication (via sshpass).
"""

import asyncio
import base64
import binascii
import hashlib
import hmac
import logging
import os
import re
import shutil
import signal
import socket
from contextlib import closing

from ..errors import ConnectorError

logger = logging.getLogger(__name__)

DEFAULT_KNOWN_HOSTS_FILE = "~/.ssh/known_hosts"
# Read as well, never written: the file ssh consults before the user's.
GLOBAL_KNOWN_HOSTS_FILE = "/etc/ssh/ssh_known_hosts"


def known_hosts_name(host: str, port: int) -> str:
    """The name ssh records a host under: ``[host]:port`` off port 22."""
    return host if port == 22 else f"[{host}]:{port}"


def _pattern_matches(pattern: str, hostname: str) -> bool:
    """An OpenSSH known_hosts host pattern: ``*`` and ``?`` wildcards."""
    regex = "".join(
        ".*" if char == "*" else "." if char == "?" else re.escape(char)
        for char in pattern
    )
    return re.fullmatch(regex, hostname, re.IGNORECASE) is not None


def _hashed_matches(name: str, hostname: str) -> bool:
    """A hashed name, ``|1|salt|hash`` with HMAC-SHA1 of the hostname."""
    try:
        _, _, salt, digest = name.split("|", 3)
        salt_bytes, digest_bytes = base64.b64decode(salt), base64.b64decode(digest)
    except (ValueError, binascii.Error):
        return False
    computed = hmac.new(salt_bytes, hostname.encode(), hashlib.sha1).digest()
    return hmac.compare_digest(computed, digest_bytes)


def _entry_names(hostnames, hostname: str) -> bool:
    """True when an entry's host list names ``hostname`` the way ssh reads it.

    Plain names and patterns with ``*`` and ``?`` match case-insensitively, a
    hashed name matches when the hash of ``hostname`` is equal, and a negated
    pattern (``!name``) excludes the entry when it matches.
    """
    matched = False
    for name in hostnames:
        if name.startswith("!"):
            if _pattern_matches(name[1:], hostname):
                return False
        elif name.startswith("|1|"):
            if _hashed_matches(name, hostname):
                matched = True
        elif _pattern_matches(name, hostname):
            matched = True
    return matched


class KnownHosts:
    """Which hosts the known_hosts files ssh reads say something about.

    Verifying keys is ssh's job; this only answers whether a host is pinned
    by an entry (of any key type), or covered by a certificate authority
    entry, so the tunnel knows what to expect after ssh connects. Wildcard
    patterns, negation, hashed names and the ``@revoked`` and
    ``@cert-authority`` markers are read as ssh reads them; a revoked entry
    pins nothing.
    """

    def __init__(self):
        self.files: list[str] = []
        self.pins: list[tuple[str, ...]] = []
        self.cert_authorities: list[tuple[str, ...]] = []

    def load(self, path: str) -> None:
        """Read ``path`` if it exists."""
        self.files.append(path)
        try:
            with open(path, encoding="utf-8") as handle:
                lines = handle.read().splitlines()
        except OSError:
            return
        for raw in lines:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            marker = None
            if line.startswith("@"):
                marker, _, line = line.partition(" ")
            fields = line.split()
            if len(fields) < 3:
                continue
            hostnames = tuple(fields[0].split(","))
            if marker == "@cert-authority":
                self.cert_authorities.append(hostnames)
            elif marker is None:
                self.pins.append(hostnames)

    def pinned_any(self, hostname: str) -> bool:
        """Whether a host-key entry of any type names ``hostname``."""
        return any(_entry_names(names, hostname) for names in self.pins)

    def certified(self, hostname: str) -> bool:
        """Whether a certificate authority entry names ``hostname``.

        Such an entry lets ssh verify a host certificate, but does not by
        itself prove that a given connection used one: under accept-new ssh
        would still take a raw key from that host. The tunnel therefore runs
        strictly in that case.
        """
        return any(_entry_names(names, hostname) for names in self.cert_authorities)


class CLISSHTunnel:
    """SSH tunnel through the system ``ssh`` command.

    Used by every connector. ``ssh`` brings its own host-key verification,
    agent and certificate support, and configuration; the tunnel only
    chooses options and checks afterwards that a first-use key was recorded.
    """

    DEFAULT_SSH_TIMEOUT = 30  # seconds — generous default to accommodate
    # system ssh interactive auth (Skotty fingerprint scan, hardware tokens,
    # etc.) before the first SSH cert lands in the agent.

    def __init__(self, ssh_config, remote_host: str, remote_port: int):
        """
        Initialize CLI SSH tunnel.

        Args:
            ssh_config: SSHTunnelConfig object (from config module)
            remote_host: Remote database host to tunnel to
            remote_port: Remote database port to tunnel to
        """
        self.ssh_config = ssh_config
        self.ssh_host = ssh_config.host
        self.ssh_user = ssh_config.user
        self.ssh_port = ssh_config.port
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.ssh_key = ssh_config.private_key
        self.ssh_password = ssh_config.password
        self.ssh_timeout = ssh_config.ssh_timeout or self.DEFAULT_SSH_TIMEOUT
        self.ssh_process = None
        self.local_port = None

    def _server_name(self) -> str:
        return known_hosts_name(self.ssh_host, self.ssh_port)

    def _known_hosts_files(self, record_file: str) -> list[str]:
        """The files ssh reads for this tunnel, the one it records into last."""
        files = [GLOBAL_KNOWN_HOSTS_FILE, record_file]
        if self.ssh_config.known_hosts_file is None:
            files.append(os.path.expanduser("~/.ssh/known_hosts2"))
        return files

    def _known(self, record_file: str) -> KnownHosts:
        known = KnownHosts()
        for path in self._known_hosts_files(record_file):
            known.load(path)
        return known

    def _pinned(self, record_file: str) -> bool:
        """Whether a host-key entry ssh would read names the bastion."""
        return self._known(record_file).pinned_any(self._server_name())

    def _certified(self, record_file: str) -> bool:
        """Whether only a certificate authority entry covers the bastion."""
        return self._known(record_file).certified(self._server_name())

    def _find_free_port(self) -> int:
        """Find a free local port"""
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(("", 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    async def start(self) -> int:
        """Start SSH tunnel and return local port"""
        if self.ssh_process:
            raise RuntimeError("SSH tunnel already running")

        try:
            return await asyncio.wait_for(
                self._start_tunnel(), timeout=self.ssh_timeout
            )
        except TimeoutError:
            # Clean up if timeout occurs
            await self.stop()
            raise TimeoutError(f"SSH: Connection timeout after {self.ssh_timeout}s")

    async def _start_tunnel(self) -> int:
        """Actually start the SSH tunnel"""

        # Find a free local port
        self.local_port = self._find_free_port()

        # Build SSH options common to all auth modes. Host keys are verified
        # the way ssh itself does: accept-new (the default) records a bastion
        # on first use and refuses it if its key changes.
        host_key_checking = self.ssh_config.host_key_checking
        record_file = ""
        must_record = False
        if host_key_checking == "no":
            # Legacy mode: trust everything and record nothing, whatever file
            # is configured.
            known_hosts_file: str | None = "/dev/null"
        else:
            known_hosts_file = self.ssh_config.known_hosts_file
            # ssh only warns when it cannot record a key and would then treat
            # every connection as first use, so make sure the directory it
            # writes to exists. Its default file lives in ~/.ssh.
            record_file = known_hosts_file or os.path.expanduser(DEFAULT_KNOWN_HOSTS_FILE)
            directory = os.path.dirname(os.path.abspath(record_file))
            try:
                os.makedirs(directory, mode=0o700, exist_ok=True)
            except OSError as exc:
                raise ConnectorError(
                    f"SSH: cannot create {directory} to record host keys: {exc}"
                ) from exc
            if host_key_checking == "accept-new" and not self._pinned(record_file):
                if self._certified(record_file):
                    # A CA entry lets ssh verify a host certificate, but under
                    # accept-new ssh would also take a raw key from that host
                    # and nothing here could tell the two apart. Run strictly:
                    # a valid certificate connects, a raw key is refused.
                    host_key_checking = "yes"
                    logger.info(
                        "SSH: %s is covered by a certificate authority entry; "
                        "requiring a valid host certificate",
                        self._server_name(),
                    )
                else:
                    # A bastion not pinned yet must be in the file once the
                    # tunnel is up; the check below fails closed if ssh could
                    # not write it.
                    must_record = True

        ssh_options = [
            "-N",  # No command execution
            "-L",
            f"{self.local_port}:{self.remote_host}:{self.remote_port}",
            "-o",
            f"StrictHostKeyChecking={host_key_checking}",
            "-o",
            "LogLevel=ERROR",  # Reduce noise
            "-o",
            "ConnectTimeout=10",  # Connection timeout
            "-o",
            "ServerAliveInterval=60",  # Keep connection alive
            "-o",
            "ExitOnForwardFailure=yes",  # Exit if port forwarding fails
            "-p",
            str(self.ssh_port),
        ]
        if known_hosts_file is not None:
            ssh_options.extend(["-o", f"UserKnownHostsFile={known_hosts_file}"])

        env = os.environ.copy()
        ssh_base_cmd = ["ssh"]

        # Prefer key auth when both credentials are provided to avoid ambiguity
        if self.ssh_password and not self.ssh_key:
            if shutil.which("sshpass") is None:
                raise ConnectorError(
                    "SSH: sshpass utility not found. Install sshpass or use key-based SSH authentication."
                )
            env["SSHPASS"] = self.ssh_password
            ssh_base_cmd = ["sshpass", "-e", "ssh"]
            # Force password auth so OpenSSH doesn't prompt for keys
            ssh_options.extend(
                [
                    "-o",
                    "PreferredAuthentications=password",
                    "-o",
                    "PubkeyAuthentication=no",
                ]
            )
        elif self.ssh_key:
            ssh_options.extend(["-i", self.ssh_key])

        # Determine destination (user@host)
        destination = (
            f"{self.ssh_user}@{self.ssh_host}" if self.ssh_user else self.ssh_host
        )

        ssh_cmd = ssh_base_cmd + ssh_options + [destination]

        logger.debug("Starting SSH tunnel: %s", " ".join(ssh_cmd))

        try:
            # Start SSH process
            self.ssh_process = await asyncio.create_subprocess_exec(
                *ssh_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
                preexec_fn=os.setsid,  # Create new process group for cleanup
            )

            # Poll the local forwarded port until ssh has finished
            # authenticating and the listener is actually accepting
            # connections. Without this loop, interactive auth (Skotty
            # fingerprint, U2F, password prompt) blows past the old
            # fixed sleep and the connector races into a Connection
            # refused on the local port.
            poll_interval = 0.25
            while True:
                if self.ssh_process.returncode is not None:
                    _, stderr = await self.ssh_process.communicate()
                    error_msg = (
                        stderr.decode() if stderr else "SSH tunnel failed to start"
                    )
                    raise ConnectorError(f"SSH: {error_msg}")
                try:
                    _reader, writer = await asyncio.wait_for(
                        asyncio.open_connection("127.0.0.1", self.local_port),
                        timeout=poll_interval,
                    )
                    writer.close()
                    await writer.wait_closed()
                    break
                except (TimeoutError, ConnectionRefusedError, OSError):
                    await asyncio.sleep(poll_interval)
                    continue

            if must_record and not self._pinned(record_file):
                await self.stop()
                raise ConnectorError(
                    f"SSH: the host key for {self._server_name()} was not recorded "
                    f"in {record_file}; ssh continued without saving it. Check that "
                    "the file is writable."
                )

            logger.info(f"SSH tunnel established on local port {self.local_port}")
            return self.local_port

        except OSError as e:
            # ssh binary missing or the local socket could not be opened
            await self.stop()
            logger.error(f"Failed to establish SSH tunnel: {e}")
            raise ConnectorError(f"SSH: {e}") from e
        except Exception:
            # Clean up, then let ConnectorError and unexpected errors propagate
            await self.stop()
            raise

    async def stop(self):
        """Stop SSH tunnel"""
        if self.ssh_process:
            try:
                # Kill the entire process group
                if self.ssh_process.pid:
                    os.killpg(os.getpgid(self.ssh_process.pid), signal.SIGTERM)

                # Wait for process to terminate
                try:
                    await asyncio.wait_for(self.ssh_process.wait(), timeout=5.0)
                except TimeoutError:
                    # Force kill if it doesn't terminate
                    os.killpg(os.getpgid(self.ssh_process.pid), signal.SIGKILL)
                    await self.ssh_process.wait()

                logger.debug(f"SSH tunnel stopped (local port {self.local_port})")
            except ProcessLookupError:
                # Process already terminated
                pass
            except Exception as e:
                logger.error(f"Error stopping SSH tunnel: {e}")
            finally:
                self.ssh_process = None
                self.local_port = None
