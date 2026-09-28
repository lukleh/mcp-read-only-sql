"""
SSH tunnel implementation using system SSH command for CLI connectors.
Supports key-based authentication and password authentication (via sshpass).
"""

import asyncio
import logging
import os
import shutil
import signal
import socket
import subprocess
from contextlib import closing
from dataclasses import dataclass

from ..errors import ConnectorError

logger = logging.getLogger(__name__)

# In accept-new mode these are passed to ssh explicitly, so the files checked
# before and after a connection are exactly the files ssh uses.
GLOBAL_KNOWN_HOSTS_FILES = ("/etc/ssh/ssh_known_hosts", "/etc/ssh/ssh_known_hosts2")
DEFAULT_USER_KNOWN_HOSTS_FILES = ("~/.ssh/known_hosts", "~/.ssh/known_hosts2")


def known_hosts_name(host: str, port: int) -> str:
    """The name ssh records a host under: ``[host]:port`` off port 22."""
    return host if port == 22 else f"[{host}]:{port}"


def _ssh_resolved_config(options: list[str], destination: str) -> dict[str, str]:
    """The options ssh would use for ``destination``, from ``ssh -G``.

    ssh resolves ``~/.ssh/config`` (including HostName and HostKeyAlias)
    without connecting. Its host-key file list is not used: paths with spaces
    are printed without unambiguous separators. An empty dict means ssh
    could not be asked.
    """
    try:
        resolved = subprocess.run(
            ["ssh", "-G", *options, destination],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return {}
    if resolved.returncode != 0:
        return {}
    config: dict[str, str] = {}
    for line in resolved.stdout.splitlines():
        key, _, value = line.partition(" ")
        if key:
            config[key.lower()] = value.strip()
    return config


def _ssh_file_option(name: str, paths: tuple[str, ...]) -> str:
    """Pass exact paths through ssh's config parser, including spaces and %."""
    quoted = []
    for path in paths:
        if "\n" in path or "\r" in path:
            raise ConnectorError(f"SSH: invalid {name} path")
        # %% prevents ssh from expanding a literal percent as a config token.
        quoted.append('"' + path.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%") + '"')
    return f"{name}=" + " ".join(quoted)


def _ssh_keygen_find(name: str, path: str) -> list[str]:
    """The lines of ``path`` that ssh matches for ``name`` and whose key parses.

    ``ssh-keygen -F`` applies the same host rules as ssh (hashed names,
    wildcard and negated patterns, markers) but reports a line whose key is
    garbage too, and ssh would ignore that line. ``ssh-keygen -l`` is asked
    about each match, marker stripped, so only lines ssh can use count.
    Nothing here interprets the file on its own.
    """
    try:
        found = subprocess.run(
            ["ssh-keygen", "-F", name, "-f", path],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return []
    if found.returncode != 0:
        return []
    matches = [line for line in found.stdout.splitlines() if line and not line.startswith("#")]
    return [line for line in matches if _key_parses(line)]


def _key_parses(line: str) -> bool:
    """Whether ssh can read the key on a known_hosts line (``ssh-keygen -l``)."""
    entry = line.split(" ", 1)[1] if line.startswith("@") and " " in line else line
    try:
        checked = subprocess.run(
            ["ssh-keygen", "-l", "-f", "-"],
            input=entry + "\n",
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return False
    return checked.returncode == 0


@dataclass(frozen=True)
class HostKeySurvey:
    """What ssh knows about the bastion before or after a connection.

    ``name`` is what ssh records the bastion under and ``record_file`` where.
    ``pinned`` means a usable host-key
    entry names the bastion in one of the files ssh reads; ``certified``
    means a certificate authority entry does.
    """

    name: str
    record_file: str
    pinned: bool
    certified: bool


class CLISSHTunnel:
    """SSH tunnel through the system ``ssh`` command.

    Used by every connector. ``ssh`` brings its own host-key verification,
    agent and certificate support, and configuration. By default the bastion
    must already be known (``StrictHostKeyChecking=yes``). In the explicit
    ``accept-new`` mode the tunnel chooses the known_hosts files, then asks
    ``ssh-keygen -F`` whether the bastion was known before and is recorded
    after, and fails closed otherwise.
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

    def _user_known_hosts_files(self) -> tuple[str, ...]:
        if self.ssh_config.known_hosts_file is not None:
            return (os.path.abspath(self.ssh_config.known_hosts_file),)
        return tuple(os.path.expanduser(path) for path in DEFAULT_USER_KNOWN_HOSTS_FILES)

    def _survey(self, ssh_options: list[str], destination: str) -> HostKeySurvey:
        """Ask ssh for the host-key name, then check the exact files passed to it.

        Blocking; run it off the event loop.
        """
        resolved = _ssh_resolved_config(ssh_options, destination)
        if not resolved:
            raise ConnectorError(f"SSH: cannot resolve host-key settings for {destination}")
        name = resolved.get("hostkeyalias") or known_hosts_name(
            resolved.get("hostname", self.ssh_host),
            int(resolved.get("port", self.ssh_port)),
        )
        user_files = self._user_known_hosts_files()
        matches = [
            line
            for path in (*GLOBAL_KNOWN_HOSTS_FILES, *user_files)
            for line in _ssh_keygen_find(name, path)
        ]
        return HostKeySurvey(
            name=name,
            record_file=user_files[0],
            pinned=any(not line.startswith("@") for line in matches),
            certified=any(line.startswith("@cert-authority") for line in matches),
        )

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
        # by ssh: yes (the default) requires a provisioned key, accept-new
        # records a bastion on first use and refuses it if its key changes.
        host_key_checking = self.ssh_config.host_key_checking
        known_hosts_file = (
            "/dev/null"  # legacy mode: trust everything and record nothing
            if host_key_checking == "no"
            else self.ssh_config.known_hosts_file
        )
        # Determine destination (user@host). In accept-new mode pass the
        # exact files we inspect to ssh, avoiding ssh -G's ambiguous unquoted
        # list of file paths.
        destination = (
            f"{self.ssh_user}@{self.ssh_host}" if self.ssh_user else self.ssh_host
        )
        lookup_options = ["-p", str(self.ssh_port)]
        known_hosts_options: list[str] = []
        if host_key_checking == "accept-new":
            known_hosts_options = [
                "-o", _ssh_file_option("UserKnownHostsFile", self._user_known_hosts_files()),
                "-o", _ssh_file_option("GlobalKnownHostsFile", GLOBAL_KNOWN_HOSTS_FILES),
            ]
            lookup_options.extend(known_hosts_options)

        # accept-new is trust on first use. ssh only warns when it cannot
        # record the key and would then treat every connection as first use,
        # so a bastion ssh does not know yet must be recorded once the tunnel
        # is up (checked below), and the directory it records into must
        # exist. ssh -G supplies the host-key name; the file paths are ours.
        must_record = False
        if host_key_checking == "accept-new":
            before = await asyncio.to_thread(self._survey, lookup_options, destination)
            directory = os.path.dirname(os.path.abspath(before.record_file))
            try:
                os.makedirs(directory, mode=0o700, exist_ok=True)
            except OSError as exc:
                raise ConnectorError(
                    f"SSH: cannot create {directory} to record host keys: {exc}"
                ) from exc
            if before.certified and not before.pinned:
                # A CA entry lets ssh verify a host certificate, but under
                # accept-new ssh would also take a raw key from that host
                # and nothing here could tell the two apart. Run strictly:
                # a valid certificate connects, a raw key is refused.
                host_key_checking = "yes"
                logger.info(
                    "SSH: %s is covered by a certificate authority entry; "
                    "requiring a valid host certificate",
                    before.name,
                )
            elif not before.pinned:
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
        if self.ssh_config.host_key_checking == "accept-new":
            ssh_options.extend(known_hosts_options)
        elif known_hosts_file is not None:
            option = (
                f"UserKnownHostsFile={known_hosts_file}"
                if self.ssh_config.host_key_checking == "no"
                else _ssh_file_option("UserKnownHostsFile", (os.path.abspath(known_hosts_file),))
            )
            ssh_options.extend(["-o", option])

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

            if must_record:
                after = await asyncio.to_thread(self._survey, lookup_options, destination)
                if not after.pinned:
                    await self.stop()
                    raise ConnectorError(
                        f"SSH: the host key for {after.name} was not recorded in "
                        f"{after.record_file}; ssh continued without saving it. "
                        "Check that the file is writable."
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
