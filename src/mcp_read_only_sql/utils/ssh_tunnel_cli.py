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
from contextlib import closing, suppress
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
    fields = line.split()
    if not fields:
        return False
    if fields[0].startswith("@"):
        if fields[0] not in {"@cert-authority", "@revoked"}:
            return False
        fields = fields[1:]
    # ssh-keygen's fingerprint reader is stricter about repeated whitespace
    # than its known_hosts matcher. The match above remains authoritative.
    entry = " ".join(fields)
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
        self._stop_task: asyncio.Task[None] | None = None
        self._stop_urgent = asyncio.Event()

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
        try:
            port = int(resolved["port"])
        except (KeyError, ValueError) as exc:
            raise ConnectorError(
                f"SSH: cannot resolve host-key port for {destination}"
            ) from exc
        name = resolved.get("hostkeyalias") or known_hosts_name(
            resolved.get("hostname", self.ssh_host),
            port,
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
            pinned=any(not line.split()[0].startswith("@") for line in matches),
            certified=any(line.split()[0] == "@cert-authority" for line in matches),
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

        startup = asyncio.create_task(self._start_tunnel())
        try:
            return await asyncio.wait_for(
                startup, timeout=self.ssh_timeout
            )
        except TimeoutError:
            # Clean up if timeout occurs
            await self.stop()
            raise TimeoutError(f"SSH: Connection timeout after {self.ssh_timeout}s")
        except asyncio.CancelledError:
            # Python 3.11's wait_for can stop awaiting its child's cleanup
            # after another cancellation without forwarding it to that child.
            # Keep startup owned and interrupt its graceful stop explicitly.
            startup.cancel()
            while not startup.done():
                try:
                    await asyncio.shield(startup)
                except asyncio.CancelledError:
                    startup.cancel()
                except Exception:
                    break
            # Cancellation wins over a startup failure delivered in the same
            # turn, but retrieving its result still observes that exception.
            with suppress(asyncio.CancelledError, Exception):
                startup.result()
            await self.stop(urgent=True)
            raise

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
        # Use the same explicit overrides for resolution and the connection;
        # omitted values must remain available to the matching Host block.
        port_options = ["-p", str(self.ssh_port)] if self.ssh_port is not None else []
        lookup_options = list(port_options)
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

        remote_host = self.remote_host
        if ":" in remote_host and not remote_host.startswith("["):
            remote_host = f"[{remote_host}]"
        ssh_options = [
            "-N",  # No command execution
            "-L",
            f"{self.local_port}:{remote_host}:{self.remote_port}",
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
            *port_options,
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
            creation = asyncio.create_task(asyncio.create_subprocess_exec(
                *ssh_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
                preexec_fn=os.setsid,  # Create new process group for cleanup
            ))
            try:
                self.ssh_process = await asyncio.shield(creation)
            except asyncio.CancelledError:
                # CPython's cancelled construction kills only the direct child.
                # sshpass starts ssh in another session, so retain construction
                # and let our cleanup send TERM through sshpass to that child.
                while not creation.done():
                    try:
                        await asyncio.shield(creation)
                    except asyncio.CancelledError:
                        continue
                    except Exception:
                        break
                with suppress(asyncio.CancelledError, Exception):
                    self.ssh_process = creation.result()
                raise

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
        except BaseException as exc:
            # Caller cancellation also owns the child once startup has spawned it.
            await self.stop(urgent=isinstance(exc, asyncio.CancelledError))
            raise

    async def stop(self, *, urgent: bool = False):
        """Keep ownership until reaping, even when cleanup is cancelled again."""
        process = self.ssh_process
        if process is None:
            return
        # A connector's finally block may enter cleanup after cancellation was
        # already delivered to its query body, without another cancel at await.
        caller = asyncio.current_task()
        urgent = urgent or (caller is not None and caller.cancelling() > 0)
        if self._stop_task is None or self._stop_task.done():
            self._stop_urgent.clear()
            self._stop_task = asyncio.create_task(self._stop_process(process))
        if urgent:
            self._stop_urgent.set()
        cleanup = self._stop_task
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            # Keep a short TERM allowance even under repeated cancellation:
            # sshpass must forward it before its own process can be killed.
            self._stop_urgent.set()
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    continue
            cleanup.result()
            raise
        finally:
            if cleanup.done() and self._stop_task is cleanup:
                self._stop_task = None

    @staticmethod
    def _signal_process(process, sig: int) -> None:
        if process.returncode is None and process.pid:
            with suppress(ProcessLookupError):
                os.killpg(os.getpgid(process.pid), sig)

    async def _stop_process(self, process) -> None:
        reaped = False
        urgent = asyncio.create_task(self._stop_urgent.wait())
        try:
            self._signal_process(process, signal.SIGTERM)
            waited = asyncio.create_task(process.wait())
            done, _ = await asyncio.wait(
                (waited, urgent), timeout=5.0, return_when=asyncio.FIRST_COMPLETED
            )
            if waited not in done and urgent in done:
                # Cancellation/deadlines get bounded cleanup, while an ordinary
                # context exit retains the five-second graceful-stop allowance.
                await asyncio.wait((waited,), timeout=0.25)
            if not waited.done():
                self._signal_process(process, signal.SIGKILL)
            await waited
            reaped = True
            logger.debug(f"SSH tunnel stopped (local port {self.local_port})")
        except Exception as exc:
            logger.error(f"Error stopping SSH tunnel: {exc}")
        finally:
            urgent.cancel()
            with suppress(asyncio.CancelledError):
                await urgent
            if reaped and self.ssh_process is process:
                self.ssh_process = None
                self.local_port = None
