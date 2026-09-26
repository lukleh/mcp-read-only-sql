import asyncio
import contextlib
import logging
import os
import re
import select
import socket
import threading
from dataclasses import dataclass

import paramiko
from paramiko.hostkeys import HostKeyEntry, InvalidHostKey

from ..errors import ConnectorError

logger = logging.getLogger(__name__)

DEFAULT_KNOWN_HOSTS_FILE = "~/.ssh/known_hosts"
# Read as well, never written: the file ssh consults before the user's.
GLOBAL_KNOWN_HOSTS_FILE = "/etc/ssh/ssh_known_hosts"
# Tunnels start on executor threads; serialise appends to one known_hosts file.
_KNOWN_HOSTS_WRITE_LOCK = threading.Lock()


def _has_line(path: str, line: str) -> bool:
    try:
        with open(path, encoding="utf-8") as handle:
            return any(existing.strip() == line for existing in handle)
    except FileNotFoundError:
        return False


def _pattern_matches(pattern: str, hostname: str) -> bool:
    """An OpenSSH known_hosts host pattern: ``*`` and ``?`` wildcards."""
    regex = "".join(
        ".*" if char == "*" else "." if char == "?" else re.escape(char)
        for char in pattern
    )
    return re.fullmatch(regex, hostname, re.IGNORECASE) is not None


def _entry_names(hostnames, hostname: str) -> bool:
    """True when an entry's host list names ``hostname`` the way ssh reads it.

    Plain names and patterns with ``*`` and ``?`` match case-insensitively, a
    hashed name (``|1|salt|hash``) matches when the hash of ``hostname`` is
    equal, and a negated pattern (``!name``) excludes the entry when it
    matches.
    """
    matched = False
    for name in hostnames:
        if name.startswith("!"):
            if _pattern_matches(name[1:], hostname):
                return False
        elif name.startswith("|1|"):
            if paramiko.HostKeys.hash_host(hostname, name) == name:
                matched = True
        elif _pattern_matches(name, hostname):
            matched = True
    return matched


@dataclass(frozen=True)
class KnownHostsEntry:
    """One known_hosts line: its host patterns and the key it pins."""

    hostnames: tuple[str, ...]
    key: paramiko.PKey


class KnownHosts:
    """The known_hosts entries ssh would consult, with OpenSSH's markers.

    Paramiko's own loader rejects marker lines and matches names literally.
    This reads the files itself: ``@revoked`` entries are kept apart so their
    keys are refused, ``@cert-authority`` entries are skipped because Paramiko
    cannot verify host certificates, and lookups honour wildcard patterns,
    negation and hashed names.
    """

    def __init__(self):
        self.files: list[str] = []
        self.entries: list[KnownHostsEntry] = []
        self.revoked: list[KnownHostsEntry] = []

    def load(self, path: str) -> None:
        """Read ``path`` if it exists; malformed lines are skipped with a warning."""
        self.files.append(path)
        try:
            with open(path, encoding="utf-8") as handle:
                lines = handle.read().splitlines()
        except OSError:
            return
        for number, raw in enumerate(lines, 1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            marker = None
            if line.startswith("@"):
                marker, _, line = line.partition(" ")
                if marker == "@cert-authority":
                    continue
                if marker != "@revoked":
                    logger.warning(
                        "SSH: ignoring unknown marker %s in %s line %d", marker, path, number
                    )
                    continue
            try:
                parsed = HostKeyEntry.from_line(line, number)
            except (InvalidHostKey, ValueError) as exc:
                logger.warning("SSH: ignoring %s line %d: %s", path, number, exc)
                continue
            if parsed is None or parsed.key is None or not parsed.hostnames:
                continue
            entry = KnownHostsEntry(tuple(parsed.hostnames), parsed.key)
            (self.revoked if marker == "@revoked" else self.entries).append(entry)

    def apply_to(self, host_keys: paramiko.HostKeys) -> None:
        """Give Paramiko the plain and hashed names for its own exact lookup.

        That lookup refuses a changed key before any policy runs and makes the
        transport prefer a key type that is already recorded.
        """
        for entry in self.entries:
            for name in entry.hostnames:
                if not name.startswith("!") and "*" not in name and "?" not in name:
                    host_keys.add(name, entry.key.get_name(), entry.key)

    def pinned_key(self, hostname: str, key_type: str):
        """The recorded key of ``key_type`` for ``hostname``, wildcards included."""
        for entry in self.entries:
            if entry.key.get_name() == key_type and _entry_names(entry.hostnames, hostname):
                return entry.key
        return None

    def is_revoked(self, hostname: str, key) -> bool:
        return any(
            entry.key == key and _entry_names(entry.hostnames, hostname)
            for entry in self.revoked
        )


class _KnownHostsPolicy(paramiko.MissingHostKeyPolicy):
    """The checks ssh makes that Paramiko's exact lookup does not.

    Paramiko consults a policy only for a host its exact lookup did not find.
    Before treating the host as new, this checks the entries ssh would still
    match: a revoked key is refused, a wildcard or negated pattern that pins
    the host is honoured (and a differing key refused), and only then does the
    mode decide what happens to an unknown host.
    """

    def __init__(self, known: "KnownHosts | None" = None):
        self.known = known or KnownHosts()

    def missing_host_key(self, client, hostname, key):
        if self.known.is_revoked(hostname, key):
            raise paramiko.SSHException(
                f"Host key for {hostname} is revoked in known_hosts"
            )
        pinned = self.known.pinned_key(hostname, key.get_name())
        if pinned is not None:
            if pinned != key:
                raise paramiko.BadHostKeyException(hostname, key, pinned)
            return
        self.unknown_host(client, hostname, key)

    def unknown_host(self, client, hostname, key):
        raise NotImplementedError


class StrictHostKeyPolicy(_KnownHostsPolicy):
    """Refuse a host that no entry pins, like StrictHostKeyChecking=yes."""

    def unknown_host(self, client, hostname, key):
        raise paramiko.SSHException(f"Server {hostname!r} not found in known_hosts")


class AcceptNewHostKeyPolicy(_KnownHostsPolicy):
    """Trust a host key on first use and record it, like OpenSSH accept-new.

    New keys are appended to the known_hosts file in OpenSSH's own line
    format, so ``ssh`` and this tunnel share one record.
    """

    def __init__(self, known_hosts_file: str, known: "KnownHosts | None" = None):
        super().__init__(known)
        self.known_hosts_file = known_hosts_file

    def unknown_host(self, client, hostname, key):
        client.get_host_keys().add(hostname, key.get_name(), key)
        line = f"{hostname} {key.get_name()} {key.get_base64()}"
        path = os.path.abspath(self.known_hosts_file)
        with _KNOWN_HOSTS_WRITE_LOCK:
            if _has_line(path, line):
                return  # another tunnel recorded it meanwhile
            os.makedirs(os.path.dirname(path), mode=0o700, exist_ok=True)
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            with os.fdopen(fd, "a", encoding="utf-8") as handle:
                handle.write(line + "\n")
        logger.info(
            "SSH: recorded new %s host key for %s in %s", key.get_name(), hostname, path
        )


class SSHTunnel:
    """SSH tunnel manager for database connections using Paramiko directly"""

    DEFAULT_SSH_TIMEOUT = 5  # seconds

    def __init__(self, ssh_config, remote_host: str, remote_port: int):
        """
        Initialize SSH tunnel.

        Args:
            ssh_config: SSHTunnelConfig object (from config module)
            remote_host: Remote database host to tunnel to
            remote_port: Remote database port to tunnel to
        """
        self.ssh_config = ssh_config
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.ssh_timeout = ssh_config.ssh_timeout or self.DEFAULT_SSH_TIMEOUT
        self.ssh_client = None
        self.transport = None
        self.local_port: int | None = None
        self.tunnel_thread = None
        self._lock = threading.Lock()
        self._stop_event = threading.Event()

    async def start(self) -> int:
        """Start SSH tunnel and return local port"""
        loop = asyncio.get_event_loop()
        try:
            return await asyncio.wait_for(
                loop.run_in_executor(None, self._start_sync), timeout=self.ssh_timeout
            )
        except TimeoutError:
            # Clean up if timeout occurs
            self._stop_sync()
            raise TimeoutError(f"SSH: Connection timeout after {self.ssh_timeout}s")

    def _start_sync(self) -> int:
        """Synchronous tunnel start using Paramiko"""
        with self._lock:
            if self.ssh_client and self.transport and self.transport.is_active():
                assert self.local_port is not None
                return self.local_port

            tunnel_established = False
            try:
                ssh_host = self.ssh_config.host
                ssh_port = self.ssh_config.port
                ssh_user = self.ssh_config.user

                # Remote bind address (database server as seen from SSH host)
                remote_host = self.remote_host
                remote_port = self.remote_port

                # Create SSH client and decide how to treat the bastion's host
                # key, mirroring OpenSSH StrictHostKeyChecking. A recorded key
                # that no longer matches raises BadHostKeyException in
                # connect() regardless of the policy.
                self.ssh_client = paramiko.SSHClient()
                host_key_checking = self.ssh_config.host_key_checking
                known_hosts_file = self.ssh_config.known_hosts_file or os.path.expanduser(
                    DEFAULT_KNOWN_HOSTS_FILE
                )
                if host_key_checking == "no":
                    # Legacy mode: trust everything and record nothing.
                    policy: paramiko.MissingHostKeyPolicy = paramiko.AutoAddPolicy()
                else:
                    # The same files ssh consults: the global one, then the
                    # user's. Both are read-only here; new keys go to the
                    # user's file through the policy below.
                    known = KnownHosts()
                    for path in (GLOBAL_KNOWN_HOSTS_FILE, known_hosts_file):
                        known.load(path)
                    known.apply_to(self.ssh_client.get_host_keys())
                    if host_key_checking == "yes":
                        policy = StrictHostKeyPolicy(known)
                    else:
                        policy = AcceptNewHostKeyPolicy(known_hosts_file, known)
                self.ssh_client.set_missing_host_key_policy(policy)

                # Authentication
                connect_kwargs = {
                    "hostname": ssh_host,
                    "port": ssh_port,
                    "username": ssh_user,
                    "timeout": self.ssh_timeout,  # Use our SSH timeout for connection
                    "look_for_keys": True,  # Allow using keys from ~/.ssh/
                    "allow_agent": True,  # Allow using SSH agent
                }

                # Configure authentication
                if self.ssh_config.private_key:
                    key_file = self.ssh_config.private_key
                    # Auto-detect key type and load it
                    # Try different key types in order (most common first)
                    private_key = None
                    key_load_errors = []

                    # List of key types to try
                    key_types = [
                        paramiko.Ed25519Key,
                        paramiko.ECDSAKey,
                        paramiko.RSAKey,
                    ]
                    # Add DSSKey if it exists (removed in newer Paramiko versions)
                    if hasattr(paramiko, "DSSKey"):
                        key_types.append(paramiko.DSSKey)

                    for key_class in key_types:
                        try:
                            private_key = key_class.from_private_key_file(key_file)
                            logger.debug(f"Loaded SSH key as {key_class.__name__}")
                            break
                        except paramiko.SSHException as e:
                            key_load_errors.append(f"{key_class.__name__}: {e}")
                            continue
                        except Exception as e:
                            # Catch any other exception (e.g., file format issues)
                            key_load_errors.append(f"{key_class.__name__}: {e}")
                            continue

                    if private_key is None:
                        error_details = "; ".join(key_load_errors)
                        raise ValueError(
                            f"Could not load SSH private key from {key_file}. Tried: {error_details}"
                        )

                    connect_kwargs["pkey"] = private_key
                elif self.ssh_config.password:
                    connect_kwargs["password"] = self.ssh_config.password
                # Otherwise fall through: paramiko will use ssh-agent and
                # ~/.ssh/* keys via look_for_keys=True / allow_agent=True.

                # Connect to SSH server
                logger.info(f"Connecting to SSH server {ssh_host}:{ssh_port}")
                self.ssh_client.connect(**connect_kwargs)
                self.transport = self.ssh_client.get_transport()

                # Get a free local port
                sock = socket.socket()
                sock.bind(("", 0))
                self.local_port = sock.getsockname()[1]
                sock.close()

                # Start port forwarding in a thread
                self._stop_event.clear()
                self.tunnel_thread = threading.Thread(
                    target=self._forward_tunnel,
                    args=(self.local_port, remote_host, remote_port),
                )
                self.tunnel_thread.daemon = True
                self.tunnel_thread.start()

                # Give the tunnel a moment to establish
                import time

                time.sleep(0.5)

                logger.info(
                    f"SSH tunnel established: localhost:{self.local_port} -> {remote_host}:{remote_port}"
                )
                tunnel_established = True
                assert self.local_port is not None
                return self.local_port

            except ValueError as e:
                # Our own validation errors - pass through as-is
                logger.error(f"SSH configuration error: {e}")
                raise
            except paramiko.AuthenticationException as e:
                logger.error(f"SSH authentication failed: {e}")
                raise ConnectorError(f"SSH: Authentication failed - {e}")
            except paramiko.BadHostKeyException as e:
                logger.error(f"SSH host key verification failed: {e}")
                raise ConnectorError(f"SSH: Host key verification failed - {e}")
            except paramiko.SSHException as e:
                logger.error(f"SSH connection error: {e}")
                raise ConnectorError(f"SSH: {e}")
            except TimeoutError:
                logger.error(f"SSH connection timed out after {self.ssh_timeout}s")
                raise TimeoutError(f"SSH: Connection timeout after {self.ssh_timeout}s")
            except OSError as e:
                logger.error(f"Network error: {e}")
                raise ConnectorError(f"SSH: Network error - {e}")
            finally:
                # Clean up on any failure
                if (
                    not tunnel_established
                    and hasattr(self, "ssh_client")
                    and self.ssh_client
                ):
                    # Don't mask the original exception
                    with contextlib.suppress(Exception):
                        self._stop_sync()

    def _forward_tunnel(self, local_port: int, remote_host: str, remote_port: int):
        """Thread function to handle port forwarding"""

        class ForwardServer(threading.Thread):
            def __init__(
                self, local_port, remote_host, remote_port, transport, stop_event
            ):
                super().__init__()
                self.local_port = local_port
                self.remote_host = remote_host
                self.remote_port = remote_port
                self.transport = transport
                self.stop_event = stop_event
                self.socket = None
                self.daemon = True

            def run(self):
                try:
                    self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    self.socket.bind(("127.0.0.1", self.local_port))
                    self.socket.listen(5)
                    self.socket.settimeout(1.0)  # Allow periodic checks for stop event

                    while not self.stop_event.is_set():
                        try:
                            client, _addr = self.socket.accept()
                            thread = threading.Thread(
                                target=self.handle_client, args=(client,)
                            )
                            thread.daemon = True
                            thread.start()
                        except TimeoutError:
                            continue
                        except Exception as e:
                            if not self.stop_event.is_set():
                                logger.error(f"Error accepting connection: {e}")
                            break
                finally:
                    if self.socket:
                        self.socket.close()

            def handle_client(self, client_socket):
                channel = None
                try:
                    channel = self.transport.open_channel(
                        "direct-tcpip",
                        (self.remote_host, self.remote_port),
                        client_socket.getpeername(),
                    )

                    while True:
                        r, _w, _x = select.select([client_socket, channel], [], [])
                        if client_socket in r:
                            data = client_socket.recv(1024)
                            if not data:
                                break
                            channel.send(data)
                        if channel in r:
                            data = channel.recv(1024)
                            if not data:
                                break
                            client_socket.send(data)

                except Exception as e:
                    logger.debug(f"Forwarding error: {e}")
                finally:
                    try:
                        if channel:
                            channel.close()
                    except (OSError, AttributeError):
                        pass  # Channel might already be closed or None
                    try:
                        client_socket.close()
                    except (OSError, AttributeError):
                        pass  # Socket might already be closed or None

        server = ForwardServer(
            local_port, remote_host, remote_port, self.transport, self._stop_event
        )
        server.run()

    async def stop(self):
        """Stop SSH tunnel (async wrapper)"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._stop_sync)

    def _stop_sync(self):
        """Synchronous tunnel stop"""
        # Try to acquire lock with timeout to prevent deadlock
        lock_acquired = self._lock.acquire(timeout=1)
        try:
            # Always signal stop event, even if we couldn't get the lock
            self._stop_event.set()

            if lock_acquired:
                try:
                    # Wait for thread to finish (with timeout)
                    if self.tunnel_thread and self.tunnel_thread.is_alive():
                        self.tunnel_thread.join(timeout=2.0)

                    # Close SSH connection
                    if self.transport:
                        self.transport.close()
                    if self.ssh_client:
                        self.ssh_client.close()

                    logger.info(f"SSH tunnel closed (was on port {self.local_port})")
                except Exception as e:
                    logger.error(f"Error closing SSH tunnel: {e}")
                finally:
                    self.ssh_client = None
                    self.transport = None
                    self.local_port = None
                    self.tunnel_thread = None
        finally:
            if lock_acquired:
                self._lock.release()

    def stop_sync(self):
        """Synchronous stop method for context manager"""
        self._stop_sync()

    def __enter__(self):
        return self._start_sync()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop_sync()
        return False
