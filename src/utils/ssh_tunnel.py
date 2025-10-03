import asyncio
import logging
import socket
import select
import threading
from typing import Any, Dict, Optional
import paramiko

logger = logging.getLogger(__name__)


class SSHTunnel:
    """SSH tunnel manager for database connections using Paramiko directly"""

    DEFAULT_SSH_TIMEOUT = 5  # seconds

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.ssh_timeout = config.get("ssh_timeout", self.DEFAULT_SSH_TIMEOUT)
        self.ssh_client = None
        self.transport = None
        self.local_port = None
        self.tunnel_thread = None
        self._lock = threading.Lock()
        self._stop_event = threading.Event()

    async def start(self) -> int:
        """Start SSH tunnel and return local port"""
        loop = asyncio.get_event_loop()
        try:
            return await asyncio.wait_for(
                loop.run_in_executor(None, self._start_sync),
                timeout=self.ssh_timeout
            )
        except asyncio.TimeoutError:
            # Clean up if timeout occurs
            self._stop_sync()
            raise TimeoutError(f"SSH: Connection timeout after {self.ssh_timeout}s")

    def _start_sync(self) -> int:
        """Synchronous tunnel start using Paramiko"""
        with self._lock:
            if self.ssh_client and self.transport and self.transport.is_active():
                return self.local_port

            tunnel_established = False
            try:
                ssh_host = self.config.get("host", "")
                ssh_port = self.config.get("port", 22)
                ssh_user = self.config.get("user", "")

                # Remote bind address (database server as seen from SSH host)
                remote_host = self.config.get("remote_host", "localhost")
                remote_port = self.config.get("remote_port", 5432)

                # Create SSH client
                self.ssh_client = paramiko.SSHClient()
                self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

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
                if "private_key" in self.config:
                    key_file = self.config["private_key"]
                    # Auto-detect key type and load it
                    # Try different key types in order (most common first)
                    private_key = None
                    key_load_errors = []

                    # List of key types to try
                    key_types = [paramiko.Ed25519Key, paramiko.ECDSAKey, paramiko.RSAKey]
                    # Add DSSKey if it exists (removed in newer Paramiko versions)
                    if hasattr(paramiko, 'DSSKey'):
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
                        raise ValueError(f"Could not load SSH private key from {key_file}. Tried: {error_details}")

                    connect_kwargs["pkey"] = private_key
                elif "password" in self.config:
                    connect_kwargs["password"] = self.config["password"]
                else:
                    raise ValueError("SSH tunnel requires either private_key or password")

                # Connect to SSH server
                logger.info(f"Connecting to SSH server {ssh_host}:{ssh_port}")
                self.ssh_client.connect(**connect_kwargs)
                self.transport = self.ssh_client.get_transport()

                # Get a free local port
                sock = socket.socket()
                sock.bind(('', 0))
                self.local_port = sock.getsockname()[1]
                sock.close()

                # Start port forwarding in a thread
                self._stop_event.clear()
                self.tunnel_thread = threading.Thread(
                    target=self._forward_tunnel,
                    args=(self.local_port, remote_host, remote_port)
                )
                self.tunnel_thread.daemon = True
                self.tunnel_thread.start()

                # Give the tunnel a moment to establish
                import time
                time.sleep(0.5)

                logger.info(f"SSH tunnel established: localhost:{self.local_port} -> {remote_host}:{remote_port}")
                tunnel_established = True
                return self.local_port

            except ValueError as e:
                # Our own validation errors - pass through as-is
                logger.error(f"SSH configuration error: {e}")
                raise
            except paramiko.AuthenticationException as e:
                logger.error(f"SSH authentication failed: {e}")
                raise RuntimeError(f"SSH: Authentication failed - {e}")
            except paramiko.BadHostKeyException as e:
                logger.error(f"SSH host key verification failed: {e}")
                raise RuntimeError(f"SSH: Host key verification failed - {e}")
            except paramiko.SSHException as e:
                logger.error(f"SSH connection error: {e}")
                raise RuntimeError(f"SSH: {e}")
            except socket.timeout as e:
                logger.error(f"SSH connection timed out after {self.ssh_timeout}s")
                raise TimeoutError(f"SSH: Connection timeout after {self.ssh_timeout}s")
            except socket.error as e:
                logger.error(f"Network error: {e}")
                raise RuntimeError(f"SSH: Network error - {e}")
            except (IOError, OSError) as e:
                logger.error(f"System error: {e}")
                raise RuntimeError(f"SSH: {e}")
            except Exception as e:
                # Catch any other unexpected exceptions
                logger.error(f"Unexpected error establishing SSH tunnel: {e}")
                # Re-raise with SSH prefix for context
                raise RuntimeError(f"SSH: Unexpected error - {e}")
            finally:
                # Clean up on any failure
                if not tunnel_established and hasattr(self, 'ssh_client') and self.ssh_client:
                    try:
                        self._stop_sync()
                    except Exception:
                        # Don't mask the original exception
                        pass

    def _forward_tunnel(self, local_port: int, remote_host: str, remote_port: int):
        """Thread function to handle port forwarding"""
        class ForwardServer(threading.Thread):
            def __init__(self, local_port, remote_host, remote_port, transport, stop_event):
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
                    self.socket.bind(('127.0.0.1', self.local_port))
                    self.socket.listen(5)
                    self.socket.settimeout(1.0)  # Allow periodic checks for stop event

                    while not self.stop_event.is_set():
                        try:
                            client, addr = self.socket.accept()
                            thread = threading.Thread(
                                target=self.handle_client,
                                args=(client,)
                            )
                            thread.daemon = True
                            thread.start()
                        except socket.timeout:
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
                        client_socket.getpeername()
                    )

                    while True:
                        r, w, x = select.select([client_socket, channel], [], [])
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

        server = ForwardServer(local_port, remote_host, remote_port, self.transport, self._stop_event)
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
