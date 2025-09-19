"""
SSH tunnel implementation using system SSH command for CLI connectors.
Supports key-based authentication and password authentication (via sshpass).
"""

import asyncio
import socket
import logging
import os
import signal
import shutil
from typing import Optional, Dict, Any
from contextlib import closing

logger = logging.getLogger(__name__)


class CLISSHTunnel:
    """SSH tunnel using system SSH command"""

    DEFAULT_SSH_TIMEOUT = 5  # seconds

    def __init__(self, config: Dict[str, Any]):
        self.ssh_host = config.get("host", "localhost")
        self.ssh_port = config.get("port", 22)
        self.ssh_user = config.get("user")
        self.remote_host = config.get("remote_host", "localhost")
        self.remote_port = config.get("remote_port", 5432)
        self.ssh_key = config.get("private_key")  # Optional SSH key file path
        self.ssh_password = config.get("password")  # Optional password (requires sshpass)
        self.ssh_timeout = config.get("ssh_timeout", self.DEFAULT_SSH_TIMEOUT)
        self.ssh_process = None
        self.local_port = None

    def _find_free_port(self) -> int:
        """Find a free local port"""
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(('', 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    async def start(self) -> int:
        """Start SSH tunnel and return local port"""
        if self.ssh_process:
            raise RuntimeError("SSH tunnel already running")

        try:
            return await asyncio.wait_for(
                self._start_tunnel(),
                timeout=self.ssh_timeout
            )
        except asyncio.TimeoutError:
            # Clean up if timeout occurs
            await self.stop()
            raise TimeoutError(f"SSH: Connection timeout after {self.ssh_timeout}s")

    async def _start_tunnel(self) -> int:
        """Actually start the SSH tunnel"""

        # Find a free local port
        self.local_port = self._find_free_port()

        # Build SSH options common to all auth modes
        ssh_options = [
            "-N",  # No command execution
            "-L", f"{self.local_port}:{self.remote_host}:{self.remote_port}",
            "-o", "StrictHostKeyChecking=no",  # Avoid interactive prompts
            "-o", "UserKnownHostsFile=/dev/null",  # Don't update known_hosts
            "-o", "LogLevel=ERROR",  # Reduce noise
            "-o", "ConnectTimeout=10",  # Connection timeout
            "-o", "ServerAliveInterval=60",  # Keep connection alive
            "-o", "ExitOnForwardFailure=yes",  # Exit if port forwarding fails
            "-p", str(self.ssh_port),
        ]

        env = os.environ.copy()
        ssh_base_cmd = ["ssh"]

        # Prefer key auth when both credentials are provided to avoid ambiguity
        if self.ssh_password and not self.ssh_key:
            if shutil.which("sshpass") is None:
                raise RuntimeError(
                    "SSH: sshpass utility not found. Install sshpass or use key-based SSH authentication."
                )
            env["SSHPASS"] = self.ssh_password
            ssh_base_cmd = ["sshpass", "-e", "ssh"]
            # Force password auth so OpenSSH doesn't prompt for keys
            ssh_options.extend([
                "-o", "PreferredAuthentications=password",
                "-o", "PubkeyAuthentication=no",
            ])
        elif self.ssh_key:
            ssh_options.extend(["-i", self.ssh_key])

        # Determine destination (user@host)
        destination = f"{self.ssh_user}@{self.ssh_host}" if self.ssh_user else self.ssh_host

        ssh_cmd = ssh_base_cmd + ssh_options + [destination]

        logger.debug("Starting SSH tunnel: %s", " ".join(ssh_cmd))

        try:
            # Start SSH process
            self.ssh_process = await asyncio.create_subprocess_exec(
                *ssh_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
                preexec_fn=os.setsid  # Create new process group for cleanup
            )

            # Give SSH time to establish the tunnel
            await asyncio.sleep(0.5)

            # Check if process is still running
            if self.ssh_process.returncode is not None:
                # Process exited, get error
                _, stderr = await self.ssh_process.communicate()
                error_msg = stderr.decode() if stderr else "SSH tunnel failed to start"
                raise RuntimeError(f"SSH: {error_msg}")

            # Verify tunnel is working by trying to connect
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection('127.0.0.1', self.local_port),
                    timeout=2.0
                )
                writer.close()
                await writer.wait_closed()
            except (asyncio.TimeoutError, ConnectionRefusedError):
                # Tunnel might still be establishing, give it more time
                await asyncio.sleep(1.0)

            logger.info(f"SSH tunnel established on local port {self.local_port}")
            return self.local_port

        except Exception as e:
            # Clean up on error
            await self.stop()
            logger.error(f"Failed to establish SSH tunnel: {e}")
            # Re-raise with SSH prefix if not already prefixed
            if not str(e).startswith("SSH:"):
                raise RuntimeError(f"SSH: {e}")
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
                except asyncio.TimeoutError:
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
