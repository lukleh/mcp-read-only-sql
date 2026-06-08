"""
Base class for CLI connectors with system SSH support
"""

from typing import Optional
from contextlib import asynccontextmanager
import logging

from .base import BaseConnector
from ..config import Connection
from ..cli_binaries import resolve_cli_binary
from ..utils.ssh_tunnel_cli import CLISSHTunnel

logger = logging.getLogger(__name__)


class BaseCLIConnector(BaseConnector):
    """Base class for CLI connectors with system SSH support"""

    DEFAULT_SSH_TIMEOUT = CLISSHTunnel.DEFAULT_SSH_TIMEOUT

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self._ssh_tunnel = None
        # Resolved CLI client paths, cached per connector instance. Connectors
        # are rebuilt on config reload, so this re-resolves after a reload while
        # avoiding a lookup (and a possible `brew --prefix` probe) per query.
        self._binary_cache: dict[str, str] = {}

    def _resolve_binary(self, name: str) -> str:
        """Resolve and cache the absolute path to a CLI client binary.

        Called synchronously from the async ``_run_query``. On the macOS
        fallback path resolution may run a blocking ``brew --prefix`` probe (see
        ``cli_binaries._brew_prefix``); caching the result here keeps that to at
        most once per connector instead of once per query. Failures are not
        cached, so resolution retries on the next query (e.g. if the client is
        installed mid-session).
        """
        cached = self._binary_cache.get(name)
        if cached is None:
            cached = resolve_cli_binary(name)
            self._binary_cache[name] = cached
        return cached

    @asynccontextmanager
    async def _get_ssh_tunnel(self, server: Optional[str] = None):
        """
        Context manager for SSH tunnel using system SSH

        Args:
            server: Optional server specification to tunnel to
        """
        if self.ssh_config:
            if self.ssh_config.password and self.ssh_config.private_key:
                logger.info(
                    "SSH tunnel configuration includes both key and password; defaulting to key-based authentication."
                )

            # Get the server to connect to
            selected_server = self._select_server(server)

            # Create tunnel with SSH config and remote server info
            tunnel = CLISSHTunnel(
                self.ssh_config, selected_server.host, selected_server.port
            )
            local_port = await tunnel.start()
            try:
                yield local_port
            finally:
                await tunnel.stop()
        else:
            yield None
