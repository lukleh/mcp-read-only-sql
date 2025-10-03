"""
Base class for CLI connectors with system SSH support
"""

import asyncio
from typing import Any, Dict, Optional
from contextlib import asynccontextmanager
import logging

from .base import BaseConnector
from ..config import Connection
from ..utils.ssh_tunnel_cli import CLISSHTunnel

logger = logging.getLogger(__name__)


class BaseCLIConnector(BaseConnector):
    """Base class for CLI connectors with system SSH support"""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self._ssh_tunnel = None

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
            tunnel = CLISSHTunnel(self.ssh_config, selected_server.host, selected_server.port)
            local_port = await tunnel.start()
            try:
                yield local_port
            finally:
                await tunnel.stop()
        else:
            yield None
