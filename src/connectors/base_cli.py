"""
Base class for CLI connectors with system SSH support
"""

import asyncio
from typing import Any, Dict, Optional
from contextlib import asynccontextmanager
import logging

from .base import BaseConnector
from ..utils.ssh_tunnel_cli import CLISSHTunnel

logger = logging.getLogger(__name__)


class BaseCLIConnector(BaseConnector):
    """Base class for CLI connectors with system SSH support"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self._ssh_tunnel = None

    @asynccontextmanager
    async def _get_ssh_tunnel(self):
        """Context manager for SSH tunnel using system SSH"""
        if self.ssh_config and self.ssh_config.get("enabled", True):
            if "password" in self.ssh_config and "private_key" in self.ssh_config:
                logger.info(
                    "SSH tunnel configuration includes both key and password; defaulting to key-based authentication."
                )

            # Get the server to connect to
            server = self._select_server()

            # Add remote host/port to SSH config
            ssh_config = self.ssh_config.copy()
            ssh_config["remote_host"] = server["host"]
            ssh_config["remote_port"] = server["port"]

            tunnel = CLISSHTunnel(ssh_config)
            local_port = await tunnel.start()
            try:
                yield local_port
            finally:
                await tunnel.stop()
        else:
            yield None
