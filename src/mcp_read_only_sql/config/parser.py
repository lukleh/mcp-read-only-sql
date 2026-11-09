import os
from pathlib import Path
from typing import Any

import yaml

from .connection import Server


class ConfigParser:
    def __init__(self, config_path: str | Path):
        self.config_path = Path(config_path).expanduser()

    def load_config(self) -> list[dict[str, Any]]:
        """Load and parse connection configuration from YAML file."""
        if not self.config_path.exists():
            return []

        with open(self.config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or []

        # Process each connection
        processed_config = []
        for conn in config:
            processed_conn = self._process_connection(conn)
            processed_config.append(processed_conn)

        return processed_config

    def _process_connection(self, conn: dict[str, Any]) -> dict[str, Any]:
        """Process a single connection configuration."""
        conn = dict(conn)

        # Note: Database defaults are handled by the Connection config:
        # - Use 'db' for a single allowed database
        # - Use 'allowed_databases' + 'default_database' for multi-DB access

        # Process SSH tunnel configuration
        if "ssh_tunnel" in conn and conn["ssh_tunnel"] is not None:
            ssh_config = dict(conn["ssh_tunnel"])
            conn["ssh_tunnel"] = ssh_config

            # Expand private key path
            if "private_key" in ssh_config:
                ssh_config["private_key"] = os.path.expanduser(
                    ssh_config["private_key"]
                )

        # Set default implementation if not specified
        if "implementation" not in conn:
            conn["implementation"] = "cli"

        # Process servers list
        if "servers" in conn:
            # Parse server strings to extract host and port
            processed_servers = []
            for server in conn["servers"]:
                parsed = Server.from_dict(server, conn.get("type", ""), conn["implementation"])
                processed_servers.append({"host": parsed.host, "port": parsed.port})
            conn["servers"] = processed_servers

        return conn
