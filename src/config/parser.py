import os
from pathlib import Path
from typing import Any, Dict, List

import yaml


class ConfigParser:
    def __init__(self, config_path: str | Path):
        self.config_path = Path(config_path).expanduser()

    def load_config(self) -> List[Dict[str, Any]]:
        """Load and parse connection configuration from YAML file."""
        if not self.config_path.exists():
            return []

        with open(self.config_path, "r") as f:
            config = yaml.safe_load(f) or []

        # Process each connection
        processed_config = []
        for conn in config:
            processed_conn = self._process_connection(conn)
            processed_config.append(processed_conn)

        return processed_config

    def _process_connection(self, conn: Dict[str, Any]) -> Dict[str, Any]:
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
                if isinstance(server, dict):
                    # Already in dict format (from test configs)
                    processed_servers.append(server)
                elif ":" in server:
                    host, port = server.rsplit(":", 1)
                    processed_servers.append({"host": host, "port": int(port)})
                else:
                    # Default ports based on database type and implementation
                    db_type = conn.get("type", "").lower()
                    implementation = conn.get("implementation", "cli")

                    if db_type == "postgresql":
                        default_port = 5432
                    elif db_type == "clickhouse":
                        # ClickHouse default port depends on implementation
                        if implementation == "cli":
                            # CLI only supports native protocol
                            default_port = 9000
                        else:
                            # Python implementation supports HTTP by default
                            default_port = 8123
                    else:
                        default_port = 0
                    processed_servers.append({"host": server, "port": default_port})
            conn["servers"] = processed_servers

        return conn

    def save_config(self, config: List[Dict[str, Any]]):
        """Save configuration to YAML file."""
        clean_config = []
        for conn in config:
            clean_conn = {}
            for key, value in conn.items():
                if value is None:
                    continue
                if key == "ssh_tunnel" and value:
                    ssh_clean = {k: v for k, v in value.items() if v is not None}
                    if ssh_clean:
                        clean_conn[key] = ssh_clean
                else:
                    clean_conn[key] = value
            clean_config.append(clean_conn)

        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_path, "w") as f:
            yaml.dump(clean_config, f, default_flow_style=False, sort_keys=False)
        os.chmod(self.config_path, 0o600)
