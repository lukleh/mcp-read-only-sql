"""
Utilities for handling database connections
"""
from typing import Dict, Any


def get_connection_target(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Determine the final connection target based on SSH and server configuration.

    Returns a dict with:
    - host: The final host to connect to
    - port: The final port
    - database: The default database name
    - connection_type: One of 'direct', 'ssh_local', 'ssh_jump'
    """
    # Required field
    if "type" not in config:
        raise ValueError("Connection configuration missing required field 'type'")

    db_type = config["type"]
    database = config.get("default_database") or config.get("db")
    if not database:
        allowed = config.get("allowed_databases", config.get("databases"))
        if isinstance(allowed, list) and allowed:
            database = allowed[0]
        else:
            database = ""
    ssh_config = config.get("ssh_tunnel")
    servers = config.get("servers", [])

    # Get first server if available
    if servers:
        # Handle both dict and string server formats
        server = servers[0]
        if isinstance(server, dict):
            # If server is a dict, host is required
            if "host" not in server:
                raise ValueError("Server configuration missing required field 'host'")
            db_host = server["host"]
            db_port = server.get("port", 5432 if db_type == "postgresql" else 8123)
        elif isinstance(server, str):
            # Parse "host:port" string
            if ":" in server:
                db_host, port_str = server.rsplit(":", 1)
                db_port = int(port_str)
            else:
                db_host = server
                db_port = 5432 if db_type == "postgresql" else 8123
        else:
            db_host = "localhost"
            db_port = 5432 if db_type == "postgresql" else 8123
    else:
        db_host = "localhost"
        db_port = 5432 if db_type == "postgresql" else 8123

    # Determine connection type and final target
    if ssh_config:
        # If SSH tunnel is configured, host is required
        if "host" not in ssh_config:
            raise ValueError("SSH tunnel configuration missing required field 'host'")
        ssh_host = ssh_config["host"]

        if db_host in ["localhost", "127.0.0.1"]:
            # SSH tunnel where DB is on the SSH host itself
            return {
                "host": ssh_host,
                "port": db_port,
                "database": database,
                "connection_type": "ssh_local"
            }
        else:
            # SSH tunnel as jump server to reach remote DB
            return {
                "host": db_host,
                "port": db_port,
                "database": database,
                "connection_type": "ssh_jump",
                "ssh_host": ssh_host  # Include SSH host for jump connections
            }
    else:
        # Direct DB connection (no SSH)
        return {
            "host": db_host,
            "port": db_port,
            "database": database,
            "connection_type": "direct"
        }
