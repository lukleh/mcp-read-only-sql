"""Connection configuration classes with validation."""

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# Default values
DEFAULT_IMPLEMENTATION = "cli"
DEFAULT_SSH_PORT = 22
DEFAULT_QUERY_TIMEOUT = 120
DEFAULT_CONNECTION_TIMEOUT = 10


def _normalize_positive_timeout(value: Any, field_name: str) -> float:
    """Validate numeric timeout values used at runtime."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"Field '{field_name}' must be a positive number")
    if value <= 0:
        raise ValueError(f"Field '{field_name}' must be a positive number")
    return value


def _normalize_database_list(value: Any, field_name: str) -> List[str]:
    """Normalize a database list field to a deduplicated list of names."""
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"'{field_name}' must be a non-empty list of database names")
    cleaned: List[str] = []
    for item in value:
        if not isinstance(item, str):
            raise ValueError(f"'{field_name}' entries must be strings")
        name = item.strip()
        if not name:
            raise ValueError(f"'{field_name}' entries must be non-empty strings")
        if name not in cleaned:
            cleaned.append(name)
    if not cleaned:
        raise ValueError(f"'{field_name}' must contain at least one database name")
    return cleaned


@dataclass
class Server:
    """Database server configuration"""

    host: str
    port: int

    @classmethod
    def from_dict(
        cls, data: Any, db_type: str = "", implementation: str = "cli"
    ) -> "Server":
        """
        Create Server from dict or string with validation.

        Args:
            data: Either a dict {"host": "...", "port": ...} or string "host:port" or "host"
            db_type: Database type for default port selection
            implementation: Implementation mode for default port selection
        """
        if isinstance(data, dict):
            if "host" not in data:
                raise ValueError("Server configuration missing required field 'host'")
            if "port" not in data:
                raise ValueError("Server configuration missing required field 'port'")
            return cls(host=data["host"], port=int(data["port"]))

        # Handle string format: "host:port" or "host"
        if isinstance(data, str):
            if ":" in data:
                host, port_str = data.rsplit(":", 1)
                return cls(host=host, port=int(port_str))
            else:
                # Host only - need to determine default port
                if db_type == "postgresql":
                    default_port = 5432
                elif db_type == "clickhouse":
                    # ClickHouse default depends on implementation
                    default_port = 9000 if implementation == "cli" else 8123
                else:
                    raise ValueError(
                        f"Cannot determine default port for server '{data}' without database type"
                    )
                return cls(host=data, port=default_port)

        raise ValueError(f"Invalid server format: {data}. Must be dict or string")


@dataclass
class SSHTunnelConfig:
    """SSH tunnel configuration with validation."""

    host: str
    port: int
    user: str
    private_key: Optional[str] = None
    password: Optional[str] = None
    ssh_timeout: Optional[int] = None

    def __post_init__(self):
        """Validate SSH tunnel configuration."""
        if not self.private_key and not self.password:
            raise ValueError(
                f"SSH tunnel to {self.host} requires either 'private_key' or 'password'"
            )
        if self.ssh_timeout is not None:
            if self.ssh_timeout <= 0:
                raise ValueError("SSH tunnel timeout must be a positive integer")

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Optional["SSHTunnelConfig"]:
        """Create SSHTunnelConfig from dict with validation."""
        if not data.get("enabled", True):
            return None

        # Required fields
        if "host" not in data:
            raise ValueError("SSH tunnel configuration missing required field 'host'")
        if "user" not in data:
            raise ValueError("SSH tunnel configuration missing required field 'user'")

        if "password_env" in data:
            raise ValueError(
                "Field 'password_env' is no longer supported; put the SSH password in 'password'"
            )

        password = data.get("password")

        # Expand private key path if present
        private_key = data.get("private_key")
        if private_key:
            private_key = os.path.expanduser(private_key)

        # Optional SSH timeout override
        ssh_timeout = data.get("ssh_timeout")
        if ssh_timeout is not None:
            try:
                ssh_timeout = int(ssh_timeout)
            except (TypeError, ValueError):
                raise ValueError("SSH tunnel timeout must be an integer value")

        return cls(
            host=data["host"],
            port=data.get("port", DEFAULT_SSH_PORT),
            user=data["user"],
            private_key=private_key,
            password=password,
            ssh_timeout=ssh_timeout,
        )


class Connection:
    """
    Validated database connection configuration.

    This class loads and validates all connection parameters,
    including validation of inline passwords.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize and validate connection configuration.

        Args:
            config: Raw configuration dict from YAML

        Raises:
            ValueError: If configuration is invalid or incomplete
        """
        # Required fields
        if "connection_name" not in config:
            raise ValueError(
                "Connection configuration missing required field 'connection_name'"
            )
        if "type" not in config:
            raise ValueError("Connection configuration missing required field 'type'")
        if "servers" not in config or not config["servers"]:
            raise ValueError(
                "Connection configuration missing required field 'servers' (must be non-empty list)"
            )
        if (
            "db" not in config
            and "default_database" not in config
            and "allowed_databases" not in config
            and "databases" not in config
        ):
            raise ValueError(
                "Connection configuration missing required field 'db' or 'allowed_databases'"
            )
        if "username" not in config:
            raise ValueError(
                "Connection configuration missing required field 'username'"
            )

        # Validate type
        db_type = config["type"]
        if db_type not in ("postgresql", "clickhouse"):
            raise ValueError(
                f"Invalid database type: '{db_type}'. Must be 'postgresql' or 'clickhouse'"
            )

        # Validate implementation
        implementation = config.get("implementation", DEFAULT_IMPLEMENTATION)
        if implementation not in ("python", "cli"):
            raise ValueError(
                f"Invalid implementation: '{implementation}'. Must be 'python' or 'cli'"
            )

        if "password_env" in config:
            raise ValueError(
                "Field 'password_env' is no longer supported; put the database password in 'password'"
            )
        if "max_result_bytes" in config:
            raise ValueError(
                "Field 'max_result_bytes' is no longer supported; successful query results are now written to managed files"
            )

        password = config.get("password", "")

        # Parse database allowlist/defaults
        if "allowed_databases" in config and "databases" in config:
            raise ValueError("Use only one of 'allowed_databases' or 'databases'")

        default_db = config.get("default_database")
        db_field = config.get("db")
        if db_field is not None and not isinstance(db_field, str):
            raise ValueError("Field 'db' must be a string database name")
        if default_db is not None and not isinstance(default_db, str):
            raise ValueError("Field 'default_database' must be a string database name")

        allowed_raw = config.get("allowed_databases", config.get("databases"))
        allowed_databases = (
            _normalize_database_list(allowed_raw, "allowed_databases")
            if allowed_raw is not None
            else []
        )

        if db_field and default_db and db_field.strip() != default_db.strip():
            raise ValueError(
                "'db' and 'default_database' must match when both are provided"
            )

        if default_db is None:
            if db_field:
                default_db = db_field
            elif allowed_databases:
                default_db = allowed_databases[0]
        default_db = (default_db or "").strip()

        if not default_db:
            raise ValueError(
                "Connection configuration missing required field 'db' or 'default_database'"
            )

        if not allowed_databases:
            allowed_databases = [default_db]
        elif default_db not in allowed_databases:
            raise ValueError(
                "'default_database' must be included in 'allowed_databases'"
            )

        # Parse servers
        servers = []
        for idx, server_data in enumerate(config["servers"]):
            try:
                servers.append(Server.from_dict(server_data, db_type, implementation))
            except ValueError as e:
                raise ValueError(f"Server #{idx+1}: {e}")

        # Parse SSH tunnel if present
        ssh_tunnel = None
        if "ssh_tunnel" in config and config["ssh_tunnel"] is not None:
            ssh_config_data = dict(config["ssh_tunnel"])

            try:
                ssh_tunnel = SSHTunnelConfig.from_dict(ssh_config_data)
            except ValueError as e:
                raise ValueError(f"SSH tunnel configuration error: {e}")

        # Store validated values
        self._name = config["connection_name"]
        self._db_type = db_type
        self._servers = servers
        self._database = default_db
        self._allowed_databases = allowed_databases
        self._username = config["username"]
        self._password = password
        self._implementation = implementation
        self._ssh_tunnel = ssh_tunnel
        self._query_timeout = _normalize_positive_timeout(
            config.get("query_timeout", DEFAULT_QUERY_TIMEOUT),
            "query_timeout",
        )
        self._connection_timeout = _normalize_positive_timeout(
            config.get("connection_timeout", DEFAULT_CONNECTION_TIMEOUT),
            "connection_timeout",
        )
        self._description = config.get("description", "")

    @property
    def name(self) -> str:
        """Connection name (unique identifier)"""
        return self._name

    @property
    def db_type(self) -> str:
        """Database type: 'postgresql' or 'clickhouse'"""
        return self._db_type

    @property
    def servers(self) -> List[Server]:
        """List of database servers"""
        return self._servers

    @property
    def database(self) -> str:
        """Default database name"""
        return self._database

    @property
    def allowed_databases(self) -> List[str]:
        """Allowed database names for this connection"""
        return list(self._allowed_databases)

    def resolve_database(self, database: Optional[str] = None) -> str:
        """Resolve and validate the database name against the allowlist."""
        if database is None:
            return self._database
        candidate = str(database).strip()
        if not candidate:
            return self._database
        if candidate not in self._allowed_databases:
            allowed = ", ".join(self._allowed_databases)
            raise ValueError(
                f"Database '{candidate}' is not allowed for connection '{self.name}'. "
                f"Allowed databases: {allowed}"
            )
        return candidate

    @property
    def username(self) -> str:
        """Database username"""
        return self._username

    @property
    def password(self) -> str:
        """Database password."""
        return self._password

    @property
    def implementation(self) -> str:
        """Implementation mode: 'python' or 'cli'"""
        return self._implementation

    @property
    def ssh_tunnel(self) -> Optional[SSHTunnelConfig]:
        """SSH tunnel configuration (None if not configured)"""
        return self._ssh_tunnel

    @property
    def query_timeout(self) -> float:
        """Query timeout in seconds"""
        return self._query_timeout

    @property
    def connection_timeout(self) -> float:
        """Connection timeout in seconds"""
        return self._connection_timeout

    @property
    def description(self) -> str:
        """Connection description (optional)"""
        return self._description

    def __repr__(self) -> str:
        return f"Connection(name={self.name!r}, type={self.db_type!r}, servers={len(self.servers)})"
