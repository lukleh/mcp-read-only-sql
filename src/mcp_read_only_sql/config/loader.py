"""Connection configuration loader."""

from pathlib import Path
from typing import Any, Dict, cast

import yaml

from .connection import Connection


def _build_connections_from_raw_configs(
    raw_configs: Any, source: str | Path
) -> Dict[str, Connection]:
    """Validate parsed YAML data and return Connection objects."""
    source_name = str(source)
    if not raw_configs:
        raise ValueError(f"Configuration file is empty: {source_name}")

    if not isinstance(raw_configs, list):
        raise ValueError("Configuration file must contain a list of connections")

    connections: Dict[str, Connection] = {}
    errors: list[str] = []

    for idx, config in enumerate(raw_configs):
        if not isinstance(config, dict):
            errors.append(f"Connection #{idx+1}: must be a dictionary")
            continue

        config_dict = cast(Dict[str, Any], config)

        try:
            conn = Connection(config_dict)

            # Check for duplicate names
            if conn.name in connections:
                errors.append(f"Duplicate connection name: '{conn.name}'")
            else:
                connections[conn.name] = conn

        except Exception as e:
            # Get connection name if available for better error messages
            name = config_dict.get("connection_name", f"#{idx+1}")
            errors.append(f"Connection '{name}': {e}")

    if errors:
        error_msg = "Configuration validation failed:\n  - " + "\n  - ".join(errors)
        raise ValueError(error_msg)

    return connections


def load_connections_from_text(
    yaml_text: str, source: str | Path = "<memory>"
) -> Dict[str, Connection]:
    """
    Load and validate connections from a YAML text snapshot.

    Args:
        yaml_text: Raw YAML document content
        source: Source label used in validation errors
    """
    raw_configs = yaml.safe_load(yaml_text)
    return _build_connections_from_raw_configs(raw_configs, source)


def load_connections(yaml_path: str | Path) -> Dict[str, Connection]:
    """
    Load and validate all connections from YAML configuration file.

    Args:
        yaml_path: Path to connections.yaml file

    Returns:
        Dictionary mapping connection name to Connection object

    Raises:
        FileNotFoundError: If YAML file doesn't exist
        ValueError: If configuration is invalid (includes all validation errors)
    """
    yaml_file = Path(yaml_path).expanduser()
    if not yaml_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {yaml_path}")

    with open(yaml_file, encoding="utf-8") as f:
        yaml_text = f.read()

    return load_connections_from_text(yaml_text, yaml_file)
