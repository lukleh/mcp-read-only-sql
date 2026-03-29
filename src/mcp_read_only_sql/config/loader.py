"""Connection configuration loader."""

from pathlib import Path
from typing import Dict

import yaml

from .connection import Connection


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
        raw_configs = yaml.safe_load(f)

    if not raw_configs:
        raise ValueError(f"Configuration file is empty: {yaml_path}")

    if not isinstance(raw_configs, list):
        raise ValueError("Configuration file must contain a list of connections")

    connections = {}
    errors = []

    for idx, config in enumerate(raw_configs):
        if not isinstance(config, dict):
            errors.append(f"Connection #{idx+1}: must be a dictionary")
            continue

        try:
            conn = Connection(config)

            # Check for duplicate names
            if conn.name in connections:
                errors.append(f"Duplicate connection name: '{conn.name}'")
            else:
                connections[conn.name] = conn

        except Exception as e:
            # Get connection name if available for better error messages
            name = config.get("connection_name", f"#{idx+1}")
            errors.append(f"Connection '{name}': {e}")

    if errors:
        error_msg = "Configuration validation failed:\n  - " + "\n  - ".join(errors)
        raise ValueError(error_msg)

    return connections
