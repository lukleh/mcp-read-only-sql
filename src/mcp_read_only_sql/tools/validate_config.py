#!/usr/bin/env python3
"""Validate SQL configuration stored in connections.yaml."""

import sys
from pathlib import Path
from typing import Any, List

import yaml

from .. import __version__
from ..config.parser import ConfigParser
from ..runtime_paths import resolve_runtime_paths


def find_legacy_credential_errors(raw_config: List[Any]) -> List[str]:
    """Return validation errors for unsupported env-era credential fields."""
    errors: List[str] = []

    for conn in raw_config:
        if not isinstance(conn, dict):
            continue

        conn_name = conn.get("connection_name", "Unknown")
        if "password_env" in conn:
            errors.append(
                f"  - {conn_name}: Field 'password_env' is no longer supported; use 'password'"
            )

        ssh_config = conn.get("ssh_tunnel", {})
        if not isinstance(ssh_config, dict):
            continue

        if "password_env" in ssh_config:
            errors.append(
                f"  - {conn_name}: Field 'ssh_tunnel.password_env' is no longer supported; use 'ssh_tunnel.password'"
            )
        if "ssh_password" in ssh_config:
            errors.append(
                f"  - {conn_name}: Field 'ssh_tunnel.ssh_password' is no longer supported; use 'ssh_tunnel.password'"
            )

    return errors


def validate_server_format(server: Any) -> List[str]:
    """Validate server format and return errors."""
    errors = []

    if isinstance(server, dict):
        # Dictionary format
        if not server.get("host"):
            errors.append("Server missing 'host'")

        port = server.get("port")
        if port is not None:
            if not isinstance(port, int):
                errors.append(f"Port must be a number, got: {type(port).__name__}")
            elif port < 1 or port > 65535:
                errors.append(f"Port must be between 1-65535, got: {port}")

    elif isinstance(server, str):
        # String format "host:port"
        if ":" in server:
            parts = server.rsplit(":", 1)
            if len(parts) == 2:
                host, port_str = parts
                if not host:
                    errors.append("Empty hostname in server string")
                try:
                    port = int(port_str)
                    if port < 1 or port > 65535:
                        errors.append(f"Port must be between 1-65535, got: {port}")
                except ValueError:
                    errors.append(f"Invalid port number: {port_str}")
            else:
                errors.append(f"Invalid server format: {server}")
        else:
            # Just hostname, port will be defaulted
            if not server:
                errors.append("Empty server string")
    else:
        errors.append(f"Server must be string or dict, got: {type(server).__name__}")

    return errors


def validate_config(
    config_path: str | Path,
) -> bool:
    """Validate configuration file and report issues."""
    config_path = str(Path(config_path).expanduser())

    print(f"Validating {config_path}...")
    print("-" * 50)

    try:
        with open(config_path, encoding="utf-8") as raw_file:
            raw_configs = yaml.safe_load(raw_file) or []

        if not isinstance(raw_configs, list):
            print("❌ Configuration file must contain a list of connections")
            return False

        legacy_errors = find_legacy_credential_errors(raw_configs)
        if legacy_errors:
            print("❌ Legacy credential fields found:")
            for error in legacy_errors:
                print(error)
            print()

        parser = ConfigParser(config_path)
        connections = parser.load_config()

        if not connections:
            print("❌ No connections found in configuration")
            return False

        print(f"✅ Found {len(connections)} connection(s)")
        print()

        has_errors = bool(legacy_errors)

        for i, conn in enumerate(connections, 1):
            name = conn.get("connection_name", f"Connection {i}")
            print(f"Connection: {name}")

            # Check required fields
            errors = []
            warnings = []
            infos = []

            # Required fields
            if not conn.get("connection_name"):
                errors.append("Missing connection_name")

            if not conn.get("type"):
                errors.append("Missing type (postgresql/clickhouse)")
            elif conn["type"] not in ["postgresql", "clickhouse"]:
                errors.append(
                    f"Invalid type: {conn['type']} (must be postgresql or clickhouse)"
                )

            if not conn.get("servers"):
                errors.append("Missing servers list")
            elif not isinstance(conn["servers"], list) or len(conn["servers"]) == 0:
                errors.append("Servers must be a non-empty list")
            else:
                # Validate each server format
                for i, server in enumerate(conn["servers"]):
                    server_errors = validate_server_format(server)
                    for err in server_errors:
                        errors.append(f"Server {i+1}: {err}")

            if (
                conn.get("db")
                and conn.get("default_database")
                and conn.get("db") != conn.get("default_database")
            ):
                errors.append(
                    "db and default_database must match when both are provided"
                )

            db_value = conn.get("default_database") or conn.get("db")
            if "allowed_databases" in conn and "databases" in conn:
                errors.append("Use only one of allowed_databases or databases")

            allowed_raw = conn.get("allowed_databases", conn.get("databases"))

            allowed_list = None
            if allowed_raw is not None:
                if not isinstance(allowed_raw, list) or not allowed_raw:
                    errors.append(
                        "allowed_databases must be a non-empty list of database names"
                    )
                else:
                    allowed_list = []
                    for item in allowed_raw:
                        if not isinstance(item, str) or not item.strip():
                            errors.append(
                                "allowed_databases entries must be non-empty strings"
                            )
                            break
                        if item not in allowed_list:
                            allowed_list.append(item)

            if not db_value and not allowed_list:
                errors.append("Missing db (or allowed_databases)")
            elif not db_value and allowed_list:
                warnings.append(
                    "No default database specified (will use first allowed)"
                )
            elif db_value and allowed_list and db_value not in allowed_list:
                errors.append(
                    "default_database/db must be included in allowed_databases"
                )

            if not conn.get("username"):
                errors.append("Missing username")

            if not conn.get("password"):
                warnings.append("Password is empty / not set")
            else:
                infos.append("ℹ️  Database password is configured in connections.yaml")

            # Check implementation
            impl = conn.get("implementation", "cli")
            if impl not in ["cli", "python"]:
                errors.append(f"Invalid implementation: {impl} (must be cli or python)")

            # Note about ClickHouse port auto-conversion
            if conn.get("type") == "clickhouse":
                servers = conn.get("servers", [])
                for idx, server in enumerate(servers):
                    port = server.get("port") if isinstance(server, dict) else None
                    if impl == "cli" and port in [8123, 8443]:
                        warnings.append(
                            f"Server {idx+1}: Port {port} will be auto-converted to {9000 if port == 8123 else 9440} for CLI"
                        )
                    elif impl == "python" and port in [9000, 9440]:
                        warnings.append(
                            f"Server {idx+1}: Port {port} will be auto-converted to {8123 if port == 9000 else 8443} for Python"
                        )

            # Validate timeout values if present
            if "query_timeout" in conn:
                timeout = conn["query_timeout"]
                if not isinstance(timeout, (int, float)):
                    errors.append(
                        f"query_timeout must be a number, got: {type(timeout).__name__}"
                    )
                elif timeout <= 0:
                    errors.append(f"query_timeout must be positive, got: {timeout}")

            if "connection_timeout" in conn:
                timeout = conn["connection_timeout"]
                if not isinstance(timeout, (int, float)):
                    errors.append(
                        f"connection_timeout must be a number, got: {type(timeout).__name__}"
                    )
                elif timeout <= 0:
                    errors.append(
                        f"connection_timeout must be positive, got: {timeout}"
                    )

            if "max_result_bytes" in conn:
                max_bytes = conn["max_result_bytes"]
                if not isinstance(max_bytes, int):
                    errors.append(
                        f"max_result_bytes must be an integer, got: {type(max_bytes).__name__}"
                    )
                elif max_bytes <= 0:
                    errors.append(
                        f"max_result_bytes must be positive, got: {max_bytes}"
                    )

            # Check SSH config if present
            if conn.get("ssh_tunnel"):
                ssh = conn["ssh_tunnel"]
                if not ssh.get("host"):
                    errors.append("SSH tunnel missing host")
                if not ssh.get("user"):
                    errors.append("SSH tunnel missing user")

                # Validate SSH port if present
                if "port" in ssh:
                    port = ssh["port"]
                    if not isinstance(port, int):
                        errors.append(
                            f"SSH port must be a number, got: {type(port).__name__}"
                        )
                    elif port < 1 or port > 65535:
                        errors.append(f"SSH port must be between 1-65535, got: {port}")

                # Check SSH authentication
                if ssh.get("private_key"):
                    key_path = Path(ssh["private_key"]).expanduser()
                    if not key_path.exists():
                        errors.append(
                            f"SSH private key not found: {ssh['private_key']}"
                        )
                    elif not key_path.is_file():
                        errors.append(
                            f"SSH private key is not a file: {ssh['private_key']}"
                        )
                    else:
                        infos.append("ℹ️  SSH tunnel uses private key authentication")
                elif ssh.get("password"):
                    infos.append(
                        "ℹ️  SSH tunnel password is configured in connections.yaml"
                    )
                else:
                    errors.append(
                        "SSH authentication missing - set ssh_tunnel.private_key or ssh_tunnel.password"
                    )

            # Report results
            if errors:
                has_errors = True
                for error in errors:
                    print(f"  ❌ {error}")
            else:
                print("  ✅ Valid configuration")

            for warning in warnings:
                print(f"  ⚠️  {warning}")

            for info in infos:
                print(f"  {info}")

            print()

        if has_errors:
            print("❌ Configuration has errors")
            return False
        else:
            print("✅ Configuration is valid")
            return True

    except FileNotFoundError:
        print(f"❌ Configuration file not found: {config_path}")
        return False
    except Exception as e:
        print(f"❌ Error parsing configuration: {e}")
        return False


def main() -> None:
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Validate MCP SQL Server configuration"
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    parser.add_argument(
        "--config-dir",
        help="Directory containing connections.yaml",
    )
    parser.add_argument(
        "--state-dir",
        help="Directory reserved for local state files",
    )
    parser.add_argument(
        "--cache-dir",
        help="Directory reserved for cache files",
    )
    parser.add_argument(
        "--print-paths",
        action="store_true",
        help="Print resolved config/state/cache paths and exit",
    )
    args = parser.parse_args()

    runtime_paths = resolve_runtime_paths(
        config_dir=args.config_dir,
        state_dir=args.state_dir,
        cache_dir=args.cache_dir,
    )

    if args.print_paths:
        print(runtime_paths.render())
        return

    success = validate_config(runtime_paths.connections_file)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
