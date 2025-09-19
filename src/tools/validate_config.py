#!/usr/bin/env python3
"""Validate connections.yaml configuration file"""

import os
import sys
import yaml
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.config.parser import ConfigParser


def check_env_var(var_name: str) -> tuple[bool, str]:
    """Check if environment variable exists in shell or .env"""
    # Check shell environment
    if os.getenv(var_name):
        return True, "shell"

    # Check .env file
    env_path = Path(".env")
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    if line.startswith(f"{var_name}="):
                        return True, ".env"

    return False, None


def check_passwords_in_yaml(config_path: str) -> List[str]:
    """Check if raw YAML contains password fields"""
    warnings = []

    try:
        with open(config_path) as f:
            raw_config = yaml.safe_load(f) or []

        for conn in raw_config:
            if not isinstance(conn, dict):
                continue

            conn_name = conn.get("connection_name", "Unknown")

            # Check for database password in YAML
            if conn.get("password"):
                warnings.append(f"  - {conn_name}: Contains 'password' field - will be IGNORED (use DB_PASSWORD_{conn_name.upper().replace('-', '_')} instead)")

            # Check for SSH password in YAML
            ssh_config = conn.get("ssh_tunnel", {})
            if isinstance(ssh_config, dict) and ssh_config.get("ssh_password"):
                warnings.append(f"  - {conn_name}: Contains 'ssh_password' field - will be IGNORED (use SSH_PASSWORD_{conn_name.upper().replace('-', '_')} instead)")

    except Exception:
        pass  # If we can't read raw YAML, skip this check

    return warnings


def validate_server_format(server: Any) -> List[str]:
    """Validate server format and return errors"""
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


def validate_config(config_path: str = "connections.yaml") -> bool:
    """Validate configuration file and report issues"""

    print(f"Validating {config_path}...")
    print("-" * 50)

    # Check for passwords in raw YAML (before parser overwrites them)
    password_warnings = check_passwords_in_yaml(config_path)
    if password_warnings:
        print("⚠️  WARNING: Passwords found in YAML file (these will be IGNORED):")
        for warning in password_warnings:
            print(warning)
        print("❌ Passwords MUST be set via environment variables, not in YAML!")
        print()

    # Load .env file if exists
    load_dotenv()

    try:
        parser = ConfigParser(config_path)
        connections = parser.load_config()

        if not connections:
            print("❌ No connections found in configuration")
            return False

        print(f"✅ Found {len(connections)} connection(s)")
        print()

        has_errors = False

        for i, conn in enumerate(connections, 1):
            name = conn.get("connection_name", f"Connection {i}")
            print(f"Connection: {name}")

            # Check required fields
            errors = []
            warnings = []

            # Required fields
            if not conn.get("connection_name"):
                errors.append("Missing connection_name")

            if not conn.get("type"):
                errors.append("Missing type (postgresql/clickhouse)")
            elif conn["type"] not in ["postgresql", "clickhouse"]:
                errors.append(f"Invalid type: {conn['type']} (must be postgresql or clickhouse)")

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

            if not conn.get("db"):
                warnings.append("No database specified (will use default)")

            if not conn.get("username"):
                errors.append("Missing username")

            # Check password
            if not conn.get("password"):
                env_var = f"DB_PASSWORD_{name.upper().replace('-', '_')}"
                exists, location = check_env_var(env_var)
                if exists:
                    warnings.append(f"Password will be read from {env_var} ({location})")
                else:
                    errors.append(f"Password not found - missing environment variable: {env_var}")

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
                        warnings.append(f"Server {idx+1}: Port {port} will be auto-converted to {9000 if port == 8123 else 9440} for CLI")
                    elif impl == "python" and port in [9000, 9440]:
                        warnings.append(f"Server {idx+1}: Port {port} will be auto-converted to {8123 if port == 9000 else 8443} for Python")

            # Validate timeout values if present
            if "query_timeout" in conn:
                timeout = conn["query_timeout"]
                if not isinstance(timeout, (int, float)):
                    errors.append(f"query_timeout must be a number, got: {type(timeout).__name__}")
                elif timeout <= 0:
                    errors.append(f"query_timeout must be positive, got: {timeout}")

            if "connection_timeout" in conn:
                timeout = conn["connection_timeout"]
                if not isinstance(timeout, (int, float)):
                    errors.append(f"connection_timeout must be a number, got: {type(timeout).__name__}")
                elif timeout <= 0:
                    errors.append(f"connection_timeout must be positive, got: {timeout}")

            if "max_result_bytes" in conn:
                max_bytes = conn["max_result_bytes"]
                if not isinstance(max_bytes, int):
                    errors.append(f"max_result_bytes must be an integer, got: {type(max_bytes).__name__}")
                elif max_bytes <= 0:
                    errors.append(f"max_result_bytes must be positive, got: {max_bytes}")

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
                        errors.append(f"SSH port must be a number, got: {type(port).__name__}")
                    elif port < 1 or port > 65535:
                        errors.append(f"SSH port must be between 1-65535, got: {port}")

                # Check SSH authentication
                if ssh.get("private_key"):
                    key_path = Path(os.path.expanduser(ssh["private_key"]))
                    if not key_path.exists():
                        errors.append(f"SSH private key not found: {ssh['private_key']}")
                    elif not key_path.is_file():
                        errors.append(f"SSH private key is not a file: {ssh['private_key']}")
                elif not ssh.get("ssh_password"):
                    ssh_env = f"SSH_PASSWORD_{name.upper().replace('-', '_')}"
                    exists, location = check_env_var(ssh_env)
                    if exists:
                        warnings.append(f"SSH password will be read from {ssh_env} ({location})")
                    else:
                        errors.append(f"SSH authentication missing - no private key and no password in {ssh_env}")

            # Report results
            if errors:
                has_errors = True
                for error in errors:
                    print(f"  ❌ {error}")
            else:
                print(f"  ✅ Valid configuration")

            for warning in warnings:
                print(f"  ⚠️  {warning}")

            print()

        if has_errors or password_warnings:
            if has_errors:
                print("❌ Configuration has errors")
            if password_warnings:
                print("❌ Remove passwords from YAML - use environment variables instead")
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


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="Validate MCP SQL Server configuration")
    parser.add_argument(
        "config",
        nargs="?",
        default="connections.yaml",
        help="Path to configuration file (default: connections.yaml)"
    )

    args = parser.parse_args()

    success = validate_config(args.config)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()