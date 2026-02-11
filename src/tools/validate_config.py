#!/usr/bin/env python3
"""Validate connections.yaml configuration file"""

import os
import sys
import yaml
from pathlib import Path
from typing import List, Dict, Any, Tuple, Set, Optional
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.config.parser import ConfigParser


def _scan_env_file_for_key(var_name: str) -> bool:
    """Return True if var_name has an entry in the .env file."""
    env_path = Path(".env")
    if not env_path.exists():
        return False

    with env_path.open() as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and line.startswith(f"{var_name}="):
                return True
    return False


def check_env_var(var_name: str, env_keys: Optional[Set[str]] = None) -> tuple[bool, Optional[str]]:
    """Check if environment variable exists in shell or .env."""
    if env_keys is not None and var_name in env_keys:
        return True, ".env"

    # Check shell environment
    if os.getenv(var_name):
        if env_keys is None and _scan_env_file_for_key(var_name):
            return True, ".env"
        return True, "shell"

    # Fallback to scanning .env when env_keys not provided
    if env_keys is None and _scan_env_file_for_key(var_name):
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


def _load_env_inventory(env_path: Path) -> Tuple[Set[str], List[Tuple[str, int, int]]]:
    """
    Return the set of keys defined in the .env file and any duplicate key definitions.
    """
    if not env_path.exists():
        return set(), []

    seen: Dict[str, int] = {}
    duplicates: List[Tuple[str, int, int]] = []
    keys: Set[str] = set()

    with env_path.open() as f:
        for lineno, raw in enumerate(f, start=1):
            if "=" not in raw:
                continue
            key = raw.split("=", 1)[0].strip()
            if not key or key.startswith("#"):
                continue
            if key in seen:
                duplicates.append((key, seen[key], lineno))
            else:
                seen[key] = lineno
                keys.add(key)

    return keys, duplicates


def validate_config(config_path: str = "connections.yaml", check_env: bool = False) -> bool:
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

    env_path = Path(".env")
    env_keys: Set[str] = set()
    env_duplicates: List[Tuple[str, int, int]] = []
    env_missing_file = False
    used_db_env: Set[str] = set()
    used_ssh_env: Set[str] = set()

    if check_env:
        if env_path.exists():
            env_keys, env_duplicates = _load_env_inventory(env_path)
        else:
            env_missing_file = True
        print("🔍 Environment variable verification enabled (--check-env)")
        print()
    else:
        print("ℹ️ Environment variable verification skipped (run with --check-env to enforce .env checks)")
        print()

    try:
        with open(config_path) as raw_file:
            raw_configs = yaml.safe_load(raw_file) or []
        raw_by_name = {
            item.get("connection_name"): item
            for item in raw_configs
            if isinstance(item, dict) and item.get("connection_name")
        }

        parser = ConfigParser(config_path)
        connections = parser.load_config()

        if not connections:
            print("❌ No connections found in configuration")
            return False

        print(f"✅ Found {len(connections)} connection(s)")
        print()

        has_errors = False

        if check_env and env_duplicates:
            print("⚠️  Duplicate entries found in .env:")
            for key, first, second in env_duplicates:
                print(f"  - {key} (lines {first} and {second})")
            print()

        for i, conn in enumerate(connections, 1):
            name = conn.get("connection_name", f"Connection {i}")
            print(f"Connection: {name}")

            # Check required fields
            errors = []
            warnings = []
            infos = []

            def record_var_status(var_name: str, kind: str) -> Tuple[bool, Optional[str]]:
                exists, location = check_env_var(
                    var_name,
                    env_keys if (check_env and not env_missing_file) else None,
                )
                if check_env and not env_missing_file:
                    in_env = var_name in env_keys
                    icon = "✅" if in_env else "❌"
                    if in_env:
                        infos.append(f"{icon} {kind} sourced from {var_name} (.env)")
                    else:
                        if location == "shell":
                            detail = "shell override; not in .env"
                        elif location:
                            detail = f"{location}; not in .env"
                        else:
                            detail = "missing in .env"
                        infos.append(f"{icon} {kind} sourced from {var_name} ({detail})")
                else:
                    if exists:
                        origin = location or "unknown source"
                        infos.append(f"ℹ️  {kind} sourced from {var_name} ({origin})")
                return exists, location

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

            if conn.get("db") and conn.get("default_database") and conn.get("db") != conn.get("default_database"):
                errors.append("db and default_database must match when both are provided")

            db_value = conn.get("default_database") or conn.get("db")
            if "allowed_databases" in conn and "databases" in conn:
                errors.append("Use only one of allowed_databases or databases")

            allowed_raw = conn.get("allowed_databases", conn.get("databases"))

            allowed_list = None
            if allowed_raw is not None:
                if not isinstance(allowed_raw, list) or not allowed_raw:
                    errors.append("allowed_databases must be a non-empty list of database names")
                else:
                    allowed_list = []
                    for item in allowed_raw:
                        if not isinstance(item, str) or not item.strip():
                            errors.append("allowed_databases entries must be non-empty strings")
                            break
                        if item not in allowed_list:
                            allowed_list.append(item)

            if not db_value and not allowed_list:
                errors.append("Missing db (or allowed_databases)")
            elif not db_value and allowed_list:
                warnings.append("No default database specified (will use first allowed)")
            elif db_value and allowed_list and db_value not in allowed_list:
                errors.append("default_database/db must be included in allowed_databases")

            if not conn.get("username"):
                errors.append("Missing username")

            # Check password
            raw_conn = raw_by_name.get(name, {})
            password_env_var = None
            if isinstance(raw_conn, dict):
                if raw_conn.get("password_env"):
                    password_env_var = raw_conn["password_env"]
                elif not raw_conn.get("password"):
                    password_env_var = f"DB_PASSWORD_{name.upper().replace('-', '_')}"
            else:
                password_env_var = f"DB_PASSWORD_{name.upper().replace('-', '_')}"

            if not conn.get("password"):
                env_var = password_env_var or f"DB_PASSWORD_{name.upper().replace('-', '_')}"
                password_env_var = env_var
                used_db_env.add(env_var)
                exists, location = record_var_status(env_var, "Password")
                if not exists:
                    errors.append(f"Password not found - missing environment variable: {env_var}")

                if check_env:
                    if env_missing_file:
                        errors.append("--check-env: .env file not found (required to verify password variables)")
                    elif env_var not in env_keys:
                        errors.append(f"--check-env: {env_var} is not defined in .env")
            elif password_env_var:
                used_db_env.add(password_env_var)
                exists, _ = record_var_status(password_env_var, "Password")
                if check_env:
                    if env_missing_file:
                        errors.append("--check-env: .env file not found (required to verify password variables)")
                    elif password_env_var not in env_keys:
                        errors.append(f"--check-env: {password_env_var} is not defined in .env")

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
                    ssh_env = None
                    raw_ssh = raw_conn.get("ssh_tunnel") if isinstance(raw_conn, dict) else None
                    if isinstance(raw_ssh, dict) and raw_ssh.get("password_env"):
                        ssh_env = raw_ssh["password_env"]
                    elif not ssh.get("private_key"):
                        ssh_env = f"SSH_PASSWORD_{name.upper().replace('-', '_')}"

                    if ssh_env:
                        used_ssh_env.add(ssh_env)
                        exists, _ = record_var_status(ssh_env, "SSH password")
                    else:
                        exists = False

                    if exists:
                        pass
                    else:
                        errors.append(f"SSH authentication missing - no private key and no password in {ssh_env}")

                    if check_env and ssh_env:
                        if env_missing_file:
                            errors.append("--check-env: .env file not found (required to verify SSH password variables)")
                        elif ssh_env not in env_keys:
                            errors.append(f"--check-env: {ssh_env} is not defined in .env")

            # Report results
            if errors:
                has_errors = True
                for error in errors:
                    print(f"  ❌ {error}")
            else:
                print(f"  ✅ Valid configuration")

            for warning in warnings:
                print(f"  ⚠️  {warning}")

            for info in infos:
                print(f"  {info}")

            print()

        if check_env and not env_missing_file:
            db_keys_in_env = {key for key in env_keys if key.startswith("DB_PASSWORD_")}
            unused_db_env = sorted(db_keys_in_env - used_db_env)
            print("DB password variables in .env without matching connection:")
            if unused_db_env:
                for key in unused_db_env:
                    print(f"  - {key}")
            else:
                print("  - (none)")

            ssh_keys_in_env = {key for key in env_keys if key.startswith("SSH_PASSWORD_")}
            unused_ssh_env = sorted(ssh_keys_in_env - used_ssh_env)
            if unused_ssh_env:
                print("SSH password variables in .env without matching connection:")
                for key in unused_ssh_env:
                    print(f"  - {key}")
            elif ssh_keys_in_env:
                print("SSH password variables in .env without matching connection:")
                print("  - (none)")
            print()

        if has_errors or password_warnings or (check_env and env_missing_file):
            if has_errors:
                print("❌ Configuration has errors")
            if password_warnings:
                print("❌ Remove passwords from YAML - use environment variables instead")
            if check_env and env_missing_file:
                print("❌ .env file not found - required when running with --check-env")
            return False
        else:
            if check_env and not env_missing_file:
                print("✅ .env password variables verified")
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
    parser.add_argument(
        "--check-env",
        action="store_true",
        help="Verify that required password environment variables are defined in .env"
    )

    args = parser.parse_args()

    success = validate_config(args.config, check_env=args.check_env)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
