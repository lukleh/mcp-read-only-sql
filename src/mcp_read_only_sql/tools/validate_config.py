"""Validate configuration with the same rules used by the MCP server."""

import sys
from pathlib import Path

from .. import __version__
from ..config import load_connections
from ..runtime_paths import resolve_runtime_paths


def validate_config(config_path: str | Path) -> bool:
    """Run runtime validation, then report environment-specific diagnostics."""
    config_path = Path(config_path).expanduser()
    print(f"Validating {config_path}...")
    print("-" * 50)
    try:
        connections = load_connections(config_path)
        print(f"✅ Found {len(connections)} connection(s)\n")
        has_errors = False
        for conn in connections.values():
            print(f"Connection: {conn.name}")
            errors: list[str] = []
            warnings: list[str] = []
            infos: list[str] = []
            if conn.default_database_inferred:
                warnings.append("No default database specified (will use first allowed)")
            if conn.password:
                infos.append("ℹ️  Database password is configured in connections.yaml")
            else:
                warnings.append("Password is empty / not set")
            if conn.db_type == "clickhouse":
                conversions = {8123: 9000, 8443: 9440} if conn.implementation == "cli" else {9000: 8123, 9440: 8443}
                for index, server in enumerate(conn.servers, 1):
                    if server.port in conversions:
                        warnings.append(
                            f"Server {index}: Port {server.port} will be auto-converted to "
                            f"{conversions[server.port]} for {conn.implementation}"
                        )
            ssh = conn.ssh_tunnel
            if ssh:
                if ssh.private_key:
                    key_path = Path(ssh.private_key).expanduser()
                    if not key_path.exists():
                        errors.append(f"SSH private key not found: {ssh.private_key}")
                    elif not key_path.is_file():
                        errors.append(f"SSH private key is not a file: {ssh.private_key}")
                    else:
                        infos.append("ℹ️  SSH tunnel uses private key authentication")
                elif ssh.password:
                    infos.append("ℹ️  SSH tunnel password is configured in connections.yaml")
                else:
                    infos.append("ℹ️  SSH tunnel will use ssh-agent identities (no inline credentials)")
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
        print("❌ Configuration has errors" if has_errors else "✅ Configuration is valid")
        return not has_errors
    except FileNotFoundError:
        print(f"❌ Configuration file not found: {config_path}")
        return False
    except Exception as exc:
        print(f"❌ Error parsing configuration: {exc}")
        return False


def main() -> None:
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(
        prog="mcp-read-only-sql validate-config",
        description="Validate MCP SQL Server configuration",
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
