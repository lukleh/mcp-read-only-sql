#!/usr/bin/env python3
"""Test database connections."""

import asyncio
import sys
from typing import Optional

from ..config import load_connections
from ..connectors.clickhouse.cli import ClickHouseCLIConnector
from ..connectors.clickhouse.python import ClickHousePythonConnector
from ..connectors.postgresql.cli import PostgreSQLCLIConnector
from ..connectors.postgresql.python import PostgreSQLPythonConnector
from ..runtime_paths import RuntimePaths, resolve_runtime_paths


async def test_connection(
    runtime_paths: RuntimePaths,
    connection_name: Optional[str] = None,
) -> bool:
    """Test database connection(s)."""
    try:
        connections = load_connections(runtime_paths.connections_file)

        if not connections:
            print("❌ No connections found in configuration")
            return False

        if connection_name:
            if connection_name not in connections:
                print(f"❌ Connection not found: {connection_name}")
                print("Available connections:")
                for name in connections:
                    print(f"  - {name}")
                return False
            connections = {connection_name: connections[connection_name]}

        all_success = True
        local_hosts = {"localhost", "127.0.0.1", "::1"}

        for name, connection in connections.items():
            db_type = connection.db_type
            impl = connection.implementation
            servers = connection.servers

            print(f"Testing connection: {name}")
            print(f"  Type: {db_type}")
            print(f"  Implementation: {impl}")

            if connection.password:
                print("  Password: configured")
            else:
                print("  Password: empty / not set")

            if connection.ssh_tunnel:
                print(f"  SSH Tunnel: {connection.ssh_tunnel.host}")

            server_entries = []
            for server in servers:
                display_host = server.host
                if connection.ssh_tunnel and server.host in local_hosts:
                    ssh_host = connection.ssh_tunnel.host
                    if ssh_host:
                        display_host = ssh_host
                server_entries.append((display_host, server))

            seen_hosts = set()
            unique_entries = []
            for entry in server_entries:
                if entry[0] not in seen_hosts:
                    seen_hosts.add(entry[0])
                    unique_entries.append(entry)

            servers_to_test = unique_entries or [("default", None)]

            display_labels = []
            for display_host, server in servers_to_test:
                if server is None:
                    display_labels.append("default")
                    continue
                canonical = f"{server.host}:{server.port}"
                if display_host == server.host:
                    display_labels.append(canonical)
                else:
                    display_labels.append(f"{display_host} (via {canonical})")

            print(f"  Servers: {', '.join(display_labels)}")
            print()

            for index, (display_host, server) in enumerate(servers_to_test, start=1):
                label = display_labels[index - 1]
                if len(servers_to_test) > 1:
                    print(f"  [{index}/{len(servers_to_test)}] Testing server: {label}")
                else:
                    print(f"  Testing server: {label}")

                connector = None
                try:
                    if db_type == "postgresql":
                        connector = (
                            PostgreSQLPythonConnector(connection)
                            if impl == "python"
                            else PostgreSQLCLIConnector(connection)
                        )
                    elif db_type == "clickhouse":
                        connector = (
                            ClickHousePythonConnector(connection)
                            if impl == "python"
                            else ClickHouseCLIConnector(connection)
                        )
                    else:
                        print(f"    ❌ Unknown database type: {db_type}")
                        all_success = False
                        continue

                    query = "SELECT version()"
                    server_param = None if display_host == "default" else display_host
                    if server_param:
                        result = await connector.execute_query(
                            query, server=server_param
                        )
                    else:
                        result = await connector.execute_query(query)

                    lines = result.strip().split("\n")
                    if len(lines) > 1:
                        version_line = lines[1].strip()
                        print("    ✅ Connected successfully")
                        print(f"    Database version: {version_line}")
                    else:
                        print("    ✅ Connected successfully")

                except FileNotFoundError as exc:
                    print(f"    ❌ CLI tool not found: {exc}")
                    all_success = False
                except TimeoutError as exc:
                    print(f"    ❌ Connection timeout: {exc}")
                    all_success = False
                except Exception as exc:
                    error_msg = str(exc)
                    lowered = error_msg.lower()

                    if (
                        db_type == "clickhouse"
                        and impl == "cli"
                        and "unexpected packet" in lowered
                    ):
                        print(
                            "    ⚠️ Native protocol rejected; retrying with clickhouse-connect (HTTP)"
                        )
                        try:
                            fallback_connector = ClickHousePythonConnector(connection)
                            if server_param:
                                result = await fallback_connector.execute_query(
                                    query, server=server_param
                                )
                            else:
                                result = await fallback_connector.execute_query(query)

                            lines = result.strip().split("\n")
                            if len(lines) > 1:
                                version_line = lines[1].strip()
                                print(
                                    "    ✅ Connected successfully via HTTP implementation"
                                )
                                print(f"    Database version: {version_line}")
                            else:
                                print(
                                    "    ✅ Connected successfully via HTTP implementation"
                                )
                            continue
                        except Exception as fallback_exc:
                            print(
                                f"    ❌ Fallback via HTTP implementation failed: {str(fallback_exc)[:200]}"
                            )
                            all_success = False
                            print()
                            continue

                    if "password" in lowered and "failed" in lowered:
                        print("    ❌ Authentication failed - check username/password")
                    elif (
                        "could not connect" in lowered
                        or "connection refused" in lowered
                    ):
                        print("    ❌ Cannot connect to server - check host/port")
                    elif "database" in lowered and "does not exist" in lowered:
                        print("    ❌ Database not found - check database name")
                    elif "read-only" in lowered:
                        print(
                            "    ✅ Connected successfully (read-only enforcement working)"
                        )
                    else:
                        print(f"    ❌ Connection failed: {error_msg[:200]}")
                    all_success = False

                print()

            print()

        return all_success

    except FileNotFoundError:
        print(f"❌ Configuration file not found: {runtime_paths.connections_file}")
        return False
    except Exception as exc:
        print(f"❌ Error: {exc}")
        return False


def main() -> None:
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Test MCP SQL Server connections")
    parser.add_argument(
        "connection",
        nargs="?",
        help="Specific connection name to test (tests all if not specified)",
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

    success = asyncio.run(test_connection(runtime_paths, args.connection))
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
