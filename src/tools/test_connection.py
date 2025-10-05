#!/usr/bin/env python3
"""Test database connections"""

import sys
import asyncio
from pathlib import Path
from typing import Optional

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.config import load_connections
from src.connectors.postgresql.python import PostgreSQLPythonConnector
from src.connectors.postgresql.cli import PostgreSQLCLIConnector
from src.connectors.clickhouse.python import ClickHousePythonConnector
from src.connectors.clickhouse.cli import ClickHouseCLIConnector


async def test_connection(config_path: str, connection_name: Optional[str] = None) -> bool:
    """Test database connection(s)"""

    try:
        # Load validated connections
        connections = load_connections(config_path)

        if not connections:
            print("❌ No connections found in configuration")
            return False

        # Filter to specific connection if requested
        if connection_name:
            if connection_name not in connections:
                print(f"❌ Connection not found: {connection_name}")
                print("Available connections:")
                for name in connections.keys():
                    print(f"  - {name}")
                return False
            # Filter to just the requested connection
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

            # SSH tunnel info
            if connection.ssh_tunnel:
                print(f"  SSH Tunnel: {connection.ssh_tunnel.host}")

            # Get list of servers to test
            server_entries = []
            for server in servers:
                display_host = server.host
                if connection.ssh_tunnel and server.host in local_hosts:
                    ssh_host = connection.ssh_tunnel.host
                    if ssh_host:
                        display_host = ssh_host
                server_entries.append((display_host, server))

            # Deduplicate by display host so we don't attempt duplicate selections
            seen_hosts = set()
            unique_entries = []
            for entry in server_entries:
                if entry[0] not in seen_hosts:
                    seen_hosts.add(entry[0])
                    unique_entries.append(entry)

            if not unique_entries:
                servers_to_test = [("default", None)]
            else:
                servers_to_test = unique_entries

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

            # Test each server
            for i, (display_host, server) in enumerate(servers_to_test, 1):
                label = display_labels[i - 1]
                if len(servers_to_test) > 1:
                    print(f"  [{i}/{len(servers_to_test)}] Testing server: {label}")
                else:
                    print(f"  Testing server: {label}")

                # Select connector
                connector = None
                try:
                    if db_type == "postgresql":
                        if impl == "python":
                            connector = PostgreSQLPythonConnector(connection)
                        else:
                            connector = PostgreSQLCLIConnector(connection)
                    elif db_type == "clickhouse":
                        if impl == "python":
                            connector = ClickHousePythonConnector(connection)
                        else:
                            connector = ClickHouseCLIConnector(connection)
                    else:
                        print(f"    ❌ Unknown database type: {db_type}")
                        all_success = False
                        continue

                    # Test with a simple query, using server parameter if not default
                    query = "SELECT version()"
                    server_param = None if display_host == "default" else display_host

                    if server_param:
                        result = await connector.execute_query(query, server=server_param)
                    else:
                        result = await connector.execute_query(query)

                    # Parse result to show version
                    lines = result.strip().split('\n')
                    if len(lines) > 1:
                        version_line = lines[1].strip()  # Skip header
                        print(f"    ✅ Connected successfully")
                        print(f"    Database version: {version_line}")
                    else:
                        print(f"    ✅ Connected successfully")

                except FileNotFoundError as e:
                    print(f"    ❌ CLI tool not found: {e}")
                    all_success = False
                except TimeoutError as e:
                    print(f"    ❌ Connection timeout: {e}")
                    all_success = False
                except Exception as e:
                    error_msg = str(e)
                    lowered = error_msg.lower()

                    # Fallback for ClickHouse CLI hitting HTTP/HAProxy endpoints
                    if (
                        db_type == "clickhouse"
                        and impl == "cli"
                        and "unexpected packet" in lowered
                    ):
                        print("    ⚠️ Native protocol rejected; retrying with clickhouse-connect (HTTP)")
                        try:
                            fallback_connector = ClickHousePythonConnector(connection)
                            if server_param:
                                result = await fallback_connector.execute_query(query, server=server_param)
                            else:
                                result = await fallback_connector.execute_query(query)

                            lines = result.strip().split('\n')
                            if len(lines) > 1:
                                version_line = lines[1].strip()
                                print("    ✅ Connected successfully via HTTP implementation")
                                print(f"    Database version: {version_line}")
                            else:
                                print("    ✅ Connected successfully via HTTP implementation")
                            continue
                        except Exception as fallback_exc:
                            error_msg = str(fallback_exc)
                            lowered = error_msg.lower()
                            print(f"    ❌ Fallback via HTTP implementation failed: {error_msg[:200]}")
                            all_success = False
                            print()
                            continue

                    # Clean up error messages
                    if "password" in lowered and "failed" in lowered:
                        print(f"    ❌ Authentication failed - check username/password")
                    elif "could not connect" in lowered or "connection refused" in lowered:
                        print(f"    ❌ Cannot connect to server - check host/port")
                    elif "database" in lowered and "does not exist" in lowered:
                        print(f"    ❌ Database not found - check database name")
                    elif "read-only" in lowered:
                        print(f"    ✅ Connected successfully (read-only enforcement working)")
                    else:
                        print(f"    ❌ Connection failed: {error_msg[:200]}")
                    all_success = False

                print()

            print()

        return all_success

    except FileNotFoundError:
        print(f"❌ Configuration file not found: {config_path}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="Test MCP SQL Server connections")
    parser.add_argument(
        "connection",
        nargs="?",
        help="Specific connection name to test (tests all if not specified)"
    )
    parser.add_argument(
        "--config",
        default="connections.yaml",
        help="Path to configuration file (default: connections.yaml)"
    )

    args = parser.parse_args()

    success = asyncio.run(test_connection(args.config, args.connection))
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
