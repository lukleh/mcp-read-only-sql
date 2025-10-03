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
            servers_to_test = [f"{server.host}:{server.port}" for server in servers]
            if not servers_to_test:
                servers_to_test = ["default"]

            print(f"  Servers: {', '.join(servers_to_test)}")
            print()

            # Test each server
            for i, server_spec in enumerate(servers_to_test, 1):
                if len(servers_to_test) > 1:
                    print(f"  [{i}/{len(servers_to_test)}] Testing server: {server_spec}")
                else:
                    print(f"  Testing server: {server_spec}")

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
                    if db_type == "postgresql":
                        if server_spec != "default":
                            result = await connector.execute_query("SELECT version()", server=server_spec)
                        else:
                            result = await connector.execute_query("SELECT version()")
                    else:  # clickhouse
                        if server_spec != "default":
                            result = await connector.execute_query("SELECT version()", server=server_spec)
                        else:
                            result = await connector.execute_query("SELECT version()")

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
                    # Clean up error messages
                    if "password authentication failed" in error_msg.lower():
                        print(f"    ❌ Authentication failed - check username/password")
                    elif "could not connect" in error_msg.lower() or "connection refused" in error_msg.lower():
                        print(f"    ❌ Cannot connect to server - check host/port")
                    elif "database" in error_msg.lower() and "does not exist" in error_msg.lower():
                        print(f"    ❌ Database not found - check database name")
                    elif "read-only" in error_msg.lower():
                        # This is actually success - we connected but query was blocked
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