#!/usr/bin/env python3
"""Test SSH tunnel connectivity"""

import sys
import asyncio
from pathlib import Path
from typing import Optional

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.config import load_connections
from src.utils.ssh_tunnel_cli import CLISSHTunnel
from src.utils.ssh_tunnel import SSHTunnel


async def test_ssh_tunnels(config_path: str, connection_name: Optional[str] = None) -> bool:
    """Test SSH tunnel connectivity for connections"""

    try:
        # Load validated connections
        all_connections = load_connections(config_path)

        if not all_connections:
            print("❌ No connections found in configuration")
            return False

        # Filter to connections with SSH tunnels
        ssh_connections = {name: conn for name, conn in all_connections.items() if conn.ssh_tunnel}

        if not ssh_connections:
            print("❌ No connections with SSH tunnels found in configuration")
            return False

        # Filter to specific connection if requested
        if connection_name:
            if connection_name not in ssh_connections:
                print(f"❌ Connection not found or has no SSH tunnel: {connection_name}")
                print("\nConnections with SSH tunnels:")
                for name in ssh_connections.keys():
                    print(f"  - {name}")
                return False
            # Filter to just the requested connection
            ssh_connections = {connection_name: ssh_connections[connection_name]}

        all_success = True

        print(f"Testing SSH tunnels for {len(ssh_connections)} connection(s)...\n")

        for name, connection in ssh_connections.items():
            ssh_config = connection.ssh_tunnel
            impl = connection.implementation
            servers = connection.servers

            print(f"Testing connection: {name}")
            print(f"  Implementation: {impl}")
            print(f"  SSH Host: {ssh_config.host}")
            print(f"  SSH User: {ssh_config.user}")
            print(f"  SSH Port: {ssh_config.port}")

            # Get authentication method
            if ssh_config.password:
                auth_method = "password"
            elif ssh_config.private_key:
                auth_method = f"key ({ssh_config.private_key})"
            else:
                auth_method = "unknown"
            print(f"  Auth: {auth_method}")

            # Test each remote server
            servers_to_test = [(server.host, server.port) for server in servers]
            if not servers_to_test:
                servers_to_test = [("localhost", 5432)]

            print(f"  Remote servers: {', '.join([f'{h}:{p}' for h, p in servers_to_test])}")
            print()

            # Test tunnel to each remote server
            for i, (remote_host, remote_port) in enumerate(servers_to_test, 1):
                if len(servers_to_test) > 1:
                    print(f"  [{i}/{len(servers_to_test)}] Testing tunnel to: {remote_host}:{remote_port}")
                else:
                    print(f"  Testing tunnel to: {remote_host}:{remote_port}")

                tunnel = None
                try:
                    # Use the appropriate SSH tunnel implementation
                    if impl == "python":
                        tunnel = SSHTunnel(ssh_config, remote_host, remote_port)
                    else:
                        tunnel = CLISSHTunnel(ssh_config, remote_host, remote_port)
                    local_port = await tunnel.start()

                    print(f"    ✅ SSH tunnel established successfully")
                    print(f"    Local port: {local_port}")
                    print(f"    Tunnel: localhost:{local_port} -> {ssh_config.host} -> {remote_host}:{remote_port}")

                    # Clean up
                    await tunnel.stop()

                except FileNotFoundError as e:
                    if "ssh" in str(e).lower():
                        print(f"    ❌ SSH client not found: {e}")
                    elif "key" in str(e).lower() or "private_key" in str(e).lower():
                        print(f"    ❌ SSH key file not found: {e}")
                    else:
                        print(f"    ❌ File not found: {e}")
                    all_success = False
                except PermissionError as e:
                    print(f"    ❌ Permission denied: {e}")
                    print(f"    Check SSH key permissions (should be 600)")
                    all_success = False
                except TimeoutError as e:
                    error_msg = str(e)
                    if "SSH:" in error_msg:
                        # Extract just the SSH part of the error
                        ssh_error = error_msg.split("SSH:", 1)[1].strip()
                        print(f"    ❌ SSH connection timeout: {ssh_error}")
                    else:
                        print(f"    ❌ Connection timeout: {e}")
                    all_success = False
                except Exception as e:
                    error_msg = str(e)
                    # Clean up error messages
                    if "authentication" in error_msg.lower():
                        print("    ❌ SSH authentication failed")
                        if ssh_config.private_key:
                            print(f"    Check SSH key: {ssh_config.private_key}")
                        else:
                            print("    Check SSH password environment variable")
                    elif "refused" in error_msg.lower():
                        print("    ❌ Connection refused by SSH server")
                        print(f"    Check if SSH service is running on {ssh_config.host}")
                    elif "host key" in error_msg.lower():
                        print("    ❌ Host key verification failed")
                        print(f"    You may need to SSH to {ssh_config.host} manually first")
                    elif "unpack requires a buffer" in error_msg.lower():
                        print("    ❌ SSH connection failed - network unreachable")
                        print(f"    Check if you're connected to VPN or can reach {ssh_config.host}")
                    elif "no route to host" in error_msg.lower() or "network is unreachable" in error_msg.lower():
                        print(f"    ❌ Network unreachable: {ssh_config.host}")
                        print("    Check VPN connection or network access")
                    else:
                        print(f"    ❌ SSH tunnel failed: {error_msg[:200]}")
                    all_success = False
                finally:
                    if tunnel:
                        try:
                            await tunnel.stop()
                        except:
                            pass

                print()

            print()

        return all_success

    except FileNotFoundError:
        print(f"❌ Configuration file not found: {config_path}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="Test SSH tunnel connectivity")
    parser.add_argument(
        "connection",
        nargs="?",
        help="Specific connection name to test (tests all SSH tunnels if not specified)"
    )
    parser.add_argument(
        "--config",
        default="connections.yaml",
        help="Path to configuration file (default: connections.yaml)"
    )

    args = parser.parse_args()

    success = asyncio.run(test_ssh_tunnels(args.config, args.connection))
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
