#!/usr/bin/env python3
"""Test SSH tunnel connectivity"""

import sys
import asyncio
from pathlib import Path
from typing import Optional

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.config.parser import ConfigParser
from src.utils.ssh_tunnel_cli import CLISSHTunnel
from src.utils.ssh_tunnel import SSHTunnel


async def test_ssh_tunnels(config_path: str, connection_name: Optional[str] = None) -> bool:
    """Test SSH tunnel connectivity for connections"""

    try:
        parser = ConfigParser(config_path)
        connections = parser.load_config()

        if not connections:
            print("❌ No connections found in configuration")
            return False

        # Filter to connections with SSH tunnels
        ssh_connections = [c for c in connections if c.get("ssh_tunnel")]

        if not ssh_connections:
            print("❌ No connections with SSH tunnels found in configuration")
            return False

        # Filter to specific connection if requested
        if connection_name:
            ssh_connections = [c for c in ssh_connections if c.get("connection_name") == connection_name]
            if not ssh_connections:
                print(f"❌ Connection not found or has no SSH tunnel: {connection_name}")
                print("\nConnections with SSH tunnels:")
                parser_again = ConfigParser(config_path)
                all_conns = parser_again.load_config()
                ssh_conns = [c for c in all_conns if c.get("ssh_tunnel")]
                for conn in ssh_conns:
                    print(f"  - {conn.get('connection_name')}")
                return False

        all_success = True

        print(f"Testing SSH tunnels for {len(ssh_connections)} connection(s)...\n")

        for conn_config in ssh_connections:
            name = conn_config.get("connection_name", "unknown")
            impl = conn_config.get("implementation", "cli")
            ssh_config = conn_config.get("ssh_tunnel")
            servers = conn_config.get("servers", [])

            print(f"Testing connection: {name}")
            print(f"  Implementation: {impl}")
            print(f"  SSH Host: {ssh_config.get('host', 'unknown')}")
            print(f"  SSH User: {ssh_config.get('user', 'unknown')}")
            print(f"  SSH Port: {ssh_config.get('port', 22)}")

            # Get authentication method
            if ssh_config.get('password'):
                auth_method = "password"
            elif ssh_config.get('private_key'):
                key_path = ssh_config.get('private_key')
                auth_method = f"key ({key_path})"
            else:
                auth_method = "unknown"
            print(f"  Auth: {auth_method}")

            # Test each remote server
            servers_to_test = []
            if servers:
                for server in servers:
                    if isinstance(server, dict):
                        servers_to_test.append((server['host'], server['port']))
                    elif ':' in server:
                        host, port = server.rsplit(':', 1)
                        servers_to_test.append((host, int(port)))
                    else:
                        # Default port will be used, but we need to know the type
                        servers_to_test.append((server, 5432))  # Default assumption
            else:
                servers_to_test.append(("localhost", 5432))

            print(f"  Remote servers: {', '.join([f'{h}:{p}' for h, p in servers_to_test])}")
            print()

            # Test tunnel to each remote server
            for i, (remote_host, remote_port) in enumerate(servers_to_test, 1):
                if len(servers_to_test) > 1:
                    print(f"  [{i}/{len(servers_to_test)}] Testing tunnel to: {remote_host}:{remote_port}")
                else:
                    print(f"  Testing tunnel to: {remote_host}:{remote_port}")

                # Prepare SSH config for this specific remote
                tunnel_config = ssh_config.copy()
                tunnel_config['remote_host'] = remote_host
                tunnel_config['remote_port'] = remote_port

                tunnel = None
                try:
                    # Use the appropriate SSH tunnel implementation
                    if impl == "python":
                        tunnel = SSHTunnel(tunnel_config)
                    else:
                        tunnel = CLISSHTunnel(tunnel_config)
                    local_port = await tunnel.start()

                    print(f"    ✅ SSH tunnel established successfully")
                    print(f"    Local port: {local_port}")
                    print(f"    Tunnel: localhost:{local_port} -> {ssh_config.get('host')} -> {remote_host}:{remote_port}")

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
                        print(f"    ❌ SSH authentication failed")
                        if "private_key" in tunnel_config:
                            print(f"    Check SSH key: {tunnel_config.get('private_key')}")
                        else:
                            print(f"    Check SSH password environment variable")
                    elif "refused" in error_msg.lower():
                        print(f"    ❌ Connection refused by SSH server")
                        print(f"    Check if SSH service is running on {ssh_config.get('host')}")
                    elif "host key" in error_msg.lower():
                        print(f"    ❌ Host key verification failed")
                        print(f"    You may need to SSH to {ssh_config.get('host')} manually first")
                    elif "unpack requires a buffer" in error_msg.lower():
                        print(f"    ❌ SSH connection failed - network unreachable")
                        print(f"    Check if you're connected to VPN or can reach {ssh_config.get('host')}")
                    elif "no route to host" in error_msg.lower() or "network is unreachable" in error_msg.lower():
                        print(f"    ❌ Network unreachable: {ssh_config.get('host')}")
                        print(f"    Check VPN connection or network access")
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
