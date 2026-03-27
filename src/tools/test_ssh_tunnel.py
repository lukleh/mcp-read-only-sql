#!/usr/bin/env python3
"""Test SSH tunnel connectivity."""

import asyncio
import sys
from pathlib import Path
from typing import Optional

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.config import load_connections
from src.config.env_files import build_runtime_env
from src.runtime_paths import resolve_runtime_paths, RuntimePaths
from src.utils.ssh_tunnel import SSHTunnel
from src.utils.ssh_tunnel_cli import CLISSHTunnel


async def test_ssh_tunnels(
    runtime_paths: RuntimePaths,
    connection_name: Optional[str] = None,
) -> bool:
    """Test SSH tunnel connectivity for connections."""
    try:
        all_connections = load_connections(
            runtime_paths.connections_file,
            env=build_runtime_env(None),
        )

        if not all_connections:
            print("❌ No connections found in configuration")
            return False

        ssh_connections = {
            name: conn for name, conn in all_connections.items() if conn.ssh_tunnel
        }

        if not ssh_connections:
            print("❌ No connections with SSH tunnels found in configuration")
            return False

        if connection_name:
            if connection_name not in ssh_connections:
                print(f"❌ Connection not found or has no SSH tunnel: {connection_name}")
                print("\nConnections with SSH tunnels:")
                for name in ssh_connections:
                    print(f"  - {name}")
                return False
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

            if ssh_config.password:
                auth_method = "password"
            elif ssh_config.private_key:
                auth_method = f"key ({ssh_config.private_key})"
            else:
                auth_method = "unknown"
            print(f"  Auth: {auth_method}")

            servers_to_test = [(server.host, server.port) for server in servers]
            if not servers_to_test:
                servers_to_test = [("localhost", 5432)]

            print(
                f"  Remote servers: {', '.join([f'{host}:{port}' for host, port in servers_to_test])}"
            )
            print()

            for index, (remote_host, remote_port) in enumerate(
                servers_to_test, start=1
            ):
                if len(servers_to_test) > 1:
                    print(
                        f"  [{index}/{len(servers_to_test)}] Testing tunnel to: {remote_host}:{remote_port}"
                    )
                else:
                    print(f"  Testing tunnel to: {remote_host}:{remote_port}")

                tunnel = None
                try:
                    if impl == "python":
                        tunnel = SSHTunnel(ssh_config, remote_host, remote_port)
                    else:
                        tunnel = CLISSHTunnel(ssh_config, remote_host, remote_port)
                    local_port = await tunnel.start()

                    print("    ✅ SSH tunnel established successfully")
                    print(f"    Local port: {local_port}")
                    print(
                        f"    Tunnel: localhost:{local_port} -> {ssh_config.host} -> {remote_host}:{remote_port}"
                    )

                    await tunnel.stop()

                except FileNotFoundError as exc:
                    if "ssh" in str(exc).lower():
                        print(f"    ❌ SSH client not found: {exc}")
                    elif "key" in str(exc).lower() or "private_key" in str(exc).lower():
                        print(f"    ❌ SSH key file not found: {exc}")
                    else:
                        print(f"    ❌ File not found: {exc}")
                    all_success = False
                except PermissionError as exc:
                    print(f"    ❌ Permission denied: {exc}")
                    print("    Check SSH key permissions (should be 600)")
                    all_success = False
                except TimeoutError as exc:
                    error_msg = str(exc)
                    if "SSH:" in error_msg:
                        ssh_error = error_msg.split("SSH:", 1)[1].strip()
                        print(f"    ❌ SSH connection timeout: {ssh_error}")
                    else:
                        print(f"    ❌ Connection timeout: {exc}")
                    all_success = False
                except Exception as exc:
                    error_msg = str(exc)
                    if "authentication" in error_msg.lower():
                        print("    ❌ SSH authentication failed")
                        if ssh_config.private_key:
                            print(f"    Check SSH key: {ssh_config.private_key}")
                        else:
                            print(
                                "    Check the injected SSH password environment variable"
                            )
                    elif "refused" in error_msg.lower():
                        print("    ❌ Connection refused by SSH server")
                        print(
                            f"    Check if SSH service is running on {ssh_config.host}"
                        )
                    elif "host key" in error_msg.lower():
                        print("    ❌ Host key verification failed")
                        print(
                            f"    You may need to SSH to {ssh_config.host} manually first"
                        )
                    elif "unpack requires a buffer" in error_msg.lower():
                        print("    ❌ SSH connection failed - network unreachable")
                        print(
                            f"    Check if you're connected to VPN or can reach {ssh_config.host}"
                        )
                    elif (
                        "no route to host" in error_msg.lower()
                        or "network is unreachable" in error_msg.lower()
                    ):
                        print(f"    ❌ Network unreachable: {ssh_config.host}")
                        print("    Check VPN connection or network access")
                    else:
                        print(f"    ❌ SSH tunnel failed: {error_msg[:200]}")
                    all_success = False
                finally:
                    if tunnel:
                        try:
                            await tunnel.stop()
                        except Exception:
                            pass

                print()

            print()

        return all_success

    except FileNotFoundError:
        print(f"❌ Configuration file not found: {runtime_paths.connections_file}")
        return False
    except Exception as exc:
        print(f"❌ Error: {exc}")
        import traceback

        traceback.print_exc()
        return False


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Test SSH tunnel connectivity")
    parser.add_argument(
        "connection",
        nargs="?",
        help="Specific connection name to test (tests all SSH tunnels if not specified)",
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

    success = asyncio.run(test_ssh_tunnels(runtime_paths, args.connection))
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
