import json
import os
import re
import getpass
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional
import logging
import sys

# Add parent directory to path to import from utils
sys.path.append(str(Path(__file__).parent.parent))
from utils.connection_utils import get_connection_target
from runtime_paths import resolve_runtime_paths

logger = logging.getLogger(__name__)


def _write_text_file_secure(path: Path, content: str) -> None:
    """Write a text file and restrict it to user-only permissions."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as handle:
        handle.write(content)
    os.chmod(path, 0o600)


class DBeaverImporter:
    def __init__(self, dbeaver_path: str):
        self.dbeaver_path = Path(dbeaver_path)
        self.data_sources_path = self.dbeaver_path / "data-sources.json"
        self.credentials_path = self.dbeaver_path / "credentials-config.json"
        self.last_imported_names: List[str] = []
        self.last_requested_names: List[str] = []
        self.last_seen_names: List[str] = []

    def _decrypt_credentials(self) -> Dict[str, Any]:
        """Decrypt DBeaver credentials file using OpenSSL with default AES key"""
        if not self.credentials_path.exists():
            return {}

        # DBeaver's default AES key and IV
        key = "babb4a9f774ab853c96c2d653dfe544a"
        iv = "00000000000000000000000000000000"

        # Use OpenSSL to decrypt
        cmd = [
            "openssl",
            "aes-128-cbc",
            "-d",
            "-K",
            key,
            "-iv",
            iv,
            "-in",
            str(self.credentials_path),
        ]

        try:
            result = subprocess.run(cmd, capture_output=True)

            if result.returncode != 0:
                logger.warning(
                    f"Could not decrypt credentials: {result.stderr.decode()}"
                )
                return {}

            # Skip the first 16 bytes (padding) and parse JSON
            decrypted = result.stdout[16:]
            credentials_data = json.loads(decrypted)

            # Extract both connection and SSH credentials
            credentials = {}
            ssh_credentials = {}
            for conn_id, conn_data in credentials_data.items():
                if isinstance(conn_data, dict):
                    if "#connection" in conn_data:
                        credentials[conn_id] = conn_data["#connection"]
                    if "network/ssh_tunnel" in conn_data:
                        ssh_credentials[conn_id] = conn_data["network/ssh_tunnel"]

            logger.info(
                f"Successfully decrypted credentials for {len(credentials)} connections"
            )
            return credentials, ssh_credentials

        except (subprocess.SubprocessError, json.JSONDecodeError) as e:
            logger.warning(f"Could not decrypt credentials file: {e}")
            return {}

    def import_connections(
        self, merge_clusters: bool = True, only_names: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Import connections from DBeaver configuration"""
        if not self.data_sources_path.exists():
            raise FileNotFoundError(
                f"DBeaver data sources file not found: {self.data_sources_path}"
            )

        with open(self.data_sources_path, "r") as f:
            data_sources = json.load(f)

        # Try to decrypt credentials
        decrypt_result = self._decrypt_credentials()
        if isinstance(decrypt_result, tuple):
            credentials, ssh_credentials = decrypt_result
        else:
            credentials = decrypt_result or {}
            ssh_credentials = {}

        if not credentials and self.credentials_path.exists():
            # Fallback: try to read as plaintext JSON (some DBeaver versions don't encrypt)
            try:
                with open(self.credentials_path, "r") as f:
                    cred_data = json.load(f)
                    for conn_id, conn_creds in cred_data.items():
                        if isinstance(conn_creds, dict) and "#connection" in conn_creds:
                            credentials[conn_id] = conn_creds["#connection"]
                    logger.info("Credentials file was not encrypted")
            except (json.JSONDecodeError, UnicodeDecodeError):
                logger.warning(
                    "Could not read credentials file. Usernames will need to be set manually."
                )

        requested = [name for name in (only_names or []) if name]
        only_set = set(requested) if requested else None
        if only_set:
            print(f"Filtering: only {len(only_set)} requested connection(s)")
        self.last_requested_names = requested

        connections = []
        imported_names: List[str] = []
        seen_names: List[str] = []
        for conn_id, conn_data in data_sources.get("connections", {}).items():
            original_name = conn_data.get("name", conn_id)
            if only_set and original_name not in only_set:
                continue
            if only_set:
                seen_names.append(original_name)
            # Pass both DB and SSH credentials
            db_creds = credentials.get(conn_id)
            ssh_creds = (
                ssh_credentials.get(conn_id) if "ssh_credentials" in locals() else None
            )
            converted = self._convert_connection(
                conn_id, conn_data, db_creds, ssh_creds
            )
            if converted:
                connections.append(converted)
                imported_names.append(original_name)

        if merge_clusters:
            connections = self._merge_cluster_connections(connections)

        self.last_imported_names = imported_names
        self.last_seen_names = seen_names
        return connections

    def _convert_connection(
        self,
        conn_id: str,
        conn_data: Dict[str, Any],
        creds: Dict[str, Any] = None,
        ssh_creds: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """Convert a DBeaver connection to our format"""
        provider = conn_data.get("provider", "")
        config = conn_data.get("configuration", {})
        conn_name = conn_data.get("name", conn_id)

        print(f"  Processing: {conn_name}...")

        # Skip non-PostgreSQL and non-ClickHouse connections
        if provider not in ["postgresql", "clickhouse"]:
            print(f"    Skipped: Unsupported provider '{provider}'")
            return None

        # Create connection configuration
        connection = {
            "connection_name": self._sanitize_name(conn_data.get("name", conn_id)),
            "type": provider,
        }

        # Extract host and port
        host = config.get("host", "localhost")
        port = config.get("port", "5432" if provider == "postgresql" else "8123")

        # Handle servers
        connection["servers"] = [f"{host}:{port}"]

        # Extract database - use actual database name, not asterisk
        database = config.get("database", "")
        if database:
            connection["db"] = database
        else:
            # Use provider-specific default database name
            default_db = "postgres" if provider == "postgresql" else "default"
            connection["db"] = default_db
            print(f"    Note: No database specified, using default '{default_db}'")

        # Extract username and password from credentials (only add if present)
        if creds:
            username = creds.get("user") or creds.get("username") or ""
            if username:
                connection["username"] = username
                print(f"    Username: {username}")
            else:
                print("    Warning: No username found in credentials")

            password = creds.get("password")
            if password:
                connection["password"] = password
                print("    Password: ******** (stored in connections.yaml)")
        else:
            print("    Warning: No credentials available for this connection")

        # Check for SSH tunnel configuration
        handlers = config.get("handlers", {})  # handlers is inside configuration
        ssh_handler = handlers.get("ssh_tunnel", {})
        ssh_props = ssh_handler.get("properties", {}) if ssh_handler else {}

        if ssh_handler and ssh_handler.get("enabled"):
            # Get SSH username from credentials or properties
            ssh_user = ""
            if ssh_creds and "user" in ssh_creds:
                ssh_user = ssh_creds["user"]
                print(f"    SSH User: {ssh_user} (from credentials)")
            else:
                ssh_user = ssh_props.get("userName", "")
                if not ssh_user:
                    ssh_user = getpass.getuser()  # Current OS user
                    print(f"    SSH: Using current OS user '{ssh_user}'")

            connection["ssh_tunnel"] = {
                "host": ssh_props.get("host", ""),
                "user": ssh_user,
            }

            # Store SSH password if available
            if ssh_creds and "password" in ssh_creds:
                connection["ssh_tunnel"]["password"] = ssh_creds["password"]
                print("    SSH Password: ******** (stored in connections.yaml)")

            # Add SSH port if non-standard
            ssh_port = ssh_props.get("port", 22)
            if ssh_port and str(ssh_port) != "22":
                connection["ssh_tunnel"]["port"] = int(float(ssh_port))

            # Add private key if specified
            key_path = ssh_props.get("keyPath", "")  # DBeaver uses 'keyPath'
            auth_type = ssh_props.get("authType", "")

            if key_path:
                connection["ssh_tunnel"]["private_key"] = key_path
                print(f"    SSH: Key authentication with {key_path}")
            elif auth_type == "PASSWORD":
                print("    SSH: Password authentication required")

        # Default to CLI implementation
        connection["implementation"] = "cli"

        # Generate description based on connection details using shared function
        original_name = conn_data.get("name", conn_id)
        target = get_connection_target(connection)

        # Build description based on connection type
        db_type = connection["type"]
        host = target["host"]
        db = target["database"]

        if target["connection_type"] == "ssh_jump":
            ssh_host = target.get("ssh_host", "")
            description = f"{db_type} on {host}/{db} via SSH jump {ssh_host}"
        elif target["connection_type"] == "ssh_local":
            description = f"{db_type} on {host}/{db} via SSH"
        else:
            description = f"{db_type} on {host}/{db}"

        # Add import source
        connection["description"] = (
            f"{description} (imported from DBeaver: {original_name})"
        )

        return connection

    def _sanitize_name(self, name: str) -> str:
        """Sanitize connection name for use as identifier"""
        # Replace spaces and special characters with underscores
        import re

        sanitized = re.sub(r"[^\w\-]", "_", name)
        # Remove consecutive underscores
        sanitized = re.sub(r"_+", "_", sanitized)
        # Remove leading/trailing underscores
        sanitized = sanitized.strip("_")
        # Convert to lowercase
        sanitized = sanitized.lower()
        return sanitized or "connection"

    def _host_pattern(self, server: str) -> Tuple[str, str]:
        """Extract pattern from host for grouping (replaces digits with #)"""
        host, _, port = server.partition(":")
        digit_re = re.compile(r"\d+")
        return digit_re.sub("#", host), port

    def _group_key(self, conn: Dict[str, Any]) -> Tuple:
        """Generate a grouping key for connection merging"""
        ssh_tunnel = conn.get("ssh_tunnel") or {}
        ssh_serialized = json.dumps(ssh_tunnel, sort_keys=True)

        fields_part = (
            conn.get("type"),
            conn.get("db", ""),  # Single database, not list
            conn.get("username"),
            conn.get("password"),
            ssh_serialized,
        )

        servers = conn.get("servers", []) or []
        patterns = tuple(sorted(self._host_pattern(server) for server in servers))

        return fields_part, patterns

    def _merge_cluster_connections(
        self, connections: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Merge connections that appear to be part of the same cluster"""
        groups: Dict[Tuple, Dict[str, Any]] = {}
        order: List[Tuple] = []

        for conn in connections:
            servers = conn.get("servers", []) or []
            ssh = conn.get("ssh_tunnel")
            conn_password = conn.get("password")
            conn_ssh_password = ssh.get("password") if isinstance(ssh, dict) else None

            # Add default SSH user if SSH tunnel exists but no user specified
            if isinstance(ssh, dict) and not ssh.get("user"):
                ssh["user"] = getpass.getuser()  # Current OS user

            key = self._group_key(conn)
            group = groups.get(key)

            if not group:
                # Create new group - will update description after merging
                group = {
                    "connection_name": conn["connection_name"],
                    "type": conn.get("type"),
                    "servers": [],
                    "db": conn.get("db", ""),  # Single database
                    "username": conn.get("username"),
                    "password": conn_password,
                    "ssh_tunnel": dict(ssh) if isinstance(ssh, dict) else ssh,
                    "implementation": conn.get("implementation", "python"),
                    "original_names": [],  # Track original DBeaver names
                }
                groups[key] = group
                order.append(key)
            else:
                if conn_ssh_password:
                    group_ssh = group.get("ssh_tunnel")
                    if isinstance(group_ssh, dict) and not group_ssh.get("password"):
                        group_ssh["password"] = conn_ssh_password
                    elif (
                        isinstance(group_ssh, dict)
                        and group_ssh.get("password") != conn_ssh_password
                    ):
                        logger.warning(
                            "Multiple SSH passwords found while merging cluster %s; keeping the first",
                            group.get("connection_name"),
                        )

            # Add servers from this connection to the group
            for server in servers:
                if server not in group["servers"]:
                    group["servers"].append(server)

            # Track original DBeaver name
            if (
                "description" in conn
                and "(imported from DBeaver:" in conn["description"]
            ):
                # Extract original name from description
                import_part = (
                    conn["description"].split("(imported from DBeaver:")[-1].rstrip(")")
                )
                if import_part not in group["original_names"]:
                    group["original_names"].append(import_part)

        # Return merged groups in original order with updated descriptions
        merged = []
        for key in order:
            group = groups[key]

            # Generate description for merged group using shared function
            target = get_connection_target(group)
            db_type = group["type"]
            host = target["host"]
            db = target["database"]

            if target["connection_type"] == "ssh_jump":
                ssh_host = target.get("ssh_host", "")
                description = f"{db_type} on {host}/{db} via SSH jump {ssh_host}"
            elif target["connection_type"] == "ssh_local":
                description = f"{db_type} on {host}/{db} via SSH"
            else:
                description = f"{db_type} on {host}/{db}"

            # Add import source with original names
            if group.get("original_names"):
                original = ", ".join(group["original_names"])
                group["description"] = (
                    f"{description} (imported from DBeaver: {original})"
                )
            else:
                group["description"] = f"{description} (imported from DBeaver)"

            # Remove temporary field
            group.pop("original_names", None)
            merged.append(group)

        # Log merge results
        original_count = len(connections)
        merged_count = len(merged)
        if original_count > merged_count:
            logger.info(
                f"Merged {original_count} connections into {merged_count} groups"
            )

        return merged

    def _build_merge_report(
        self, connections: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Build a report of which connections would be merged together"""
        groups: Dict[Tuple, Dict[str, Any]] = {}
        order: List[Tuple] = []

        for conn in connections:
            ssh = conn.get("ssh_tunnel")
            if isinstance(ssh, dict) and not ssh.get("user"):
                ssh["user"] = getpass.getuser()

            key = self._group_key(conn)
            group = groups.get(key)
            if not group:
                group = {
                    "name": conn.get("connection_name", ""),
                    "members": [],
                    "servers": [],
                }
                groups[key] = group
                order.append(key)

            group["members"].append(conn.get("connection_name", ""))
            for server in conn.get("servers", []) or []:
                if server not in group["servers"]:
                    group["servers"].append(server)

        return [groups[key] for key in order]


def main():
    """Command-line entry point for importing DBeaver connections"""
    import sys
    import yaml
    import argparse
    from datetime import datetime

    parser = argparse.ArgumentParser(description="Import DBeaver connections")
    parser.add_argument(
        "dbeaver_path",
        nargs="?",
        help="Path to DBeaver .dbeaver directory",
    )
    parser.add_argument(
        "--no-merge", action="store_true", help="Don't merge cluster connections"
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Output file (default: ~/.config/lukleh/mcp-read-only-sql/connections.yaml)",
    )
    parser.add_argument("--config-dir", help="Directory containing connections.yaml")
    parser.add_argument("--state-dir", help="Directory reserved for local state files")
    parser.add_argument("--cache-dir", help="Directory reserved for cache files")
    parser.add_argument(
        "--print-paths",
        action="store_true",
        help="Print resolved config/state/cache paths and exit",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Show results without writing files"
    )
    parser.add_argument(
        "--only",
        action="append",
        help="Import only connections with these DBeaver names (repeatable or comma-separated)",
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

    if not args.dbeaver_path:
        parser.error("the following arguments are required: dbeaver_path")

    dbeaver_path = args.dbeaver_path
    merge_clusters = not args.no_merge
    output_path = (
        Path(args.output).expanduser()
        if args.output
        else runtime_paths.connections_file
    )
    dry_run = args.dry_run
    only_names: List[str] = []
    if args.only:
        for entry in args.only:
            only_names.extend(
                [name.strip() for name in entry.split(",") if name.strip()]
            )

    print(f"\nImporting DBeaver connections from: {dbeaver_path}")
    print(f"Output file: {output_path}")
    print(f"Merge clusters: {merge_clusters}\n")
    if dry_run:
        print("Dry run: no files will be written\n")

    try:
        importer = DBeaverImporter(dbeaver_path)
        # Always import raw connections first so we can report on merges clearly.
        raw_connections = importer.import_connections(
            merge_clusters=False, only_names=only_names
        )
        merge_report = []
        if merge_clusters:
            merge_report = importer._build_merge_report(raw_connections)
            connections = importer._merge_cluster_connections(raw_connections)
        else:
            connections = raw_connections

        if only_names:
            missing = sorted(set(only_names) - set(importer.last_seen_names))
            if missing:
                print("\n⚠ Requested connection(s) not found in DBeaver:")
                for name in missing:
                    print(f"  • {name}")

        backup_path = None
        new_connections = []
        imported_db_passwords = 0
        imported_ssh_passwords = 0
        missing_db_passwords: List[str] = []
        missing_ssh_passwords: List[str] = []

        print("\nImporting connections...")
        for conn in connections:
            conn_name = conn["connection_name"]
            new_connections.append(conn_name)
            print(f"  ✓ Imported: {conn_name}")

            if conn.get("password"):
                imported_db_passwords += 1
            else:
                missing_db_passwords.append(conn_name)

            ssh = conn.get("ssh_tunnel")
            if ssh:
                if ssh.get("password"):
                    imported_ssh_passwords += 1
                elif not ssh.get("private_key"):
                    missing_ssh_passwords.append(conn_name)

        # Save new config (replacing the old one completely, unless --only is used)
        # Clean out null values before saving
        clean_connections = []
        for conn in connections:
            clean_conn = {}
            for key, value in conn.items():
                if value is not None:
                    if key == "ssh_tunnel" and isinstance(value, dict):
                        # Clean ssh_tunnel nested dict
                        clean_ssh = {k: v for k, v in value.items() if v is not None}
                        if clean_ssh:
                            clean_conn[key] = clean_ssh
                    else:
                        clean_conn[key] = value
            clean_connections.append(clean_conn)

        output_connections = clean_connections
        if only_names and output_path.exists():
            # Merge into existing instead of replacing when importing a subset.
            try:
                with open(output_path, "r") as f:
                    existing_connections = yaml.safe_load(f) or []
            except Exception as e:
                existing_connections = []
                print(f"\n⚠ Could not read existing {output_path} for merge: {e}")

            new_by_name = {
                c.get("connection_name"): c
                for c in clean_connections
                if isinstance(c, dict)
            }
            used = set()
            merged_connections = []
            for conn in existing_connections:
                name = conn.get("connection_name") if isinstance(conn, dict) else None
                if name in new_by_name:
                    merged_connections.append(new_by_name[name])
                    used.add(name)
                else:
                    merged_connections.append(conn)

            for conn in clean_connections:
                name = conn.get("connection_name") if isinstance(conn, dict) else None
                if name and name not in used:
                    merged_connections.append(conn)
                    used.add(name)

            output_connections = merged_connections

        if dry_run:
            print(f"\nDry run: skipping write to {output_path}")
        else:
            new_yaml = yaml.dump(
                output_connections, default_flow_style=False, sort_keys=False
            )
            existing_yaml = None
            if output_path.exists():
                with open(output_path, "r") as f:
                    existing_yaml = f.read()

            if existing_yaml is not None and existing_yaml == new_yaml:
                os.chmod(output_path, 0o600)
                print(f"\n✓ {output_path} unchanged; skipped write and backup")
            else:
                if existing_yaml is not None:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    # Create backup filename: connections.yaml.bak.20241230_143022
                    backup_name = f"{output_path.stem}.yaml.bak.{timestamp}"
                    backup_path = output_path.parent / backup_name
                    _write_text_file_secure(backup_path, existing_yaml)
                    print(f"✓ Created backup: {backup_path}")

                _write_text_file_secure(output_path, new_yaml)

        # Dry-run preview of changes vs existing connections.yaml
        if dry_run:

            def _normalize(conn: Dict[str, Any]) -> Dict[str, Any]:
                normalized = dict(conn)
                servers = normalized.get("servers")
                if isinstance(servers, list):
                    normalized["servers"] = sorted(servers)
                ssh = normalized.get("ssh_tunnel")
                if isinstance(ssh, dict):
                    normalized["ssh_tunnel"] = dict(ssh)
                return normalized

            if output_path.exists():
                try:
                    with open(output_path, "r") as f:
                        existing = yaml.safe_load(f) or []
                except Exception as e:
                    existing = []
                    print(f"\nDry run: could not read {output_path}: {e}")

                existing_by_name = {
                    c.get("connection_name"): _normalize(c)
                    for c in existing
                    if isinstance(c, dict)
                }
                new_by_name = {
                    c.get("connection_name"): _normalize(c)
                    for c in output_connections
                    if isinstance(c, dict)
                }

                added = sorted(
                    [n for n in new_by_name.keys() if n not in existing_by_name]
                )
                removed = sorted(
                    [n for n in existing_by_name.keys() if n not in new_by_name]
                )
                changed = sorted(
                    [
                        n
                        for n in new_by_name.keys()
                        if n in existing_by_name
                        and new_by_name[n] != existing_by_name[n]
                    ]
                )

                print("\nDry run: connections.yaml change preview")
                print(
                    f"  • Added ({len(added)}): {', '.join(added) if added else 'none'}"
                )
                print(
                    f"  • Removed ({len(removed)}): {', '.join(removed) if removed else 'none'}"
                )
                print(
                    f"  • Changed ({len(changed)}): {', '.join(changed) if changed else 'none'}"
                )
            else:
                print(
                    f"\nDry run: {output_path} does not exist; it would be created with {len(clean_connections)} connections"
                )

            if merge_clusters:
                merged_groups = [
                    g for g in merge_report if len(g.get("members", [])) > 1
                ]
                print("\nDry run: merge preview")
                if merged_groups:
                    for group in merged_groups:
                        members = ", ".join(group.get("members", []))
                        print(f"  • {group.get('name')}: {members}")
                else:
                    print("  • No cluster merges detected")

        # Summary
        print(f"\n{'='*60}")
        print("Import Summary:")
        print(f"  • Imported: {len(connections)} connections")
        print(f"  • Output file: {output_path}")
        if backup_path:
            print(f"  • Backup saved: {backup_path}")
        print(f"  • Database passwords imported: {imported_db_passwords}")
        if imported_ssh_passwords:
            print(f"  • SSH passwords imported: {imported_ssh_passwords}")
        if missing_db_passwords:
            print(
                f"  • Database passwords still missing: {', '.join(sorted(missing_db_passwords))}"
            )
        if missing_ssh_passwords:
            print(
                f"  • SSH passwords still missing: {', '.join(sorted(missing_ssh_passwords))}"
            )

        if missing_db_passwords or missing_ssh_passwords:
            print(
                "\n⚠ Some passwords were not available from DBeaver. Fill them in manually in connections.yaml before validating or connecting."
            )

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
