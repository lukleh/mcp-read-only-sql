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

logger = logging.getLogger(__name__)


class DBeaverImporter:
    def __init__(self, dbeaver_path: str):
        self.dbeaver_path = Path(dbeaver_path)
        self.data_sources_path = self.dbeaver_path / "data-sources.json"
        self.credentials_path = self.dbeaver_path / "credentials-config.json"

    def _decrypt_credentials(self) -> Dict[str, Any]:
        """Decrypt DBeaver credentials file using OpenSSL with default AES key"""
        if not self.credentials_path.exists():
            return {}

        # DBeaver's default AES key and IV
        key = "babb4a9f774ab853c96c2d653dfe544a"
        iv = "00000000000000000000000000000000"

        # Use OpenSSL to decrypt
        cmd = [
            "openssl", "aes-128-cbc",
            "-d",
            "-K", key,
            "-iv", iv,
            "-in", str(self.credentials_path)
        ]

        try:
            result = subprocess.run(cmd, capture_output=True)

            if result.returncode != 0:
                logger.warning(f"Could not decrypt credentials: {result.stderr.decode()}")
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

            logger.info(f"Successfully decrypted credentials for {len(credentials)} connections")
            return credentials, ssh_credentials

        except (subprocess.SubprocessError, json.JSONDecodeError) as e:
            logger.warning(f"Could not decrypt credentials file: {e}")
            return {}

    def import_connections(self, merge_clusters: bool = True) -> List[Dict[str, Any]]:
        """Import connections from DBeaver configuration"""
        if not self.data_sources_path.exists():
            raise FileNotFoundError(f"DBeaver data sources file not found: {self.data_sources_path}")

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
                logger.warning("Could not read credentials file. Usernames will need to be set manually.")

        connections = []
        for conn_id, conn_data in data_sources.get("connections", {}).items():
            # Pass both DB and SSH credentials
            db_creds = credentials.get(conn_id)
            ssh_creds = ssh_credentials.get(conn_id) if 'ssh_credentials' in locals() else None
            converted = self._convert_connection(conn_id, conn_data, db_creds, ssh_creds)
            if converted:
                connections.append(converted)

        if merge_clusters:
            connections = self._merge_cluster_connections(connections)

        return connections

    def _convert_connection(self, conn_id: str, conn_data: Dict[str, Any], creds: Dict[str, Any] = None, ssh_creds: Dict[str, Any] = None) -> Dict[str, Any]:
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
            "type": provider
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
                print(f"    Warning: No username found in credentials")

            # Store password for later .env update (don't put in YAML)
            password = creds.get("password")
            if password:
                connection["_password"] = password  # Temporary field, removed before saving
                print(f"    Password: {'*' * 8} (will be added to .env)")
        else:
            print(f"    Warning: No credentials available for this connection")

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
                connection["_ssh_password"] = ssh_creds["password"]
                print(f"    SSH Password: {'*' * 8} (will be added to .env)")

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
                # Mark that password auth is needed
                connection["ssh_tunnel"]["auth_type"] = "password"
                print(f"    SSH: Password authentication required")

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
        connection["description"] = f"{description} (imported from DBeaver: {original_name})"

        return connection

    def _sanitize_name(self, name: str) -> str:
        """Sanitize connection name for use as identifier"""
        # Replace spaces and special characters with underscores
        import re
        sanitized = re.sub(r'[^\w\-]', '_', name)
        # Remove consecutive underscores
        sanitized = re.sub(r'_+', '_', sanitized)
        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')
        # Convert to lowercase
        sanitized = sanitized.lower()
        return sanitized or "connection"

    def _host_pattern(self, server: str) -> Tuple[str, str]:
        """Extract pattern from host for grouping (replaces digits with #)"""
        host, _, port = server.partition(":")
        digit_re = re.compile(r'\d+')
        return digit_re.sub("#", host), port

    def _group_key(self, conn: Dict[str, Any]) -> Tuple:
        """Generate a grouping key for connection merging"""
        ssh_tunnel = conn.get("ssh_tunnel") or {}
        ssh_serialized = json.dumps(ssh_tunnel, sort_keys=True)

        fields_part = (
            conn.get("type"),
            conn.get("db", ""),  # Single database, not list
            conn.get("username"),
            ssh_serialized,
        )

        servers = conn.get("servers", []) or []
        patterns = tuple(sorted(self._host_pattern(server) for server in servers))

        return fields_part, patterns

    def _merge_cluster_connections(self, connections: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Merge connections that appear to be part of the same cluster"""
        groups: Dict[Tuple, Dict[str, Any]] = {}
        order: List[Tuple] = []

        for conn in connections:
            servers = conn.get("servers", []) or []
            ssh = conn.get("ssh_tunnel")

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
                    "ssh_tunnel": ssh,
                    "implementation": conn.get("implementation", "python"),
                    "original_names": []  # Track original DBeaver names
                }
                groups[key] = group
                order.append(key)

            # Add servers from this connection to the group
            for server in servers:
                if server not in group["servers"]:
                    group["servers"].append(server)

            # Track original DBeaver name
            if "description" in conn and "(imported from DBeaver:" in conn["description"]:
                # Extract original name from description
                import_part = conn["description"].split("(imported from DBeaver:")[-1].rstrip(")")
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
                group["description"] = f"{description} (imported from DBeaver: {original})"
            else:
                group["description"] = f"{description} (imported from DBeaver)"

            # Remove temporary field
            group.pop("original_names", None)
            merged.append(group)

        # Log merge results
        original_count = len(connections)
        merged_count = len(merged)
        if original_count > merged_count:
            logger.info(f"Merged {original_count} connections into {merged_count} groups")

        return merged


def main():
    """Command-line entry point for importing DBeaver connections"""
    import sys
    import yaml
    import argparse
    from datetime import datetime

    parser = argparse.ArgumentParser(description="Import DBeaver connections")
    parser.add_argument("dbeaver_path", help="Path to DBeaver .dbeaver directory")
    parser.add_argument("--no-merge", action="store_true", help="Don't merge cluster connections")
    parser.add_argument("--output", "-o", help="Output file (default: connections.yaml)")
    parser.add_argument("--env-file", "-e", help="Environment file (default: .env)")
    parser.add_argument("--update-passwords", action="store_true", help="Update passwords in .env from DBeaver")
    args = parser.parse_args()

    dbeaver_path = args.dbeaver_path
    merge_clusters = not args.no_merge
    output_path = Path(args.output) if args.output else Path("connections.yaml")
    env_path = Path(args.env_file) if args.env_file else Path(".env")

    print(f"\nImporting DBeaver connections from: {dbeaver_path}")
    print(f"Output file: {output_path}")
    print(f"Environment file: {env_path}")
    print(f"Merge clusters: {merge_clusters}\n")

    try:
        importer = DBeaverImporter(dbeaver_path)
        connections = importer.import_connections(merge_clusters=merge_clusters)

        # Backup existing file if it exists
        backup_path = None
        if output_path.exists():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # Create backup filename: connections.yaml.bak.20241230_143022
            backup_name = f"{output_path.stem}.yaml.bak.{timestamp}"
            backup_path = output_path.parent / backup_name

            # Read existing file
            with open(output_path, "r") as f:
                existing_content = f.read()

            # Write backup
            with open(backup_path, "w") as f:
                f.write(existing_content)

            print(f"✓ Created backup: {backup_path}")

        # Track credentials that need to be added to .env
        ssh_credentials = []
        new_connections = []
        password_updates = {}  # Track passwords to update

        print(f"\nImporting connections...")
        for conn in connections:
            conn_name = conn["connection_name"]
            new_connections.append(conn_name)
            print(f"  ✓ Imported: {conn_name}")

            # Extract password if present (stored temporarily)
            if "_password" in conn:
                password_updates[conn_name] = conn.pop("_password")

            # Extract SSH password if present
            if "_ssh_password" in conn:
                ssh_pass_key = f"{conn_name.upper().replace('-', '_')}_SSH_PASSWORD"
                password_updates[ssh_pass_key] = conn.pop("_ssh_password")

            # Track SSH credentials if needed
            ssh = conn.get("ssh_tunnel")
            if ssh:
                ssh_cred = {
                    "connection": conn_name,
                    "user": ssh.get("user"),
                    "needs_password": ssh.get("auth_type") == "password"
                }
                ssh_credentials.append(ssh_cred)

        # Save new config (replacing the old one completely)
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

        with open(output_path, "w") as f:
            yaml.dump(clean_connections, f, default_flow_style=False, sort_keys=False)

        # Update .env file with required credentials
        if new_connections:
            env_lines = []
            if env_path.exists():
                with open(env_path, "r") as f:
                    env_lines = f.readlines()

            existing_env_keys = set()
            for line in env_lines:
                if "=" in line:
                    key = line.split("=")[0].strip()
                    existing_env_keys.add(key)

            new_env_entries = []

            # Add database passwords
            for name in new_connections:
                env_key = f"DB_PASSWORD_{name.upper().replace('-', '_')}"
                # Check if we have a password from DBeaver
                if name in password_updates and args.update_passwords:
                    new_env_entries.append((env_key, password_updates[name]))
                elif env_key not in existing_env_keys:
                    new_env_entries.append((env_key, ""))

            # Add SSH passwords (usernames are already in YAML config)
            for ssh_cred in ssh_credentials:
                conn_name = ssh_cred["connection"].upper().replace('-', '_')

                # SSH password (if password auth)
                if ssh_cred["needs_password"]:
                    ssh_pass_key = f"SSH_PASSWORD_{conn_name}"
                    if ssh_pass_key not in existing_env_keys:
                        new_env_entries.append((ssh_pass_key, ""))

            # Append new entries to .env file
            if new_env_entries:
                # Parse existing lines into key-value pairs
                env_dict = {}
                comment_lines = []

                for line in env_lines:
                    line = line.rstrip('\n')
                    if line.startswith('#') or not line.strip():
                        comment_lines.append(line)
                    elif '=' in line:
                        key, value = line.split('=', 1)
                        env_dict[key.strip()] = value.strip()

                # Add new entries to dictionary
                for key, value in new_env_entries:
                    env_dict[key] = value

                # Sort all entries by key
                sorted_entries = sorted(env_dict.items())

                # Write back sorted entries
                with open(env_path, "w") as f:
                    # Write comments first if any
                    if comment_lines:
                        for comment in comment_lines:
                            f.write(f"{comment}\n")
                        f.write("\n")

                    # Write sorted environment variables
                    for key, value in sorted_entries:
                        f.write(f"{key}={value}\n")

                print(f"\n✓ Updated {env_path} with {len(new_env_entries)} new credential entries (sorted)")

        # Summary
        print(f"\n{'='*60}")
        print(f"Import Summary:")
        print(f"  • Imported: {len(connections)} connections")
        print(f"  • Output file: {output_path}")
        if backup_path:
            print(f"  • Backup saved: {backup_path}")

        if new_connections:
            if args.update_passwords and password_updates:
                print(f"\n✓ Added {len(password_updates)} passwords to {env_path}")
            else:
                # Only show missing passwords
                missing_passwords = []
                for name in new_connections:
                    env_key = f"DB_PASSWORD_{name.upper().replace('-', '_')}"
                    if name not in password_updates:
                        missing_passwords.append(env_key)

                if missing_passwords:
                    print(f"\n⚠ Remember to update passwords in {env_path}:")
                    for env_key in missing_passwords:
                        print(f"  • {env_key}")

                for ssh_cred in ssh_credentials:
                    if ssh_cred["needs_password"]:
                        conn_name = ssh_cred["connection"].upper().replace('-', '_')
                        print(f"  • SSH_PASSWORD_{conn_name}")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()