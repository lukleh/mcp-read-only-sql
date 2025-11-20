# MCP Read-Only SQL Server

[![Tests](https://github.com/lukleh/mcp-read-only-sql/actions/workflows/test.yml/badge.svg)](https://github.com/lukleh/mcp-read-only-sql/actions/workflows/test.yml)

A secure MCP (Model Context Protocol) server that provides **read-only** SQL access to PostgreSQL and ClickHouse databases with built-in safety features.

## Security

The server implements a **three-layer security model**:

1. **Database-level read-only** - Sessions forced to read-only mode
2. **Timeout protection** - Connection timeout (5s), query timeout (10s) - configurable per connection
3. **Result size limits** - Default 5KB, prevents memory exhaustion

All write operations (INSERT, UPDATE, DELETE, etc.) are blocked at the database level.

### How Read-Only Is Enforced

- **PostgreSQL (Python)** – Connections are opened with `default_transaction_read_only=on`, sessions are set to read-only, and every statement runs with a configurable `statement_timeout`.
- **PostgreSQL (CLI)** – Queries are wrapped in a transaction that issues `SET TRANSACTION READ ONLY;` before execution. Input is sanitized so only a single statement (plus optional trailing semicolon) is forwarded, transaction-control keywords are rejected up front, and all `psql` invocations include `--single-transaction`, `-v ON_ERROR_STOP=1`, and `PGOPTIONS=-c default_transaction_read_only=on` for defence in depth.
- **ClickHouse (Python)** – The driver sets `readonly=1` plus connection/query timeouts, forcing the server to reject any write or DDL attempt.
- **ClickHouse (CLI)** – `clickhouse-client` is invoked with `--readonly=1`, `--max_execution_time`, and connection timeouts, turning the session into a read-only context.

The shared connector base also applies hard timeouts and result-size ceilings, giving the MCP server deterministic behaviour even if the database misbehaves.

See [READ_ONLY_ENFORCEMENT_MATRIX.md](READ_ONLY_ENFORCEMENT_MATRIX.md) for a statement-by-statement view of every write-capable command and the tests that enforce it.

## Key Features

- **Read-only enforcement** - Multiple layers of protection against writes
- **Multi-database support** - PostgreSQL and ClickHouse
- **Dual implementations** - Choose between Python (pure Python, no dependencies) or CLI (uses `psql`/`clickhouse-client`)
- **SSH tunnel support** - Both implementations support key authentication; Python uses Paramiko for passwords and CLI uses `sshpass` for password-based tunnels
- **Security built-in** - Timeouts, size limits, session controls
- **DBeaver import** - Import existing connections easily

## Prerequisites

- [uv](https://github.com/astral-sh/uv) - Fast Python package installer and resolver
- [just](https://github.com/casey/just) - Command runner (for project tasks)

## Quick Start

### 1. Install Dependencies

```bash
uv sync
```

If you plan to use CLI connectors with SSH password authentication, install `sshpass` as well (for example, `brew install sshpass` on macOS or `apt-get install sshpass` on Debian-based Linux).

### 2. Configure Database Connections

**Option A: Create from sample**
```bash
cp connections.yaml.sample connections.yaml
# Edit connections.yaml with your database details
```

**Option B: Import from DBeaver**
```bash
just import-dbeaver
# This creates connections.yaml from your DBeaver workspace
```

> **Note:** The server reads `connections.yaml` during startup. Restart the MCP
> process after editing the file so changes take effect.

### 3. Validate and Test Connections

```bash
# Validate configuration file
just validate

# Test database connectivity
just test-connection              # Test all connections
just test-connection my_postgres  # Test specific connection
```

### 4. Add MCP Server to Your Client

**For Claude Code:**
```bash
claude mcp add mcp-read-only-sql -- uv --directory {PATH_TO_MCP_READ_ONLY_SQL} run python -m src.server
```

**For Codex:**
```bash
codex mcp add mcp-read-only-sql -- uv --directory {PATH_TO_MCP_READ_ONLY_SQL} run python -m src.server
```

Replace `{PATH_TO_MCP_READ_ONLY_SQL}` with the full path to where you cloned this repository (e.g., `/Users/yourname/projects/mcp-read-only-sql`).

## MCP Tools

### `run_query_read_only`
Execute read-only SQL queries on configured databases.

```json
{
  "connection_name": "my_postgres",
  "query": "SELECT * FROM users LIMIT 10",
  "server": "db2.example.com",
  "file_path": "~/Downloads/query.tsv"
}
```

**Parameters:**
- `connection_name` (required): Identifier returned by list_connections
- `query` (required): SQL text that must remain read-only
- `server` (optional): Hostname to target a specific server. If not provided, uses the first server in the connection's list.
- `file_path` (optional): When provided, results are written to this path (parents created if needed) and the tool returns the absolute path string instead of TSV content. The file must not already exist; if it does, the tool returns an error instead of overwriting.

**Returns:** Tab-separated text (TSV) with a header row followed by data rows.
The structured MCP payload mirrors the same TSV string. If results exceed
`max_result_bytes`, a trailing notice indicates truncation. When `file_path`
is supplied, the returned value is the absolute path of the written file and the tool refuses to overwrite existing files.

### `list_connections`
List all available database connections.

**Returns:** Tab-separated text with columns `name`, `type`, `description`,
`servers`, `database`, and `user`. The `servers` column lists comma-separated
hostnames after resolving SSH/VPN tunnels, so entries reflect the endpoints the
agent should reference.

## Implementation Matrix

### Database Support by Implementation

| Feature | PostgreSQL CLI | PostgreSQL Python | ClickHouse CLI | ClickHouse Python |
|---------|---------------|-------------------|----------------|-------------------|
| **Protocol** | Native PostgreSQL | Native PostgreSQL | Native ClickHouse | HTTP/HTTPS |
| **Default Port** | 5432 | 5432 | 9000 | 8123 |
| **Supported Ports** | Any PostgreSQL port | Any PostgreSQL port | 9000, 9440 (native + TLS) | 8123 (HTTP), 8443 (HTTPS) |
| **TLS/SSL Support** | ✅ Yes | ✅ Yes | ✅ Yes (--secure for 9440) | ✅ Yes (HTTPS on 8443) |
| **Read-Only Method** | `SET TRANSACTION READ ONLY` | `default_transaction_read_only=on` | `--readonly=1` flag | `readonly=1` setting |
| **SSH Key Auth** | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes |
| **SSH Password Auth** | ✅ Yes (requires `sshpass`) | ✅ Yes (Paramiko) | ✅ Yes (requires `sshpass`) | ✅ Yes (Paramiko) |
| **Timeout Control** | ✅ Via SQL | ✅ Driver-level | ✅ CLI flags | ✅ Driver-level |
| **Result Streaming** | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes |
| **Binary Required** | `psql` | None | `clickhouse-client` | None |

### ClickHouse Port Compatibility

| Port | Protocol | CLI Support | Python Support | Notes |
|------|----------|-------------|----------------|-------|
| 8123 | HTTP | ⚠️ Auto-converts to 9000 | ✅ Native support | Default HTTP interface |
| 8443 | HTTPS | ⚠️ Auto-converts to 9440 (--secure) | ✅ Native support | Secure HTTP interface |
| 9000 | Native TCP | ✅ Native support | ⚠️ Auto-converts to 8123 | Default native protocol |
| 9440 | Native TCP (TLS) | ✅ Native support (--secure) | ⚠️ Auto-converts to 8443 (HTTPS) | Secure native protocol |
| Custom (e.g., 2650) | Usually HTTP | ❌ No conversion | ✅ Yes | HAProxy/Load balancers - NO auto-conversion |

**Important Notes:**
- **ClickHouse CLI** (`clickhouse-client`) uses native protocol ports (9000, 9440)
- **ClickHouse Python** (using `clickhouse-connect`) uses HTTP/HTTPS ports (8123, 8443)
- Port mismatches are automatically handled - see below

**Automatic Port Handling (Bidirectional):**

*ClickHouse Python Implementation:*
- **Direct connections**: Port 9000 → automatically uses port 8123 on the same host
- **SSH tunnels**: Port 9000 → automatically tunnels to remote port 8123
- **SSH tunnels**: Port 9440 → automatically tunnels to remote port 8443

*ClickHouse CLI Implementation:*
- **Direct connections**: Port 8123 → automatically uses port 9000 on the same host
- **SSH tunnels**: Port 8123 → automatically tunnels to remote port 9000
- **SSH tunnels**: Port 8443 → automatically tunnels to remote port 9440

✨ **This means you can use the same configuration for both CLI and Python implementations, regardless of which port you specify (8123 or 9000) - each implementation will automatically convert to the correct protocol port it needs!**

### Choosing an Implementation

**Use CLI implementation when:**
- You have the database CLI tools installed (`psql`, `clickhouse-client`)
- You prefer not to install Python database drivers
- You're connecting to ClickHouse on native ports (9000, 9440)
- You want the exact behavior of the official CLI tools

**Use Python implementation when:**
- You want a pure Python solution with no external dependencies
- You're connecting to ClickHouse HTTP interface (port 8123, 8443)
- You need SSH password authentication without installing `sshpass`
- You want more programmatic control over connections

## Configuration Notes

### HAProxy and Custom Ports

When using **HAProxy** or other proxy servers with ClickHouse:

- **HAProxy typically provides HTTP interface** on custom ports (e.g., 2650, 8000, etc.)
- **Custom ports are NOT auto-converted** - the system only converts standard ports (8123, 8443, 9000, 9440)
- **For HAProxy connections**: Use `implementation: python` since HAProxy usually proxies HTTP traffic
- If you get "Unexpected packet" errors with CLI on custom ports, switch to Python implementation

Example HAProxy configuration:
```yaml
- connection_name: clickhouse_haproxy
  type: clickhouse
  servers:
  - haproxy-server:2650  # Custom HAProxy port
  implementation: python  # Use Python for HTTP protocol
  # ... other settings
```

### Multiple Servers
When multiple servers are specified in a connection's configuration, the system currently uses only the first server in the list. Load balancing across servers is not implemented.

### SSH Authentication
- **Python implementation**: Supports both SSH password authentication (via `SSH_PASSWORD_<CONNECTION_NAME>` environment variable) and SSH key files
- **CLI implementation**: Supports key-based authentication and can use passwords when `sshpass` is installed
