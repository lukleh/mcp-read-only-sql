# MCP Read-Only SQL Server

[![Tests](https://github.com/lukleh/mcp-read-only-sql/actions/workflows/test.yml/badge.svg)](https://github.com/lukleh/mcp-read-only-sql/actions/workflows/test.yml)

A secure MCP (Model Context Protocol) server that provides **read-only** SQL access to PostgreSQL and ClickHouse databases with built-in safety features.

> Default layout:
> - Config: `~/.config/lukleh/mcp-read-only-sql/connections.yaml`
> - Credentials: stored in `connections.yaml`
> - State: `~/.local/state/lukleh/mcp-read-only-sql/`
> - Cache: `~/.cache/lukleh/mcp-read-only-sql/`

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

- [uv](https://github.com/astral-sh/uv) for package installs and ephemeral `uvx` runs
- `psql` if you want PostgreSQL connections with `implementation: cli`
- `clickhouse-client` if you want ClickHouse connections with `implementation: cli`
- `sshpass` only if you want CLI-based SSH tunnels with password authentication
- [just](https://github.com/casey/just) is optional and only needed for repo-local contributor workflows

Install the optional CLI binaries with your operating system's package manager or the official PostgreSQL / ClickHouse packages for your environment.

The SQL package keeps both execution models first-class:

- `implementation: cli` uses the official database client binaries you already trust in operations.
- `implementation: python` stays fully supported when you want a pure-Python setup with no external database client binaries.

You can verify optional CLI dependencies with:

```bash
psql --version
clickhouse-client --version
sshpass -V
```

## Quick Start

### 1. Install or Run from This Checkout

For one-off runs from this checkout, use `uvx --from .`:

```bash
uvx --from . mcp-read-only-sql --write-sample-config
```

For a persistent local install from this checkout:

```bash
uv tool install .
mcp-read-only-sql --write-sample-config
```

After the package is published to PyPI, you can replace `.` with `mcp-read-only-sql`.

That creates:

- `~/.config/lukleh/mcp-read-only-sql/connections.yaml`
- `~/.local/state/lukleh/mcp-read-only-sql/`
- `~/.cache/lukleh/mcp-read-only-sql/`

### 2. Choose an Implementation Per Connection

`connections.yaml` supports both implementations side by side:

```yaml
- connection_name: postgres_cli
  type: postgresql
  implementation: cli
  servers:
    - "db.example.com:5432"
  db: analytics
  username: analyst
  password: change_me

- connection_name: clickhouse_python
  type: clickhouse
  implementation: python
  servers:
    - "analytics.example.com:8123"
  db: default
  username: analyst
  password: change_me
```

Use CLI mode when you want the behavior of `psql` or `clickhouse-client`, or when those tools are already part of your operational setup. Use Python mode when you want a package-only setup with no extra system binaries.

### 3. Import or Edit `connections.yaml`

You can edit the generated sample directly, or import a DBeaver workspace:

```bash
uvx --from . mcp-read-only-sql import-dbeaver \
  ~/Library/DBeaverData/workspace6/General/.dbeaver
```

That writes `connections.yaml` with any decrypted passwords stored directly in the file. The importer writes user-only permissions and keeps timestamped backups when it overwrites an existing file.

> `connections.yaml` contains credentials. Keep it private, do not commit it, and restart the MCP process after editing it so changes take effect.

To allow a connection to access multiple databases, add an explicit allowlist:

```yaml
- connection_name: analytics_multi
  type: postgresql
  servers:
    - "analytics.example.com:5432"
  allowed_databases:
    - analytics
    - reporting
  default_database: analytics
  username: analyst
  password: change_me
```

If you only set `db`, that single database is implicitly the allowlist.

### 4. Validate and Test Connections

The package includes management subcommands for connection validation and dry-run testing:

```bash
uvx --from . mcp-read-only-sql validate-config
uvx --from . mcp-read-only-sql test-connection
uvx --from . mcp-read-only-sql test-connection my_postgres
uvx --from . mcp-read-only-sql test-ssh-tunnel
uvx --from . mcp-read-only-sql --print-paths
```

If you are working from a clone, the same helpers are available through `just`:

```bash
just validate
just test-connection
just test-connection my_postgres
just print-paths
```

### 5. Add the MCP Server to Your Client

For Claude Code:

```bash
claude mcp add mcp-read-only-sql -- uvx --from . mcp-read-only-sql
```

For Codex:

```bash
codex mcp add mcp-read-only-sql -- uvx --from . mcp-read-only-sql
```

For manual testing with a different config root:

```bash
uvx --from . mcp-read-only-sql --config-dir /path/to/config-dir --print-paths
```

## MCP Tools

### `run_query_read_only`
Execute read-only SQL queries on configured databases.

```json
{
  "connection_name": "my_postgres",
  "query": "SELECT * FROM users LIMIT 10",
  "database": "analytics",
  "server": "db2.example.com",
  "file_path": "~/Downloads/query.tsv"
}
```

**Parameters:**
- `connection_name` (required): Identifier returned by list_connections
- `query` (required): SQL text that must remain read-only
- `database` (optional): Database to use (must be listed in the connection's allowlist).
- `server` (optional): Hostname to target a specific server. If not provided, uses the first server in the connection's list.
- `file_path` (optional): When provided, results are written to this path (parents created if needed) and the tool returns the absolute path string instead of TSV content. The file must not already exist; if it does, the tool returns an error instead of overwriting. The result-size limit is skipped when saving to a file so the full output is streamed to disk.

**Returns:** Tab-separated text (TSV) with a header row followed by data rows.
The structured MCP payload mirrors the same TSV string. If results exceed
`max_result_bytes`, a trailing notice indicates truncation. When `file_path`
is supplied, the returned value is the absolute path of the written file, the tool refuses to overwrite existing files, and result-size truncation is not applied (full result is written).

### `list_connections`
List all available database connections.

**Returns:** Tab-separated text with columns `name`, `type`, `description`,
`servers`, `database`, `databases`, and `user`. `database` is the default database,
while `databases` lists the allowlisted databases (comma-separated). The `servers`
column lists comma-separated hostnames after resolving SSH/VPN tunnels, so entries
reflect the endpoints the agent should reference.

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
- **Python implementation**: Supports both `ssh_tunnel.password` and `ssh_tunnel.private_key`
- **CLI implementation**: Supports key-based authentication and can use passwords when `sshpass` is installed
