# Test Suite Organization

This test suite is organized by functionality to clearly test the core security features of the MCP Read-Only SQL Server.

## Core Functionality Tests

### Configuration & Setup
- **test_config_parser.py** - Tests YAML configuration parsing
- **test_connector_implementations.py** - Tests CLI vs Python connector implementations

### MCP Protocol
- **test_mcp_protocol.py** - Tests MCP server/client communication
- **test_mcp_server.py** - Tests MCP server functionality

### Database Connectivity
- **test_docker_connectivity.py** - Verifies Docker databases are accessible

## Security Tests

### Layer 1: Client-side statement policy
- **test_sql_guard_postgresql.py** - The PostgreSQL parse-tree allow-list: accepted shapes, refused statements and functions, `allowed_functions`

### Layer 2: Database-level read-only
- **test_security_readonly.py** - Read-only sessions for both implementations, with mocked clients
  - Blocks INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE
- **test_security_readonly_integration.py** - The same against the Docker databases, including with the client-side guard bypassed

### Layer 3: Timeouts
- **test_limits.py** - Tests timeout enforcement and hard timeout behavior
  - Ensures long-running queries are terminated

### Managed result files
- **test_run_query_file_output.py** - Tests managed result-file creation
  - Ensures query results are written under the managed state directory with `0600` permissions

### Integration
- **test_security_layers.py** - Tests the layers working together

## Running Tests

```bash
# Run all tests
just test

# Run specific test categories
pytest tests/test_security_*.py  # All security tests
pytest tests/test_mcp_*.py       # All MCP protocol tests

# Run with Docker containers
docker compose up -d
pytest -m docker                 # Tests requiring Docker

# Override the Docker-exposed host/ports when localhost is not correct
TEST_DOCKER_HOST=your-db-host TEST_SSH_HOST=your-ssh-host pytest -m docker
```

Docker-backed tests default to `localhost` plus the standard published ports.
You can override them when your Docker runtime exposes services elsewhere:
- `TEST_DOCKER_HOST` for PostgreSQL and ClickHouse direct connections
- `TEST_SSH_HOST` for SSH-tunnel tests
- `TEST_POSTGRES_PORT`, `TEST_CLICKHOUSE_PORT`, and `TEST_SSH_PORT` for nonstandard published ports
- `TEST_CLICKHOUSE_HTTP_PORT` for `run_tests.sh` preflight checks of ClickHouse's HTTP port

On macOS with Colima, if published container ports are not reachable from `localhost`,
enable a host-reachable VM address before running the Docker-backed suite:

```bash
colima start --network-address
```

## Test Markers

- `@pytest.mark.docker` - Requires Docker containers running
- `@pytest.mark.security` - Security-related tests
- `@pytest.mark.slow` - Tests that may take longer (timeouts)
- `@pytest.mark.integration` - Integration tests using real MCP protocol
