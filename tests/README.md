# Test Suite Organization

This test suite is organized by functionality to clearly test the core security features of the MCP Read-Only SQL Server.

## Core Functionality Tests

### Configuration & Setup
- **test_config_parser.py** - Tests YAML configuration parsing
- **test_connector_implementations.py** - Tests CLI vs Python connector implementations

### MCP Protocol
- **test_mcp_protocol.py** - Tests MCP server/client communication
- **test_mcp_server.py** - Tests MCP server functionality
- **test_concurrent_queries.py** - Tests concurrent query handling

### Database Connectivity
- **test_docker_connectivity.py** - Verifies Docker databases are accessible

## Security Tests (Three-Layer Model)

### Layer 1 & 2: Read-Only Enforcement
- **test_security_readonly.py** - Tests database-level read-only enforcement
  - Blocks INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE
  - Tests both CLI and Python implementations

### Layer 3: Resource Limits
- **test_security_timeout.py** - Tests query timeout enforcement
  - Ensures long-running queries are terminated

- **test_security_size_limits.py** - Tests result size limit enforcement
  - Ensures large results are truncated or blocked

### Integration
- **test_security_layers.py** - Tests all three security layers working together

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
```

## Test Markers

- `@pytest.mark.docker` - Requires Docker containers running
- `@pytest.mark.security` - Security-related tests
- `@pytest.mark.slow` - Tests that may take longer (timeouts)
- `@pytest.mark.integration` - Integration tests using real MCP protocol