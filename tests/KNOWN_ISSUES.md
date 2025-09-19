# Known Test Issues

## RuntimeError: Attempted to exit cancel scope in a different task

### Description
When running tests that use MCP client fixtures (`integration_client`, `mcp_client`), you may see errors during teardown like:
```
ERROR at teardown of TestClass.test_method
RuntimeError: Attempted to exit cancel scope in a different task than it was entered in
```

### Cause
This is caused by an incompatibility between:
- **anyio** (used internally by the MCP library)
- **pytest-asyncio** (used by our test suite)

The issue occurs because:
1. anyio creates a "cancel scope" when entering an async context manager
2. pytest-asyncio runs fixture setup and teardown in different async tasks
3. anyio detects the task mismatch during teardown and raises an error

### Impact
- **Tests still pass** ✅ - The actual test execution is not affected
- **Results are valid** ✅ - All assertions and test logic work correctly
- **Only affects teardown** - The error occurs after the test completes

### Workarounds

#### Option 1: Run tests with minimal output
```bash
# Run tests showing only failures
pytest -q --tb=short

# Run specific test file
pytest tests/test_docker_connectivity.py -q
```

#### Option 2: Filter the output
```bash
# Run tests and filter out ERROR lines
just test 2>&1 | grep -v "ERROR at teardown"
```

#### Option 3: Check only test results
```bash
# Just check if tests pass (exit code)
pytest && echo "All tests passed!" || echo "Tests failed"
```

### Long-term Solutions

Potential fixes being considered:
1. **Switch to anyio's pytest plugin** - Would require refactoring all async tests
2. **Use synchronous MCP client** - Would lose async benefits
3. **Custom fixture implementation** - Complex but would solve the issue
4. **Wait for library updates** - MCP or pytest-asyncio might fix this upstream

### Tests Affected

The following test files use MCP client fixtures and show teardown errors:
- `test_concurrent_queries.py`
- `test_mcp_protocol.py`
- `test_mcp_server.py`
- `test_result_serialization.py` (MCP tests only)

All other tests (security, connectors, config, etc.) are **not affected**.