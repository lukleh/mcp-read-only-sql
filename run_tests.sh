#!/bin/bash
# Test runner with automatic Docker setup for immutable test data

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "Running MCP Read-Only SQL Server tests..."
echo "=========================================="
echo ""

echo -e "${BLUE}Setting up test Docker environment...${NC}"

# Stop any existing containers
docker-compose --profile test down 2>/dev/null

# Start test containers with tmpfs (in-memory) storage
# This includes test databases and SSH infrastructure
docker-compose --profile test up -d

# Copy SSH key from container for CLI tests
sleep 3  # Give SSH container time to generate keys
docker cp mcp-ssh-bastion:/tmp/test_key /tmp/docker_test_key 2>/dev/null && chmod 600 /tmp/docker_test_key 2>/dev/null || true

# Wait for databases to be ready
echo "Waiting for databases to be ready..."
sleep 5

# Verify databases are accessible
docker exec mcp-postgres-test pg_isready -U testuser -d testdb >/dev/null 2>&1
if [ $? -ne 0 ]; then
    echo -e "${RED}PostgreSQL is not ready${NC}"
    docker-compose --profile test logs postgres-test
    exit 1
fi

docker exec mcp-clickhouse-test clickhouse-client --query "SELECT 1" >/dev/null 2>&1
if [ $? -ne 0 ]; then
    echo -e "${RED}ClickHouse is not ready${NC}"
    docker-compose --profile test logs clickhouse-test
    exit 1
fi

# Check if SSH container is ready (optional, for SSH tests)
docker exec mcp-ssh-bastion nc -z localhost 22 >/dev/null 2>&1
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Test databases and SSH container ready (using tmpfs - data resets each run)${NC}"
else
    echo -e "${GREEN}✅ Test databases ready (using tmpfs - data resets each run)${NC}"
    echo -e "${BLUE}Note: SSH container not ready, SSH tests may be skipped${NC}"
fi
echo ""

# Run tests with real-time output
# Using Python unbuffered mode for immediate feedback
echo "Running tests..."
echo ""
# Use python -u for unbuffered output
# --tb=short for readable error messages
# --color=yes for colored dots (green=pass, red=fail)
# The output will show dots in real-time as tests run
PYTHONUNBUFFERED=1 uv run python -u -m pytest --tb=short --color=yes --durations=1 "$@"
exit_code=$?

# Clean up test Docker
echo ""
echo -e "${BLUE}Cleaning up test Docker environment...${NC}"
docker-compose --profile test down

# Show summary
echo ""
echo "=========================================="
if [ $exit_code -eq 0 ]; then
    echo -e "${GREEN}✅ All tests passed!${NC}"
    echo "   Tests completed successfully"
    echo "   Data was reset between runs (tmpfs)"
else
    echo -e "${RED}❌ Some tests failed${NC}"
    echo "   Check output above for details"
fi

echo ""
echo "Note: Test databases used tmpfs (in-memory) storage."
echo "Data was automatically reset for this test run."

exit $exit_code