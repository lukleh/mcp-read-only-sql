#!/bin/bash
# Test runner with automatic Docker setup for immutable test data

set -u
set -o pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

SETUP_TIMEOUT_SECONDS="${TEST_SETUP_TIMEOUT_SECONDS:-180}"
SEED_TIMEOUT_SECONDS="${TEST_SEED_TIMEOUT_SECONDS:-180}"
SSH_TIMEOUT_SECONDS="${TEST_SSH_TIMEOUT_SECONDS:-120}"
WAIT_INTERVAL_SECONDS="${TEST_WAIT_INTERVAL_SECONDS:-1}"

echo "Running MCP Read-Only SQL Server tests..."
echo "=========================================="
echo ""

echo -e "${BLUE}Setting up test Docker environment...${NC}"

if docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD=(docker compose)
    COMPOSE_NAME="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_CMD=(docker-compose)
    COMPOSE_NAME="docker-compose"
else
    echo -e "${RED}Neither 'docker compose' nor 'docker-compose' is available${NC}"
    exit 1
fi

DOCKER_CONTEXT="$(docker context show 2>/dev/null || echo unknown)"

cleanup_done=0

cleanup() {
    if [ "$cleanup_done" -eq 1 ]; then
        return
    fi
    cleanup_done=1

    echo ""
    echo -e "${BLUE}Cleaning up test Docker environment...${NC}"
    "${COMPOSE_CMD[@]}" --profile test down || true
}

on_interrupt() {
    cleanup
    exit 130
}

show_setup_logs() {
    "${COMPOSE_CMD[@]}" --profile test logs postgres-test clickhouse-test ssh-bastion || true
}

show_host_forwarding_hint() {
    local service_name="$1"
    local host="$2"
    local port="$3"

    echo -e "${RED}${service_name} is healthy in Docker, but ${host}:${port} is still unreachable from this shell.${NC}"
    echo "Current Docker context: ${DOCKER_CONTEXT}"

    if [ "${DOCKER_CONTEXT}" = "colima" ]; then
        echo "This machine is using Colima, and the containers appear to be listening inside the VM."
        echo "The remaining failure is host port forwarding from Colima back to macOS."
        echo "Try restarting Colima, switching to a Docker context with working localhost forwarding,"
        echo "or adjusting Colima networking before rerunning the suite."
    else
        echo "Check Docker port forwarding, localhost binding, and any local VPN or firewall rules."
    fi
}

wait_for() {
    local description="$1"
    local timeout_seconds="$2"
    local interval_seconds="$3"
    shift 3

    local start_time
    start_time=$(date +%s)

    while true; do
        if "$@" >/dev/null 2>&1; then
            return 0
        fi

        local now
        now=$(date +%s)
        if [ $((now - start_time)) -ge "$timeout_seconds" ]; then
            echo -e "${RED}${description} did not become ready within ${timeout_seconds}s${NC}"
            return 1
        fi

        sleep "$interval_seconds"
    done
}

check_host_port() {
    local host="$1"
    local port="$2"
    nc -z "$host" "$port"
}

check_postgres_seed_data() {
    docker exec mcp-postgres-test \
        psql -U testuser -d testdb -tAc "SELECT COUNT(*) FROM users" |
        tr -d '[:space:]' |
        grep -Eq '^[1-9][0-9]*$'
}

check_clickhouse_seed_data() {
    docker exec mcp-clickhouse-test \
        clickhouse-client --query "SELECT count() FROM testdb.events FORMAT TabSeparatedRaw" |
        tr -d '[:space:]' |
        grep -Eq '^[1-9][0-9]*$'
}

copy_ssh_key() {
    docker cp mcp-ssh-bastion:/tmp/test_key /tmp/docker_test_key &&
        chmod 600 /tmp/docker_test_key
}

check_ssh_login() {
    ssh \
        -o BatchMode=yes \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null \
        -o ConnectTimeout=2 \
        -i /tmp/docker_test_key \
        -p 2222 \
        tunnel@localhost true
}

trap on_interrupt INT TERM
trap cleanup EXIT

# Stop any existing containers
"${COMPOSE_CMD[@]}" --profile test down 2>/dev/null || true

# Start test containers with tmpfs (in-memory) storage
# This includes test databases and SSH infrastructure
"${COMPOSE_CMD[@]}" --profile test up -d

echo "Using Compose command: ${COMPOSE_NAME}"
echo "Using Docker context: ${DOCKER_CONTEXT}"
echo "Waiting for databases and SSH infrastructure to be ready..."
rm -f /tmp/docker_test_key

if ! wait_for "PostgreSQL container" "${SETUP_TIMEOUT_SECONDS}" "${WAIT_INTERVAL_SECONDS}" docker exec mcp-postgres-test pg_isready -U testuser -d testdb; then
    show_setup_logs
    exit 1
fi

if ! wait_for "ClickHouse container" "${SETUP_TIMEOUT_SECONDS}" "${WAIT_INTERVAL_SECONDS}" docker exec mcp-clickhouse-test clickhouse-client --query "SELECT 1"; then
    show_setup_logs
    exit 1
fi

if ! wait_for "PostgreSQL host port 5432" "${SETUP_TIMEOUT_SECONDS}" "${WAIT_INTERVAL_SECONDS}" check_host_port localhost 5432; then
    show_host_forwarding_hint "PostgreSQL" "localhost" "5432"
    show_setup_logs
    exit 1
fi

if ! wait_for "ClickHouse host port 8123" "${SETUP_TIMEOUT_SECONDS}" "${WAIT_INTERVAL_SECONDS}" check_host_port localhost 8123; then
    show_host_forwarding_hint "ClickHouse HTTP" "localhost" "8123"
    show_setup_logs
    exit 1
fi

if ! wait_for "ClickHouse host port 9000" "${SETUP_TIMEOUT_SECONDS}" "${WAIT_INTERVAL_SECONDS}" check_host_port localhost 9000; then
    show_host_forwarding_hint "ClickHouse native" "localhost" "9000"
    show_setup_logs
    exit 1
fi

if ! wait_for "PostgreSQL seed data" "${SEED_TIMEOUT_SECONDS}" "${WAIT_INTERVAL_SECONDS}" check_postgres_seed_data; then
    show_setup_logs
    exit 1
fi

if ! wait_for "ClickHouse seed data" "${SEED_TIMEOUT_SECONDS}" "${WAIT_INTERVAL_SECONDS}" check_clickhouse_seed_data; then
    show_setup_logs
    exit 1
fi

if ! wait_for "SSH bastion host port 2222" "${SSH_TIMEOUT_SECONDS}" "${WAIT_INTERVAL_SECONDS}" check_host_port localhost 2222; then
    show_host_forwarding_hint "SSH bastion" "localhost" "2222"
    show_setup_logs
    exit 1
fi

if ! wait_for "SSH test key copy" "${SSH_TIMEOUT_SECONDS}" "${WAIT_INTERVAL_SECONDS}" copy_ssh_key; then
    show_setup_logs
    exit 1
fi

if ! wait_for "SSH login with copied test key" "${SSH_TIMEOUT_SECONDS}" "${WAIT_INTERVAL_SECONDS}" check_ssh_login; then
    show_setup_logs
    exit 1
fi

echo -e "${GREEN}✅ Test databases and SSH container ready (using tmpfs - data resets each run)${NC}"
echo ""

# Run tests with real-time output
# Using Python unbuffered mode for immediate feedback
echo "Running tests..."
echo ""
# Use python -u for unbuffered output
# --tb=short for readable error messages
# --color=yes for colored dots (green=pass, red=fail)
# Emit JUnit XML alongside console output for CI reporting
mkdir -p test-results
PYTHONUNBUFFERED=1 uv run python -u -m pytest \
    --tb=short --color=yes --durations=1 \
    --junitxml=test-results/pytest.xml "$@"
exit_code=$?

cleanup

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
